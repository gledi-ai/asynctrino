"""The ``asynctrino://`` SQLAlchemy dialect for Trino."""

import json
import logging
from collections.abc import Mapping, Sequence
from typing import cast, override

from sqlalchemy import exc, text
from sqlalchemy.engine.base import Connection as SQLAlchemyConnection
from sqlalchemy.engine.default import DefaultDialect, DefaultExecutionContext
from sqlalchemy.engine.interfaces import (
    ConnectArgsType,
    DBAPIConnection,
    DBAPICursor,
    DBAPIModule,
    ReflectedColumn,
    ReflectedIndex,
)
from sqlalchemy.engine.interfaces import IsolationLevel as SQLAlchemyIsolationLevel
from sqlalchemy.engine.url import URL
from sqlalchemy.pool import PoolProxiedConnection
from sqlalchemy.sql import sqltypes
from sqlalchemy.sql.base import DialectKWArgs
from sqlalchemy.sql.ddl import CreateTableAs
from sqlalchemy.sql.schema import Table
from sqlalchemy.util import VersionInfo, parse_version_string

import asynctrino
from asynctrino.auth import BasicAuthentication, JWTAuthentication
from asynctrino.dbapi import Connection, IsolationLevel
from asynctrino.exceptions import TrinoConnectionError
from asynctrino.sqlalchemy._adapter import AsyncAdaptConnection, AsyncAdaptDBAPI
from asynctrino.sqlalchemy._compiler import (
    TrinoDDLCompiler,
    TrinoIdentifierPreparer,
    TrinoSQLCompiler,
    TrinoTypeCompiler,
)
from asynctrino.sqlalchemy._types import (
    VARBINARY,
    FloatAsDouble,
    JSONIndexType,
    JSONPathType,
    parse_sqltype,
)
from asynctrino.sqlalchemy._url import DEFAULT_SOURCE

_PLAIN_QUERY_OPTIONS = ("source", "timezone")
_JSON_QUERY_OPTIONS = (
    "session_properties",
    "http_headers",
    "client_tags",
    "roles",
    "legacy_primitive_types",
    "verify",
    "allow_insecure_auth",
)


logger = logging.getLogger("asynctrino")

_ICEBERG_PARTITIONS = ("partition", "record_count", "file_count", "total_size", "data")
_ICEBERG_PARTITION_TYPES = ("row(", "bigint", "bigint", "bigint", "row(")

_TABLE_ARGUMENTS: Mapping[str, None] = {"catalog": None, "*": None}
_CREATE_TABLE_AS_ARGUMENTS: Mapping[str, None] = {"*": None}


class TrinoExecutionContext(DefaultExecutionContext):
    """Runs statements on a server-side cursor when results are streamed."""

    @override
    def create_server_side_cursor(self) -> DBAPICursor:
        return self._dbapi_connection.cursor(server_side=True)


class TrinoDialect(DefaultDialect):
    """Trino through asynctrino, for SQLAlchemy's asyncio extension."""

    name = "trino"
    driver = "asynctrino"
    is_async = True
    supports_statement_cache = False
    supports_server_side_cursors = True
    has_terminate = True
    execution_ctx_cls = TrinoExecutionContext
    statement_compiler = TrinoSQLCompiler
    ddl_compiler = TrinoDDLCompiler
    type_compiler_cls = TrinoTypeCompiler
    preparer = TrinoIdentifierPreparer
    colspecs = {  # noqa: RUF012
        sqltypes.Float: FloatAsDouble,
        sqltypes.JSON.JSONIndexType: JSONIndexType,
        sqltypes.JSON.JSONPathType: JSONPathType,
        sqltypes._Binary: VARBINARY,  # noqa: SLF001
    }

    construct_arguments = [  # noqa: RUF012
        (Table, _TABLE_ARGUMENTS),
        (CreateTableAs, _CREATE_TABLE_AS_ARGUMENTS),
    ]

    supports_native_enum = False
    supports_native_boolean = True
    supports_native_decimal = True
    supports_native_uuid = True
    supports_sequences = False
    supports_comments = True
    inline_comments = True
    supports_default_values = False
    supports_alter = True
    supports_empty_insert = False
    supports_multivalues_insert = True
    postfetch_lastrowid = False
    cte_follows_insert = True

    @override
    @classmethod
    def import_dbapi(cls) -> DBAPIModule:
        return cast("DBAPIModule", AsyncAdaptDBAPI())

    @override
    def retrieve_dbapi_version(self, dbapi: DBAPIModule) -> VersionInfo:
        return parse_version_string(asynctrino.__version__)

    @override
    def get_driver_connection(self, connection: DBAPIConnection) -> Connection:
        return _adapted(connection).driver_connection

    @override
    def do_terminate(self, dbapi_connection: DBAPIConnection) -> None:
        _adapted(dbapi_connection).terminate()

    @override
    def is_disconnect(
        self,
        e: DBAPIModule.Error,
        connection: PoolProxiedConnection | DBAPIConnection | None,
        cursor: DBAPICursor | None,
    ) -> bool:
        """Whether ``e`` means the Coordinator can't be reached, so the connection is replaced."""
        return isinstance(e, TrinoConnectionError)

    @override
    def get_isolation_level_values(
        self, dbapi_conn: DBAPIConnection
    ) -> Sequence[SQLAlchemyIsolationLevel]:
        return [cast("SQLAlchemyIsolationLevel", level.value) for level in IsolationLevel]

    @override
    def get_isolation_level(self, dbapi_connection: DBAPIConnection) -> SQLAlchemyIsolationLevel:
        level = _adapted(dbapi_connection).isolation_level
        return cast("SQLAlchemyIsolationLevel", level.value)

    @override
    def set_isolation_level(
        self, dbapi_connection: DBAPIConnection, level: SQLAlchemyIsolationLevel
    ) -> None:
        _adapted(dbapi_connection).set_isolation_level(IsolationLevel(level))

    @override
    def create_connect_args(self, url: URL) -> ConnectArgsType:
        """Map the URL onto :func:`asynctrino.connect`; ``connect_args`` are applied over it.

        The database is ``catalog[/schema]`` and defaults to the ``system`` catalog. A password
        selects Basic authentication and the ``access_token`` query option JWT authentication;
        either one switches to ``https`` unless ``allow_insecure_auth`` is set. Structured query
        options are JSON.

        Warning:
            SQLAlchemy masks only the password when it renders a URL, so an ``access_token`` in
            the query string appears in ``str(engine.url)``, reprs, and logs. For secrets, pass
            ``connect_args={"auth": JWTAuthentication(token)}`` instead.

        Raises:
            ValueError: The URL has a password but no user, or more than catalog and schema.
        """
        kwargs: dict[str, object] = {"host": url.host}
        if url.port is not None:
            kwargs["port"] = url.port
        kwargs |= _catalog_and_schema(url.database)
        if url.username is not None:
            kwargs["user"] = url.username
        if url.password is not None:
            if url.username is None or url.username == "":
                msg = "Username is required when specifying a password in the connection URL"
                raise ValueError(msg)
            kwargs["auth"] = BasicAuthentication(url.username, url.password)

        options = {name: values[-1] for name, values in url.normalized_query.items()}
        kwargs["source"] = DEFAULT_SOURCE
        kwargs |= {name: options[name] for name in _PLAIN_QUERY_OPTIONS if name in options}
        kwargs |= {
            name: json.loads(options[name]) for name in _JSON_QUERY_OPTIONS if name in options
        }
        if "extra_credential" in options:
            pairs: list[list[str]] = json.loads(options["extra_credential"])
            kwargs["extra_credential"] = [(key, value) for key, value in pairs]
        if "encoding" in options:
            encoding = options["encoding"]
            kwargs["encoding"] = json.loads(encoding) if encoding.startswith("[") else encoding
        if "access_token" in options:
            kwargs["auth"] = JWTAuthentication(options["access_token"])
        if "auth" in kwargs and kwargs.get("allow_insecure_auth") is not True:
            kwargs["http_scheme"] = "https"
        return [], kwargs

    @override
    def _get_default_schema_name(  # pyrefly: ignore[bad-override]
        self, connection: SQLAlchemyConnection
    ) -> str | None:
        """The connection's current schema, which ``USE`` changes."""
        return _driver_connection(connection).schema

    @override
    def has_table(
        self,
        connection: SQLAlchemyConnection,
        table_name: str,
        schema: str | None = None,
        **kw: object,
    ) -> bool:
        schema = self._schema(connection, schema)
        if schema is None:
            return False
        query = text(
            'SELECT "table_name" FROM "information_schema"."tables"'
            ' WHERE "table_schema" = :schema AND "table_name" = :table'
        )
        result = connection.execute(query, {"schema": schema, "table": table_name})
        return result.first() is not None

    @override
    def get_columns(
        self,
        connection: SQLAlchemyConnection,
        table_name: str,
        schema: str | None = None,
        **kw: object,
    ) -> list[ReflectedColumn]:
        """The table's columns, their types parsed from Trino's type strings.

        Raises:
            NoSuchTableError: The table doesn't exist.
        """
        self._require_table(connection, table_name, schema)
        query = text(
            'SELECT "column_name", "data_type", "column_default", "is_nullable", "comment"'
            ' FROM "information_schema"."columns"'
            ' WHERE "table_schema" = :schema AND "table_name" = :table'
            ' ORDER BY "ordinal_position"'
        )
        parameters = {"schema": self._required_schema(connection, schema), "table": table_name}
        return [
            {
                "name": row.column_name,
                "type": parse_sqltype(row.data_type),
                "nullable": row.is_nullable.upper() == "YES",
                "default": row.column_default,
                "comment": row.comment,
            }
            for row in connection.execute(query, parameters)
        ]

    @override
    def get_indexes(
        self,
        connection: SQLAlchemyConnection,
        table_name: str,
        schema: str | None = None,
        **kw: object,
    ) -> list[ReflectedIndex]:
        """The table's partition columns as one non-unique index named ``partition``, if any.

        Raises:
            NoSuchTableError: The table doesn't exist.
        """
        self._require_table(connection, table_name, schema)
        try:
            columns = self._partition_columns(connection, table_name, schema)
        except exc.DBAPIError as e:
            logger.debug("No partition columns for %s.%s: %s", schema, table_name, e)
            return []
        if len(columns) == 0:
            return []
        return [{"name": "partition", "column_names": columns, "unique": False}]

    def _partition_columns(
        self, connection: SQLAlchemyConnection, table_name: str, schema: str | None
    ) -> list[str]:
        quote = self.identifier_preparer.quote_identifier
        schema = self._required_schema(connection, schema)
        partitions = f"{quote(schema)}.{quote(f'{table_name}$partitions')}"
        # Only the columns are read, so the query is cancelled rather than drained.
        result = connection.execute(
            text(f"SELECT * FROM {partitions}"),  # noqa: S608
            execution_options={"stream_results": True},
        )
        try:
            description = result.cursor.description
        finally:
            result.close()
        names = tuple(str(column[0]) for column in description)
        types = [str(column[1]) for column in description]
        if names == _ICEBERG_PARTITIONS and all(
            type_.startswith(prefix)
            for type_, prefix in zip(types, _ICEBERG_PARTITION_TYPES, strict=True)
        ):
            return []
        return list(names)

    def _require_table(
        self, connection: SQLAlchemyConnection, table_name: str, schema: str | None
    ) -> None:
        if not self.has_table(connection, table_name, schema):
            msg = f"schema={schema}, table={table_name}"
            raise exc.NoSuchTableError(msg)

    def _schema(self, connection: SQLAlchemyConnection, schema: str | None) -> str | None:
        return schema or self._get_default_schema_name(connection)

    def _required_schema(self, connection: SQLAlchemyConnection, schema: str | None) -> str:
        schema = self._schema(connection, schema)
        if schema is None:
            msg = "schema is required"
            raise exc.NoSuchTableError(msg)
        return schema


def _driver_connection(connection: SQLAlchemyConnection) -> Connection:
    return cast("Connection", connection.connection.driver_connection)


def _adapted(dbapi_connection: DBAPIConnection) -> AsyncAdaptConnection:
    return cast("AsyncAdaptConnection", dbapi_connection)


def _catalog_and_schema(database: str | None) -> dict[str, str]:
    parts = ("system" if database is None or database == "" else database).split("/")
    match parts:
        case [catalog]:
            return {"catalog": catalog}
        case [catalog, schema]:
            return {"catalog": catalog, "schema": schema}
        case _:
            msg = f"Unexpected database format {database}"
            raise ValueError(msg)


def _validate_trino_keyword_arguments_with(dialect: type[DefaultDialect]) -> None:
    # SQLAlchemy validates `trino_*` arguments with the dialect registered as "trino", which is
    # missing or Upstream's since this one is registered as "asynctrino"; use this one instead.
    arguments = dialect.construct_arguments
    DialectKWArgs._kw_registry[dialect.name] = (  # noqa: SLF001
        None if arguments is None else dict(arguments)
    )


_validate_trino_keyword_arguments_with(TrinoDialect)
