"""The ``asynctrino://`` SQLAlchemy dialect for Trino."""

import json
from collections.abc import Sequence
from typing import cast, override

from sqlalchemy.engine.default import DefaultDialect, DefaultExecutionContext
from sqlalchemy.engine.interfaces import (
    ConnectArgsType,
    DBAPIConnection,
    DBAPICursor,
    DBAPIModule,
)
from sqlalchemy.engine.interfaces import IsolationLevel as SQLAlchemyIsolationLevel
from sqlalchemy.engine.url import URL
from sqlalchemy.util import VersionInfo, parse_version_string

import asynctrino
from asynctrino.auth import BasicAuthentication, JWTAuthentication
from asynctrino.dbapi import IsolationLevel
from asynctrino.sqlalchemy._adapter import AsyncAdaptConnection, AsyncAdaptDBAPI
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
    execution_ctx_cls = TrinoExecutionContext

    @override
    @classmethod
    def import_dbapi(cls) -> DBAPIModule:
        return cast("DBAPIModule", AsyncAdaptDBAPI())

    @override
    def retrieve_dbapi_version(self, dbapi: DBAPIModule) -> VersionInfo:
        return parse_version_string(asynctrino.__version__)

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
