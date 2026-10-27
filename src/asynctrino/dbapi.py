"""An asyncio flavour of the Python DB-API 2.0 (PEP 249) for Trino.

Example:
    >>> async with connect(host="localhost", user="me") as conn:  # doctest: +SKIP
    ...     async with conn.cursor() as cur:
    ...         await cur.execute("SELECT 1")
    ...         await cur.fetchall()
    [[1]]
"""

from collections.abc import Mapping, Sequence
from types import TracebackType
from typing import Final, NamedTuple, Self

import httpx2

from asynctrino._core.coordinator import Coordinator, HttpScheme
from asynctrino._core.query import Query, StatsCallback
from asynctrino._core.session import DEFAULT_SOURCE, ClientSession, SessionState, format_roles
from asynctrino._core.status import Column, Row
from asynctrino._http import Transport, fetch_more, submit
from asynctrino.exceptions import (
    DatabaseError,
    DataError,
    Error,
    IntegrityError,
    InterfaceError,
    InternalError,
    NotSupportedError,
    OperationalError,
    ProgrammingError,
    Warning,  # noqa: A004
)

__all__ = [
    "ColumnDescription",
    "Connection",
    "Cursor",
    "DataError",
    "DatabaseError",
    "Error",
    "IntegrityError",
    "InterfaceError",
    "InternalError",
    "NotSupportedError",
    "OperationalError",
    "ProgrammingError",
    "Row",
    "Warning",
    "apilevel",
    "connect",
    "paramstyle",
    "threadsafety",
]

apilevel: Final = "2.0"
threadsafety: Final = 1
paramstyle: Final = "qmark"

_LENGTH_TYPES = frozenset({"char", "varchar"})
_PRECISION_TYPES = frozenset(
    {"time", "time with time zone", "timestamp", "timestamp with time zone", "decimal"}
)
_SCALE_TYPES = frozenset({"decimal"})


class ColumnDescription(NamedTuple):
    """One entry of :attr:`Cursor.description`: the PEP 249 seven-item column sequence."""

    name: str
    type_code: str
    display_size: int | None
    internal_size: int | None
    precision: int | None
    scale: int | None
    null_ok: bool | None

    @classmethod
    def from_column(cls, column: Column) -> ColumnDescription:
        """Describe a column reported by the Coordinator."""
        signature = column["typeSignature"]
        raw_type = signature["rawType"]
        values = [arg["value"] for arg in signature["arguments"]]

        def argument(index: int, types: frozenset[str]) -> int | None:
            value = values[index] if raw_type in types and len(values) > index else None
            return value if isinstance(value, int) else None

        return cls(
            name=column["name"],
            type_code=column["type"],
            display_size=None,
            internal_size=argument(0, _LENGTH_TYPES),
            precision=argument(0, _PRECISION_TYPES),
            scale=argument(1, _SCALE_TYPES),
            null_ok=None,
        )


class Connection:
    """A connection to a Trino Coordinator. Create it with :func:`connect`."""

    def __init__(
        self, coordinator: Coordinator, session: ClientSession, transport: Transport
    ) -> None:
        self._coordinator = coordinator
        self._session = SessionState(session)
        self._transport = transport

    def cursor(self, *, stats_callback: StatsCallback | None = None) -> Cursor:
        """Return a new cursor. No I/O happens until :meth:`Cursor.execute`.

        Args:
            stats_callback: Called with a copy of the query statistics after every Coordinator
                response, so progress can be reported while a statement runs.
        """
        return Cursor(self, self._coordinator, self._session, self._transport, stats_callback)

    async def close(self) -> None:
        """Close the connection and the HTTP client, if asynctrino created it."""
        await self._transport.aclose()

    async def __aenter__(self) -> Self:
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        await self.close()


class Cursor:
    """Executes statements and fetches their rows. Create it with :meth:`Connection.cursor`.

    Attributes:
        arraysize: How many rows :meth:`fetchmany` returns when no size is given.
    """

    def __init__(
        self,
        connection: Connection,
        coordinator: Coordinator,
        session: SessionState,
        transport: Transport,
        stats_callback: StatsCallback | None = None,
    ) -> None:
        self._connection = connection
        self._coordinator = coordinator
        self._session = session
        self._transport = transport
        self._stats_callback = stats_callback
        self._query: Query | None = None
        self._closed = False
        self.arraysize = 1

    @property
    def connection(self) -> Connection:
        """The connection this cursor belongs to."""
        return self._connection

    @property
    def description(self) -> list[ColumnDescription] | None:
        """The result columns of the last executed statement, or ``None`` if there are none."""
        if self._query is None or self._query.columns is None:
            return None
        return [ColumnDescription.from_column(column) for column in self._query.columns]

    @property
    def rowcount(self) -> int:
        """The number of rows an update statement changed, or -1 when Trino didn't report one."""
        if self._query is None or self._query.update_count is None:
            return -1
        return self._query.update_count

    @property
    def query_id(self) -> str | None:
        """The Trino query id of the last executed statement."""
        return None if self._query is None else self._query.query_id

    @property
    def query(self) -> str | None:
        """The SQL text of the last executed statement."""
        return None if self._query is None else self._query.sql

    @property
    def stats(self) -> dict[str, object] | None:
        """The latest query statistics reported by the Coordinator, including ``queryId``."""
        return None if self._query is None else self._query.stats

    @property
    def warnings(self) -> list[dict[str, object]] | None:
        """The warnings Trino raised for the last executed statement."""
        return None if self._query is None else self._query.warnings

    @property
    def info_uri(self) -> str | None:
        """The URL of the last executed statement's page in the Trino web UI."""
        return None if self._query is None else self._query.info_uri

    @property
    def update_type(self) -> str | None:
        """The kind of update statement executed, such as ``"INSERT"``, or ``None`` for queries."""
        return None if self._query is None else self._query.update_type

    async def execute(self, operation: str) -> Self:
        """Run a statement.

        Returns once its first rows are available, its columns are known, or it has finished,
        so :attr:`description` is accurate afterwards.
        """
        self._check_open()
        self._query = Query(operation, self._coordinator, self._session, self._stats_callback)
        await submit(self._query, self._transport)
        return self

    async def fetchone(self) -> Row | None:
        """Return the next row, or ``None`` when no rows are left."""
        rows = await self.fetchmany(1)
        return rows[0] if len(rows) > 0 else None

    async def fetchmany(self, size: int | None = None) -> list[Row]:
        """Return up to ``size`` rows (default :attr:`arraysize`); fewer means none are left."""
        query = self._executed_query()
        size = self.arraysize if size is None else size
        rows = query.take_rows(size)
        while len(rows) < size and await fetch_more(query, self._transport):
            rows.extend(query.take_rows(size - len(rows)))
        return rows

    async def fetchall(self) -> list[Row]:
        """Return every remaining row."""
        query = self._executed_query()
        rows = query.take_rows()
        while await fetch_more(query, self._transport):
            rows.extend(query.take_rows())
        return rows

    async def close(self) -> None:
        """Close the cursor; any further use raises :class:`ProgrammingError`."""
        self._closed = True

    def __aiter__(self) -> Self:
        return self

    async def __anext__(self) -> Row:
        row = await self.fetchone()
        if row is None:
            raise StopAsyncIteration
        return row

    async def __aenter__(self) -> Self:
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        await self.close()

    def _check_open(self) -> None:
        if self._closed:
            msg = "cursor is closed"
            raise ProgrammingError(msg)

    def _executed_query(self) -> Query:
        self._check_open()
        if self._query is None:
            msg = "execute() must be called before fetching rows"
            raise ProgrammingError(msg)
        return self._query


def connect(  # noqa: PLR0913
    *,
    host: str,
    port: int | None = None,
    user: str | None = None,
    source: str = DEFAULT_SOURCE,
    catalog: str | None = None,
    schema: str | None = None,
    session_properties: Mapping[str, str] | None = None,
    client_tags: Sequence[str] = (),
    roles: Mapping[str, str] | str | None = None,
    extra_credential: Sequence[tuple[str, str]] = (),
    timezone: str | None = None,
    http_headers: Mapping[str, str] | None = None,
    http_scheme: HttpScheme | None = None,
    http_client: httpx2.AsyncClient | None = None,
) -> Connection:
    """Create a connection to a Trino Coordinator without doing any I/O.

    Args:
        host: The Coordinator's host name, or a URL such as ``https://trino.example.com:8443``
            whose scheme and port take precedence over ``http_scheme`` and ``port``.
        port: The Coordinator's port. Defaults to 443 for ``https`` and 8080 otherwise.
        user: The user to run queries as.
        source: The source name reported to Trino.
        catalog: The default catalog.
        schema: The default schema.
        session_properties: Trino session properties, as ``SET SESSION`` would set them.
        client_tags: Tags that identify the queries' origin, for resource group selection.
        roles: Roles to enable, per catalog (``{"hive": "admin"}``); a plain string is the
            ``system`` role. ``ALL`` and ``NONE`` are passed through.
        extra_credential: ``(key, value)`` credentials passed to connectors.
        timezone: The IANA time zone queries run in, such as ``"Europe/Brussels"``.
        http_headers: Extra HTTP headers for every Coordinator request. They can't replace
            the headers asynctrino sends itself, and never go to Segment storage.
        http_scheme: ``"http"`` or ``"https"``. Defaults to ``https`` when the port is 443.
        http_client: An ``httpx2.AsyncClient`` to send requests with. asynctrino never closes
            it or changes its defaults. Without one, a client is created on the first request
            and closed with the connection.

    Returns:
        A new :class:`Connection`.
    """
    return Connection(
        Coordinator.resolve(host, port, http_scheme),
        ClientSession(
            user=user,
            source=source,
            catalog=catalog,
            schema=schema,
            properties={} if session_properties is None else dict(session_properties),
            client_tags=tuple(client_tags),
            roles=format_roles(roles),
            extra_credential=tuple(extra_credential),
            timezone=timezone,
            http_headers={} if http_headers is None else dict(http_headers),
        ),
        Transport(http_client),
    )
