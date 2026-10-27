"""An asyncio flavour of the Python DB-API 2.0 (PEP 249) for Trino.

Example:
    >>> async with connect(host="localhost", user="me") as conn:  # doctest: +SKIP
    ...     async with conn.cursor() as cur:
    ...         await cur.execute("SELECT 1")
    ...         await cur.fetchall()
    [[1]]
"""

import ssl
from types import TracebackType
from typing import Final, NamedTuple, Self

import httpx2

from asynctrino._core.coordinator import DEFAULT_SOURCE, ClientSession, Coordinator, HttpScheme
from asynctrino._core.query import Query
from asynctrino._core.retry import DEFAULT_MAX_ATTEMPTS, RetryPolicy
from asynctrino._core.status import Column, Row
from asynctrino._http import DEFAULT_REQUEST_TIMEOUT, Transport, fetch_more, submit
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
    TrinoAuthError,
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

_INSECURE_AUTH_MESSAGE = (
    "TLS/SSL is required for authentication. To use HTTPS, specify 'https://' in the host URL "
    "(which takes precedence over http_scheme), or, if the host URL has no scheme, pass "
    "http_scheme='https'. If the transport is already encrypted below the application layer "
    "(e.g. mTLS), pass allow_insecure_auth=True and ensure "
    "http-server.authentication.allow-insecure-over-http=true is set on the coordinator."
)

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
        self._session = session
        self._transport = transport

    def cursor(self) -> Cursor:
        """Return a new cursor. No I/O happens until :meth:`Cursor.execute`."""
        return Cursor(self, self._coordinator, self._session, self._transport)

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
        session: ClientSession,
        transport: Transport,
    ) -> None:
        self._connection = connection
        self._coordinator = coordinator
        self._session = session
        self._transport = transport
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

    async def execute(self, operation: str) -> Self:
        """Run a statement.

        Returns once its first rows are available, its columns are known, or it has finished,
        so :attr:`description` is accurate afterwards.
        """
        self._check_open()
        self._query = Query(operation, self._coordinator, self._session)
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
    http_scheme: HttpScheme | None = None,
    auth: httpx2.Auth | None = None,
    allow_insecure_auth: bool = False,
    verify: bool | ssl.SSLContext = True,
    max_attempts: int = DEFAULT_MAX_ATTEMPTS,
    request_timeout: float | httpx2.Timeout = DEFAULT_REQUEST_TIMEOUT,
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
        http_scheme: ``"http"`` or ``"https"``. Defaults to ``https`` when the port is 443.
        auth: How to authenticate Coordinator requests: a scheme from :mod:`asynctrino.auth`
            or any ``httpx2.Auth``. It is applied per request, never to Segment storage.
        allow_insecure_auth: Send ``auth`` credentials over plain ``http``. Only for transports
            already encrypted below HTTP, such as an mTLS mesh.
        verify: How to verify the Coordinator's TLS certificate: ``True`` trusts the operating
            system's store, ``False`` skips verification, and an ``ssl.SSLContext`` is used as
            is, so it can trust a private CA or present a client certificate. Ignored with
            ``http_client``.
        max_attempts: How many times each request is attempted, including the first. Requests
            are retried on HTTP 429, 502, 503 and 504, on an empty 200 response to a statement
            request, and on connect, read and timeout errors. Waits honour ``Retry-After`` and
            otherwise back off exponentially with jitter. ``1`` disables retries.
        request_timeout: Seconds, or an ``httpx2.Timeout`` for separate connect, read, write
            and pool timeouts, applied to the client asynctrino creates. Ignored when
            ``http_client`` is given: configure that client's own timeout instead.
        http_client: An ``httpx2.AsyncClient`` to send requests with, keeping its own TLS and
            proxy settings. asynctrino never closes it or changes its defaults. Without one, a
            client is created on the first request, honours the proxy environment variables
            (``HTTPS_PROXY``, ``NO_PROXY``, ...), and is closed with the connection.

    Returns:
        A new :class:`Connection`.

    Raises:
        TrinoAuthError: ``auth`` is set for an ``http`` Coordinator without
            ``allow_insecure_auth``.
        ValueError: ``max_attempts`` is less than 1, or ``http_scheme`` is unknown.
    """
    coordinator = Coordinator.resolve(host, port, http_scheme)
    if auth is not None and not coordinator.uses_tls and not allow_insecure_auth:
        raise TrinoAuthError(_INSECURE_AUTH_MESSAGE)
    return Connection(
        coordinator,
        ClientSession(user=user, source=source, catalog=catalog, schema=schema),
        Transport(
            http_client,
            auth=auth,
            verify=verify,
            retry=RetryPolicy(max_attempts=max_attempts),
            timeout=request_timeout,
        ),
    )
