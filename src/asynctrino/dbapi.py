"""An asyncio flavour of the Python DB-API 2.0 (PEP 249) for Trino.

Example:
    >>> async with connect(host="localhost", user="me") as conn:  # doctest: +SKIP
    ...     async with conn.cursor() as cur:
    ...         await cur.execute("SELECT 1")
    ...         await cur.fetchall()
    [[1]]
"""

import ssl
from collections.abc import Mapping, Sequence
from types import TracebackType
from typing import Final, NamedTuple, Self

import httpx2

from asynctrino._core.coordinator import Coordinator, HttpScheme
from asynctrino._core.parameters import execute_immediate
from asynctrino._core.query import Query, StatsCallback
from asynctrino._core.retry import DEFAULT_MAX_ATTEMPTS, RetryPolicy
from asynctrino._core.session import (
    DEFAULT_SOURCE,
    ClientSession,
    SessionState,
    format_roles,
    session_timezone,
)
from asynctrino._core.status import Column, Row
from asynctrino._http import DEFAULT_REQUEST_TIMEOUT, QueryRunner, Transport
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
        self,
        coordinator: Coordinator,
        session: ClientSession,
        transport: Transport,
        *,
        legacy_primitive_types: bool,
    ) -> None:
        self._coordinator = coordinator
        self._session = SessionState(session)
        self._runner = QueryRunner(transport)
        self._legacy_primitive_types = legacy_primitive_types

    def cursor(
        self,
        *,
        stats_callback: StatsCallback | None = None,
        legacy_primitive_types: bool | None = None,
    ) -> Cursor:
        """Return a new cursor. No I/O happens until :meth:`Cursor.execute`.

        Args:
            stats_callback: Called with a copy of the query statistics after every Coordinator
                response, so progress can be reported while a statement runs.
            legacy_primitive_types: Override the connection's ``legacy_primitive_types`` for
                this cursor.
        """
        if legacy_primitive_types is None:
            legacy_primitive_types = self._legacy_primitive_types
        return Cursor(
            self,
            self._coordinator,
            self._session,
            self._runner,
            stats_callback,
            legacy_primitive_types=legacy_primitive_types,
        )

    async def close(self) -> None:
        """Close the connection and the HTTP client, if asynctrino created it.

        Unfinished queries of its cursors are cancelled first, as :meth:`Cursor.close` does.
        """
        await self._runner.abandon_all()
        await self._runner.transport.aclose()

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

    Cursors of one autocommit connection can be used from concurrent tasks, one task per cursor.
    If the task awaiting :meth:`execute` or a fetch is cancelled, for instance by
    ``asyncio.timeout``, the query is cancelled on the Coordinator best-effort before the
    ``CancelledError`` propagates.

    Attributes:
        arraysize: How many rows :meth:`fetchmany` returns when no size is given.
    """

    def __init__(  # noqa: PLR0913
        self,
        connection: Connection,
        coordinator: Coordinator,
        session: SessionState,
        runner: QueryRunner,
        stats_callback: StatsCallback | None = None,
        *,
        legacy_primitive_types: bool,
    ) -> None:
        self._connection = connection
        self._coordinator = coordinator
        self._session = session
        self._runner = runner
        self._stats_callback = stats_callback
        self._legacy_primitive_types = legacy_primitive_types
        self._query: Query | None = None
        self._earlier_update_count = 0
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
        """The number of rows an update statement changed, or -1 when Trino didn't report one.

        After :meth:`executemany`, the total over every parameter set.
        """
        if self._query is None or self._query.update_count is None:
            return -1
        return self._earlier_update_count + self._query.update_count

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

    async def execute(self, operation: str, params: Sequence[object] | None = None) -> Self:
        """Run a statement, binding ``params`` to its ``?`` placeholders.

        Parameters are formatted as Trino literals and bound with ``EXECUTE IMMEDIATE``.
        Returns once the first rows are available, the columns are known, or the statement
        has finished, so :attr:`description` is accurate afterwards.

        Args:
            operation: The SQL statement, with a ``?`` for each parameter.
            params: A list or tuple of values: ``None``, ``bool``, ``int``, ``float``,
                ``Decimal``, ``str``, ``bytes``, ``date``, ``time``, ``datetime``,
                ``uuid.UUID``, or a ``list`` (ARRAY), ``tuple`` (ROW) or ``dict`` (MAP) of them.
                Empty or ``None`` runs ``operation`` as is.

        Raises:
            ProgrammingError: ``params`` is not a list or tuple.
            NotSupportedError: A parameter has a type that can't be bound.
        """
        self._earlier_update_count = 0
        await self._execute(operation, params)
        return self

    async def executemany(self, operation: str, seq_of_params: Sequence[Sequence[object]]) -> Self:
        """Run a statement once per parameter set; :attr:`rowcount` is then their total.

        Every set but the last must run an update statement, such as ``INSERT``. With no
        sets, ``operation`` runs once as is.

        Raises:
            NotSupportedError: A statement other than the last returned no update type.
        """
        self._earlier_update_count = earlier = 0
        for params in seq_of_params[:-1]:
            query = await self._execute(operation, params)
            await self.fetchall()
            if query.update_type is None:
                msg = "Query must return update type"
                raise NotSupportedError(msg)
            if query.update_count is not None:
                earlier += query.update_count
        last = seq_of_params[-1] if len(seq_of_params) > 0 else None
        await self._execute(operation, last)
        self._earlier_update_count = earlier
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
        while len(rows) < size and await self._runner.fetch_more(query):
            rows.extend(query.take_rows(size - len(rows)))
        return rows

    async def fetchall(self) -> list[Row]:
        """Return every remaining row."""
        query = self._executed_query()
        rows = query.take_rows()
        while await self._runner.fetch_more(query):
            rows.extend(query.take_rows())
        return rows

    async def cancel(self) -> None:
        """Cancel the running statement on the Coordinator; it then returns no more rows.

        Does nothing if no statement is running.

        Raises:
            TrinoConnectionError: The Coordinator could not be reached or refused to cancel.
        """
        if self._query is not None:
            await self._runner.cancel(self._query)

    async def close(self) -> None:
        """Close the cursor; any further use raises :class:`ProgrammingError`.

        An unfinished query is cancelled on the Coordinator best-effort: a failure to cancel it
        is logged to the ``asynctrino`` logger, not raised.
        """
        self._closed = True
        if self._query is not None:
            await self._runner.abandon(self._query)

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

    async def _execute(self, operation: str, params: Sequence[object] | None) -> Query:
        self._check_open()
        if params is not None and not isinstance(params, (list, tuple)):
            msg = f"params must be a list or tuple of parameter values, not {type(params)}"
            raise ProgrammingError(msg)
        if params is not None and len(params) > 0:
            operation = execute_immediate(operation, params)
        query = Query(
            operation,
            self._coordinator,
            self._session,
            self._stats_callback,
            legacy_primitive_types=self._legacy_primitive_types,
        )
        self._query = query
        await self._runner.submit(query)
        return query

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
    auth: httpx2.Auth | None = None,
    allow_insecure_auth: bool = False,
    verify: bool | ssl.SSLContext = True,
    max_attempts: int = DEFAULT_MAX_ATTEMPTS,
    request_timeout: float | httpx2.Timeout = DEFAULT_REQUEST_TIMEOUT,
    http_client: httpx2.AsyncClient | None = None,
    legacy_primitive_types: bool = False,
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
        timezone: The IANA time zone queries run in, such as ``"Europe/Brussels"``. Defaults to
            the local time zone.
        http_headers: Extra HTTP headers for every Coordinator request. They can't replace
            the headers asynctrino sends itself, and never go to Segment storage.
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
        legacy_primitive_types: Return every value as the Coordinator sent it, without mapping
            it to a Python type. :meth:`Connection.cursor` can override it per cursor.

    Returns:
        A new :class:`Connection`.

    Raises:
        TrinoAuthError: ``auth`` is set for an ``http`` Coordinator without
            ``allow_insecure_auth``.
        ValueError: ``max_attempts`` is less than 1, ``http_scheme`` is unknown, an
            ``extra_credential`` key is not ASCII or contains whitespace or ``=``, or
            ``http_headers`` would override a header asynctrino sends itself.
        zoneinfo.ZoneInfoNotFoundError: ``timezone`` is not a known IANA time zone.
    """
    coordinator = Coordinator.resolve(host, port, http_scheme)
    if auth is not None and not coordinator.uses_tls and not allow_insecure_auth:
        raise TrinoAuthError(_INSECURE_AUTH_MESSAGE)
    return Connection(
        coordinator,
        ClientSession(
            user=user,
            source=source,
            catalog=catalog,
            schema=schema,
            properties={} if session_properties is None else dict(session_properties),
            client_tags=tuple(client_tags),
            roles=format_roles(roles),
            extra_credential=tuple(extra_credential),
            timezone=session_timezone(timezone),
            http_headers={} if http_headers is None else dict(http_headers),
        ),
        Transport(
            http_client,
            auth=auth,
            verify=verify,
            retry=RetryPolicy(max_attempts=max_attempts),
            timeout=request_timeout,
        ),
        legacy_primitive_types=legacy_primitive_types,
    )
