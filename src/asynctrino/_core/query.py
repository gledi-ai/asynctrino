"""The state of one statement's execution, advanced by feeding it Coordinator responses."""

import copy
from collections import deque
from collections.abc import Callable
from importlib.metadata import version

from asynctrino._core.coordinator import Coordinator
from asynctrino._core.mapper import RowMapper
from asynctrino._core.messages import HttpRequest, HttpResponse
from asynctrino._core.session import SessionState
from asynctrino._core.status import Column, Row, parse_status

type StatsCallback = Callable[[dict[str, object]], None]

USER_AGENT = f"asynctrino/{version('asynctrino')}"


class Query:
    """Follows a statement from its POST through every ``nextUri`` until it finishes.

    The I/O layer sends :meth:`start`, then repeatedly :meth:`advance`, handing each response
    to :meth:`receive`. Received rows are mapped to Python types, unless
    ``legacy_primitive_types`` is set, and queue up until taken with :meth:`take_rows`.
    """

    def __init__(
        self,
        sql: str,
        coordinator: Coordinator,
        session: SessionState,
        stats_callback: StatsCallback | None = None,
        *,
        legacy_primitive_types: bool = False,
    ) -> None:
        self.sql = sql
        self._coordinator = coordinator
        self._session = session
        self._stats_callback = stats_callback
        self._legacy_primitive_types = legacy_primitive_types
        self.query_id: str | None = None
        self.info_uri: str | None = None
        self.stats: dict[str, object] = {}
        self.warnings: list[dict[str, object]] = []
        self.update_type: str | None = None
        self.update_count: int | None = None
        self.columns: list[Column] | None = None
        self._row_mapper: RowMapper | None = None
        self._next_uri: str | None = None
        self._started = False
        self.cancelled = False
        self._rows: deque[Row] = deque()

    @property
    def finished(self) -> bool:
        """Whether the Coordinator has no more pages for this query."""
        return self._started and self._next_uri is None

    @property
    def running(self) -> bool:
        """Whether the query was submitted and has neither finished nor been cancelled."""
        return self._next_uri is not None and not self.cancelled

    @property
    def ready(self) -> bool:
        """Whether ``execute`` can return.

        A query is ready once rows are buffered or its columns are known. An update statement is
        only ready when finished, so its update count is known and it is never cancelled on close.
        """
        if self.finished or self.cancelled:
            return True
        return self.update_type is None and (bool(self._rows) or self.columns is not None)

    def start(self) -> HttpRequest:
        """The statement POST that submits the query."""
        headers = {
            **self._headers(),
            "Content-Type": "text/plain; charset=utf-8",
        }
        url = self._coordinator.statement_url
        return HttpRequest("POST", url, headers, self.sql.encode(), statement=True)

    def advance(self) -> HttpRequest | None:
        """The GET for the next page, or ``None`` once the query has finished or was cancelled."""
        if self._next_uri is None or self.cancelled:
            return None
        return HttpRequest("GET", self._next_uri, self._headers(), statement=True)

    def cancel(self) -> HttpRequest | None:
        """Abandon the query: the ``DELETE`` that stops it, or ``None`` if nothing is running.

        The query counts as cancelled from now on, whatever the Coordinator answers, and its
        buffered rows are dropped.
        """
        if self.cancelled or self._next_uri is None:
            return None
        self.cancelled = True
        self._rows.clear()
        return HttpRequest("DELETE", self._next_uri, self._headers())

    def receive(self, response: HttpResponse) -> None:
        """Apply a Coordinator response to the query's state and buffer its rows."""
        status = parse_status(response)
        self._session.apply(response.headers)
        self._started = True
        self.query_id = status.id
        self.info_uri = status.info_uri
        self.stats.update(status.stats, queryId=status.id)
        if self._stats_callback is not None:
            self._stats_callback(copy.deepcopy(self.stats))
        if len(status.warnings) > 0:
            self.warnings = status.warnings
        if status.update_type is not None:
            self.update_type = status.update_type
        if status.update_count is not None:
            self.update_count = status.update_count
        self._next_uri = status.next_uri
        if status.columns is not None:
            self.columns = status.columns
            if not self._legacy_primitive_types:
                self._row_mapper = RowMapper(status.columns)
        mapper = self._row_mapper
        self._rows.extend(status.rows if mapper is None else map(mapper.map, status.rows))

    def take_rows(self, limit: int | None = None) -> list[Row]:
        """Remove and return up to ``limit`` buffered rows (all of them when ``None``)."""
        count = len(self._rows) if limit is None else min(limit, len(self._rows))
        return [self._rows.popleft() for _ in range(count)]

    def _headers(self) -> dict[str, str]:
        return {**self._session.current.headers(), "User-Agent": USER_AGENT}
