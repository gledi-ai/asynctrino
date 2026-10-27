"""The state of one statement's execution, advanced by feeding it Coordinator responses."""

from collections import deque
from importlib.metadata import version

from asynctrino._core.coordinator import ClientSession, Coordinator
from asynctrino._core.messages import HttpRequest, HttpResponse
from asynctrino._core.status import Column, Row, parse_status

USER_AGENT = f"asynctrino/{version('asynctrino')}"


class Query:
    """Follows a statement from its POST through every ``nextUri`` until it finishes.

    The I/O layer sends :meth:`start`, then repeatedly :meth:`advance`, handing each response
    to :meth:`receive`. Received rows queue up until taken with :meth:`take_rows`.
    """

    def __init__(self, sql: str, coordinator: Coordinator, session: ClientSession) -> None:
        self.sql = sql
        self._coordinator = coordinator
        self._session = session
        self.query_id: str | None = None
        self.info_uri: str | None = None
        self.columns: list[Column] | None = None
        self._next_uri: str | None = None
        self._started = False
        self._rows: deque[Row] = deque()

    @property
    def finished(self) -> bool:
        """Whether the Coordinator has no more pages for this query."""
        return self._started and self._next_uri is None

    @property
    def ready(self) -> bool:
        """Whether rows are buffered, the columns are known, or the query has finished."""
        return bool(self._rows) or self.columns is not None or self.finished

    def start(self) -> HttpRequest:
        """The statement POST that submits the query."""
        headers = {
            **self._headers(),
            "Content-Type": "text/plain; charset=utf-8",
        }
        return HttpRequest("POST", self._coordinator.statement_url, headers, self.sql.encode())

    def advance(self) -> HttpRequest | None:
        """The GET for the next page, or ``None`` once the query has finished."""
        if self._next_uri is None:
            return None
        return HttpRequest("GET", self._next_uri, self._headers())

    def receive(self, response: HttpResponse) -> None:
        """Apply a Coordinator response to the query's state and buffer its rows."""
        status = parse_status(response)
        self._started = True
        self.query_id = status.id
        self.info_uri = status.info_uri
        self._next_uri = status.next_uri
        if status.columns is not None:
            self.columns = status.columns
        self._rows.extend(status.rows)

    def take_rows(self, limit: int | None = None) -> list[Row]:
        """Remove and return up to ``limit`` buffered rows (all of them when ``None``)."""
        count = len(self._rows) if limit is None else min(limit, len(self._rows))
        return [self._rows.popleft() for _ in range(count)]

    def _headers(self) -> dict[str, str]:
        return {**self._session.headers(), "User-Agent": USER_AGENT}
