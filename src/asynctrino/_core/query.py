"""The state of one statement's execution, advanced by feeding it Coordinator responses."""

import copy
from collections import deque
from collections.abc import Callable
from importlib.metadata import version

from asynctrino._core.coordinator import Coordinator
from asynctrino._core.mapper import RowMapper
from asynctrino._core.messages import HttpRequest, HttpResponse
from asynctrino._core.segments import InlineSegment, Segment, SpooledSegment
from asynctrino._core.session import SessionState
from asynctrino._core.status import Column, Row, parse_status
from asynctrino.exceptions import NotSupportedError

type StatsCallback = Callable[[dict[str, object]], None]

USER_AGENT = f"asynctrino/{version('asynctrino')}"


class Query:
    """Follows a statement from its POST through every ``nextUri`` until it finishes.

    The I/O layer sends :meth:`start`, then repeatedly :meth:`advance`, handing each response
    to :meth:`receive`. Received rows are mapped to Python types, unless
    ``legacy_primitive_types`` is set, and queue up until taken with :meth:`take_rows`.

    Spooling-protocol results queue up as Segments, decoded in order as rows are taken. When the
    next one is spooled, :attr:`pending_segment` names it, and its download goes to
    :meth:`receive_segment` before any further page is fetched. With ``keep_segments``, Segments
    are instead taken undecoded with :meth:`take_segments`.
    """

    def __init__(  # noqa: PLR0913
        self,
        sql: str,
        coordinator: Coordinator,
        session: SessionState,
        stats_callback: StatsCallback | None = None,
        *,
        legacy_primitive_types: bool = False,
        keep_segments: bool = False,
    ) -> None:
        self.sql = sql
        self._coordinator = coordinator
        self._session = session
        self._stats_callback = stats_callback
        self._legacy_primitive_types = legacy_primitive_types
        self._keep_segments = keep_segments
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
        self._segments: deque[Segment] = deque()

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
        buffered = len(self._rows) > 0 or len(self._segments) > 0
        return self.update_type is None and (buffered or self.columns is not None)

    @property
    def pending_segment(self) -> SpooledSegment | None:
        """The spooled Segment to download before more rows can be taken, if any."""
        if self._keep_segments or len(self._rows) > 0 or len(self._segments) == 0:
            return None
        head = self._segments[0]
        return head if isinstance(head, SpooledSegment) else None

    def start(self) -> HttpRequest:
        """The statement POST that submits the query."""
        headers = {
            **self._headers(),
            "Content-Type": "text/plain; charset=utf-8",
        }
        url = self._coordinator.statement_url
        return HttpRequest("POST", url, headers, self.sql.encode(), kind="statement")

    def advance(self) -> HttpRequest | None:
        """The GET for the next page, or ``None`` once the query has finished or was cancelled."""
        if self._next_uri is None or self.cancelled:
            return None
        return HttpRequest("GET", self._next_uri, self._headers(), kind="statement")

    def heartbeat(self) -> HttpRequest | None:
        """The ``HEAD`` that keeps the query alive, or ``None`` once it has ended."""
        if self._next_uri is None or self.cancelled:
            return None
        return HttpRequest("HEAD", self._next_uri, self._headers())

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
        """Apply a Coordinator response to the query's state and buffer its rows.

        Raises:
            TrinoAuthError: The response's ``nextUri`` is on another origin than the Coordinator.
        """
        status = parse_status(response)
        if status.next_uri is not None:
            self._coordinator.check_origin(status.next_uri)
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
        if self._keep_segments and len(status.rows) > 0:
            msg = "the Coordinator answered with the Direct protocol, which yields no Segments"
            raise NotSupportedError(msg)
        self._rows.extend(self.map_rows(status.rows))
        self._segments.extend(status.segments)

    def receive_segment(self, data: bytes) -> None:
        """Decode the downloaded data of :attr:`pending_segment` and buffer its rows.

        Raises:
            TrinoDataError: The data can't be decoded; the Segment stays pending.
        """
        segment = self.pending_segment
        if segment is None:
            msg = "no spooled Segment is pending"
            raise RuntimeError(msg)
        self._rows.extend(self.map_rows(segment.decode(data)))
        _ = self._segments.popleft()

    def take_segments(self, limit: int | None = None) -> list[Segment]:
        """Remove and return up to ``limit`` undecoded Segments (all of them when ``None``)."""
        count = len(self._segments) if limit is None else min(limit, len(self._segments))
        return [self._segments.popleft() for _ in range(count)]

    def take_rows(self, limit: int | None = None) -> list[Row]:
        """Remove and return up to ``limit`` buffered rows (all of them when ``None``).

        Fewer rows are returned when :attr:`pending_segment` must be downloaded first.
        """
        rows: list[Row] = []
        while limit is None or len(rows) < limit:
            if len(self._rows) == 0 and not self._decode_inline_segment():
                break
            count = len(self._rows) if limit is None else min(limit - len(rows), len(self._rows))
            rows.extend(self._rows.popleft() for _ in range(count))
        return rows

    def map_rows(self, rows: list[list[object]]) -> list[Row]:
        """Map decoded rows to Python types, unless ``legacy_primitive_types`` is set."""
        mapper = self._row_mapper
        return rows if mapper is None else [mapper.map(row) for row in rows]

    def _decode_inline_segment(self) -> bool:
        if self._keep_segments or len(self._segments) == 0:
            return False
        head = self._segments[0]
        if not isinstance(head, InlineSegment):
            return False
        self._rows.extend(self.map_rows(head.decode()))
        _ = self._segments.popleft()
        return True

    def _headers(self) -> dict[str, str]:
        return {**self._session.current.headers(), "User-Agent": USER_AGENT}
