"""The thin async layer that sends the protocol core's requests through httpx2."""

import asyncio
import logging
import random
import ssl
from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager
from datetime import UTC, datetime
from itertools import count

import httpx2
import truststore

from asynctrino._core.messages import HttpRequest, HttpResponse, redact
from asynctrino._core.query import Query
from asynctrino._core.retry import RetryPolicy
from asynctrino._core.segments import SpooledSegment
from asynctrino._core.status import check_status
from asynctrino.exceptions import Error, TrinoConnectionError

DEFAULT_REQUEST_TIMEOUT = 30.0
DEFAULT_HEARTBEAT_INTERVAL = 30.0
CANCEL_TIMEOUT = 5.0
MAX_HEARTBEAT_FAILURES = 3

logger = logging.getLogger("asynctrino")

_abandoning: set[asyncio.Task[None]] = set()


class Transport:
    """Sends core requests with an ``httpx2.AsyncClient``, retrying transient failures.

    An injected client is used as is and never closed. Otherwise a client is created on the first
    request, inside the running event loop, with ``timeout``, and closed by :meth:`aclose`; it
    verifies certificates as ``verify`` says and takes proxies from the environment. Headers and
    auth are passed per request, so no client's defaults are ever changed.
    """

    def __init__(
        self,
        http_client: httpx2.AsyncClient | None,
        *,
        auth: httpx2.Auth | None,
        verify: bool | ssl.SSLContext,
        retry: RetryPolicy,
        timeout: float | httpx2.Timeout = DEFAULT_REQUEST_TIMEOUT,
    ) -> None:
        self._client = http_client
        self._owns_client = http_client is None
        self._auth = auth
        self._verify = verify
        self._retry = retry
        self._timeout = timeout

    async def send(self, request: HttpRequest) -> HttpResponse:
        """Send ``request`` and read the whole response, retrying as the core decides.

        Raises:
            TrinoConnectionError: The last attempt failed without an HTTP response.
        """
        for attempt in count(1):
            outcome = await self._attempt(request)
            delay = self._retry.delay(
                outcome,
                request,
                attempt,
                now=datetime.now(UTC),
                jitter=random.random(),  # noqa: S311
            )
            if delay is None:
                break
            await asyncio.sleep(delay)
        if isinstance(outcome, Exception):
            target = "Segment storage" if request.segment else "the Coordinator"
            url = redact(request.url)
            reason = str(outcome).replace(request.url, url)
            msg = f"failed to reach {target} at {url}: {type(outcome).__name__}: {reason}"
            raise TrinoConnectionError(msg) from None if request.segment else outcome
        return outcome

    async def _attempt(self, request: HttpRequest) -> HttpResponse | Exception:
        if self._client is None:
            self._client = httpx2.AsyncClient(
                verify=_ssl_context(verify=self._verify), timeout=self._timeout
            )
        try:
            if request.segment:
                # send() skips the client's default headers and cookies, which build_request merges.
                response = await self._client.send(
                    httpx2.Request(request.method, request.url, headers=request.headers),
                    auth=None,
                )
            else:
                response = await self._client.request(
                    request.method,
                    request.url,
                    headers=request.headers,
                    content=request.content,
                    auth=httpx2.USE_CLIENT_DEFAULT if self._auth is None else self._auth,
                )
        except (httpx2.HTTPError, httpx2.InvalidURL) as e:
            return e
        return HttpResponse(response.status_code, response.content, response.headers)

    async def aclose(self) -> None:
        """Close the client if this transport created it."""
        if self._owns_client and self._client is not None:
            await self._client.aclose()
            self._client = None


def _ssl_context(*, verify: bool | ssl.SSLContext) -> bool | ssl.SSLContext:
    if verify is True:
        # httpx2 uses OpenSSL's bundle rather than the OS trust store on Linux.
        return truststore.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
    return verify


class QueryRunner:
    """Sends the requests of one connection's queries and cancels the ones left running.

    It tracks every query that may still be running on the Coordinator until it finishes or is
    cancelled. If the task awaiting a query's request is cancelled, the query is abandoned.
    """

    def __init__(self, transport: Transport, *, heartbeat_interval: float | None = None) -> None:
        self.transport = transport
        self.heartbeat_interval = heartbeat_interval
        self._running: set[Query] = set()

    async def submit(self, query: Query) -> None:
        """Submit ``query`` and poll until it is ready: rows or columns are known, or it ended."""
        async with self._tracking(query):
            query.receive(await self.transport.send(query.start()))
            while not query.ready:
                await self._fetch(query)

    async def fetch_more(self, query: Query) -> bool:
        """Fetch the next page of ``query``; return ``False`` if it had already ended."""
        async with self._tracking(query):
            return await self._fetch(query)

    async def cancel(self, query: Query) -> None:
        """Cancel ``query`` on the Coordinator, if it is still running.

        Raises:
            TrinoConnectionError: The Coordinator could not be reached or refused the ``DELETE``.
        """
        self._running.discard(query)
        request = query.cancel()
        if request is not None:
            check_status(await self.transport.send(request))

    async def abandon(self, query: Query) -> None:
        """Cancel ``query`` best-effort, even if the calling task is cancelled meanwhile.

        The ``DELETE`` runs in its own task, shielded from the caller's cancellation and bounded
        by :data:`CANCEL_TIMEOUT`. Its failures are logged, never raised.
        """
        self._running.discard(query)
        request = query.cancel()
        if request is None:
            return
        task = asyncio.create_task(_delete_quietly(query, request, self.transport))
        _abandoning.add(task)
        task.add_done_callback(_abandoning.discard)
        await asyncio.shield(task)

    async def abandon_all(self) -> None:
        """Abandon every query still running, concurrently."""
        await asyncio.gather(*(self.abandon(query) for query in list(self._running)))

    async def _fetch(self, query: Query) -> bool:
        segment = query.pending_segment
        if segment is not None:
            await download_segment(
                query, segment, self.transport, heartbeat_interval=self.heartbeat_interval
            )
            return True
        request = query.advance()
        if request is None:
            return False
        query.receive(await self.transport.send(request))
        return True

    @asynccontextmanager
    async def _tracking(self, query: Query) -> AsyncGenerator[None]:
        self._running.add(query)
        try:
            yield
        except asyncio.CancelledError:
            await self.abandon(query)
            raise
        finally:
            if not query.running:
                self._running.discard(query)


async def _delete_quietly(query: Query, request: HttpRequest, transport: Transport) -> None:
    try:
        async with asyncio.timeout(CANCEL_TIMEOUT):
            check_status(await transport.send(request))
    except Exception:
        logger.warning("failed to cancel query %s", query.query_id, exc_info=True)


async def download_segment(
    query: Query,
    segment: SpooledSegment,
    transport: Transport,
    *,
    heartbeat_interval: float | None = None,
) -> None:
    """Download and decode ``segment`` into ``query``'s rows, then acknowledge it.

    With a ``heartbeat_interval``, the query is kept alive while the download runs, as
    :func:`heartbeat` describes.
    """
    async with heartbeat(query, transport, heartbeat_interval):
        data = await segment_data(segment, transport)
    query.receive_segment(data)
    await acknowledge_segment(segment, transport)


@asynccontextmanager
async def heartbeat(
    query: Query, transport: Transport, interval: float | None
) -> AsyncGenerator[None]:
    """Send ``HEAD`` to ``query``'s ``nextUri`` every ``interval`` seconds while the block runs.

    The Coordinator abandons a query whose client stays silent for too long, as it does while a
    slow Segment downloads. The heartbeat stops when the block exits, when the query has ended,
    when the Coordinator answers 404 or 405, or after :data:`MAX_HEARTBEAT_FAILURES` consecutive
    failures. Its failures are logged, never raised. ``None`` or ``0`` disables it.
    """
    if interval is None or interval == 0:
        yield
        return
    task = asyncio.create_task(_beat(query, transport, interval))
    try:
        yield
    finally:
        _ = task.cancel()
        _ = await asyncio.wait([task])


async def _beat(query: Query, transport: Transport, interval: float) -> None:
    failures = 0
    while failures < MAX_HEARTBEAT_FAILURES:
        await asyncio.sleep(interval)
        request = query.heartbeat()
        if request is None:
            return
        try:
            response = await transport.send(request)
        except Exception:
            logger.debug("heartbeat for query %s failed", query.query_id, exc_info=True)
            failures += 1
            continue
        if response.status_code in {404, 405}:
            logger.warning("the Coordinator does not support heartbeats; stopping them")
            return
        failures = 0 if 200 <= response.status_code < 300 else failures + 1  # noqa: PLR2004
    logger.warning(
        "stopping the heartbeat for query %s after %d consecutive failures",
        query.query_id,
        MAX_HEARTBEAT_FAILURES,
    )


async def segment_data(segment: SpooledSegment, transport: Transport) -> bytes:
    """Download ``segment``'s data, still encoded.

    Raises:
        HttpError: Segment storage answered with an error status.
        TrinoConnectionError: Segment storage could not be reached.
    """
    response = await transport.send(segment.download_request())
    check_status(response)
    return response.content


async def acknowledge_segment(segment: SpooledSegment, transport: Transport) -> None:
    """Tell the server ``segment`` can be deleted; a failure is only logged."""
    try:
        response = await transport.send(segment.ack_request())
    except Error as e:
        logger.warning("failed to acknowledge Segment %s: %s", redact(segment.ack_uri), e)
        return
    if not 200 <= response.status_code < 300:  # noqa: PLR2004
        logger.warning(
            "failed to acknowledge Segment %s: HTTP %d",
            redact(segment.ack_uri),
            response.status_code,
        )
