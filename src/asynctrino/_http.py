"""The thin async layer that sends the protocol core's requests through httpx2."""

import asyncio
import logging
import random
import ssl
from datetime import UTC, datetime
from itertools import count

import httpx2
import truststore

from asynctrino._core.messages import HttpRequest, HttpResponse
from asynctrino._core.query import Query
from asynctrino._core.retry import RetryPolicy
from asynctrino._core.segments import SpooledSegment
from asynctrino._core.status import http_error
from asynctrino.exceptions import Error, TrinoConnectionError

DEFAULT_REQUEST_TIMEOUT = 30.0

logger = logging.getLogger("asynctrino")


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
            msg = f"failed to reach {target} at {request.url}: {type(outcome).__name__}: {outcome}"
            raise TrinoConnectionError(msg) from outcome
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


async def submit(query: Query, transport: Transport) -> None:
    """Submit ``query`` and poll until it is ready: rows or columns are known, or it finished."""
    query.receive(await transport.send(query.start()))
    while not query.ready:
        await fetch_more(query, transport)


async def fetch_more(query: Query, transport: Transport) -> bool:
    """Fetch more of ``query``'s rows; return ``False`` if it had already finished.

    A pending spooled Segment is downloaded and acknowledged before the next page is fetched.
    """
    segment = query.pending_segment
    if segment is not None:
        await download_segment(query, segment, transport)
        return True
    request = query.advance()
    if request is None:
        return False
    query.receive(await transport.send(request))
    return True


async def download_segment(query: Query, segment: SpooledSegment, transport: Transport) -> None:
    """Download and decode ``segment`` into ``query``'s rows, then acknowledge it."""
    query.receive_segment(await segment_data(segment, transport))
    await acknowledge_segment(segment, transport)


async def segment_data(segment: SpooledSegment, transport: Transport) -> bytes:
    """Download ``segment``'s data, still encoded.

    Raises:
        HttpError: Segment storage answered with an error status.
        TrinoConnectionError: Segment storage could not be reached.
    """
    response = await transport.send(segment.download_request())
    if not 200 <= response.status_code < 300:  # noqa: PLR2004
        raise http_error(response)
    return response.content


async def acknowledge_segment(segment: SpooledSegment, transport: Transport) -> None:
    """Tell the server ``segment`` can be deleted; a failure is only logged."""
    try:
        response = await transport.send(segment.ack_request())
    except Error as e:
        logger.warning("failed to acknowledge Segment %s: %s", segment.ack_uri, e)
        return
    if not 200 <= response.status_code < 300:  # noqa: PLR2004
        logger.warning(
            "failed to acknowledge Segment %s: HTTP %d", segment.ack_uri, response.status_code
        )
