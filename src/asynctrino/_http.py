"""The thin async layer that sends the protocol core's requests through httpx2."""

import httpx2

from asynctrino._core.messages import HttpRequest, HttpResponse
from asynctrino._core.query import Query
from asynctrino.exceptions import TrinoConnectionError


class Transport:
    """Sends core requests with an ``httpx2.AsyncClient``.

    An injected client is used as is and never closed. Otherwise a client is created on the first
    request, inside the running event loop, and closed by :meth:`aclose`. Headers are passed per
    request, so no client's defaults are ever changed.
    """

    def __init__(self, http_client: httpx2.AsyncClient | None) -> None:
        self._client = http_client
        self._owns_client = http_client is None

    async def send(self, request: HttpRequest) -> HttpResponse:
        """Send ``request`` and read the whole response."""
        if self._client is None:
            self._client = httpx2.AsyncClient()
        try:
            response = await self._client.request(
                request.method, request.url, headers=request.headers, content=request.content
            )
        except httpx2.HTTPError as e:
            msg = f"failed to reach the Coordinator at {request.url}: {e}"
            raise TrinoConnectionError(msg) from e
        return HttpResponse(
            response.status_code, response.content, tuple(response.headers.multi_items())
        )

    async def aclose(self) -> None:
        """Close the client if this transport created it."""
        if self._owns_client and self._client is not None:
            await self._client.aclose()
            self._client = None


async def submit(query: Query, transport: Transport) -> None:
    """Submit ``query`` and poll until it is ready: rows or columns are known, or it finished."""
    query.receive(await transport.send(query.start()))
    while not query.ready:
        await fetch_more(query, transport)


async def fetch_more(query: Query, transport: Transport) -> bool:
    """Fetch the next page of ``query``; return ``False`` if it had already finished."""
    request = query.advance()
    if request is None:
        return False
    query.receive(await transport.send(request))
    return True
