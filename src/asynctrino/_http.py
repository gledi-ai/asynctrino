"""The thin async layer that sends the protocol core's requests through httpx2."""

import ssl

import httpx2
import truststore

from asynctrino._core.messages import HttpRequest, HttpResponse
from asynctrino._core.query import Query
from asynctrino.exceptions import TrinoConnectionError


class Transport:
    """Sends core requests with an ``httpx2.AsyncClient``.

    An injected client is used as is and never closed. Otherwise a client is created on the first
    request, inside the running event loop, and closed by :meth:`aclose`; it verifies
    certificates as ``verify`` says and takes proxies from the environment. Headers and auth are
    passed per request, so no client's defaults are ever changed.
    """

    def __init__(
        self,
        http_client: httpx2.AsyncClient | None,
        *,
        auth: httpx2.Auth | None,
        verify: bool | ssl.SSLContext,
    ) -> None:
        self._client = http_client
        self._owns_client = http_client is None
        self._auth = auth
        self._verify = verify

    async def send(self, request: HttpRequest) -> HttpResponse:
        """Send ``request`` and read the whole response."""
        if self._client is None:
            self._client = httpx2.AsyncClient(verify=_ssl_context(verify=self._verify))
        try:
            response = await self._client.request(
                request.method,
                request.url,
                headers=request.headers,
                content=request.content,
                auth=httpx2.USE_CLIENT_DEFAULT if self._auth is None else self._auth,
            )
        except httpx2.HTTPError as e:
            msg = f"failed to reach the Coordinator at {request.url}: {e}"
            raise TrinoConnectionError(msg) from e
        return HttpResponse(response.status_code, response.content)

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
    """Fetch the next page of ``query``; return ``False`` if it had already finished."""
    request = query.advance()
    if request is None:
        return False
    query.receive(await transport.send(request))
    return True
