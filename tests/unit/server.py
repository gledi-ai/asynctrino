"""A real HTTP(S) server on localhost that answers every request with one finished query page.

For behaviour that ``MockTransport`` bypasses: TLS handshakes and proxy routing of the client
asynctrino creates itself.
"""

import asyncio
import json
import ssl
from contextlib import suppress
from dataclasses import dataclass, field
from types import TracebackType
from typing import Self

from tests.unit.coordinator import column, statement_pages


@dataclass(frozen=True)
class ReceivedRequest:
    method: str
    target: str
    headers: dict[str, str]


@dataclass
class OneRowServer:
    """Answers each request with a finished query whose only row is ``[1]``."""

    tls: ssl.SSLContext | None = None
    requests: list[ReceivedRequest] = field(default_factory=list[ReceivedRequest])
    _server: asyncio.Server | None = None

    @property
    def port(self) -> int:
        assert self._server is not None
        port: int = self._server.sockets[0].getsockname()[1]
        return port

    @property
    def url(self) -> str:
        return f"{'http' if self.tls is None else 'https'}://127.0.0.1:{self.port}"

    async def __aenter__(self) -> Self:
        self._server = await asyncio.start_server(self._handle, "127.0.0.1", 0, ssl=self.tls)
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        assert self._server is not None
        self._server.close()
        await self._server.wait_closed()

    async def _handle(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        try:
            head = await reader.readuntil(b"\r\n\r\n")
            request_line, *header_lines = head.decode().split("\r\n")
            method, target, _ = request_line.split(" ", 2)
            headers = dict(line.split(": ", 1) for line in header_lines if line != "")
            headers = {name.lower(): value for name, value in headers.items()}
            await reader.readexactly(int(headers.get("content-length", "0")))
            self.requests.append(ReceivedRequest(method, target, headers))
            body = json.dumps(self._page()).encode()
            writer.write(
                b"HTTP/1.1 200 OK\r\nContent-Type: application/json\r\n"
                + f"Content-Length: {len(body)}\r\nConnection: close\r\n\r\n".encode()
                + body
            )
            await writer.drain()
        except asyncio.IncompleteReadError, ConnectionError, ssl.SSLError:
            pass
        finally:
            writer.close()
            with suppress(ConnectionError, ssl.SSLError):
                await writer.wait_closed()

    def _page(self) -> dict[str, object]:
        [page] = statement_pages(
            columns=[column("_col0", "integer")], batches=[], queued_pages=0, base_url=self.url
        )
        page["data"] = [[1]]
        return page
