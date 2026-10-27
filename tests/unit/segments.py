import base64
import json
from collections.abc import Callable, Sequence
from compression import zstd
from dataclasses import dataclass, field

import httpx2
import lz4.block

from tests.unit.coordinator import BASE_URL, JSON, FakeCoordinator, statement_pages

STORAGE_URL = "http://storage:9000"
SIGNATURE = "s3cr3t5ignature"

type Handler = Callable[[httpx2.Request], httpx2.Response]


@dataclass(frozen=True)
class Encoded:
    data: bytes
    metadata: dict[str, int]


def encode(rows: Sequence[Sequence[object]], encoding: str, *, compress: bool = True) -> Encoded:
    """Encode ``rows`` as Trino does: compressed unless ``json`` or below the threshold."""
    raw = json.dumps(rows).encode()
    if encoding == "json" or not compress:
        return Encoded(raw, {"segmentSize": len(raw)})
    if encoding == "json+lz4":
        packed = lz4.block.compress(raw, store_size=False)
    else:
        packed = zstd.compress(raw)
    return Encoded(packed, {"segmentSize": len(packed), "uncompressedSize": len(raw)})


def inline(
    rows: Sequence[Sequence[object]],
    *,
    encoding: str = "json",
    offset: int = 0,
    compress: bool = True,
) -> JSON:
    """An inline Segment holding ``rows``."""
    encoded = encode(rows, encoding, compress=compress)
    return {
        "type": "inline",
        "data": base64.b64encode(encoded.data).decode(),
        "metadata": {"rowOffset": offset, "rowsCount": len(rows), **encoded.metadata},
    }


def spooled_pages(
    *, columns: Sequence[JSON], encoding: str, pages: Sequence[Sequence[JSON]]
) -> list[JSON]:
    """Script a Spooling-protocol query whose data pages carry ``pages`` of Segments."""
    script = statement_pages(columns=columns, batches=[[] for _ in pages])
    for page, segments in zip(script[1:], pages, strict=False):
        page["data"] = {"encoding": encoding, "segments": list(segments)}
    return script


@dataclass
class SegmentStorage:
    """Serves spooled Segments and their acks, recording every request it sees.

    Requests for anything else go to ``coordinator``, so ``requests`` is the full ordered log.
    A URL's scripted ``failures`` answer, or are raised for, its first requests.
    """

    coordinator: FakeCoordinator
    segments: dict[str, bytes] = field(default_factory=dict[str, bytes])
    failures: dict[str, list[httpx2.Response | Exception]] = field(
        default_factory=dict[str, list[httpx2.Response | Exception]]
    )
    requests: list[httpx2.Request] = field(default_factory=list[httpx2.Request])

    def spooled(  # noqa: PLR0913
        self,
        name: str,
        rows: Sequence[Sequence[object]],
        *,
        encoding: str = "json",
        offset: int = 0,
        compress: bool = True,
        headers: dict[str, list[str]] | None = None,
    ) -> JSON:
        """Store ``rows`` as Segment ``name`` and return the spooled Segment pointing at it."""
        encoded = encode(rows, encoding, compress=compress)
        uri = self.uri(name)
        self.segments[uri] = encoded.data
        segment: JSON = {
            "type": "spooled",
            "uri": uri,
            "ackUri": self.ack_uri(name),
            "metadata": {
                "rowOffset": offset,
                "rowsCount": len(rows),
                "expiresAt": "2026-10-28T12:00:00.000",
                **encoded.metadata,
            },
        }
        if headers is not None:
            segment["headers"] = headers
        return segment

    @staticmethod
    def uri(name: str) -> str:
        """A pre-signed URL, whose query string is a credential."""
        return f"{STORAGE_URL}/segments/{name}?X-Amz-Signature={SIGNATURE}-{name}"

    @staticmethod
    def ack_uri(name: str) -> str:
        return f"{BASE_URL}/v1/spooled/ack/{name}?token={SIGNATURE}-ack-{name}"

    def handler(self, request: httpx2.Request) -> httpx2.Response:
        self.requests.append(request)
        url = str(request.url)
        failures = self.failures.get(url, [])
        if len(failures) > 0:
            failure = failures.pop(0)
            if isinstance(failure, Exception):
                raise failure
            return failure
        if url in self.segments:
            return httpx2.Response(200, content=self.segments[url])
        if request.url.path.startswith("/v1/spooled/ack/"):
            return httpx2.Response(200)
        return self.coordinator.handler(request)

    def client(self) -> httpx2.AsyncClient:
        return httpx2.AsyncClient(transport=httpx2.MockTransport(self.handler))

    def segment_requests(self) -> list[str]:
        """The Segment downloads and acks so far, as ``"GET <path>"``."""
        return [
            f"{r.method} {r.url.path}"
            for r in self.requests
            if r.url.path.startswith(("/segments/", "/v1/spooled/"))
        ]
