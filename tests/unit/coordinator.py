"""A scripted fake Coordinator served through ``httpx2.MockTransport``."""

from collections.abc import Sequence
from dataclasses import dataclass, field

import httpx2

type JSON = dict[str, object]

BASE_URL = "http://coordinator:8080"


def column(name: str, raw_type: str, *arguments: int) -> JSON:
    """Build a column as the Coordinator reports it."""
    rendered = f"{raw_type}({','.join(map(str, arguments))})" if len(arguments) > 0 else raw_type
    return {
        "name": name,
        "type": rendered,
        "typeSignature": {
            "rawType": raw_type,
            "arguments": [{"kind": "LONG", "value": a} for a in arguments],
        },
    }


def statement_pages(
    *,
    columns: Sequence[JSON] | None,
    batches: Sequence[Sequence[Sequence[object]]],
    query_id: str = "20261027_000000_00000_abcde",
    base_url: str = BASE_URL,
    queued_pages: int = 1,
) -> list[JSON]:
    """Script a Direct-protocol query: queued pages, then one page per row batch, then a final page.

    The first page answers the statement POST; each page's ``nextUri`` points at the next one.
    """
    pages: list[JSON] = [{"stats": {"state": "QUEUED"}} for _ in range(queued_pages)]
    pages.extend(
        {
            "stats": {"state": "RUNNING"},
            "columns": [] if columns is None else list(columns),
            "data": list(map(list, b)),
        }
        for b in batches
    )
    final: JSON = {"stats": {"state": "FINISHED"}}
    if columns is not None:
        final["columns"] = list(columns)
    pages.append(final)
    for index, page in enumerate(pages):
        page["id"] = query_id
        page["infoUri"] = f"{base_url}/ui/query.html?{query_id}"
        if index + 1 < len(pages):
            page["nextUri"] = f"{base_url}/v1/statement/executing/{query_id}/y/{index + 1}"
    return pages


@dataclass
class FakeCoordinator:
    """Answers the statement POST with the first page and each ``nextUri`` GET with the next.

    Every ``DELETE`` is accepted with 204, as the Coordinator does when cancelling a query.
    """

    pages: list[JSON]
    requests: list[httpx2.Request] = field(default_factory=list[httpx2.Request])

    def handler(self, request: httpx2.Request) -> httpx2.Response:
        """Record ``request`` and answer it with the scripted page."""
        self.requests.append(request)
        if request.method == "DELETE":
            return httpx2.Response(204)
        if request.method == "POST" and request.url.path.endswith("/v1/statement"):
            return httpx2.Response(200, json=self.pages[0])
        for index, page in enumerate(self.pages, start=1):
            next_uri = page.get("nextUri")
            if isinstance(next_uri, str) and httpx2.URL(next_uri) == request.url:
                return httpx2.Response(200, json=self.pages[index])
        return httpx2.Response(404, text=f"unexpected {request.method} {request.url}")

    def client(self) -> httpx2.AsyncClient:
        """Return an ``AsyncClient`` routed to this fake Coordinator."""
        return httpx2.AsyncClient(transport=httpx2.MockTransport(self.handler))
