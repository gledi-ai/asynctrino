import asyncio
from collections.abc import Sequence
from dataclasses import dataclass, field
from itertools import count

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


@dataclass
class TransactionCoordinator:
    """Starts, commits and rolls back transactions as Trino does, through response headers.

    Other statements finish in one page, unless they are listed in ``open_statements``: those
    return a page of rows with a ``nextUri``, then finish on the GET. Statements listed in
    ``failing_statements`` fail with a user error, on the GET if they are also open.
    """

    open_statements: set[str] = field(default_factory=set[str])
    failing_statements: set[str] = field(default_factory=set[str])
    requests: list[httpx2.Request] = field(default_factory=list[httpx2.Request])
    _transaction_ids: count[int] = field(default_factory=lambda: count(1))
    _failing_uris: set[str] = field(default_factory=set[str])

    async def handler(self, request: httpx2.Request) -> httpx2.Response:
        await asyncio.sleep(0)
        self.requests.append(request)
        query_id = f"q{len(self.requests)}"
        page: JSON = {
            "id": query_id,
            "infoUri": f"{BASE_URL}/ui/query.html?{query_id}",
            "stats": {"state": "FINISHED"},
        }
        headers: dict[str, str] = {}
        failure = {"message": "statement failed", "errorType": "USER_ERROR"}
        if request.method == "GET":
            if str(request.url) in self._failing_uris:
                page["error"] = failure
            return httpx2.Response(200, json=page)
        sql = request.content.decode()
        if sql in self.open_statements:
            next_uri = f"{BASE_URL}/v1/statement/executing/{query_id}/y/1"
            page |= {"stats": {"state": "RUNNING"}, "columns": [column("_col0", "integer")]}
            page["data"] = [[1]]
            page["nextUri"] = next_uri
            if sql in self.failing_statements:
                self._failing_uris.add(next_uri)
        elif sql in self.failing_statements:
            page["error"] = failure
        elif sql.startswith("START TRANSACTION"):
            headers["X-Trino-Started-Transaction-Id"] = f"tx-{next(self._transaction_ids)}"
        elif sql in {"COMMIT", "ROLLBACK"}:
            headers["X-Trino-Clear-Transaction-Id"] = "true"
        return httpx2.Response(200, json=page, headers=headers)

    def client(self) -> httpx2.AsyncClient:
        return httpx2.AsyncClient(transport=httpx2.MockTransport(self.handler))

    def statements(self) -> list[tuple[str, str | None]]:
        """Each POSTed statement with the transaction id it was sent in."""
        return [
            (r.content.decode(), r.headers.get("X-Trino-Transaction-Id"))
            for r in self.requests
            if r.method == "POST"
        ]


@dataclass
class CatalogCoordinator:
    """Answers each statement with the pages scripted for the first fragment its SQL contains.

    Statements that match no fragment finish with one ``varchar`` column and no rows. Every
    ``DELETE`` is accepted with 204.
    Scripts are built with :func:`statement_pages`; give each one its own ``query_id``.
    """

    scripts: dict[str, list[JSON]] = field(default_factory=dict[str, list[JSON]])
    requests: list[httpx2.Request] = field(default_factory=list[httpx2.Request])

    def handler(self, request: httpx2.Request) -> httpx2.Response:
        """Record ``request`` and answer it from the scripts."""
        self.requests.append(request)
        if request.method == "DELETE":
            return httpx2.Response(204)
        if request.method == "POST":
            sql = request.content.decode()
            for fragment, pages in self.scripts.items():
                if fragment in sql:
                    return httpx2.Response(200, json=pages[0])
            [finished] = statement_pages(
                columns=[column("_col0", "varchar")], batches=[], queued_pages=0
            )
            return httpx2.Response(200, json=finished)
        for pages in self.scripts.values():
            for index, page in enumerate(pages, start=1):
                next_uri = page.get("nextUri")
                if isinstance(next_uri, str) and httpx2.URL(next_uri) == request.url:
                    return httpx2.Response(200, json=pages[index])
        return httpx2.Response(404, text=f"unexpected {request.method} {request.url}")

    def client(self) -> httpx2.AsyncClient:
        """Return an ``AsyncClient`` routed to this fake Coordinator."""
        return httpx2.AsyncClient(transport=httpx2.MockTransport(self.handler))

    def statements(self) -> list[str]:
        """The SQL of every POSTed statement, in order."""
        return [r.content.decode() for r in self.requests if r.method == "POST"]
