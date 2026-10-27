import asyncio
import gc
import logging
from collections.abc import Awaitable, Callable
from dataclasses import dataclass

import httpx2
import pytest

import asynctrino._http
from asynctrino.dbapi import Connection, Cursor, Row, connect
from asynctrino.exceptions import TrinoConnectionError
from tests.unit.coordinator import JSON, FakeCoordinator, column, statement_pages


def unfinished_select() -> FakeCoordinator:
    return FakeCoordinator(
        statement_pages(columns=[column("n", "integer")], batches=[[[1]], [[2]], [[3]]])
    )


def deletes(coordinator: FakeCoordinator) -> list[str]:
    return [str(r.url) for r in coordinator.requests if r.method == "DELETE"]


async def test_cancel_deletes_the_latest_next_uri_and_stops_the_rows() -> None:
    coordinator = unfinished_select()
    async with coordinator.client() as client:
        cur = connect(host="coordinator", user="alice", http_client=client).cursor()
        await cur.execute("SELECT n FROM t")
        assert await cur.fetchone() == [1]
        next_uri = coordinator.pages[1]["nextUri"]

        await cur.cancel()

        assert deletes(coordinator) == [next_uri]
        assert coordinator.requests[-1].headers["X-Trino-User"] == "alice"
        assert await cur.fetchall() == []


async def test_cancel_without_an_unfinished_query_sends_nothing() -> None:
    coordinator = FakeCoordinator(
        statement_pages(columns=[column("_col0", "integer")], batches=[[[1]]])
    )
    async with coordinator.client() as client:
        cur = connect(host="coordinator", http_client=client).cursor()
        await cur.cancel()
        await cur.execute("SELECT 1")
        await cur.fetchall()
        await cur.cancel()

    assert deletes(coordinator) == []


def refusing_deletes(coordinator: FakeCoordinator) -> httpx2.AsyncClient:
    def handler(request: httpx2.Request) -> httpx2.Response:
        if request.method == "DELETE":
            coordinator.requests.append(request)
            return httpx2.Response(500, text="cannot cancel")
        return coordinator.handler(request)

    return httpx2.AsyncClient(transport=httpx2.MockTransport(handler))


async def test_an_explicit_cancel_raises_when_the_coordinator_refuses() -> None:
    coordinator = unfinished_select()
    async with refusing_deletes(coordinator) as client:
        cur = connect(host="coordinator", http_client=client, max_attempts=1).cursor()
        await cur.execute("SELECT n FROM t")

        with pytest.raises(TrinoConnectionError, match="error 500: cannot cancel"):
            await cur.cancel()


def stalling(
    coordinator: FakeCoordinator, page: int, reached: asyncio.Event | None = None
) -> httpx2.AsyncClient:
    """A client whose GET for ``page`` never gets an answer; ``reached`` is set when it's sent."""
    stalled = coordinator.pages[page - 1]["nextUri"]

    async def handler(request: httpx2.Request) -> httpx2.Response:
        if request.method == "GET" and str(request.url) == stalled:
            coordinator.requests.append(request)
            if reached is not None:
                reached.set()
            _ = await asyncio.Event().wait()
        return coordinator.handler(request)

    return httpx2.AsyncClient(transport=httpx2.MockTransport(handler))


async def test_a_timeout_during_execute_deletes_the_query_then_times_out() -> None:
    coordinator = unfinished_select()
    async with stalling(coordinator, page=1) as client:
        cur = connect(host="coordinator", http_client=client).cursor()

        with pytest.raises(TimeoutError):
            async with asyncio.timeout(0.05):
                await cur.execute("SELECT n FROM t")

    assert deletes(coordinator) == [coordinator.pages[0]["nextUri"]]


async def drain(cur: Cursor, fetch: str) -> None:
    match fetch:
        case "fetchone":
            while await cur.fetchone() is not None:
                pass
        case "fetchmany":
            _ = await cur.fetchmany()
        case "fetchall":
            _ = await cur.fetchall()
        case _:
            _ = [row async for row in cur]


@pytest.mark.parametrize("fetch", ["fetchone", "fetchmany", "fetchall", "async for"])
async def test_a_timeout_during_a_fetch_deletes_the_query_then_times_out(fetch: str) -> None:
    coordinator = unfinished_select()
    async with stalling(coordinator, page=3) as client:
        cur = connect(host="coordinator", http_client=client).cursor()
        cur.arraysize = 5
        await cur.execute("SELECT n FROM t")

        with pytest.raises(TimeoutError):
            async with asyncio.timeout(0.05):
                await drain(cur, fetch)

    assert deletes(coordinator) == [coordinator.pages[2]["nextUri"]]


async def test_cancelling_the_awaiting_task_propagates_cancelled_error_after_the_delete() -> None:
    coordinator = unfinished_select()
    reached = asyncio.Event()
    async with stalling(coordinator, page=1, reached=reached) as client:
        cur = connect(host="coordinator", http_client=client).cursor()
        task = asyncio.create_task(cur.execute("SELECT n FROM t"))
        _ = await reached.wait()

        _ = task.cancel("stop")
        with pytest.raises(asyncio.CancelledError, match="stop"):
            await task

    assert deletes(coordinator) == [coordinator.pages[0]["nextUri"]]


def stalling_and(
    coordinator: FakeCoordinator, page: int, delete: Callable[[], Awaitable[httpx2.Response]]
) -> httpx2.AsyncClient:
    stalled = coordinator.pages[page - 1]["nextUri"]

    async def handler(request: httpx2.Request) -> httpx2.Response:
        if request.method == "DELETE":
            coordinator.requests.append(request)
            return await delete()
        if request.method == "GET" and str(request.url) == stalled:
            _ = await asyncio.Event().wait()
        return coordinator.handler(request)

    return httpx2.AsyncClient(transport=httpx2.MockTransport(handler))


async def test_a_failed_delete_after_task_cancellation_is_logged_not_raised(
    caplog: pytest.LogCaptureFixture,
) -> None:
    async def refuse() -> httpx2.Response:
        return httpx2.Response(500, text="cannot cancel")

    coordinator = unfinished_select()
    async with stalling_and(coordinator, page=1, delete=refuse) as client:
        cur = connect(host="coordinator", http_client=client, max_attempts=1).cursor()

        with pytest.raises(TimeoutError):
            async with asyncio.timeout(0.05):
                await cur.execute("SELECT n FROM t")

    assert len(deletes(coordinator)) == 1
    [record] = caplog.records
    assert record.name == "asynctrino"
    assert record.levelno == logging.WARNING
    assert str(coordinator.pages[0]["id"]) in record.getMessage()
    assert record.exc_info is not None
    assert "cannot cancel" in str(record.exc_info[1])


async def test_a_hanging_delete_after_task_cancellation_gives_up_quickly(
    monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    monkeypatch.setattr(asynctrino._http, "CANCEL_TIMEOUT", 0.05)

    async def hang() -> httpx2.Response:
        _ = await asyncio.Event().wait()
        raise AssertionError

    coordinator = unfinished_select()
    async with stalling_and(coordinator, page=1, delete=hang) as client:
        cur = connect(host="coordinator", http_client=client).cursor()

        async with asyncio.timeout(1):
            with pytest.raises(TimeoutError):
                async with asyncio.timeout(0.05):
                    await cur.execute("SELECT n FROM t")

    assert len(deletes(coordinator)) == 1
    assert [r.levelno for r in caplog.records] == [logging.WARNING]


async def test_closing_a_cursor_deletes_its_unfinished_query() -> None:
    coordinator = unfinished_select()
    async with coordinator.client() as client:
        conn = connect(host="coordinator", http_client=client)
        async with conn.cursor() as cur:
            await cur.execute("SELECT n FROM t")
            await cur.fetchone()

    assert deletes(coordinator) == [coordinator.pages[1]["nextUri"]]


async def test_closing_a_cursor_after_an_update_leaves_the_finished_query_alone() -> None:
    pages = statement_pages(columns=[column("rows", "bigint")], batches=[[[1000]]])
    for page in pages:
        page["updateType"] = "INSERT"
    pages[-1]["updateCount"] = 1000
    coordinator = FakeCoordinator(pages)
    async with (
        coordinator.client() as client,
        connect(host="coordinator", http_client=client).cursor() as cur,
    ):
        await cur.execute("INSERT INTO t VALUES (1)")
        assert cur.rowcount == 1000

    assert [r.method for r in coordinator.requests] == ["POST", "GET", "GET"]


async def test_a_failed_delete_on_close_is_logged_not_raised(
    caplog: pytest.LogCaptureFixture,
) -> None:
    coordinator = unfinished_select()
    async with refusing_deletes(coordinator) as client:
        cur = connect(host="coordinator", http_client=client, max_attempts=1).cursor()
        await cur.execute("SELECT n FROM t")

        await cur.close()

    assert len(deletes(coordinator)) == 1
    assert [r.levelno for r in caplog.records] == [logging.WARNING]


async def test_closing_a_connection_deletes_the_unfinished_queries_of_its_cursors() -> None:
    coordinator = unfinished_select()
    async with (
        coordinator.client() as client,
        connect(host="coordinator", http_client=client) as conn,
    ):
        running = conn.cursor()
        await running.execute("SELECT n FROM t")
        drained = conn.cursor()
        await drained.execute("SELECT n FROM t")
        _ = await drained.fetchall()
        _ = conn.cursor()

    assert deletes(coordinator) == [coordinator.pages[1]["nextUri"]]


async def test_closing_a_connection_deletes_queries_of_cursors_no_longer_referenced() -> None:
    coordinator = unfinished_select()
    async with (
        coordinator.client() as client,
        connect(host="coordinator", http_client=client) as conn,
    ):
        await conn.cursor().execute("SELECT n FROM t")
        _ = gc.collect()

    assert deletes(coordinator) == [coordinator.pages[1]["nextUri"]]


@dataclass
class ManyQueriesCoordinator:
    """Serves one scripted query per statement text, interleaving all requests in flight."""

    scripts: dict[str, list[JSON]]
    in_flight: int = 0
    peak_in_flight: int = 0

    async def handler(self, request: httpx2.Request) -> httpx2.Response:
        self.in_flight += 1
        self.peak_in_flight = max(self.peak_in_flight, self.in_flight)
        for _ in range(3):
            await asyncio.sleep(0)
        self.in_flight -= 1
        if request.method == "POST":
            return httpx2.Response(200, json=self.scripts[request.content.decode()][0])
        for pages in self.scripts.values():
            for index, page in enumerate(pages[:-1], start=1):
                if httpx2.URL(str(page["nextUri"])) == request.url:
                    return httpx2.Response(200, json=pages[index])
        return httpx2.Response(404)


async def test_tasks_run_their_own_cursors_of_one_connection_concurrently() -> None:
    statements = {
        f"SELECT n FROM t{task}": [[[task, row]] for row in range(task + 2)]  # noqa: S608
        for task in range(8)
    }
    coordinator = ManyQueriesCoordinator(
        {
            sql: statement_pages(
                columns=[column("t", "integer"), column("n", "integer")],
                batches=batches,
                query_id=f"20261027_000000_{task:05}_abcde",
            )
            for task, (sql, batches) in enumerate(statements.items())
        }
    )

    async def run(conn: Connection, sql: str) -> tuple[str | None, list[Row]]:
        async with conn.cursor() as cur:
            await cur.execute(sql)
            first = await cur.fetchone()
            rest = await cur.fetchall()
            return cur.query_id, [] if first is None else [first, *rest]

    async with (
        httpx2.AsyncClient(transport=httpx2.MockTransport(coordinator.handler)) as client,
        connect(host="coordinator", http_client=client) as conn,
        asyncio.TaskGroup() as tasks,
    ):
        results = [tasks.create_task(run(conn, sql)) for sql in statements]

    assert coordinator.peak_in_flight == len(statements)
    for task, (result, batches) in enumerate(zip(results, statements.values(), strict=True)):
        assert result.result() == (
            f"20261027_000000_{task:05}_abcde",
            [row for batch in batches for row in batch],
        )
