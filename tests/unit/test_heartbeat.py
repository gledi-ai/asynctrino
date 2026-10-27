import asyncio
from dataclasses import dataclass, field

import httpx2
import pytest

from asynctrino.auth import BasicAuthentication
from asynctrino.dbapi import Cursor, connect
from asynctrino.exceptions import HttpError
from tests.unit.coordinator import FakeCoordinator, column
from tests.unit.segments import SegmentStorage, spooled_pages

INTERVAL = 0.01
ROWS = [[1, "one"], [2, "two"]]


@dataclass
class SlowStorage:
    """Segment storage whose downloads hang until :attr:`release` is set.

    The Coordinator answers each ``HEAD`` with the next scripted answer, then with 200.
    """

    storage: SegmentStorage
    answers: list[httpx2.Response | Exception] = field(
        default_factory=list[httpx2.Response | Exception]
    )
    heads: list[httpx2.Request] = field(default_factory=list[httpx2.Request])
    release: asyncio.Event = field(default_factory=asyncio.Event)
    _beat: asyncio.Condition = field(default_factory=asyncio.Condition)

    @property
    def next_uri(self) -> str:
        """The ``nextUri`` of the page holding the spooled Segment."""
        uri = self.storage.coordinator.pages[1]["nextUri"]
        assert isinstance(uri, str)
        return uri

    async def handler(self, request: httpx2.Request) -> httpx2.Response:
        if request.method == "HEAD":
            async with self._beat:
                self.heads.append(request)
                self._beat.notify_all()
            if len(self.answers) > 0:
                answer = self.answers.pop(0)
                if isinstance(answer, Exception):
                    raise answer
                return answer
            return httpx2.Response(200)
        if str(request.url) in self.storage.segments:
            await self.release.wait()
        return self.storage.handler(request)

    def client(self) -> httpx2.AsyncClient:
        return httpx2.AsyncClient(transport=httpx2.MockTransport(self.handler))

    async def wait_for_heads(self, count: int) -> None:
        async with asyncio.timeout(1), self._beat:
            await self._beat.wait_for(lambda: len(self.heads) >= count)


def slow_storage() -> SlowStorage:
    coordinator = FakeCoordinator([])
    storage = SegmentStorage(coordinator)
    coordinator.pages = spooled_pages(
        columns=[column("n", "integer"), column("s", "varchar")],
        encoding="json",
        pages=[[storage.spooled("a", ROWS)]],
    )
    return SlowStorage(storage)


async def quiet_for_a_while() -> None:
    """Give a heartbeat that should have stopped ample time to beat again."""
    await asyncio.sleep(INTERVAL * 10)


async def execute(
    client: httpx2.AsyncClient, *, heartbeat_interval: float | None = INTERVAL
) -> Cursor:
    conn = connect(
        host="coordinator",
        user="alice",
        auth=BasicAuthentication("alice", "s3cret"),
        allow_insecure_auth=True,
        http_headers={"X-Gateway": "g"},
        encoding="json",
        heartbeat_interval=heartbeat_interval,
        max_attempts=1,
        http_client=client,
    )
    cur = conn.cursor()
    await cur.execute("SELECT n, s FROM t")
    return cur


async def test_heads_go_to_the_next_uri_with_coordinator_auth_while_a_segment_downloads() -> None:
    slow = slow_storage()
    async with slow.client() as client:
        cur = await execute(client)
        fetch = asyncio.create_task(cur.fetchall())

        await slow.wait_for_heads(2)
        slow.release.set()

        assert await fetch == ROWS
    for head in slow.heads:
        assert str(head.url) == slow.next_uri
        assert head.headers["Authorization"].startswith("Basic ")
        assert head.headers["X-Trino-User"] == "alice"
        assert head.headers["X-Gateway"] == "g"


async def test_the_heartbeat_stops_when_the_download_finishes() -> None:
    slow = slow_storage()
    async with slow.client() as client:
        cur = await execute(client)
        fetch = asyncio.create_task(cur.fetchall())
        await slow.wait_for_heads(1)

        slow.release.set()
        assert await fetch == ROWS
        beats = len(slow.heads)
        await quiet_for_a_while()

        assert len(slow.heads) == beats


@pytest.mark.parametrize("status", [404, 405])
async def test_the_heartbeat_stops_when_the_coordinator_does_not_support_it(status: int) -> None:
    slow = slow_storage()
    slow.answers = [httpx2.Response(status)]
    async with slow.client() as client:
        cur = await execute(client)
        fetch = asyncio.create_task(cur.fetchall())
        await slow.wait_for_heads(1)

        await quiet_for_a_while()
        assert len(slow.heads) == 1
        slow.release.set()
        assert await fetch == ROWS


@pytest.mark.parametrize(
    "failure",
    [httpx2.Response(500, text="boom"), httpx2.ConnectError("refused")],
    ids=["500", "connect-error"],
)
async def test_the_heartbeat_stops_after_three_consecutive_failures_without_failing_the_query(
    failure: httpx2.Response | Exception,
) -> None:
    slow = slow_storage()
    slow.answers = [failure] * 3
    async with slow.client() as client:
        cur = await execute(client)
        fetch = asyncio.create_task(cur.fetchall())
        await slow.wait_for_heads(3)

        await quiet_for_a_while()
        assert len(slow.heads) == 3
        slow.release.set()
        assert await fetch == ROWS


async def test_a_successful_heartbeat_resets_the_failure_count() -> None:
    slow = slow_storage()
    fail = httpx2.Response(500, text="boom")
    slow.answers = [fail, fail, httpx2.Response(200), fail, fail, fail]
    async with slow.client() as client:
        cur = await execute(client)
        fetch = asyncio.create_task(cur.fetchall())
        await slow.wait_for_heads(6)

        await quiet_for_a_while()
        assert len(slow.heads) == 6
        slow.release.set()
        assert await fetch == ROWS


async def test_the_heartbeat_stops_when_the_cursor_is_closed_mid_download() -> None:
    slow = slow_storage()
    async with slow.client() as client:
        cur = await execute(client)
        fetch = asyncio.create_task(cur.fetchall())
        await slow.wait_for_heads(1)

        await cur.close()
        beats = len(slow.heads)
        await quiet_for_a_while()

        assert len(slow.heads) == beats
        _ = fetch.cancel()
        _ = await asyncio.wait([fetch])


async def test_the_heartbeat_stops_when_the_download_fails() -> None:
    slow = slow_storage()
    slow.storage.failures[SegmentStorage.uri("a")] = [httpx2.Response(500, text="boom")]
    async with slow.client() as client:
        cur = await execute(client)
        fetch = asyncio.create_task(cur.fetchall())
        await slow.wait_for_heads(1)

        slow.release.set()
        with pytest.raises(HttpError, match="500"):
            _ = await fetch
        beats = len(slow.heads)
        await quiet_for_a_while()

        assert len(slow.heads) == beats


@pytest.mark.parametrize("interval", [None, 0])
async def test_no_heartbeat_is_sent_when_disabled(interval: float | None) -> None:
    slow = slow_storage()
    async with slow.client() as client:
        cur = await execute(client, heartbeat_interval=interval)
        fetch = asyncio.create_task(cur.fetchall())

        await quiet_for_a_while()
        slow.release.set()
        assert await fetch == ROWS

    assert slow.heads == []


def test_a_negative_heartbeat_interval_is_rejected() -> None:
    with pytest.raises(ValueError, match="heartbeat_interval"):
        _ = connect(host="coordinator", heartbeat_interval=-1)
