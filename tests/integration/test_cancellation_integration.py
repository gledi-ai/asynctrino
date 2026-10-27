import asyncio

import pytest

from asynctrino.dbapi import Connection, Cursor, Row, connect

NON_TERMINAL_STATES = frozenset(
    {
        "QUEUED",
        "PLANNING",
        "STARTING",
        "RUNNING",
        "FINISHING",
        "DISPATCHING",
        "WAITING_FOR_RESOURCES",
    }
)
LONG_QUERY = "SELECT * FROM tpch.sf1.customer"
ENDLESS_QUERY = "SELECT count(*) FROM tpch.sf100000.lineitem"


async def final_state(conn: Connection, query_id: str | None) -> str:
    """Wait for ``query_id`` to reach a terminal state on the Coordinator and return it."""
    assert query_id is not None
    async with conn.cursor() as probe:
        async with asyncio.timeout(30):
            while True:
                await probe.execute(
                    f"SELECT state FROM system.runtime.queries WHERE query_id = '{query_id}'"  # noqa: S608
                )
                [[state]] = await probe.fetchall()
                if state not in NON_TERMINAL_STATES:
                    return str(state)
                await asyncio.sleep(0.1)


async def started(cur: Cursor, sql: str = LONG_QUERY) -> str | None:
    await cur.execute(sql)
    await cur.fetchone()
    return cur.query_id


async def run(cur: Cursor, sql: str) -> list[Row]:
    await cur.execute(sql)
    return await cur.fetchall()


async def test_cancel_query(conn: Connection) -> None:
    async with conn.cursor() as cur:
        query_id = await started(cur)
        await cur.cancel()

        assert await cur.fetchall() == []

    assert await final_state(conn, query_id) == "FAILED"

    async with conn.cursor() as cur:
        await cur.cancel()


async def test_close_cursor(conn: Connection) -> None:
    cur = conn.cursor()
    query_id = await started(cur)
    await cur.close()

    assert await final_state(conn, query_id) == "FAILED"

    cur = conn.cursor()
    await cur.close()


async def test_close_connection(conn: Connection, trino_address: tuple[str, int]) -> None:
    host, port = trino_address
    async with connect(host=host, port=port, user="test") as other:
        query_id = await started(other.cursor())

    assert await final_state(conn, query_id) == "FAILED"


async def test_a_timed_out_fetch_cancels_the_query(conn: Connection) -> None:
    async with conn.cursor() as cur:
        with pytest.raises(TimeoutError):
            async with asyncio.timeout(2):
                await run(cur, ENDLESS_QUERY)

        assert await final_state(conn, cur.query_id) == "FAILED"


async def test_a_finished_insert_is_not_cancelled_on_close(conn: Connection) -> None:
    async with conn.cursor() as cur:
        await cur.execute("CREATE TABLE IF NOT EXISTS memory.default.cancel_insert (n integer)")
        await cur.fetchall()
    async with conn.cursor() as cur:
        await cur.execute("INSERT INTO memory.default.cancel_insert VALUES (1), (2)")
        query_id = cur.query_id

    assert await final_state(conn, query_id) == "FINISHED"
    async with conn.cursor() as cur:
        await cur.execute("DROP TABLE memory.default.cancel_insert")
        await cur.fetchall()


async def test_concurrent_cursors_on_one_connection(conn: Connection) -> None:
    async def count_nations(region: int) -> list[object]:
        async with conn.cursor() as cur:
            await cur.execute(f"SELECT count(*) FROM nation WHERE regionkey = {region}")  # noqa: S608
            return [row[0] for row in await cur.fetchall()]

    async with asyncio.TaskGroup() as tasks:
        counts = [tasks.create_task(count_nations(region)) for region in range(5)]

    assert [c.result() for c in counts] == [[5], [5], [5], [5], [5]]
