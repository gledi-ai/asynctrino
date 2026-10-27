"""Transactions against a real Coordinator.

The memory catalog only accepts writes in autocommit, so these tests observe a transaction's
lifecycle through ``system.runtime.transactions`` while reading from memory inside it.
"""

from collections.abc import AsyncGenerator, AsyncIterator
from contextlib import AbstractContextManager, asynccontextmanager, nullcontext

import httpx2
import pytest

from asynctrino.dbapi import Connection, IsolationLevel, ProgrammingError, connect
from asynctrino.exceptions import TrinoUserError

MEMORY_READ = "SELECT count(*) FROM memory.information_schema.tables"


@asynccontextmanager
async def transactional(
    trino_address: tuple[str, int], level: IsolationLevel = IsolationLevel.READ_COMMITTED
) -> AsyncGenerator[tuple[Connection, list[str]]]:
    """Yield a connection in ``level`` and the ids of the transactions it starts, in order."""
    host, port = trino_address
    started: list[str] = []

    async def record_started(response: httpx2.Response) -> None:
        transaction_id = response.headers.get("X-Trino-Started-Transaction-Id")
        if transaction_id is not None:
            started.append(transaction_id)

    async with (
        httpx2.AsyncClient(event_hooks={"response": [record_started]}) as client,
        connect(
            host=host,
            port=port,
            user="test",
            catalog="memory",
            schema="default",
            isolation_level=level,
            http_client=client,
        ) as conn,
    ):
        yield conn, started


@pytest.fixture
async def observer(trino_address: tuple[str, int]) -> AsyncIterator[Connection]:
    host, port = trino_address
    async with connect(host=host, port=port, user="observer") as conn:
        yield conn


async def open_transactions(observer: Connection) -> dict[str, str]:
    """The isolation level of each open transaction, by id."""
    cur = observer.cursor()
    await cur.execute(
        "SELECT transaction_id, isolation_level FROM system.runtime.transactions "
        "WHERE NOT auto_commit_context"
    )
    return dict(await cur.fetchall())


async def run(conn: Connection, sql: str) -> list[list[object]]:
    cur = conn.cursor()
    await cur.execute(sql)
    return await cur.fetchall()


@pytest.mark.parametrize(
    "level",
    [
        IsolationLevel.READ_UNCOMMITTED,
        IsolationLevel.READ_COMMITTED,
        IsolationLevel.REPEATABLE_READ,
        IsolationLevel.SERIALIZABLE,
    ],
)
async def test_transaction_runs_at_the_chosen_isolation_level(
    trino_address: tuple[str, int], observer: Connection, level: IsolationLevel
) -> None:
    async with transactional(trino_address, level) as (conn, started):
        assert started == []
        await run(conn, MEMORY_READ)

        assert (await open_transactions(observer))[started[0]] == level.value


@pytest.mark.parametrize("end", ["commit", "rollback"])
async def test_ending_a_transaction_closes_it_and_the_next_statement_opens_another(
    trino_address: tuple[str, int], observer: Connection, end: str
) -> None:
    async with transactional(trino_address) as (conn, started):
        await run(conn, MEMORY_READ)
        await run(conn, MEMORY_READ)
        assert len(started) == 1

        await getattr(conn, end)()

        assert started[0] not in await open_transactions(observer)
        await run(conn, MEMORY_READ)
        assert len(started) == 2
        assert started[1] in await open_transactions(observer)


async def test_rollback_recovers_a_transaction_aborted_by_a_failed_statement(
    trino_address: tuple[str, int],
) -> None:
    async with transactional(trino_address) as (conn, _):
        with pytest.raises(TrinoUserError, match="Division by zero"):
            await run(conn, "SELECT 1 / 0")
        with pytest.raises(TrinoUserError, match="aborted"):
            await run(conn, "SELECT 1")

        await conn.rollback()

        assert await run(conn, "SELECT 1") == [[1]]


@pytest.mark.parametrize("fails", [False, True])
async def test_async_with_ends_the_transaction(
    trino_address: tuple[str, int], observer: Connection, *, fails: bool
) -> None:
    raises: AbstractContextManager[object] = pytest.raises(LookupError) if fails else nullcontext()
    with raises:
        async with transactional(trino_address) as (conn, started):
            await run(conn, MEMORY_READ)
            assert started[0] in await open_transactions(observer)
            if fails:
                raise LookupError

    assert started[0] not in await open_transactions(observer)


async def test_a_second_statement_in_flight_is_refused(trino_address: tuple[str, int]) -> None:
    async with transactional(trino_address) as (conn, _):
        first = conn.cursor()
        await first.execute("SELECT * FROM tpch.sf1.lineitem")

        with pytest.raises(ProgrammingError, match="in flight"):
            await conn.cursor().execute("SELECT 1")

        await first.close()
        await conn.rollback()
