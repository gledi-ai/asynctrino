import asyncio
import uuid
from collections.abc import AsyncGenerator, AsyncIterator, Mapping
from contextlib import AbstractContextManager, asynccontextmanager, nullcontext
from types import MappingProxyType

import httpx2
import pytest
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine, create_async_engine

from asynctrino.dbapi import Connection
from asynctrino.sqlalchemy import URL


@asynccontextmanager
async def engine_for(
    trino_address: tuple[str, int],
    catalog: str,
    connect_args: Mapping[str, object] = MappingProxyType({}),
    **kwargs: object,
) -> AsyncGenerator[AsyncEngine]:
    host, port = trino_address
    url = URL(host, port=port, user="test", catalog=catalog)
    engine = create_async_engine(url, connect_args={"max_attempts": 1, **connect_args}, **kwargs)
    try:
        yield engine
    finally:
        await engine.dispose()


@pytest.fixture
async def tpch(trino_address: tuple[str, int]) -> AsyncIterator[AsyncEngine]:
    async with engine_for(trino_address, "tpch") as engine:
        yield engine


@pytest.fixture
async def users(trino_address: tuple[str, int]) -> AsyncIterator[tuple[AsyncEngine, str]]:
    name = f"memory.default.test_sqlalchemy_{uuid.uuid4().hex}"
    async with engine_for(trino_address, "memory") as engine:
        async with engine.connect() as conn:
            _ = await conn.execute(text(f"CREATE TABLE {name} (id integer, name varchar)"))
        yield engine, name
        async with engine.connect() as conn:
            _ = await conn.execute(text(f"DROP TABLE IF EXISTS {name}"))


async def test_textual_sql_binds_parameters(tpch: AsyncEngine) -> None:
    async with tpch.connect() as conn:
        result = await conn.execute(
            text("SELECT * FROM tiny.customer WHERE nationkey = :e1 AND acctbal < :e2"),
            {"e1": 15, "e2": 0},
        )
        rows = result.all()

    assert len(rows) == 3
    for row in rows:
        assert row.nationkey == 15
        assert isinstance(row.custkey, int)
        assert isinstance(row.name, str)
        assert isinstance(row.acctbal, float)
        assert row.acctbal < 0


async def test_stream_yields_every_row(tpch: AsyncEngine) -> None:
    async with tpch.connect() as conn:
        result = await conn.stream(text("SELECT name FROM tiny.nation ORDER BY nationkey"))
        names = [row.name async for row in result]

    assert len(names) == 25
    assert names[:2] == ["ALGERIA", "ARGENTINA"]


async def test_stream_fetches_lazily_and_cancels_on_close(tpch: AsyncEngine) -> None:
    async with asyncio.timeout(60), tpch.connect() as conn:
        result = await conn.stream(text("SELECT orderkey FROM sf1.orders"))
        rows = await result.fetchmany(10)
        await result.close()

    assert len(rows) == 10


async def test_insert_runs_to_completion(users: tuple[AsyncEngine, str]) -> None:
    engine, table = users
    async with engine.connect() as conn:
        inserted = await conn.execute(
            text(f"INSERT INTO {table} VALUES (:id, :name)"),  # noqa: S608
            {"id": 2, "name": "wendy"},
        )
        rows = (await conn.execute(text(f"SELECT id, name FROM {table}"))).all()  # noqa: S608

    assert inserted.rowcount == 1
    assert [tuple(row) for row in rows] == [(2, "wendy")]


async def open_transactions(observer: Connection) -> dict[str, str]:
    """The isolation level of each open transaction, by id."""
    cur = observer.cursor()
    await cur.execute(
        "SELECT transaction_id, isolation_level FROM system.runtime.transactions "
        "WHERE NOT auto_commit_context"
    )
    return dict(await cur.fetchall())


@pytest.mark.parametrize("fails", [False, True])
async def test_begin_block_runs_in_a_transaction(
    trino_address: tuple[str, int], conn: Connection, *, fails: bool
) -> None:
    started: list[str] = []

    async def record_started(response: httpx2.Response) -> None:
        transaction_id = response.headers.get("X-Trino-Started-Transaction-Id")
        if transaction_id is not None:
            started.append(transaction_id)

    async with (
        httpx2.AsyncClient(event_hooks={"response": [record_started]}) as client,
        engine_for(
            trino_address,
            "memory",
            connect_args={"http_client": client},
            isolation_level="REPEATABLE READ",
        ) as engine,
    ):
        raises: AbstractContextManager[object] = (
            pytest.raises(LookupError) if fails else nullcontext()
        )
        with raises:
            async with engine.begin() as in_transaction:
                _ = await in_transaction.execute(
                    text("SELECT count(*) FROM information_schema.tables")
                )
                assert (await open_transactions(conn))[started[0]] == "REPEATABLE READ"
                if fails:
                    raise LookupError

    assert len(started) == 1
    assert started[0] not in await open_transactions(conn)
