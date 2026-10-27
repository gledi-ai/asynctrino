from collections.abc import AsyncGenerator, Mapping
from contextlib import asynccontextmanager
from importlib.metadata import version
from types import MappingProxyType
from typing import Protocol

import httpx2
import pytest
from sqlalchemy import exc, text
from sqlalchemy.ext.asyncio import AsyncConnection, AsyncEngine, create_async_engine

from asynctrino.auth import BasicAuthentication
from asynctrino.sqlalchemy import URL
from tests.unit.coordinator import (
    FakeCoordinator,
    TransactionCoordinator,
    column,
    statement_pages,
)


@asynccontextmanager
async def engine_for(
    client: httpx2.AsyncClient,
    url: str = "asynctrino://alice@coordinator:8080/tpch/tiny",
    connect_args: Mapping[str, object] = MappingProxyType({}),
    **kwargs: object,
) -> AsyncGenerator[AsyncEngine]:
    engine = create_async_engine(
        url, connect_args={"http_client": client, **connect_args}, **kwargs
    )
    try:
        yield engine
    finally:
        await engine.dispose()


class Scripted(Protocol):
    def client(self) -> httpx2.AsyncClient: ...


@asynccontextmanager
async def connection_to(coordinator: Scripted) -> AsyncGenerator[AsyncConnection]:
    async with (
        coordinator.client() as client,
        engine_for(client) as engine,
        engine.connect() as conn,
    ):
        yield conn


async def query(engine: AsyncEngine, sql: str) -> list[tuple[object, ...]]:
    async with engine.connect() as conn:
        result = await conn.execute(text(sql))
        return [tuple(row) for row in result.all()]


def one_row_coordinator() -> FakeCoordinator:
    return FakeCoordinator(
        statement_pages(columns=[column("_col0", "integer")], batches=[[[1]]], queued_pages=0)
    )


async def test_text_query_runs_through_the_async_engine() -> None:
    coordinator = one_row_coordinator()
    async with coordinator.client() as client, engine_for(client) as engine:
        rows = await query(engine, "SELECT 1")

        assert engine.dialect.name == "trino"
        assert engine.dialect.driver == "asynctrino"

    assert rows == [(1,)]
    [post] = [r for r in coordinator.requests if r.method == "POST"]
    assert post.content == b"SELECT 1"
    assert post.headers["X-Trino-User"] == "alice"
    assert post.headers["X-Trino-Catalog"] == "tpch"
    assert post.headers["X-Trino-Schema"] == "tiny"


def test_dbapi_version_is_the_installed_asynctrino_release() -> None:
    engine = create_async_engine("asynctrino://alice@coordinator:8080")

    release = ".".join(map(str, engine.dialect.dbapi_version[:3]))

    assert version("asynctrino").startswith(release)


async def test_url_options_reach_the_coordinator() -> None:
    coordinator = one_row_coordinator()
    url = URL(
        "coordinator",
        port=8443,
        user="alice",
        password="secret",
        catalog="tpch",
        session_properties={"query_max_run_time": "1d"},
        client_tags=["etl"],
        timezone="Europe/Brussels",
    )
    async with coordinator.client() as client, engine_for(client, url) as engine:
        _ = await query(engine, "SELECT 1")

    [post] = [r for r in coordinator.requests if r.method == "POST"]
    assert str(post.url) == "https://coordinator:8443/v1/statement"
    assert post.headers["Authorization"] == BasicAuthentication("alice", "secret").authorization()
    assert post.headers["X-Trino-Source"] == "asynctrino-sqlalchemy"
    assert post.headers["X-Trino-Session"] == "query_max_run_time=1d"
    assert post.headers["X-Trino-Client-Tags"] == "etl"
    assert post.headers["X-Trino-Time-Zone"] == "Europe/Brussels"


async def test_connect_args_override_url_options() -> None:
    coordinator = one_row_coordinator()
    url = "asynctrino://alice@coordinator:8080/tpch?source=from-url"
    overrides: dict[str, object] = {"source": "from-connect-args", "user": "bob"}
    async with coordinator.client() as client, engine_for(client, url, overrides) as engine:
        _ = await query(engine, "SELECT 1")

    [post] = [r for r in coordinator.requests if r.method == "POST"]
    assert post.headers["X-Trino-Source"] == "from-connect-args"
    assert post.headers["X-Trino-User"] == "bob"


async def test_bound_parameters_are_sent_as_qmark_parameters() -> None:
    coordinator = one_row_coordinator()
    async with connection_to(coordinator) as conn:
        _ = await conn.execute(text("SELECT :a, :b"), {"a": 1, "b": "it's"})

    [post] = [r for r in coordinator.requests if r.method == "POST"]
    assert post.content.decode() == "EXECUTE IMMEDIATE 'SELECT ?, ?' USING 1,'it''s'"


def paged_coordinator() -> FakeCoordinator:
    return FakeCoordinator(
        statement_pages(
            columns=[column("x", "integer")], batches=[[[1], [2]], [[3]], [[4]]], queued_pages=0
        )
    )


async def test_stream_fetches_pages_as_rows_are_consumed() -> None:
    coordinator = paged_coordinator()
    async with connection_to(coordinator) as conn:
        result = await conn.stream(text("SELECT x"))
        first = await result.fetchone()
        gets_after_first_row = [r for r in coordinator.requests if r.method == "GET"]
        rest = [tuple(row) for row in await result.all()]

    assert first is not None
    assert tuple(first) == (1,)
    assert gets_after_first_row == []
    assert rest == [(2,), (3,), (4,)]


async def test_isolation_level_set_on_a_connection_is_reset_when_it_returns_to_the_pool() -> None:
    coordinator = TransactionCoordinator()
    async with coordinator.client() as client, engine_for(client) as engine:
        async with engine.connect() as conn:
            assert await conn.get_isolation_level() == "AUTOCOMMIT"
            _ = await conn.execution_options(isolation_level="READ COMMITTED")
            assert await conn.get_isolation_level() == "READ COMMITTED"
            _ = await conn.execute(text("SELECT 1"))
            await conn.commit()
        async with engine.connect() as conn:
            assert await conn.get_isolation_level() == "AUTOCOMMIT"
            _ = await conn.execute(text("SELECT 2"))

    assert coordinator.statements() == [
        ("START TRANSACTION ISOLATION LEVEL READ COMMITTED", "NONE"),
        ("SELECT 1", "tx-1"),
        ("COMMIT", "tx-1"),
        ("SELECT 2", None),
    ]


async def fail_in_transaction(engine: AsyncEngine, sql: str) -> None:
    async with engine.begin() as conn:
        _ = await conn.execute(text(sql))
        raise RuntimeError


async def test_engine_isolation_level_runs_begin_blocks_in_transactions() -> None:
    coordinator = TransactionCoordinator()
    async with (
        coordinator.client() as client,
        engine_for(client, isolation_level="SERIALIZABLE") as engine,
    ):
        async with engine.begin() as conn:
            _ = await conn.execute(text("SELECT 1"))
        with pytest.raises(RuntimeError):
            await fail_in_transaction(engine, "SELECT 2")

    assert coordinator.statements() == [
        ("START TRANSACTION ISOLATION LEVEL SERIALIZABLE", "NONE"),
        ("SELECT 1", "tx-1"),
        ("COMMIT", "tx-1"),
        ("START TRANSACTION ISOLATION LEVEL SERIALIZABLE", "NONE"),
        ("SELECT 2", "tx-2"),
        ("ROLLBACK", "tx-2"),
    ]


async def test_unknown_isolation_level_is_rejected() -> None:
    async with connection_to(TransactionCoordinator()) as conn:
        with pytest.raises(exc.ArgumentError, match="SNAPSHOT"):
            _ = await conn.execution_options(isolation_level="SNAPSHOT")


async def test_setting_the_isolation_level_ends_an_open_transaction() -> None:
    coordinator = TransactionCoordinator()
    async with connection_to(coordinator) as conn:
        _ = await conn.execution_options(isolation_level="READ COMMITTED")
        _ = await conn.execute(text("SELECT 1"))
        dialect = conn.dialect
        raw = (await conn.get_raw_connection()).dbapi_connection
        assert raw is not None
        await conn.run_sync(lambda _: dialect.set_isolation_level(raw, "AUTOCOMMIT"))
        _ = await conn.execute(text("SELECT 2"))

    assert coordinator.statements() == [
        ("START TRANSACTION ISOLATION LEVEL READ COMMITTED", "NONE"),
        ("SELECT 1", "tx-1"),
        ("ROLLBACK", "tx-1"),
        ("SELECT 2", None),
    ]
