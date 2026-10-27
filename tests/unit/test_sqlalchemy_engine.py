from collections.abc import AsyncGenerator, Mapping
from contextlib import asynccontextmanager
from importlib.metadata import version
from types import MappingProxyType

import httpx2
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncConnection, AsyncEngine, create_async_engine

from asynctrino.auth import BasicAuthentication
from asynctrino.sqlalchemy import URL
from tests.unit.coordinator import FakeCoordinator, column, statement_pages


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


@asynccontextmanager
async def connection_to(coordinator: FakeCoordinator) -> AsyncGenerator[AsyncConnection]:
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
