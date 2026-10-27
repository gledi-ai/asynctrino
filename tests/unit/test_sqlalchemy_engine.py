from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager
from importlib.metadata import version

import httpx2
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine, create_async_engine

from tests.unit.coordinator import FakeCoordinator, column, statement_pages


@asynccontextmanager
async def engine_for(
    client: httpx2.AsyncClient,
    url: str = "asynctrino://alice@coordinator:8080/tpch/tiny",
    **kwargs: object,
) -> AsyncGenerator[AsyncEngine]:
    engine = create_async_engine(url, connect_args={"http_client": client}, **kwargs)
    try:
        yield engine
    finally:
        await engine.dispose()


def one_row_coordinator() -> FakeCoordinator:
    return FakeCoordinator(
        statement_pages(columns=[column("_col0", "integer")], batches=[[[1]]], queued_pages=0)
    )


async def test_text_query_runs_through_the_async_engine() -> None:
    coordinator = one_row_coordinator()
    async with coordinator.client() as client, engine_for(client) as engine:
        async with engine.connect() as conn:
            result = await conn.execute(text("SELECT 1"))
            rows = result.all()

        assert engine.dialect.name == "trino"
        assert engine.dialect.driver == "asynctrino"

    assert [tuple(row) for row in rows] == [(1,)]
    [post] = [r for r in coordinator.requests if r.method == "POST"]
    assert post.content == b"SELECT 1"
    assert post.headers["X-Trino-User"] == "alice"
    assert post.headers["X-Trino-Catalog"] == "tpch"
    assert post.headers["X-Trino-Schema"] == "tiny"


def test_dbapi_version_is_the_installed_asynctrino_release() -> None:
    engine = create_async_engine("asynctrino://alice@coordinator:8080")

    release = ".".join(map(str, engine.dialect.dbapi_version[:3]))

    assert version("asynctrino").startswith(release)
