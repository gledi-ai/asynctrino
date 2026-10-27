from collections.abc import AsyncGenerator, Mapping
from contextlib import asynccontextmanager
from dataclasses import dataclass, field
from decimal import Decimal
from importlib.metadata import version
from types import MappingProxyType
from typing import Protocol

import httpx2
import pytest
from sqlalchemy import Column, Double, Float, MetaData, Table, exc, literal, select, text
from sqlalchemy.ext.asyncio import AsyncConnection, AsyncEngine, create_async_engine

from asynctrino.auth import BasicAuthentication
from asynctrino.dbapi import Connection
from asynctrino.sqlalchemy import JSON, URL
from tests.unit.coordinator import (
    BASE_URL,
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


def one_row_coordinator(base_url: str = BASE_URL) -> FakeCoordinator:
    return FakeCoordinator(
        statement_pages(
            columns=[column("_col0", "integer")], batches=[[[1]]], queued_pages=0, base_url=base_url
        )
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


async def test_raw_connection_exposes_the_asynctrino_connection() -> None:
    async with connection_to(one_row_coordinator()) as conn:
        raw = await conn.get_raw_connection()

        assert isinstance(raw.driver_connection, Connection)


def test_dbapi_version_is_the_installed_asynctrino_release() -> None:
    engine = create_async_engine("asynctrino://alice@coordinator:8080")

    release = ".".join(map(str, engine.dialect.dbapi_version[:3]))

    assert version("asynctrino").startswith(release)


async def test_url_options_reach_the_coordinator() -> None:
    coordinator = one_row_coordinator("https://coordinator:8443")
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


async def test_url_encoding_selects_the_spooling_protocol() -> None:
    coordinator = one_row_coordinator()
    url = URL("coordinator", user="alice", encoding=["json+zstd", "json"])
    async with coordinator.client() as client, engine_for(client, url) as engine:
        _ = await query(engine, "SELECT 1")

    [post] = [r for r in coordinator.requests if r.method == "POST"]
    assert post.headers["X-Trino-Query-Data-Encoding"] == "json+zstd,json"


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


documents = Table("documents", MetaData(), Column("data", JSON))


@pytest.mark.parametrize(
    ("index", "path"),
    [
        ("a", """'$["a"]'"""),
        ('a"]', """'$["a\\"]"]'"""),
        ("a\\b", """'$["a\\\\b"]'"""),
        (2, "'$[2]'"),
        (("a", 1), """'$["a"][1]'"""),
    ],
    ids=repr,
)
async def test_json_paths_are_bound_with_keys_quoted(
    index: str | int | tuple[str | int, ...], path: str
) -> None:
    coordinator = one_row_coordinator()
    async with connection_to(coordinator) as conn:
        _ = await conn.execute(select(documents.c.data[index]))

    [post] = [r for r in coordinator.requests if r.method == "POST"]
    assert post.content.decode() == (
        "EXECUTE IMMEDIATE 'SELECT JSON_EXTRACT(documents.data, ?) AS anon_1 \n"
        f"FROM documents' USING {path}"
    )


async def test_json_path_with_an_unsupported_key_is_refused() -> None:
    coordinator = one_row_coordinator()
    async with connection_to(coordinator) as conn:
        with pytest.raises(exc.StatementError, match="JSON path element"):
            _ = await conn.execute(select(documents.c.data[1.5]))

    assert [r for r in coordinator.requests if r.method == "POST"] == []


async def test_values_bound_to_float_types_are_sent_as_doubles() -> None:
    coordinator = one_row_coordinator()
    async with connection_to(coordinator) as conn:
        _ = await conn.execute(
            select(
                literal(Decimal("1.25"), Float),
                literal(2, Double),
                literal(0.5),
                literal(None, Float),
            )
        )

    [post] = [r for r in coordinator.requests if r.method == "POST"]
    assert post.content.decode().endswith("USING DOUBLE '1.25',DOUBLE '2.0',DOUBLE '0.5',NULL")


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


@dataclass
class FlakyCoordinator:
    """Finishes every statement with one row, but refuses the next connection when told to."""

    refuse_next: bool = False
    statements: list[str] = field(default_factory=list[str])

    def handler(self, request: httpx2.Request) -> httpx2.Response:
        if request.method != "POST":
            return httpx2.Response(204)
        self.statements.append(request.content.decode())
        if self.refuse_next:
            self.refuse_next = False
            msg = "connection refused"
            raise httpx2.ConnectError(msg, request=request)
        [page] = statement_pages(columns=[column("_col0", "integer")], batches=[], queued_pages=0)
        page["data"] = [[1]]
        return httpx2.Response(200, json=page)

    def client(self) -> httpx2.AsyncClient:
        return httpx2.AsyncClient(transport=httpx2.MockTransport(self.handler))


async def test_pre_ping_replaces_a_connection_that_lost_the_coordinator() -> None:
    coordinator = FlakyCoordinator()
    no_retries: dict[str, object] = {"max_attempts": 1}
    async with (
        coordinator.client() as client,
        engine_for(client, connect_args=no_retries, pool_pre_ping=True) as engine,
    ):
        _ = await query(engine, "SELECT 'warm'")
        coordinator.refuse_next = True
        rows = await query(engine, "SELECT 'after'")

    assert rows == [(1,)]
    assert coordinator.statements == ["SELECT 'warm'", "SELECT 1", "SELECT 'after'"]


async def test_invalidating_a_connection_terminates_its_running_query() -> None:
    coordinator = paged_coordinator()
    async with connection_to(coordinator) as conn:
        result = await conn.stream(text("SELECT x"))
        _ = await result.fetchone()
        await conn.invalidate()

        assert conn.invalidated
    [delete] = [r for r in coordinator.requests if r.method == "DELETE"]
    assert delete.url.path.startswith("/v1/statement/executing/")
