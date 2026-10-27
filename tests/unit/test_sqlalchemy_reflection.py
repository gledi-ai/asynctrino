"""Reflection through the async engine, against a scripted Coordinator."""

from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager

import pytest
from sqlalchemy import Connection, exc, inspect
from sqlalchemy.engine.interfaces import ReflectedIndex
from sqlalchemy.ext.asyncio import AsyncConnection, create_async_engine

from tests.unit.coordinator import JSON, CatalogCoordinator, column, statement_pages

TABLES = '"information_schema"."tables"'


def result(query_id: str, columns: list[JSON], rows: list[list[object]]) -> list[JSON]:
    """A query whose first page carries ``rows`` and whose final page is never needed."""
    return statement_pages(columns=columns, batches=[rows], query_id=query_id, queued_pages=0)


def table_exists(name: str) -> list[JSON]:
    return result("tables", [column("table_name", "varchar")], [[name]])


def row_column(name: str, field: str) -> JSON:
    """A ``row(<field> integer)`` column."""
    integer = {"rawType": "integer", "arguments": []}
    named = {"fieldName": {"name": field}, "typeSignature": integer}
    signature = {"rawType": "row", "arguments": [{"kind": "NAMED_TYPE", "value": named}]}
    return {"name": name, "type": f"row({field} integer)", "typeSignature": signature}


@asynccontextmanager
async def connection_to(
    coordinator: CatalogCoordinator, url: str = "asynctrino://alice@coordinator:8080/hive/web"
) -> AsyncGenerator[AsyncConnection]:
    async with coordinator.client() as client:
        engine = create_async_engine(url, connect_args={"http_client": client})
        try:
            async with engine.connect() as conn:
                yield conn
        finally:
            await engine.dispose()


async def indexes_of(coordinator: CatalogCoordinator, table: str) -> list[ReflectedIndex]:
    def get_indexes(conn: Connection) -> list[ReflectedIndex]:
        return inspect(conn).get_indexes(table)

    async with connection_to(coordinator) as conn:
        return await conn.run_sync(get_indexes)


def cancelled(coordinator: CatalogCoordinator, query: list[JSON]) -> bool:
    next_uri = query[0]["nextUri"]
    sent = {(r.method, str(r.url)) for r in coordinator.requests}
    return ("DELETE", next_uri) in sent and ("GET", next_uri) not in sent


async def test_hive_partition_columns_reflect_as_one_index_and_the_query_is_cancelled() -> None:
    partitions = result(
        "partitions",
        [column("name", "varchar"), column("region", "varchar")],
        [["alice", "us-east"]],
    )
    coordinator = CatalogCoordinator(
        {TABLES: table_exists("visits"), '"visits$partitions"': partitions}
    )

    indexes = await indexes_of(coordinator, "visits")

    assert indexes == [{"name": "partition", "column_names": ["name", "region"], "unique": False}]
    assert cancelled(coordinator, partitions)


def failed(query_id: str, error_name: str) -> list[JSON]:
    error = {"message": "failed", "errorName": error_name, "errorType": "USER_ERROR"}
    return [{"id": query_id, "stats": {"state": "FAILED"}, "error": error}]


async def test_iceberg_partitions_table_reflects_no_index_and_the_query_is_cancelled() -> None:
    partitions = result(
        "partitions",
        [
            row_column("partition", "year"),
            column("record_count", "bigint"),
            column("file_count", "bigint"),
            column("total_size", "bigint"),
            row_column("data", "year"),
        ],
        [[[2023], 1, 1, 400, [2023]]],
    )
    coordinator = CatalogCoordinator(
        {TABLES: table_exists("events"), '"events$partitions"': partitions}
    )

    assert await indexes_of(coordinator, "events") == []
    assert cancelled(coordinator, partitions)


async def test_table_without_partitions_reflects_no_index() -> None:
    coordinator = CatalogCoordinator(
        {
            TABLES: table_exists("users"),
            '"users$partitions"': failed("partitions", "TABLE_NOT_FOUND"),
        }
    )

    assert await indexes_of(coordinator, "users") == []


async def test_partitions_are_read_from_the_connection_schema() -> None:
    coordinator = CatalogCoordinator({TABLES: table_exists("visits")})

    _ = await indexes_of(coordinator, "visits")

    assert coordinator.statements()[-1] == 'SELECT * FROM "web"."visits$partitions"'


async def test_indexes_of_a_missing_table_raise() -> None:
    with pytest.raises(exc.NoSuchTableError):
        _ = await indexes_of(CatalogCoordinator(), "missing")

