"""Reflection through the async engine, against a scripted Coordinator."""

from collections.abc import AsyncGenerator, Callable
from contextlib import asynccontextmanager

import pytest
from sqlalchemy import Connection, exc, inspect, types
from sqlalchemy.engine.interfaces import ReflectedIndex
from sqlalchemy.engine.reflection import Inspector
from sqlalchemy.ext.asyncio import AsyncConnection, create_async_engine

from asynctrino.sqlalchemy import MAP, TrinoDialect
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


COLUMNS = '"information_schema"."columns"'


async def inspected[T](
    coordinator: CatalogCoordinator,
    reflect: Callable[[Inspector], T],
    url: str = "asynctrino://alice@coordinator:8080/hive/web",
) -> T:
    async with connection_to(coordinator, url) as conn:
        return await conn.run_sync(lambda sync_conn: reflect(inspect(sync_conn)))


async def test_columns_reflect_with_parsed_types_and_comments() -> None:
    columns = result(
        "columns",
        [
            column("column_name", "varchar"),
            column("data_type", "varchar"),
            column("column_default", "varchar"),
            column("is_nullable", "varchar"),
            column("comment", "varchar"),
        ],
        [
            ["id", "bigint", None, "NO", "the id"],
            ["tags", "map(varchar, array(integer))", None, "YES", None],
        ],
    )
    coordinator = CatalogCoordinator({TABLES: table_exists("visits"), COLUMNS: columns})

    [id_column, tags_column] = await inspected(coordinator, lambda i: i.get_columns("visits"))

    assert (id_column["name"], id_column["nullable"], id_column.get("comment")) == (
        "id",
        False,
        "the id",
    )
    assert isinstance(id_column["type"], types.BIGINT)
    assert (tags_column["name"], tags_column["nullable"], tags_column.get("comment")) == (
        "tags",
        True,
        None,
    )
    assert isinstance(tags_column["type"], MAP)
    assert isinstance(tags_column["type"].value_type, types.ARRAY)


async def test_columns_of_a_missing_table_raise() -> None:
    with pytest.raises(exc.NoSuchTableError):
        _ = await inspected(CatalogCoordinator(), lambda i: i.get_columns("missing"))


def names(query_id: str, *values: str) -> list[JSON]:
    return result(query_id, [column("name", "varchar")], [[value] for value in values])


async def test_schema_table_and_view_names_reflect() -> None:
    coordinator = CatalogCoordinator(
        {
            '"information_schema"."schemata"': names("schemata", "information_schema", "web"),
            "'BASE TABLE'": names("tables", "visits", "users"),
            "'VIEW'": names("views", "recent_visits"),
        }
    )

    reflected = await inspected(
        coordinator,
        lambda i: (i.get_schema_names(), i.get_table_names(), i.get_view_names("web")),
    )

    assert reflected == (["information_schema", "web"], ["visits", "users"], ["recent_visits"])
    assert all("'web'" in sql for sql in coordinator.statements()[1:])


def table_names(inspector: Inspector) -> object:
    return inspector.get_table_names()


def view_names(inspector: Inspector) -> object:
    return inspector.get_view_names()


def view_definition(inspector: Inspector) -> object:
    return inspector.get_view_definition("v")


def table_comment(inspector: Inspector) -> object:
    return inspector.get_table_comment("t")


@pytest.mark.parametrize(
    "reflect",
    [table_names, view_names, view_definition, table_comment],
    ids=lambda reflect: reflect.__name__,
)
async def test_names_in_the_default_schema_need_a_schema_on_the_connection(
    reflect: Callable[[Inspector], object],
) -> None:
    with pytest.raises(exc.NoSuchTableError, match="schema is required"):
        _ = await inspected(CatalogCoordinator(), reflect, "asynctrino://alice@coordinator/hive")


async def test_view_definition_reflects() -> None:
    definition = "SELECT * FROM visits WHERE day > current_date - INTERVAL '7' DAY"
    coordinator = CatalogCoordinator({'"information_schema"."views"': names("views", definition)})

    reflected = await inspected(coordinator, lambda i: i.get_view_definition("recent_visits"))

    assert reflected == definition


async def test_definition_of_a_missing_view_raises() -> None:
    with pytest.raises(exc.NoSuchTableError, match="missing"):
        _ = await inspected(CatalogCoordinator(), lambda i: i.get_view_definition("missing"))


async def test_has_table_and_has_schema_tell_whether_they_exist() -> None:
    coordinator = CatalogCoordinator(
        {
            TABLES: table_exists("visits"),
            "USING 'web'": names("schemata", "web"),
        }
    )

    reflected = await inspected(
        coordinator,
        lambda i: (
            i.has_table("visits"),
            i.has_schema("web"),
            i.has_schema("missing"),
        ),
    )

    assert reflected == (True, True, False)


async def test_has_table_is_false_without_a_schema() -> None:
    coordinator = CatalogCoordinator({TABLES: table_exists("visits")})

    reflected = await inspected(
        coordinator, lambda i: i.has_table("visits"), "asynctrino://alice@coordinator/hive"
    )

    assert reflected is False
    assert coordinator.statements() == []


async def test_catalog_names_reflect() -> None:
    coordinator = CatalogCoordinator({'"system"."jdbc"."catalogs"': names("c", "hive", "system")})

    async with connection_to(coordinator) as conn:
        dialect = conn.dialect
        assert isinstance(dialect, TrinoDialect)
        catalogs = await conn.run_sync(dialect.get_catalog_names)

    assert catalogs == ["hive", "system"]


async def test_tables_have_no_primary_key_or_other_constraints() -> None:
    coordinator = CatalogCoordinator()

    reflected = await inspected(
        coordinator,
        lambda i: (
            i.get_pk_constraint("visits"),
            i.get_foreign_keys("visits"),
            i.get_unique_constraints("visits"),
            i.get_check_constraints("visits"),
        ),
    )

    assert reflected == ({"name": None, "constrained_columns": []}, [], [], [])
    assert coordinator.statements() == []


TABLE_COMMENTS = '"system"."metadata"."table_comments"'


async def test_table_comment_reflects_from_the_connection_catalog() -> None:
    coordinator = CatalogCoordinator({TABLE_COMMENTS: names("comments", "page visits")})

    reflected = await inspected(coordinator, lambda i: i.get_table_comment("visits"))

    assert reflected == {"text": "page visits"}
    [sql] = coordinator.statements()
    assert "'hive'" in sql
    assert "'web'" in sql


async def test_table_comment_is_none_when_access_is_denied() -> None:
    coordinator = CatalogCoordinator({TABLE_COMMENTS: failed("comments", "PERMISSION_DENIED")})

    reflected = await inspected(coordinator, lambda i: i.get_table_comment("visits"))

    assert reflected == {"text": None}


async def test_table_comment_failures_other_than_denied_access_raise() -> None:
    coordinator = CatalogCoordinator({TABLE_COMMENTS: failed("comments", "CATALOG_NOT_FOUND")})

    with pytest.raises(exc.DBAPIError, match="CATALOG_NOT_FOUND"):
        _ = await inspected(coordinator, lambda i: i.get_table_comment("visits"))
