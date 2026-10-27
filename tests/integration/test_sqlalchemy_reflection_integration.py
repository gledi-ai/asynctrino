import asyncio
import uuid
from collections.abc import AsyncGenerator, AsyncIterator, Callable
from contextlib import asynccontextmanager
from dataclasses import dataclass

import pytest
from sqlalchemy import (
    ARRAY,
    BIGINT,
    BOOLEAN,
    DATE,
    DECIMAL,
    INTEGER,
    VARBINARY,
    VARCHAR,
    Column,
    MetaData,
    Table,
    exc,
    inspect,
    text,
)
from sqlalchemy.engine.reflection import Inspector
from sqlalchemy.ext.asyncio import AsyncEngine, create_async_engine
from sqlalchemy.schema import CreateSchema, DropSchema

from asynctrino.dbapi import connect
from asynctrino.sqlalchemy import DOUBLE, MAP, ROW, TIMESTAMP, URL, TrinoDialect

TERMINAL_STATES = frozenset({"FINISHED", "FAILED"})


def engine_at(trino_address: tuple[str, int], catalog: str, schema: str | None) -> AsyncEngine:
    host, port = trino_address
    url = URL(host, port=port, user="test", catalog=catalog, schema=schema)
    return create_async_engine(url, connect_args={"max_attempts": 1})


@dataclass
class Schema:
    """A fresh schema, and an engine whose connections use it."""

    engine: AsyncEngine
    name: str

    async def run(self, sql: str) -> None:
        async with self.engine.connect() as conn:
            _ = await conn.execute(text(sql))

    async def inspected[T](self, reflect: Callable[[Inspector], T]) -> T:
        async with self.engine.connect() as conn:
            return await conn.run_sync(lambda sync_conn: reflect(inspect(sync_conn)))


@asynccontextmanager
async def schema_in(trino_address: tuple[str, int], catalog: str) -> AsyncGenerator[Schema]:
    """Create a uniquely named schema in ``catalog``, and drop it with its tables on exit."""
    name = f"reflection_{uuid.uuid4().hex}"
    schema = Schema(engine_at(trino_address, catalog, name), name)
    admin = engine_at(trino_address, catalog, None)
    try:
        async with admin.connect() as conn:
            _ = await conn.execute(CreateSchema(name))
        yield schema
    finally:
        async with admin.connect() as conn:
            _ = await conn.execute(DropSchema(name, cascade=True, if_exists=True))
        await schema.engine.dispose()
        await admin.dispose()


@pytest.fixture
async def memory(trino_address: tuple[str, int]) -> AsyncIterator[Schema]:
    async with schema_in(trino_address, "memory") as schema:
        yield schema


async def test_catalog_names_include_the_configured_catalogs(
    trino_address: tuple[str, int],
) -> None:
    engine = engine_at(trino_address, "system", None)
    try:
        async with engine.connect() as conn:
            dialect = conn.dialect
            assert isinstance(dialect, TrinoDialect)
            catalogs = await conn.run_sync(dialect.get_catalog_names)
    finally:
        await engine.dispose()

    assert {"hive", "iceberg", "memory", "system", "tpch"} <= set(catalogs)


async def test_schema_names_and_has_schema(memory: Schema) -> None:
    schemas, exists, missing = await memory.inspected(
        lambda i: (i.get_schema_names(), i.has_schema(memory.name), i.has_schema("missing"))
    )

    assert memory.name in schemas
    assert "information_schema" in schemas
    assert (exists, missing) == (True, False)


@pytest.mark.parametrize("source", ["connection", "given"])
async def test_table_and_view_names_are_listed_apart(memory: Schema, source: str) -> None:
    await memory.run("CREATE TABLE visits (id integer)")
    await memory.run("CREATE VIEW recent_visits AS SELECT * FROM visits")
    schema = memory.name if source == "given" else None

    tables, views = await memory.inspected(
        lambda i: (i.get_table_names(schema), i.get_view_names(schema))
    )

    assert (tables, views) == (["visits"], ["recent_visits"])


async def test_table_and_view_names_need_a_schema(trino_address: tuple[str, int]) -> None:
    engine = engine_at(trino_address, "memory", None)
    try:
        async with engine.connect() as conn:
            with pytest.raises(exc.NoSuchTableError):
                _ = await conn.run_sync(lambda c: inspect(c).get_table_names())
            with pytest.raises(exc.NoSuchTableError):
                _ = await conn.run_sync(lambda c: inspect(c).get_view_names())
    finally:
        await engine.dispose()


async def test_view_definition_and_has_table(memory: Schema) -> None:
    await memory.run("CREATE TABLE visits (id integer)")
    await memory.run("CREATE VIEW recent_visits AS SELECT id FROM visits WHERE id > 10")

    definition, has_visits, has_missing = await memory.inspected(
        lambda i: (
            i.get_view_definition("recent_visits"),
            i.has_table("visits"),
            i.has_table("missing"),
        )
    )

    assert "WHERE (id > 10)" in definition
    assert (has_visits, has_missing) == (True, False)


async def test_definition_of_a_missing_view_raises(memory: Schema) -> None:
    with pytest.raises(exc.NoSuchTableError):
        _ = await memory.inspected(lambda i: i.get_view_definition("missing"))


async def test_columns_reflect_with_types_and_comments(memory: Schema) -> None:
    await memory.run(
        "CREATE TABLE visits ("
        "id bigint NOT NULL COMMENT 'the id', "
        "path varchar(200), "
        "tags map(varchar, array(integer)), "
        "client row(ip varchar, port integer), "
        "at timestamp(6) with time zone"
        ")"
    )

    columns = await memory.inspected(lambda i: i.get_columns("visits"))

    by_name = {c["name"]: c for c in columns}
    assert list(by_name) == ["id", "path", "tags", "client", "at"]
    assert (by_name["id"].get("comment"), by_name["id"]["nullable"]) == ("the id", False)
    assert (by_name["path"].get("comment"), by_name["path"]["nullable"]) == (None, True)
    assert isinstance(by_name["id"]["type"], BIGINT)
    path_type = by_name["path"]["type"]
    assert isinstance(path_type, VARCHAR)
    assert path_type.length == 200
    tags_type = by_name["tags"]["type"]
    assert isinstance(tags_type, MAP)
    assert isinstance(tags_type.value_type, ARRAY)
    client_type = by_name["client"]["type"]
    assert isinstance(client_type, ROW)
    assert [name for name, _ in client_type.attr_types] == ["ip", "port"]
    at_type = by_name["at"]["type"]
    assert isinstance(at_type, TIMESTAMP)
    assert (at_type.precision, at_type.timezone) == (6, True)


async def test_columns_of_a_missing_table_raise(memory: Schema) -> None:
    with pytest.raises(exc.NoSuchTableError):
        _ = await memory.inspected(lambda i: i.get_columns("missing"))


async def test_tables_have_no_primary_key(memory: Schema) -> None:
    await memory.run("CREATE TABLE visits (id integer)")

    constraint = await memory.inspected(lambda i: i.get_pk_constraint("visits"))

    assert constraint == {"name": None, "constrained_columns": []}


async def test_table_comment(memory: Schema) -> None:
    await memory.run("CREATE TABLE visits (id integer) COMMENT 'page visits'")
    await memory.run("CREATE TABLE users (id integer)")

    comments = await memory.inspected(
        lambda i: (i.get_table_comment("visits"), i.get_table_comment("users"))
    )

    assert comments == ({"text": "page visits"}, {"text": None})


async def test_metadata_reflect_round_trips_tables_created_through_the_dialect(
    memory: Schema,
) -> None:
    created = MetaData()
    _ = Table(
        "orders",
        created,
        Column("id", BIGINT, nullable=False, comment="order id"),
        Column("customer", VARCHAR(100)),
        Column("total", DECIMAL(12, 2)),
        Column("weight", DOUBLE),
        Column("paid", BOOLEAN),
        Column("placed", DATE),
        Column("receipt", VARBINARY),
        Column("lines", ARRAY(INTEGER)),
        Column("attributes", MAP(VARCHAR, VARCHAR)),
        comment="customer orders",
    )
    async with memory.engine.connect() as conn:
        await conn.run_sync(created.create_all)

    reflected = MetaData()
    async with memory.engine.connect() as conn:
        await conn.run_sync(lambda c: reflected.reflect(c, schema=memory.name))

    orders = reflected.tables[f"{memory.name}.orders"]
    assert orders.comment == "customer orders"
    assert [c.name for c in orders.columns] == [c.name for c in created.tables["orders"].columns]
    compiled = {c.name: c.type.compile(dialect=TrinoDialect()) for c in orders.columns}
    assert compiled == {
        "id": "BIGINT",
        "customer": "VARCHAR(100)",
        "total": "DECIMAL(12, 2)",
        "weight": "DOUBLE",
        "paid": "BOOLEAN",
        "placed": "DATE",
        "receipt": "VARBINARY",
        "lines": "ARRAY(INTEGER)",
        "attributes": "MAP(VARCHAR, VARCHAR)",
    }
    assert orders.c.id.nullable is False
    assert orders.c.id.comment == "order id"


async def assert_no_lingering_query(trino_address: tuple[str, int], fragment: str) -> None:
    """Wait for the query containing ``fragment`` to end, as cancellation does asynchronously."""
    host, port = trino_address
    state: object = None
    async with connect(host=host, port=port, user="test") as conn:
        cursor = conn.cursor()
        for _ in range(20):
            _ = await cursor.execute(
                "SELECT state FROM system.runtime.queries"
                " WHERE query LIKE ? AND query NOT LIKE '%system.runtime.queries%'"
                " ORDER BY created DESC LIMIT 1",
                [f"%{fragment}%"],
            )
            rows = await cursor.fetchall()
            assert len(rows) == 1, f"no query ran with {fragment!r}"
            state = rows[0][0]
            if state in TERMINAL_STATES:
                return
            await asyncio.sleep(0.5)
    pytest.fail(f"the query with {fragment!r} is still {state}")


async def test_hive_partition_columns_reflect_as_an_index(trino_address: tuple[str, int]) -> None:
    async with schema_in(trino_address, "hive") as hive:
        await hive.run(
            "CREATE TABLE visits (id integer, name varchar, region varchar)"
            " WITH (partitioned_by = ARRAY['name', 'region'])"
        )
        await hive.run("INSERT INTO visits VALUES (1, 'alice', 'us-east'), (2, 'bob', 'eu')")

        indexes = await hive.inspected(lambda i: i.get_indexes("visits"))

        assert indexes == [
            {"name": "partition", "column_names": ["name", "region"], "unique": False}
        ]
        await assert_no_lingering_query(trino_address, f'{hive.name}"."visits$partitions')


async def test_unpartitioned_hive_table_reflects_no_index(trino_address: tuple[str, int]) -> None:
    async with schema_in(trino_address, "hive") as hive:
        await hive.run("CREATE TABLE visits (id integer)")

        assert await hive.inspected(lambda i: i.get_indexes("visits")) == []


async def test_iceberg_partitioned_table_reflects_no_index(
    trino_address: tuple[str, int],
) -> None:
    async with schema_in(trino_address, "iceberg") as iceberg:
        await iceberg.run(
            "CREATE TABLE events (id integer, year integer) WITH (partitioning = ARRAY['year'])"
        )
        await iceberg.run("INSERT INTO events VALUES (1, 2023), (2, 2024)")

        indexes = await iceberg.inspected(lambda i: i.get_indexes("events"))

        assert indexes == []
        await assert_no_lingering_query(trino_address, f'{iceberg.name}"."events$partitions')
