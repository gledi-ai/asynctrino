import math
import uuid
from collections.abc import AsyncIterator
from decimal import Decimal

import pytest
from sqlalchemy import (
    ARRAY,
    BIGINT,
    BINARY,
    CHAR,
    DECIMAL,
    REAL,
    VARCHAR,
    Boolean,
    Column,
    CreateTableAs,
    Double,
    Float,
    Integer,
    MetaData,
    String,
    Table,
    Uuid,
    and_,
    column,
    func,
    insert,
    literal,
    not_,
    or_,
    select,
    values,
)
from sqlalchemy.exc import SAWarning
from sqlalchemy.ext.asyncio import (
    AsyncConnection,
    AsyncEngine,
    async_sessionmaker,
    create_async_engine,
)
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column
from sqlalchemy.schema import CreateTable, DropTable

from asynctrino.sqlalchemy import JSON, MAP, ROW, URL
from asynctrino.sqlalchemy._types import SQLType

tpch = MetaData(schema="tiny")
nation = Table(
    "nation",
    tpch,
    Column("nationkey", BIGINT),
    Column("name", VARCHAR),
    Column("regionkey", BIGINT),
    Column("comment", VARCHAR),
)
customer = Table(
    "customer",
    tpch,
    Column("custkey", BIGINT),
    Column("name", VARCHAR),
    Column("nationkey", BIGINT),
    Column("acctbal", Double),
    Column("mktsegment", VARCHAR),
)
nodes = Table("nodes", MetaData(), Column("node_id", VARCHAR), Column("state", VARCHAR))


def engine_at(trino_address: tuple[str, int], catalog: str, schema: str) -> AsyncEngine:
    host, port = trino_address
    url = URL(host, port=port, user="test", catalog=catalog, schema=schema)
    return create_async_engine(url, connect_args={"max_attempts": 1})


@pytest.fixture
async def tpch_conn(trino_address: tuple[str, int]) -> AsyncIterator[AsyncConnection]:
    engine = engine_at(trino_address, "tpch", "tiny")
    try:
        async with engine.connect() as conn:
            yield conn
    finally:
        await engine.dispose()


@pytest.fixture
async def memory(trino_address: tuple[str, int]) -> AsyncIterator[AsyncEngine]:
    engine = engine_at(trino_address, "memory", "default")
    try:
        yield engine
    finally:
        await engine.dispose()


def unique(name: str) -> str:
    return f"{name}_{uuid.uuid4().hex}"


async def create(conn: AsyncConnection, table: Table) -> None:
    _ = await conn.execute(CreateTable(table))


async def drop(engine: AsyncEngine, table: Table) -> None:
    async with engine.connect() as conn:
        _ = await conn.execute(DropTable(table, if_exists=True))


async def test_select_query(tpch_conn: AsyncConnection) -> None:
    rows = (await tpch_conn.execute(select(nation))).all()

    assert len(rows) == 25
    for row in rows:
        assert isinstance(row.nationkey, int)
        assert isinstance(row.name, str)
        assert isinstance(row.regionkey, int)
        assert isinstance(row.comment, str)


async def test_select_specific_columns(trino_address: tuple[str, int]) -> None:
    engine = engine_at(trino_address, "system", "runtime")
    try:
        async with engine.connect() as conn:
            rows = (await conn.execute(select(nodes.c.node_id, nodes.c.state))).all()
    finally:
        await engine.dispose()

    assert len(rows) > 0
    assert all(isinstance(row.node_id, str) and row.state == "active" for row in rows)


async def test_limit_and_offset(tpch_conn: AsyncConnection) -> None:
    statement = select(nation.c.nationkey).order_by(nation.c.nationkey).limit(3).offset(2)

    rows = (await tpch_conn.execute(statement)).scalars().all()

    assert rows == [2, 3, 4]


async def test_operators(tpch_conn: AsyncConnection) -> None:
    rows = (await tpch_conn.execute(select(nation).where(nation.c.nationkey == 2))).all()

    assert [row.name for row in rows] == ["BRAZIL"]


async def test_conjunctions(tpch_conn: AsyncConnection) -> None:
    statement = select(customer).where(
        and_(
            customer.c.name.like("%12%"),
            customer.c.nationkey == 15,
            or_(customer.c.mktsegment == "AUTOMOBILE", customer.c.mktsegment == "HOUSEHOLD"),
            not_(customer.c.acctbal < 0),
        )
    )

    rows = (await tpch_conn.execute(statement)).all()

    assert len(rows) == 1


async def test_floats_are_bound_as_doubles(tpch_conn: AsyncConnection) -> None:
    statement = select(literal(0.1) + literal(Decimal("0.2"), Double))

    total = (await tpch_conn.execute(statement)).scalar_one()

    assert total == 0.1 + 0.2


async def test_float_literal_is_a_double(tpch_conn: AsyncConnection) -> None:
    statement = select(func.typeof(literal(1.5, literal_execute=True)))

    assert (await tpch_conn.execute(statement)).scalar_one() == "double"


async def test_alias(tpch_conn: AsyncConnection) -> None:
    nations1 = nation.alias("o1")
    nations2 = nation.alias("o2")
    statement = (
        select(nations1)
        .join(
            nations2,
            and_(
                nations1.c.regionkey == nations2.c.regionkey,
                nations1.c.nationkey != nations2.c.nationkey,
                nations1.c.regionkey == 1,
            ),
        )
        .distinct()
    )

    rows = (await tpch_conn.execute(statement)).all()

    assert len(rows) == 5


async def test_subquery(tpch_conn: AsyncConnection) -> None:
    in_debt = select(customer.c.nationkey).where(customer.c.acctbal < -900).subquery()
    statement = select(nation.c.name).where(nation.c.nationkey.in_(select(in_debt)))

    rows = (await tpch_conn.execute(statement)).all()

    assert len(rows) == 15


async def test_joins(tpch_conn: AsyncConnection) -> None:
    statement = (
        select(nation.c.name)
        .select_from(nation.join(customer, nation.c.nationkey == customer.c.nationkey))
        .where(customer.c.acctbal < -900)
        .distinct()
    )

    rows = (await tpch_conn.execute(statement)).all()

    assert len(rows) == 15


async def test_cte(tpch_conn: AsyncConnection) -> None:
    in_debt = select(customer.c.nationkey).where(customer.c.acctbal < -900).cte()
    statement = select(nation).where(nation.c.nationkey.in_(select(in_debt)))

    rows = (await tpch_conn.execute(statement)).all()

    assert len(rows) == 15


async def test_window_function_with_a_frame(tpch_conn: AsyncConnection) -> None:
    running = func.sum(nation.c.nationkey).over(order_by=nation.c.nationkey, rows=(-2, 0))
    statement = select(nation.c.nationkey, running).order_by(nation.c.nationkey).limit(4)

    rows = [tuple(row) for row in (await tpch_conn.execute(statement)).all()]

    assert rows == [(0, 0), (1, 1), (2, 3), (3, 6)]


async def test_window_functions_ignoring_nulls(tpch_conn: AsyncConnection) -> None:
    rows = values(column("i", Integer), column("x", Integer), name="v").data(
        [(1, 1), (2, None), (3, 3), (4, 4)]
    )
    statement = select(
        func.lag(rows.c.x, ignore_nulls=True).over(order_by=rows.c.i),
        func.lag(rows.c.x).over(order_by=rows.c.i),
        func.last_value(rows.c.x, ignore_nulls=True).over(order_by=rows.c.i),
    ).order_by(rows.c.i)

    result = [tuple(row) for row in (await tpch_conn.execute(statement)).all()]

    assert result == [(None, None, 1), (1, 1, 1), (1, None, 3), (3, 3, 4)]


async def test_define_insert_and_select(memory: AsyncEngine) -> None:
    users = Table(
        unique("users"),
        MetaData(),
        Column("id", Integer),
        Column("name", String),
        Column("fullname", String),
        Column("guid", Uuid),
    )
    guid = uuid.uuid4()
    try:
        async with memory.connect() as conn:
            await create(conn, users)
            _ = await conn.execute(
                insert(users), {"id": 2, "name": "wendy", "fullname": "Wendy W", "guid": guid}
            )
            rows = [tuple(row) for row in (await conn.execute(select(users))).all()]
    finally:
        await drop(memory, users)

    assert rows == [(2, "wendy", "Wendy W", guid)]


async def test_insert_multiple_rows(memory: AsyncEngine) -> None:
    users = Table(unique("users"), MetaData(), Column("id", Integer), Column("name", String))
    try:
        async with memory.connect() as conn:
            await create(conn, users)
            _ = await conn.execute(
                insert(users).values([{"id": 1, "name": "ann"}, {"id": 2, "name": "bo"}])
            )
            _ = await conn.execute(
                insert(users), [{"id": 3, "name": "cy"}, {"id": 4, "name": "di"}]
            )
            rows = (await conn.execute(select(users.c.name).order_by(users.c.id))).scalars().all()
    finally:
        await drop(memory, users)

    assert rows == ["ann", "bo", "cy", "di"]


async def test_create_table_as_select(memory: AsyncEngine) -> None:
    name = unique("nations")
    statement = CreateTableAs(
        select(literal(1).label("one"), literal(1.5).label("half")), name, schema="default"
    )
    try:
        async with memory.connect() as conn:
            _ = await conn.execute(statement)
            rows = [tuple(row) for row in (await conn.execute(select(statement.table))).all()]
    finally:
        await drop(memory, statement.table)

    assert rows == [(1, 1.5)]


async def test_create_table_as_with_table_properties(trino_address: tuple[str, int]) -> None:
    engine = engine_at(trino_address, "blackhole", "default")
    name = unique("generated")
    statement = CreateTableAs(
        select(literal(1).label("one")),
        name,
        schema="default",
        trino_split_count=1,
        trino_pages_per_split=2,
        trino_rows_per_page=3,
    )
    try:
        async with engine.connect() as conn:
            _ = await conn.execute(statement)
            count = (await conn.execute(select(func.count()).select_from(statement.table))).scalar()
    finally:
        await drop(engine, statement.table)
        await engine.dispose()

    assert count == 6


async def test_create_table_with_table_properties(trino_address: tuple[str, int]) -> None:
    engine = engine_at(trino_address, "memory", "default")
    generated = Table(
        unique("generated"),
        MetaData(),
        Column("x", Integer),
        schema="default",
        trino_catalog="blackhole",
        trino_split_count=2,
        trino_pages_per_split=1,
        trino_rows_per_page=5,
    )
    try:
        async with engine.connect() as conn:
            await create(conn, generated)
            count = (await conn.execute(select(func.count()).select_from(generated))).scalar()
    finally:
        await drop(engine, generated)
        await engine.dispose()

    assert count == 10


async def test_orm_round_trip(memory: AsyncEngine) -> None:
    class Base(DeclarativeBase):
        pass

    class Measurement(Base):
        __tablename__ = unique("measurements")

        id: Mapped[int] = mapped_column(primary_key=True, autoincrement=False)
        label: Mapped[str] = mapped_column()
        value: Mapped[float] = mapped_column(Double)
        tags: Mapped[dict[str, int]] = mapped_column(MAP(String, Integer))

    try:
        async with memory.connect() as conn:
            with pytest.warns(SAWarning, match="PRIMARY KEY"):
                await conn.run_sync(Base.metadata.create_all, checkfirst=False)
        sessions = async_sessionmaker(memory, expire_on_commit=False)
        async with sessions() as session:
            session.add_all(
                [
                    Measurement(id=1, label="a", value=0.5, tags={"x": 1}),
                    Measurement(id=2, label="b", value=2, tags={}),
                ]
            )
            await session.commit()
        async with sessions() as session:
            found = (
                await session.scalars(
                    select(Measurement).where(Measurement.value > 1.0).order_by(Measurement.id)
                )
            ).all()
    finally:
        async with memory.connect() as conn:
            await conn.run_sync(Base.metadata.drop_all, checkfirst=False)

    assert [(m.id, m.label, m.value, m.tags) for m in found] == [(2, "b", 2.0, {})]


@pytest.mark.parametrize("document", [None, 1, "test", [1, "test"], {"test": 1}], ids=repr)
async def test_json_column(memory: AsyncEngine, document: object) -> None:
    documents = Table(unique("documents"), MetaData(), Column("id", Integer), Column("data", JSON))
    try:
        async with memory.connect() as conn:
            await create(conn, documents)
            _ = await conn.execute(insert(documents), {"id": 1, "data": document})
            rows = [tuple(row) for row in (await conn.execute(select(documents))).all()]
    finally:
        await drop(memory, documents)

    assert rows == [(1, document)]


async def test_json_column_operations(memory: AsyncEngine) -> None:
    document = {
        "a": {"c": 1},
        "b": 2,
        "list": [10, {"z": 20}],
        "foo-bar": {"z": 200},
        'quo"te': 3,
        "back\\slash": 4,
        'x"]': 5,
    }
    documents = Table(unique("documents"), MetaData(), Column("data", JSON))
    try:
        async with memory.connect() as conn:
            await create(conn, documents)
            _ = await conn.execute(insert(documents), {"data": document})

            async def extract(index: str | int | tuple[str | int, ...]) -> object:
                return (await conn.execute(select(documents.c.data[index]))).scalar_one()

            assert await extract(("a", "c")) == 1
            assert await extract(("foo-bar", "z")) == 200
            assert await extract(("list", 1, "z")) == 20
            assert await extract("b") == 2
            assert await extract("foo-bar") == {"z": 200}
            assert await extract('quo"te') == 3
            assert await extract("back\\slash") == 4
            assert await extract('x"]') == 5
            assert await extract("missing") is None
    finally:
        await drop(memory, documents)


@pytest.mark.parametrize(
    ("value", "type_"),
    [
        (None, MAP(String, Integer)),
        ({}, MAP(String, Integer)),
        ({True: False, False: True}, MAP(Boolean, Boolean)),
        ({1: 1, 2: None}, MAP(Integer, Integer)),
        ({1.4: 1.4, math.inf: math.inf}, MAP(Float(precision=53), Float(precision=53))),
        ({1.5: 1.5, math.inf: math.inf}, MAP(REAL, REAL)),
        ({Decimal("1.2"): Decimal("1.2")}, MAP(DECIMAL(2, 1), DECIMAL(2, 1))),
        ({"hello": "world"}, MAP(String, String)),
        ({"a   ": "a", "null": "n"}, MAP(CHAR(4), CHAR(1))),
        ({b"": b"eh?", b"\x00": None}, MAP(BINARY, BINARY)),
        (None, ARRAY(String)),
        ([], ARRAY(String)),
        ([True, False, True], ARRAY(Boolean)),
        ([1, 2, None], ARRAY(Integer)),
        ([1.4, 2.3, math.inf], ARRAY(Double)),
        ([Decimal("1.2"), Decimal("2.3")], ARRAY(DECIMAL(2, 1))),
        (["a   ", "null"], ARRAY(CHAR(4))),
        ([b"eh?", None, b"\x00"], ARRAY(BINARY)),
        ([[1, 2], [3]], ARRAY(Integer, dimensions=2)),
        (None, ROW([("field1", String), ("field2", String)])),
        (("hello", "world"), ROW([("field1", String), ("field2", String)])),
        ((True, False), ROW([("field1", Boolean), ("field2", Boolean)])),
        ((1, 2), ROW([("field1", Integer), ("field2", Integer)])),
        ((1.4, math.inf), ROW([("field1", Double), ("field2", Double)])),
        ((Decimal("1.2"), Decimal("2.3")), ROW([("f1", DECIMAL(2, 1)), ("f2", DECIMAL(3, 1))])),
        (("a   ", "null"), ROW([("field1", CHAR(4)), ("field2", CHAR(4))])),
        ((b"eh?", b"oh?"), ROW([("field1", BINARY), ("field2", BINARY)])),
        ((1, "x"), ROW([("first name", Integer), ("select", String)])),
    ],
    ids=repr,
)
async def test_composite_column_round_trip(
    memory: AsyncEngine, value: object, type_: SQLType
) -> None:
    values = Table(unique("composites"), MetaData(), Column("id", Integer), Column("v", type_))
    try:
        async with memory.connect() as conn:
            await create(conn, values)
            _ = await conn.execute(insert(values), {"id": 1, "v": value})
            rows = [tuple(row) for row in (await conn.execute(select(values))).all()]
    finally:
        await drop(memory, values)

    assert rows == [(1, value)]
