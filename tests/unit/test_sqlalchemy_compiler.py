"""SQLAlchemy statements compiled to Trino SQL, with no I/O."""

import pytest
from sqlalchemy import (
    ARRAY,
    BINARY,
    BLOB,
    NCHAR,
    NVARCHAR,
    Column,
    DateTime,
    Double,
    Float,
    ForeignKey,
    Integer,
    LargeBinary,
    MetaData,
    Numeric,
    String,
    Table,
    Text,
    Time,
    UniqueConstraint,
    Uuid,
    cast,
    func,
    insert,
    literal,
    select,
    try_cast,
    types,
)
from sqlalchemy.exc import CompileError, SAWarning
from sqlalchemy.schema import CreateTable
from sqlalchemy.sql import column, table
from sqlalchemy.sql.elements import Over

from asynctrino.sqlalchemy import (
    DOUBLE,
    JSON,
    MAP,
    ROW,
    TIME,
    TIMESTAMP,
    VARBINARY,
    TrinoDialect,
)
from asynctrino.sqlalchemy._types import SQLType

metadata = MetaData()
table_without_catalog = Table("table", metadata, Column("id", Integer), Column("name", String))
table_with_catalog = Table(
    "table", metadata, Column("id", Integer), schema="default", trino_catalog="other"
)


@pytest.fixture
def dialect() -> TrinoDialect:
    return TrinoDialect()


def test_limit_offset(dialect: TrinoDialect) -> None:
    statement = select(table_without_catalog).limit(10).offset(0)

    query = statement.compile(dialect=dialect)

    assert str(query) == (
        'SELECT "table".id, "table".name \nFROM "table"\nOFFSET :param_1\nLIMIT :param_2'
    )


def test_limit(dialect: TrinoDialect) -> None:
    statement = select(table_without_catalog).limit(10)

    query = statement.compile(dialect=dialect)

    assert str(query) == 'SELECT "table".id, "table".name \nFROM "table"\nLIMIT :param_1'


def test_offset(dialect: TrinoDialect) -> None:
    statement = select(table_without_catalog).offset(0)

    query = statement.compile(dialect=dialect)

    assert str(query) == 'SELECT "table".id, "table".name \nFROM "table"\nOFFSET :param_1'


def test_cte_insert_order(dialect: TrinoDialect) -> None:
    cte = select(table_without_catalog).cte("cte")
    statement = insert(table_without_catalog).from_select(list(table_without_catalog.c), cte)

    query = statement.compile(dialect=dialect)

    assert str(query) == (
        'INSERT INTO "table" (id, name) WITH cte AS \n'
        '(SELECT "table".id AS id, "table".name AS name \n'
        'FROM "table")\n'
        " SELECT cte.id, cte.name \n"
        "FROM cte"
    )


def test_table_clause(dialect: TrinoDialect) -> None:
    statement = select(table("user", column("id"), column("name"), column("description")))

    query = statement.compile(dialect=dialect)

    assert str(query) == "SELECT user.id, user.name, user.description \nFROM user"


def test_catalogs_argument(dialect: TrinoDialect) -> None:
    statement = select(table_with_catalog)

    query = statement.compile(dialect=dialect)

    assert str(query) == 'SELECT default."table".id \nFROM "other".default."table"'


def test_catalogs_create_table(dialect: TrinoDialect) -> None:
    query = CreateTable(table_with_catalog).compile(dialect=dialect)

    assert str(query) == '\nCREATE TABLE "other".default."table" (\n\tid INTEGER\n)\n\n'


@pytest.mark.parametrize("function", ["first_value", "last_value", "nth_value", "lead", "lag"])
def test_ignore_nulls(dialect: TrinoDialect, function: str) -> None:
    element = getattr(func, function)
    statement = select(
        element(table_without_catalog.c.id, ignore_nulls=True)
        .over(partition_by=table_without_catalog.c.name)
        .label("window")
    )

    query = statement.compile(dialect=dialect)

    assert str(query) == (
        f'SELECT {function}("table".id) IGNORE NULLS OVER (PARTITION BY "table".name) AS window '
        '\nFROM "table"'
    )


@pytest.mark.parametrize("function", ["first_value", "last_value", "nth_value", "lead", "lag"])
def test_respect_nulls(dialect: TrinoDialect, function: str) -> None:
    element = getattr(func, function)
    statement = select(
        element(table_without_catalog.c.id, ignore_nulls=False)
        .over(partition_by=table_without_catalog.c.name)
        .label("window")
    )

    query = statement.compile(dialect=dialect)

    assert str(query) == (
        f'SELECT {function}("table".id) OVER (PARTITION BY "table".name) AS window \nFROM "table"'
    )


def test_try_cast(dialect: TrinoDialect) -> None:
    statement = select(try_cast(table_without_catalog.c.id, String))

    query = statement.compile(dialect=dialect)

    assert str(query) == 'SELECT try_cast("table".id as VARCHAR) AS id \nFROM "table"'


documents = Table("documents", MetaData(), Column("data", JSON))


def test_json_index_is_extracted_with_a_bound_path(dialect: TrinoDialect) -> None:
    query = select(documents.c.data["a"]).compile(dialect=dialect)

    assert str(query) == "SELECT JSON_EXTRACT(documents.data, :data_1) AS anon_1 \nFROM documents"


@pytest.mark.parametrize(
    ("index", "expected"),
    [
        ("a", """'$["a"]'"""),
        ('a"]', """'$["a\\"]"]'"""),
        ("a\\b", """'$["a\\\\b"]'"""),
        ("it's", """'$["it''s"]'"""),
        (2, "'$[2]'"),
        (("a", 1, "b"), """'$["a"][1]["b"]'"""),
    ],
    ids=repr,
)
def test_json_paths_quote_keys_and_index_arrays(
    dialect: TrinoDialect, index: str | int | tuple[str | int, ...], expected: str
) -> None:
    query = select(documents.c.data[index]).compile(
        dialect=dialect, compile_kwargs={"literal_binds": True}
    )

    assert (
        str(query) == f"SELECT JSON_EXTRACT(documents.data, {expected}) AS anon_1 \nFROM documents"
    )


@pytest.mark.parametrize("index", [True, 1.5, ("a", None)], ids=repr)
def test_json_path_rejects_other_keys(dialect: TrinoDialect, index: object) -> None:
    with pytest.raises(CompileError) as raised:
        _ = select(documents.c.data[index]).compile(
            dialect=dialect, compile_kwargs={"literal_binds": True}
        )

    assert isinstance(raised.value.__cause__, TypeError)
    assert "JSON path" in str(raised.value.__cause__)


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        (b"", "X''"),
        (b"hello", "X'68656c6c6f'"),
        (b"\xca\xfe\xba\xbe", "X'cafebabe'"),
        (b"\xff\xfe\x00\x01", "X'fffe0001'"),
        (bytearray(b"abc"), "X'616263'"),
        (memoryview(b"abc"), "X'616263'"),
    ],
)
@pytest.mark.parametrize("type_", [VARBINARY(), types.VARBINARY(), LargeBinary()])
def test_varbinary_literal_binds(
    dialect: TrinoDialect, value: bytes, expected: str, type_: SQLType
) -> None:
    expression = literal(value, type_=type_)

    query = expression.compile(dialect=dialect, compile_kwargs={"literal_binds": True})

    assert str(query) == expected


def test_varbinary_literal_binds_rejects_non_bytes(dialect: TrinoDialect) -> None:
    expression = literal("not-bytes", type_=VARBINARY())

    with pytest.raises(CompileError):
        _ = expression.compile(dialect=dialect, compile_kwargs={"literal_binds": True})


@pytest.mark.parametrize("type_", [MAP(String, Integer), ROW([("a", Integer)])])
def test_composite_types_support_comparison(dialect: TrinoDialect, type_: SQLType) -> None:
    left = column("l", type_)
    right = column("r", type_)

    assert str(left.is_(None).compile(dialect=dialect)) == "l IS NULL"
    assert str((left == right).compile(dialect=dialect)) == "l = r"
    assert str(left.in_([right]).compile(dialect=dialect)) == "l IN (r)"


def test_varbinary_create_table(dialect: TrinoDialect) -> None:
    table_with_binary = Table("table_with_binary", MetaData(), Column("data", LargeBinary))

    query = CreateTable(table_with_binary).compile(dialect=dialect)

    assert "data VARBINARY" in str(query)


@pytest.mark.parametrize(
    ("type_", "expected"),
    [
        (Float(), "REAL"),
        (Float(precision=32), "REAL"),
        (Float(precision=53), "DOUBLE"),
        (Double(), "DOUBLE"),
        (DOUBLE(), "DOUBLE"),
        (Numeric(10, 2), "DECIMAL(10, 2)"),
        (Text(), "VARCHAR"),
        (NVARCHAR(10), "VARCHAR(10)"),
        (NCHAR(3), "CHAR(3)"),
        (BLOB(), "VARBINARY"),
        (BINARY(), "VARBINARY"),
        (DateTime(), "TIMESTAMP"),
        (DateTime(timezone=True), "TIMESTAMP WITH TIME ZONE"),
        (TIMESTAMP(6), "TIMESTAMP(6)"),
        (TIMESTAMP(3, timezone=True), "TIMESTAMP(3) WITH TIME ZONE"),
        (Time(), "TIME"),
        (TIME(9), "TIME(9)"),
        (TIME(0, timezone=True), "TIME(0) WITH TIME ZONE"),
        (JSON(), "JSON"),
        (Uuid(), "UUID"),
        (LargeBinary(16), "VARBINARY"),
        (MAP(String, Float), "MAP(VARCHAR, REAL)"),
        (ARRAY(Double), "ARRAY(DOUBLE)"),
        (ARRAY(Integer, dimensions=2), "ARRAY(ARRAY(INTEGER))"),
        (ROW([("a", Integer), ("b", ARRAY(String))]), "ROW(a INTEGER, b ARRAY(VARCHAR))"),
    ],
    ids=repr,
)
def test_cast_to_trino_type(dialect: TrinoDialect, type_: SQLType, expected: str) -> None:
    query = cast(column("x"), type_).compile(dialect=dialect)

    assert str(query) == f"CAST(x AS {expected})"


@pytest.mark.parametrize("type_", [Float(precision=65), TIMESTAMP(13), TIME(-1)], ids=repr)
def test_type_out_of_range_is_rejected(dialect: TrinoDialect, type_: SQLType) -> None:
    with pytest.raises(ValueError, match="precision"):
        _ = cast(column("x"), type_).compile(dialect=dialect)


def test_create_table_leaves_out_constraints_with_a_warning(dialect: TrinoDialect) -> None:
    parents = Table("parents", MetaData(), Column("id", Integer, primary_key=True))
    children = Table(
        "children",
        parents.metadata,
        Column("id", Integer, primary_key=True),
        Column("parent_id", Integer, ForeignKey("parents.id")),
        UniqueConstraint("parent_id"),
    )

    with pytest.warns(SAWarning) as warnings:
        query = CreateTable(children).compile(dialect=dialect)

    assert str(query) == (
        "\nCREATE TABLE children (\n\tid INTEGER NOT NULL, \n\tparent_id INTEGER\n)\n\n"
    )
    assert sorted(str(warning.message) for warning in warnings) == [
        f"Trino does not support {kind} constraints. Constraint will be ignored."
        for kind in ("FOREIGN KEY", "PRIMARY KEY", "UNIQUE")
    ]


summed = func.sum(table_without_catalog.c.id)
by_id = table_without_catalog.c.id


@pytest.mark.parametrize(
    ("window", "expected"),
    [
        (summed.over(order_by=by_id, rows=(-2, 0)), "ROWS BETWEEN 2 PRECEDING AND CURRENT ROW"),
        (
            summed.over(order_by=by_id, rows=(None, 3)),
            "ROWS BETWEEN UNBOUNDED PRECEDING AND 3 FOLLOWING",
        ),
        (
            summed.over(order_by=by_id, range_=(1, None)),
            "RANGE BETWEEN 1 FOLLOWING AND UNBOUNDED FOLLOWING",
        ),
        (summed.over(order_by=by_id, groups=(-1, 1)), "GROUPS BETWEEN 1 PRECEDING AND 1 FOLLOWING"),
    ],
)
def test_window_frame_bounds_are_literals(
    dialect: TrinoDialect, window: Over[int], expected: str
) -> None:
    query = select(window).compile(dialect=dialect)

    assert str(query) == (
        f'SELECT sum("table".id) OVER (ORDER BY "table".id {expected}) AS anon_1 \nFROM "table"'
    )
    assert query.params == {}
