import pytest
from sqlalchemy import Uuid
from sqlalchemy.sql.sqltypes import (
    ARRAY,
    BIGINT,
    BOOLEAN,
    CHAR,
    DATE,
    DECIMAL,
    INTEGER,
    NUMERIC,
    REAL,
    SMALLINT,
    VARCHAR,
    NullType,
)

from asynctrino.sqlalchemy import DOUBLE, JSON, MAP, ROW, TIME, TIMESTAMP, VARBINARY
from asynctrino.sqlalchemy._types import SQLType, aware_split, parse_sqltype


def assert_sqltype(this: SQLType, that: SQLType) -> None:
    assert type(this) is type(that)
    match this, that:
        case ARRAY(), ARRAY():
            assert_sqltype(this.item_type, that.item_type)
            assert (
                this.dimensions in {None, 1}
                if that.dimensions in {None, 1}
                else this.dimensions == that.dimensions
            )
        case MAP(), MAP():
            assert_sqltype(this.key_type, that.key_type)
            assert_sqltype(this.value_type, that.value_type)
        case ROW(), ROW():
            assert [name for name, _ in this.attr_types] == [name for name, _ in that.attr_types]
            for (_, this_type), (_, that_type) in zip(
                this.attr_types, that.attr_types, strict=True
            ):
                assert_sqltype(this_type, that_type)
        case (TIME(), TIME()) | (TIMESTAMP(), TIMESTAMP()):
            assert (this.precision, this.timezone) == (that.precision, that.timezone)
        case _:
            assert repr(this) == repr(that)


simple_types: dict[str, SQLType] = {
    "boolean": BOOLEAN(),
    "tinyint": SMALLINT(),
    "smallint": SMALLINT(),
    "int": INTEGER(),
    "integer": INTEGER(),
    "bigint": BIGINT(),
    "real": REAL(),
    "double": DOUBLE(),
    "decimal": DECIMAL(),
    "number": NUMERIC(),
    "varchar": VARCHAR(),
    "char": CHAR(),
    "varbinary": VARBINARY(),
    "json": JSON(),
    "date": DATE(),
    "time": TIME(),
    "time with time zone": TIME(timezone=True),
    "timestamp": TIMESTAMP(),
    "timestamp with time zone": TIMESTAMP(timezone=True),
    "uuid": Uuid(),
}


@pytest.mark.parametrize(("type_str", "sql_type"), simple_types.items(), ids=simple_types.keys())
def test_parse_simple_type(type_str: str, sql_type: SQLType) -> None:
    assert_sqltype(parse_sqltype(type_str), sql_type)


cases: dict[str, SQLType] = {
    "char(10)": CHAR(10),
    "Char(10)": CHAR(10),
    "char": CHAR(),
    "cHaR": CHAR(),
    "VARCHAR(10)": VARCHAR(10),
    "varCHAR(10)": VARCHAR(10),
    "VARchar(10)": VARCHAR(10),
    "VARCHAR": VARCHAR(),
    "VaRchAr": VARCHAR(),
}


@pytest.mark.parametrize(("type_str", "sql_type"), cases.items(), ids=cases.keys())
def test_parse_cases(type_str: str, sql_type: SQLType) -> None:
    assert_sqltype(parse_sqltype(type_str), sql_type)


type_options: dict[str, SQLType] = {
    "CHAR(10)": CHAR(10),
    "VARCHAR(10)": VARCHAR(10),
    "DECIMAL(20)": DECIMAL(20),
    "DECIMAL(20, 3)": DECIMAL(20, 3),
}


@pytest.mark.parametrize(("type_str", "sql_type"), type_options.items(), ids=type_options.keys())
def test_parse_type_options(type_str: str, sql_type: SQLType) -> None:
    assert_sqltype(parse_sqltype(type_str), sql_type)


arrays: dict[str, SQLType] = {
    "array(integer)": ARRAY(INTEGER()),
    "array(varchar(10))": ARRAY(VARCHAR(10)),
    "array(decimal(20,3))": ARRAY(DECIMAL(20, 3)),
    "array(array(varchar(10)))": ARRAY(VARCHAR(10), dimensions=2),
    "array(map(char, integer))": ARRAY(MAP(CHAR(), INTEGER())),
    "array(row(a integer, b varchar))": ARRAY(ROW([("a", INTEGER()), ("b", VARCHAR())])),
}


@pytest.mark.parametrize(("type_str", "sql_type"), arrays.items(), ids=arrays.keys())
def test_parse_array(type_str: str, sql_type: SQLType) -> None:
    assert_sqltype(parse_sqltype(type_str), sql_type)


maps: dict[str, SQLType] = {
    "map(char, integer)": MAP(CHAR(), INTEGER()),
    "map(varchar(10), varchar(10))": MAP(VARCHAR(10), VARCHAR(10)),
    "map(varchar(10), decimal(20,3))": MAP(VARCHAR(10), DECIMAL(20, 3)),
    "map(char, array(varchar(10)))": MAP(CHAR(), ARRAY(VARCHAR(10))),
    "map(varchar(10), array(varchar(10)))": MAP(VARCHAR(10), ARRAY(VARCHAR(10))),
    "map(varchar(10), array(array(varchar(10))))": MAP(
        VARCHAR(10), ARRAY(VARCHAR(10), dimensions=2)
    ),
}


@pytest.mark.parametrize(("type_str", "sql_type"), maps.items(), ids=maps.keys())
def test_parse_map(type_str: str, sql_type: SQLType) -> None:
    assert_sqltype(parse_sqltype(type_str), sql_type)


rows: dict[str, SQLType] = {
    "row(a integer, b varchar)": ROW([("a", INTEGER()), ("b", VARCHAR())]),
    "row(a varchar(20), b decimal(20,3))": ROW([("a", VARCHAR(20)), ("b", DECIMAL(20, 3))]),
    "row(x array(varchar(10)), y array(array(varchar(10))), z decimal(20,3))": ROW(
        [
            ("x", ARRAY(VARCHAR(10))),
            ("y", ARRAY(VARCHAR(10), dimensions=2)),
            ("z", DECIMAL(20, 3)),
        ]
    ),
    "row(min timestamp(6) with time zone, max timestamp(6) with time zone)": ROW(
        [("min", TIMESTAMP(6, timezone=True)), ("max", TIMESTAMP(6, timezone=True))]
    ),
    'row("first name" varchar, "last name" varchar)': ROW(
        [("first name", VARCHAR()), ("last name", VARCHAR())]
    ),
    'ROW("First Name" VARCHAR)': ROW([("First Name", VARCHAR())]),
    'row("a""b" integer, "c,""d" integer)': ROW([('a"b', INTEGER()), ('c,"d', INTEGER())]),
    'row("foo,bar" varchar, "foo(bar)" varchar, "foo\\"bar" varchar)': ROW(
        [("foo,bar", VARCHAR()), ("foo(bar)", VARCHAR()), ('foo"bar', VARCHAR())]
    ),
}


@pytest.mark.parametrize(("type_str", "sql_type"), rows.items(), ids=rows.keys())
def test_parse_row(type_str: str, sql_type: SQLType) -> None:
    assert_sqltype(parse_sqltype(type_str), sql_type)


datetimes: dict[str, SQLType] = {
    "date": DATE(),
    "time": TIME(),
    "time(0)": TIME(0),
    "time(3)": TIME(3, timezone=False),
    "time(6)": TIME(6),
    "time(13)": TIME(13),
    "time(12) with time zone": TIME(12, timezone=True),
    "time with time zone": TIME(timezone=True),
    "timestamp(0)": TIMESTAMP(0),
    "timestamp(3)": TIMESTAMP(3, timezone=False),
    "timestamp(6)": TIMESTAMP(6),
    "timestamp(12) with time zone": TIMESTAMP(12, timezone=True),
    "timestamp with time zone": TIMESTAMP(timezone=True),
}


@pytest.mark.parametrize(("type_str", "sql_type"), datetimes.items(), ids=datetimes.keys())
def test_parse_datetime(type_str: str, sql_type: SQLType) -> None:
    assert_sqltype(parse_sqltype(type_str), sql_type)


@pytest.mark.parametrize("type_str", ["interval day to second", "hyperloglog", "(", ""])
def test_unknown_type_warns_and_parses_as_null(type_str: str) -> None:
    with pytest.warns(match="type"):
        parsed = parse_sqltype(type_str)

    assert isinstance(parsed, NullType)


split_strings: dict[str, list[str]] = {
    "10": ["10"],
    "10,3": ["10", "3"],
    '"a,b",c': ['"a,b"', "c"],
    '"a,b","c,d"': ['"a,b"', '"c,d"'],
    r'"a,\"b\",c",d': [r'"a,\"b\",c"', "d"],
    r'"foo(bar,\"baz\")",quiz': [r'"foo(bar,\"baz\")"', "quiz"],
    "varchar": ["varchar"],
    "varchar,int": ["varchar", "int"],
    "varchar,int,float": ["varchar", "int", "float"],
    "array(varchar)": ["array(varchar)"],
    "array(varchar),int": ["array(varchar)", "int"],
    "array(varchar(20))": ["array(varchar(20))"],
    "array(varchar(20)),int": ["array(varchar(20))", "int"],
    "array(varchar(20)),array(varchar(20))": ["array(varchar(20))", "array(varchar(20))"],
    "map(varchar, integer),int": ["map(varchar, integer)", "int"],
    "map(varchar(20), integer),int": ["map(varchar(20), integer)", "int"],
    "map(varchar(20), varchar(20)),int": ["map(varchar(20), varchar(20))", "int"],
    "map(varchar(20), varchar(20)),array(varchar)": [
        "map(varchar(20), varchar(20))",
        "array(varchar)",
    ],
    "row(first_name varchar(20), last_name varchar(20)),int": [
        "row(first_name varchar(20), last_name varchar(20))",
        "int",
    ],
    'row("first name" varchar(20), "last name" varchar(20)),int': [
        'row("first name" varchar(20), "last name" varchar(20))',
        "int",
    ],
}


@pytest.mark.parametrize(
    ("input_string", "output_strings"), split_strings.items(), ids=split_strings.keys()
)
def test_split_string(input_string: str, output_strings: list[str]) -> None:
    assert list(aware_split(input_string)) == output_strings


@pytest.mark.parametrize(
    ("input_string", "delimiter", "output_strings"),
    [
        ("first,second", ",", ["first", "second"]),
        ("first second", " ", ["first", "second"]),
        ("first|second", "|", ["first", "second"]),
        ("first,second third", ",", ["first", "second third"]),
        ("first,second third", " ", ["first,second", "third"]),
    ],
)
def test_split_delimiter(input_string: str, delimiter: str, output_strings: list[str]) -> None:
    assert list(aware_split(input_string, delimiter=delimiter)) == output_strings


@pytest.mark.parametrize(
    ("input_string", "maxsplit", "output_strings"),
    [
        ("one,two,three", -1, ["one", "two", "three"]),
        ("one,two,three", 0, ["one,two,three"]),
        ("one,two,three", 1, ["one", "two,three"]),
        ("one,two,three", 2, ["one", "two", "three"]),
        ("one,two,three", 3, ["one", "two", "three"]),
        ("one,two,three", 10, ["one", "two", "three"]),
        (",one,two,three", 0, [",one,two,three"]),
        (",one,two,three", 1, ["", "one,two,three"]),
        ("one,two,three,", 2, ["one", "two", "three,"]),
        ("one,two,three,", 3, ["one", "two", "three", ""]),
    ],
)
def test_split_maxsplit(input_string: str, maxsplit: int, output_strings: list[str]) -> None:
    assert list(aware_split(input_string, maxsplit=maxsplit)) == output_strings


def test_split_rejects_maxsplit_below_minus_one() -> None:
    with pytest.raises(ValueError, match="maxsplit"):
        _ = list(aware_split("a,b", maxsplit=-2))
