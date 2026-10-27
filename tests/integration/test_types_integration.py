import math
import uuid
from collections.abc import Mapping, Sequence
from datetime import date, datetime, time, timedelta, timezone, tzinfo
from decimal import Decimal
from zoneinfo import ZoneInfo

import pytest
from dateutil.relativedelta import relativedelta

from asynctrino.dbapi import Connection, Cursor, Row, connect
from asynctrino.exceptions import Error, TrinoDataError


@pytest.fixture
def conn(encoded_conn: Connection) -> Connection:
    """Run every test here over the Direct protocol and the Spooling protocol's encodings."""
    return encoded_conn


type Field = tuple[str, object]


async def select_all(conn: Connection, sql: str) -> list[Row]:
    cur = conn.cursor()
    await cur.execute(sql)
    return await cur.fetchall()


async def assert_selects(conn: Connection, *fields: Field) -> None:
    """Select every field's SQL in one row and compare each value, NaN-aware, to its Python."""
    [row] = await select_all(conn, "SELECT " + ",\n".join(sql for sql, _ in fields))

    assert len(row) == len(fields)
    for (sql, expected), actual in zip(fields, row, strict=True):
        assert same(actual, expected), f"{sql}: {actual!r} != {expected!r}"


async def assert_fails(conn: Connection, sql: str) -> None:
    with pytest.raises(Error):
        _ = await select_all(conn, f"SELECT {sql}")


def same(actual: object, expected: object) -> bool:
    """Equality that treats NaN as equal to NaN, inside collections too."""
    if isinstance(actual, float) and isinstance(expected, float):
        return actual == expected or (math.isnan(actual) and math.isnan(expected))
    if isinstance(actual, (list, tuple)) and isinstance(expected, (list, tuple)):
        actual_items, expected_items = cast_sequence(actual), cast_sequence(expected)
        return (
            isinstance(actual, tuple) == isinstance(expected, tuple)
            and len(actual_items) == len(expected_items)
            and all(same(a, e) for a, e in zip(actual_items, expected_items, strict=True))
        )
    if isinstance(actual, dict) and isinstance(expected, dict):
        actual_items, expected_items = cast_mapping(actual), cast_mapping(expected)
        return len(actual_items) == len(expected_items) and all(
            any(same(k, ek) and same(v, ev) for ek, ev in expected_items.items())
            for k, v in actual_items.items()
        )
    return actual == expected


def cast_sequence(value: object) -> Sequence[object]:
    assert isinstance(value, (list, tuple))
    return [*value]


def cast_mapping(value: object) -> Mapping[object, object]:
    assert isinstance(value, dict)
    return {**value}


def zone(name: str) -> tzinfo:
    if name[0] in "+-":
        offset = timedelta(hours=int(name[1:3]), minutes=int(name[4:6]))
        return timezone(-offset if name[0] == "-" else offset)
    return ZoneInfo(name)


async def test_boolean(conn: Connection) -> None:
    await assert_selects(conn, ("CAST(null AS BOOLEAN)", None), ("false", False), ("true", True))


@pytest.mark.parametrize(
    ("trino_type", "low", "high"),
    [
        ("TINYINT", -128, 127),
        ("SMALLINT", -32768, 32767),
        ("INTEGER", -2147483648, 2147483647),
        ("BIGINT", -9223372036854775808, 9223372036854775807),
    ],
)
async def test_integers(conn: Connection, trino_type: str, low: int, high: int) -> None:
    await assert_selects(
        conn,
        (f"CAST(null AS {trino_type})", None),
        (f"CAST({low} AS {trino_type})", low),
        (f"CAST(42 AS {trino_type})", 42),
        (f"CAST({high} AS {trino_type})", high),
    )


async def test_real(conn: Connection) -> None:
    await assert_selects(
        conn,
        ("CAST(null AS REAL)", None),
        ("CAST('NaN' AS REAL)", math.nan),
        ("CAST('-Infinity' AS REAL)", -math.inf),
        ("CAST(3.4028235E38 AS REAL)", 3.4028235e38),
        ("CAST(1.4E-45 AS REAL)", 1.4e-45),
        ("CAST('Infinity' AS REAL)", math.inf),
    )


async def test_double(conn: Connection) -> None:
    await assert_selects(
        conn,
        ("CAST(null AS DOUBLE)", None),
        ("CAST('NaN' AS DOUBLE)", math.nan),
        ("CAST('-Infinity' AS DOUBLE)", -math.inf),
        ("CAST(1.7976931348623157E308 AS DOUBLE)", 1.7976931348623157e308),
        ("CAST(4.9E-324 AS DOUBLE)", 5e-324),
        ("CAST('Infinity' AS DOUBLE)", math.inf),
    )


async def test_decimal(conn: Connection) -> None:
    await assert_selects(
        conn,
        ("CAST(null AS DECIMAL)", None),
        ("CAST(null AS DECIMAL(38,0))", None),
        ("DECIMAL '10.3'", Decimal("10.3")),
        ("CAST('0.123456789123456789' AS DECIMAL(18,18))", Decimal("0.123456789123456789")),
        ("CAST(null AS DECIMAL(18,18))", None),
        ("CAST('234.123456789123456789' AS DECIMAL(18,4))", Decimal("234.1235")),
        ("CAST('10.3' AS DECIMAL(38,1))", Decimal("10.3")),
        ("CAST('0.123456789123456789' AS DECIMAL(18,2))", Decimal("0.12")),
        ("CAST('0.3123' AS DECIMAL(38,38))", Decimal("0.3123")),
    )


async def test_number(conn: Connection) -> None:
    await assert_selects(
        conn,
        ("CAST(null AS NUMBER)", None),
        ("CAST(1.5 AS NUMBER)", Decimal("1.5")),
        (
            "CAST('123456789012345678901234567890.0123456789' AS NUMBER)",
            Decimal("123456789012345678901234567890.0123456789"),
        ),
    )


async def test_varchar(conn: Connection) -> None:
    await assert_selects(
        conn,
        ("'aaa'", "aaa"),
        ("U&'Hello winter \\2603 !'", "Hello winter ☃ !"),
        ("CAST(null AS VARCHAR)", None),
        ("CAST('bbb' AS VARCHAR(1))", "b"),
        ("CAST(null AS VARCHAR(1))", None),
    )


async def test_char(conn: Connection) -> None:
    await assert_selects(
        conn,
        ("CAST('ccc' AS CHAR)", "c"),
        ("CAST('ccc' AS CHAR(5))", "ccc  "),
        ("CAST(null AS CHAR)", None),
        ("CAST('ddd' AS CHAR(1))", "d"),
        ("CAST('\U0001f602' AS CHAR(1))", "\U0001f602"),
        ("CAST(null AS CHAR(1))", None),
    )


async def test_varbinary(conn: Connection) -> None:
    await assert_selects(
        conn,
        ("X'65683F'", b"eh?"),
        ("X''", b""),
        ("CAST('' AS VARBINARY)", b""),
        ("CAST(null AS VARBINARY)", None),
    )


async def test_varbinary_failure(conn: Connection) -> None:
    await assert_fails(conn, "CAST(42 AS VARBINARY)")


async def test_json(conn: Connection) -> None:
    await assert_selects(
        conn,
        ("CAST('{}' AS JSON)", '"{}"'),
        ("CAST('null' AS JSON)", '"null"'),
        ("CAST(null AS JSON)", None),
        ("CAST('3.14' AS JSON)", '"3.14"'),
        ("CAST('a string' AS JSON)", '"a string"'),
        ("CAST('a \" complex '' string :' AS JSON)", '"a \\" complex \' string :"'),
        ("CAST('[]' AS JSON)", '"[]"'),
    )


async def test_date(conn: Connection) -> None:
    await assert_selects(
        conn,
        ("CAST(null AS DATE)", None),
        ("DATE '0001-01-01'", date(1, 1, 1)),
        ("DATE '1582-10-04'", date(1582, 10, 4)),
        ("DATE '1582-10-05'", date(1582, 10, 5)),
        ("DATE '1582-10-14'", date(1582, 10, 14)),
        ("DATE '1952-04-03'", date(1952, 4, 3)),
        ("DATE '1970-01-01'", date(1970, 1, 1)),
        ("DATE '2001-08-22'", date(2001, 8, 22)),
        ("DATE '9999-12-31'", date(9999, 12, 31)),
    )


@pytest.mark.parametrize(
    "unsupported", ["-0001-01-01", "0000-01-01", "10000-01-01", "-4999999-01-01", "5000000-12-31"]
)
async def test_dates_python_cannot_represent(conn: Connection, unsupported: str) -> None:
    with pytest.raises(TrinoDataError):
        _ = await select_all(conn, f"SELECT DATE '{unsupported}'")


TIME_CASES: list[tuple[str, time]] = [
    ("00:00:00", time(0, 0, 0)),
    ("12:34:56.123", time(12, 34, 56, 123000)),
    ("23:59:59.999", time(23, 59, 59, 999000)),
    ("00:00:00.1", time(0, 0, 0, 100000)),
    ("00:00:00.01", time(0, 0, 0, 10000)),
    ("00:00:00.001", time(0, 0, 0, 1000)),
    ("00:00:00.0001", time(0, 0, 0, 100)),
    ("00:00:00.00001", time(0, 0, 0, 10)),
    ("00:00:00.000001", time(0, 0, 0, 1)),
    ("23:59:59", time(23, 59, 59)),
    ("23:59:59.9", time(23, 59, 59, 900000)),
    ("23:59:59.99", time(23, 59, 59, 990000)),
    ("23:59:59.9999", time(23, 59, 59, 999900)),
    ("23:59:59.99999", time(23, 59, 59, 999990)),
    ("23:59:59.999999", time(23, 59, 59, 999999)),
    ("12:34:56.1234561", time(12, 34, 56, 123456)),
    ("00:00:00.000000000001", time(0, 0, 0, 0)),
    ("00:00:00.0000001", time(0, 0, 0, 0)),
    ("00:00:00.0000004", time(0, 0, 0, 0)),
    ("00:00:00.00000049", time(0, 0, 0, 0)),
    ("00:00:00.0000005", time(0, 0, 0, 0)),
    ("00:00:00.00000050", time(0, 0, 0, 0)),
    ("23:59:59.9999994", time(23, 59, 59, 999999)),
    ("23:59:59.9999994999", time(23, 59, 59, 999999)),
    ("12:34:56.123456789", time(12, 34, 56, 123457)),
    ("00:00:00.000000500001", time(0, 0, 0, 1)),
    ("00:00:00.0000009", time(0, 0, 0, 1)),
    ("00:00:00.00000099999", time(0, 0, 0, 1)),
    ("23:59:59.9999995", time(0, 0, 0)),
    ("23:59:59.999999500001", time(0, 0, 0)),
    ("23:59:59.9999999", time(0, 0, 0)),
    ("23:59:59.999999999", time(0, 0, 0)),
]


async def test_time(conn: Connection) -> None:
    await assert_selects(
        conn,
        ("CAST(null AS TIME)", None),
        *((f"TIME '{literal}'", expected) for literal, expected in TIME_CASES),
    )


@pytest.mark.parametrize("offset", ["-08:00", "+08:00", "+05:30"])
async def test_time_with_time_zone(conn: Connection, offset: str) -> None:
    tz = zone(offset)
    await assert_selects(
        conn,
        ("CAST(null AS TIME WITH TIME ZONE)", None),
        *(
            (f"TIME '{literal} {offset}'", expected.replace(tzinfo=tz))
            for literal, expected in TIME_CASES
        ),
    )


TIMESTAMP_CASES: list[tuple[str, datetime]] = [
    ("2001-08-22 00:00:00", datetime(2001, 8, 22, 0, 0, 0)),
    ("2001-08-22 12:34:56.123", datetime(2001, 8, 22, 12, 34, 56, 123000)),
    ("2001-08-22 23:59:59.999", datetime(2001, 8, 22, 23, 59, 59, 999000)),
    ("2001-08-22 00:00:00.1", datetime(2001, 8, 22, 0, 0, 0, 100000)),
    ("2001-08-22 00:00:00.01", datetime(2001, 8, 22, 0, 0, 0, 10000)),
    ("2001-08-22 00:00:00.001", datetime(2001, 8, 22, 0, 0, 0, 1000)),
    ("2001-08-22 00:00:00.0001", datetime(2001, 8, 22, 0, 0, 0, 100)),
    ("2001-08-22 00:00:00.00001", datetime(2001, 8, 22, 0, 0, 0, 10)),
    ("2001-08-22 00:00:00.000001", datetime(2001, 8, 22, 0, 0, 0, 1)),
    ("2001-08-22 23:59:59", datetime(2001, 8, 22, 23, 59, 59)),
    ("2001-08-22 23:59:59.9", datetime(2001, 8, 22, 23, 59, 59, 900000)),
    ("2001-08-22 23:59:59.99", datetime(2001, 8, 22, 23, 59, 59, 990000)),
    ("2001-08-22 23:59:59.9999", datetime(2001, 8, 22, 23, 59, 59, 999900)),
    ("2001-08-22 23:59:59.99999", datetime(2001, 8, 22, 23, 59, 59, 999990)),
    ("2001-08-22 23:59:59.999999", datetime(2001, 8, 22, 23, 59, 59, 999999)),
    ("2001-08-22 12:34:56.1234561", datetime(2001, 8, 22, 12, 34, 56, 123456)),
    ("2001-08-22 00:00:00.000000000001", datetime(2001, 8, 22, 0, 0, 0, 0)),
    ("2001-08-22 00:00:00.0000001", datetime(2001, 8, 22, 0, 0, 0, 0)),
    ("2001-08-22 00:00:00.0000004", datetime(2001, 8, 22, 0, 0, 0, 0)),
    ("2001-08-22 00:00:00.00000049", datetime(2001, 8, 22, 0, 0, 0, 0)),
    ("2001-08-22 00:00:00.0000005", datetime(2001, 8, 22, 0, 0, 0, 0)),
    ("2001-08-22 00:00:00.00000050", datetime(2001, 8, 22, 0, 0, 0, 0)),
    ("2001-08-22 23:59:59.9999994", datetime(2001, 8, 22, 23, 59, 59, 999999)),
    ("2001-08-22 23:59:59.9999994999", datetime(2001, 8, 22, 23, 59, 59, 999999)),
    ("2001-08-22 12:34:56.123456789", datetime(2001, 8, 22, 12, 34, 56, 123457)),
    ("2001-08-22 00:00:00.000000500001", datetime(2001, 8, 22, 0, 0, 0, 1)),
    ("2001-08-22 00:00:00.0000009", datetime(2001, 8, 22, 0, 0, 0, 1)),
    ("2001-08-22 00:00:00.00000099999", datetime(2001, 8, 22, 0, 0, 0, 1)),
    ("2001-08-22 23:59:59.9999995", datetime(2001, 8, 23, 0, 0, 0, 0)),
    ("2001-08-22 23:59:59.999999500001", datetime(2001, 8, 23, 0, 0, 0, 0)),
    ("2001-08-22 23:59:59.9999999", datetime(2001, 8, 23, 0, 0, 0, 0)),
    ("2001-08-22 23:59:59.999999999", datetime(2001, 8, 23, 0, 0, 0, 0)),
    ("0001-01-01 01:23:45.123", datetime(1, 1, 1, 1, 23, 45, 123000)),
    ("1582-10-04 01:23:45.123", datetime(1582, 10, 4, 1, 23, 45, 123000)),
    ("1582-10-05 01:23:45.123", datetime(1582, 10, 5, 1, 23, 45, 123000)),
    ("1582-10-14 01:23:45.123", datetime(1582, 10, 14, 1, 23, 45, 123000)),
]


async def test_timestamp(conn: Connection) -> None:
    await assert_selects(
        conn,
        ("CAST(null AS TIMESTAMP)", None),
        *((f"TIMESTAMP '{literal}'", expected) for literal, expected in TIMESTAMP_CASES),
    )


@pytest.mark.parametrize("zone_name", ["-08:00", "+08:00", "US/Eastern", "Asia/Kolkata", "GMT"])
async def test_timestamp_with_time_zone(conn: Connection, zone_name: str) -> None:
    tz = zone(zone_name)
    await assert_selects(
        conn,
        ("CAST(null AS TIMESTAMP WITH TIME ZONE)", None),
        *(
            (f"TIMESTAMP '{literal} {zone_name}'", expected.replace(tzinfo=tz))
            for literal, expected in TIMESTAMP_CASES
        ),
    )


async def test_timestamp_with_time_zone_dst(conn: Connection) -> None:
    brussels = ZoneInfo("Europe/Brussels")
    await assert_selects(
        conn,
        (
            "TIMESTAMP '2022-03-27 01:59:59.999999999 Europe/Brussels'",
            datetime(2022, 3, 27, 2, 0, 0, tzinfo=brussels),
        ),
        (
            "TIMESTAMP '2022-10-30 02:59:59.999999999 Europe/Brussels'",
            datetime(2022, 10, 30, 3, 0, 0, tzinfo=brussels),
        ),
    )


async def test_interval_year_to_month(conn: Connection) -> None:
    await assert_selects(
        conn,
        ("CAST(null AS INTERVAL YEAR TO MONTH)", None),
        ("INTERVAL '10' YEAR", relativedelta(years=10)),
        ("INTERVAL '-5' YEAR", relativedelta(years=-5)),
        ("INTERVAL '3' MONTH", relativedelta(months=3)),
        ("INTERVAL '-18' MONTH", relativedelta(years=-1, months=-6)),
        ("INTERVAL '30' MONTH", relativedelta(years=2, months=6)),
        ("INTERVAL '178956970-7' YEAR TO MONTH", relativedelta(years=178956970, months=7)),
        ("INTERVAL '-178956970-8' YEAR TO MONTH", relativedelta(years=-178956970, months=-8)),
    )


async def test_interval_day_to_second(conn: Connection) -> None:
    widest = timedelta(days=106751991, hours=4, seconds=54, milliseconds=775)
    await assert_selects(
        conn,
        ("CAST(null AS INTERVAL DAY TO SECOND)", None),
        ("INTERVAL '2' DAY", timedelta(days=2)),
        ("INTERVAL '-2' DAY", timedelta(days=-2)),
        ("INTERVAL '-2' SECOND", timedelta(seconds=-2)),
        (
            "INTERVAL '1 11:11:11.116' DAY TO SECOND",
            timedelta(days=1, seconds=40271, microseconds=116000),
        ),
        ("INTERVAL '-5 23:59:57.000' DAY TO SECOND", timedelta(days=-6, seconds=3)),
        ("INTERVAL '12 10:45' DAY TO MINUTE", timedelta(days=12, seconds=38700)),
        ("INTERVAL '45:32.123' MINUTE TO SECOND", timedelta(seconds=2732, microseconds=123000)),
        ("INTERVAL '32.123' SECOND", timedelta(seconds=32, microseconds=123000)),
        ("INTERVAL '106751991 04:00:54.775' DAY TO SECOND", widest),
        ("INTERVAL '-106751991 04:00:54.775' DAY TO SECOND", -widest),
    )
    await assert_fails(conn, "INTERVAL '1000000000' DAY")
    await assert_fails(conn, "INTERVAL '-1000000000' DAY")


async def test_array_of_primitives(conn: Connection) -> None:
    await assert_selects(
        conn,
        ("CAST(null AS ARRAY(VARCHAR))", None),
        ("ARRAY[]", []),
        ("ARRAY[true, false, null]", [True, False, None]),
        ("ARRAY[1, 2, null]", [1, 2, None]),
        (
            (
                "ARRAY[CAST('NaN' AS REAL), CAST('-Infinity' AS REAL), CAST(3.4028235E38 AS REAL), "
                "CAST(1.4E-45 AS REAL), CAST('Infinity' AS REAL), null]"
            ),
            [math.nan, -math.inf, 3.4028235e38, 1.4e-45, math.inf, None],
        ),
        (
            (
                "ARRAY[CAST('NaN' AS DOUBLE), CAST('-Infinity' AS DOUBLE), "
                "CAST(1.7976931348623157E308 AS DOUBLE), CAST(4.9E-324 AS DOUBLE), "
                "CAST('Infinity' AS DOUBLE), null]"
            ),
            [math.nan, -math.inf, 1.7976931348623157e308, 5e-324, math.inf, None],
        ),
        ("ARRAY[1.2, 2.4, null]", [Decimal("1.2"), Decimal("2.4"), None]),
        ("ARRAY[CAST('hello' AS VARCHAR), null]", ["hello", None]),
        ("ARRAY[CAST('a' AS CHAR(3)), null]", ["a  ", None]),
        ("ARRAY[X'', X'65683F', null]", [b"", b"eh?", None]),
        ("ARRAY[JSON 'null', JSON '{}', null]", ["null", "{}", None]),
    )


async def test_array_of_temporals(conn: Connection) -> None:
    india = zone("+05:30")
    await assert_selects(
        conn,
        ("ARRAY[DATE '1970-01-01', null]", [date(1970, 1, 1), None]),
        ("ARRAY[TIME '01:01:01', null]", [time(1, 1, 1), None]),
        ("ARRAY[TIME '01:01:01 +05:30', null]", [time(1, 1, 1, tzinfo=india), None]),
        ("ARRAY[TIMESTAMP '1970-01-01 01:01:01', null]", [datetime(1970, 1, 1, 1, 1, 1), None]),
        (
            "ARRAY[TIMESTAMP '1970-01-01 01:01:01 +05:30', null]",
            [datetime(1970, 1, 1, 1, 1, 1, tzinfo=india), None],
        ),
    )


async def test_array_of_structures(conn: Connection) -> None:
    await assert_selects(
        conn,
        ("ARRAY[ARRAY[1, null], ARRAY[2, 3], null]", [[1, None], [2, 3], None]),
        (
            "ARRAY[MAP(ARRAY['foo', 'bar', 'baz'], ARRAY['one', 'two', null]), MAP(), null]",
            [{"foo": "one", "bar": "two", "baz": None}, {}, None],
        ),
        ("ARRAY[ROW(1, 2), ROW(1, null), null]", [(1, 2), (1, None), None]),
    )


async def test_map_of_primitives(conn: Connection) -> None:
    reals = (
        "CAST('NaN' AS REAL), CAST('-Infinity' AS REAL), CAST(3.4028235E38 AS REAL), "
        "CAST(1.4E-45 AS REAL), CAST('Infinity' AS REAL)"
    )
    doubles = (
        "CAST('NaN' AS DOUBLE), CAST('-Infinity' AS DOUBLE), "
        "CAST(1.7976931348623157E308 AS DOUBLE), CAST(4.9E-324 AS DOUBLE), "
        "CAST('Infinity' AS DOUBLE)"
    )
    await assert_selects(
        conn,
        ("CAST(null AS MAP(VARCHAR, INTEGER))", None),
        ("MAP()", {}),
        ("MAP(ARRAY[true, false], ARRAY[false, true])", {True: False, False: True}),
        ("MAP(ARRAY[true, false], ARRAY[true, null])", {True: True, False: None}),
        ("MAP(ARRAY[1, 2], ARRAY[1, null])", {1: 1, 2: None}),
        (
            f"MAP(ARRAY[{reals}, CAST(1 AS REAL)], ARRAY[{reals}, null])",
            {
                math.nan: math.nan,
                -math.inf: -math.inf,
                3.4028235e38: 3.4028235e38,
                1.4e-45: 1.4e-45,
                math.inf: math.inf,
                1.0: None,
            },
        ),
        (
            f"MAP(ARRAY[{doubles}, CAST(1 AS DOUBLE)], ARRAY[{doubles}, null])",
            {
                math.nan: math.nan,
                -math.inf: -math.inf,
                1.7976931348623157e308: 1.7976931348623157e308,
                5e-324: 5e-324,
                math.inf: math.inf,
                1.0: None,
            },
        ),
        (
            "MAP(ARRAY[1.2, 2.4, 4.8], ARRAY[1.2, 2.4, null])",
            {Decimal("1.2"): Decimal("1.2"), Decimal("2.4"): Decimal("2.4"), Decimal("4.8"): None},
        ),
        (
            (
                "MAP(ARRAY[CAST('hello' AS VARCHAR), CAST('null' AS VARCHAR)], "
                "ARRAY[CAST('hello' AS VARCHAR), null])"
            ),
            {"hello": "hello", "null": None},
        ),
        (
            (
                "MAP(ARRAY[CAST('a' AS CHAR(4)), CAST('null' AS CHAR(4))], "
                "ARRAY[CAST('a' AS CHAR), null])"
            ),
            {"a   ": "a", "null": None},
        ),
        (
            "MAP(ARRAY[X'', X'65683F', X'00'], ARRAY[X'', X'65683F', null])",
            {b"": b"", b"eh?": b"eh?", b"\x00": None},
        ),
        (
            "MAP(ARRAY[JSON '1', JSON '{}', JSON 'null'], ARRAY[JSON '1', JSON '{}', null])",
            {"1": "1", "{}": "{}", "null": None},
        ),
    )


async def test_map_of_temporals(conn: Connection) -> None:
    india, new_york, los_angeles = zone("+05:30"), zone("-05:00"), zone("America/Los_Angeles")
    time_1, time_2 = time(1, 1, 1), time(23, 59, 59)
    datetime_1, datetime_2 = datetime(1970, 1, 1, 1, 1, 1), datetime(2023, 1, 1, 23, 59, 59)
    await assert_selects(
        conn,
        (
            "MAP(ARRAY[DATE '1970-01-01', DATE '2023-01-01'], ARRAY[DATE '1970-01-01', null])",
            {date(1970, 1, 1): date(1970, 1, 1), date(2023, 1, 1): None},
        ),
        (
            "MAP(ARRAY[TIME '01:01:01', TIME '23:59:59'], ARRAY[TIME '01:01:01', null])",
            {time_1: time_1, time_2: None},
        ),
        (
            (
                "MAP(ARRAY[TIME '01:01:01 +05:30', TIME '23:59:59 -05:00'], "
                "ARRAY[TIME '01:01:01 +05:30', null])"
            ),
            {
                time_1.replace(tzinfo=india): time_1.replace(tzinfo=india),
                time_2.replace(tzinfo=new_york): None,
            },
        ),
        (
            (
                "MAP(ARRAY[TIMESTAMP '1970-01-01 01:01:01', TIMESTAMP '2023-01-01 23:59:59'], "
                "ARRAY[TIMESTAMP '1970-01-01 01:01:01', null])"
            ),
            {datetime_1: datetime_1, datetime_2: None},
        ),
        (
            (
                "MAP(ARRAY[TIMESTAMP '1970-01-01 01:01:01 +05:30', "
                "TIMESTAMP '2023-01-01 23:59:59 America/Los_Angeles'], "
                "ARRAY[TIMESTAMP '1970-01-01 01:01:01 +05:30', null])"
            ),
            {
                datetime_1.replace(tzinfo=india): datetime_1.replace(tzinfo=india),
                datetime_2.replace(tzinfo=los_angeles): None,
            },
        ),
    )


async def test_row(conn: Connection) -> None:
    await assert_selects(
        conn,
        ("CAST(null AS ROW(x BIGINT, y DOUBLE))", None),
        ("CAST(ROW(1, 2e0, null) AS ROW(x BIGINT, y DOUBLE, z DOUBLE))", (1, 2.0, None)),
    )


async def test_ipaddress(conn: Connection) -> None:
    await assert_selects(
        conn, ("CAST(null AS IPADDRESS)", None), ("IPADDRESS '2001:db8::1'", "2001:db8::1")
    )


async def test_uuid(conn: Connection) -> None:
    await assert_selects(
        conn,
        ("CAST(null AS UUID)", None),
        (
            "UUID '12151fd2-7586-11e9-8f9e-2a86e4085a59'",
            uuid.UUID("12151fd2-7586-11e9-8f9e-2a86e4085a59"),
        ),
    )


async def test_digest(conn: Connection) -> None:
    await assert_selects(
        conn,
        ("CAST(null AS HyperLogLog)", None),
        ("CAST(null AS P4HyperLogLog)", None),
        ("CAST(null AS SetDigest)", None),
        ("CAST(null AS QDigest(BIGINT))", None),
        ("CAST(null AS TDigest)", None),
        ("approx_set(1)", "AgwBAIADRAA="),
        ("CAST(approx_set(1) AS P4HyperLogLog)", "AwwAAAAg" + "A" * 2730 + "=="),
        ("make_set_digest(1)", "AQgAAAACCwEAgANEAAAgAAABAAAASsQF+7cDRAABAA=="),
        (
            "tdigest_agg(1)",
            "AAAAAAAAAPA/AAAAAAAA8D8AAAAAAABZQAAAAAAAAPA/AQAAAAAAAAAAAPA/AAAAAAAA8D8=",
        ),
    )


async def test_named_row(conn: Connection) -> None:
    cur = conn.cursor()
    await cur.execute("SELECT CAST(ROW(1, 2e0) AS ROW(x BIGINT, y DOUBLE))")
    [[value]] = await cur.fetchall()

    assert value == (1, 2.0)
    assert value.x == 1
    assert value.y == 2.0
    assert value.__annotations__["names"] == ["x", "y"]
    assert value.__annotations__["types"] == ["bigint", "double"]


async def test_named_row_duplicate_names(conn: Connection) -> None:
    cur = conn.cursor()
    await cur.execute("SELECT CAST(ROW(1, 2e0) AS ROW(x BIGINT, x DOUBLE))")
    [[value]] = await cur.fetchall()

    assert value == (1, 2.0)
    with pytest.raises(ValueError, match="Ambiguous row field reference: x"):
        _ = value.x
    assert value.__annotations__["names"] == ["x", "x"]
    assert value.__annotations__["types"] == ["bigint", "double"]
    assert str(value) == "(1, 2.0)"


async def test_nested_named_row(conn: Connection) -> None:
    cur = conn.cursor()
    await cur.execute(
        "SELECT CAST(ROW(DECIMAL '2.3', ROW(1, 'test')) "
        "AS ROW(x DECIMAL(3,2), y ROW(x BIGINT, y VARCHAR)))"
    )
    [[value]] = await cur.fetchall()

    assert value == (Decimal("2.3"), (1, "test"))
    assert value.x == Decimal("2.3")
    assert value.y.x == 1
    assert value.y.y == "test"
    assert value.__annotations__["types"] == ["decimal", "row"]
    assert value.y.__annotations__["names"] == ["x", "y"]
    assert value.y.__annotations__["types"] == ["bigint", "varchar"]
    assert str(value) == "(x: Decimal('2.30'), y: (x: 1, y: 'test'))"


@pytest.mark.parametrize("on_connection", [None, True, False])
@pytest.mark.parametrize("on_cursor", [None, True, False])
async def test_legacy_primitive_types_with_connection_and_cursor(
    trino_address: tuple[str, int], *, on_connection: bool | None, on_cursor: bool | None
) -> None:
    host, port = trino_address
    legacy = on_cursor if on_cursor is not None else bool(on_connection)
    sql = """
        SELECT
            DECIMAL '0.142857',
            DATE '2018-01-01',
            TIMESTAMP '2019-01-01 00:00:00.000+01:00',
            TIMESTAMP '2019-01-01 00:00:00.000 UTC',
            TIMESTAMP '2019-01-01 00:00:00.000',
            TIME '00:00:00.000'
    """
    if legacy:
        sql += ", DATE '-2001-08-22'"

    async with connect(
        host=host, port=port, user="test", legacy_primitive_types=bool(on_connection)
    ) as conn:
        cur = conn.cursor(legacy_primitive_types=on_cursor)
        await cur.execute(sql)
        [row] = await cur.fetchall()

    if legacy:
        assert row == [
            "0.142857",
            "2018-01-01",
            "2019-01-01 00:00:00.000 +01:00",
            "2019-01-01 00:00:00.000 UTC",
            "2019-01-01 00:00:00.000",
            "00:00:00.000",
            "-2001-08-22",
        ]
    else:
        assert row == [
            Decimal("0.142857"),
            date(2018, 1, 1),
            datetime(2019, 1, 1, tzinfo=timezone(timedelta(hours=1))),
            datetime(2019, 1, 1, tzinfo=ZoneInfo("UTC")),
            datetime(2019, 1, 1),
            time(0, 0, 0, 0),
        ]


async def test_session_time_zone_is_sent_to_trino(trino_address: tuple[str, int]) -> None:
    host, port = trino_address
    async with connect(host=host, port=port, user="test", timezone="Asia/Kolkata") as conn:
        cur = conn.cursor()
        await cur.execute("SELECT current_timezone()")

        assert await cur.fetchall() == [["Asia/Kolkata"]]


def assert_description(
    cur: Cursor,
    type_code: str,
    *,
    size: int | None = None,
    precision: int | None = None,
    scale: int | None = None,
) -> None:
    assert cur.description is not None
    assert cur.description[0][1:] == (type_code, None, size, precision, scale, None)


@pytest.mark.parametrize(
    ("sql", "expected", "type_code", "size", "precision", "scale"),
    [
        ("CAST(NULL AS DECIMAL)", None, "decimal(38, 0)", None, 38, 0),
        ("DECIMAL '1.20'", Decimal("1.20"), "decimal(3, 2)", None, 3, 2),
        (
            "TIMESTAMP '2001-08-22 03:04:05.321000'",
            datetime(2001, 8, 22, 3, 4, 5, 321000),
            "timestamp(6)",
            None,
            6,
            None,
        ),
        (
            "CAST(NULL AS TIMESTAMP WITH TIME ZONE)",
            None,
            "timestamp(3) with time zone",
            None,
            3,
            None,
        ),
        (
            "TIMESTAMP '2001-08-22 03:04:05.321 -08:00'",
            datetime(2001, 8, 22, 3, 4, 5, 321000, tzinfo=timezone(timedelta(hours=-8))),
            "timestamp(3) with time zone",
            None,
            3,
            None,
        ),
        ("CAST(NULL AS DATE)", None, "date", None, None, None),
        ("CHAR 'trino'", "trino", "char(5)", 5, None, None),
        ("TIME '01:02:03.456'", time(1, 2, 3, 456000), "time(3)", None, 3, None),
        ("CAST(NULL AS TIME)", None, "time(3)", None, 3, None),
        (
            "TIME '01:02:03.456 -08:00'",
            time(1, 2, 3, 456000, tzinfo=timezone(timedelta(hours=-8))),
            "time(3) with time zone",
            None,
            3,
            None,
        ),
        (
            "TIME '01:02:03.456 +08:00'",
            time(1, 2, 3, 456000, tzinfo=timezone(timedelta(hours=8))),
            "time(3) with time zone",
            None,
            3,
            None,
        ),
        ("CAST(NULL AS TIME WITH TIME ZONE)", None, "time(3) with time zone", None, 3, None),
    ],
)
async def test_value_and_description(  # noqa: PLR0913
    conn: Connection,
    *,
    sql: str,
    expected: object,
    type_code: str,
    size: int | None,
    precision: int | None,
    scale: int | None,
) -> None:
    cur = conn.cursor()
    await cur.execute(f"SELECT {sql}")

    assert await cur.fetchall() == [[expected]]
    assert_description(cur, type_code, size=size, precision=precision, scale=scale)


async def test_description_is_known_before_and_after_fetching(conn: Connection) -> None:
    expected = [
        ("node_id", "varchar", None, 2147483647, None, None, None),
        ("http_uri", "varchar", None, 2147483647, None, None, None),
        ("node_version", "varchar", None, 2147483647, None, None, None),
        ("coordinator", "boolean", None, None, None, None, None),
        ("state", "varchar", None, 2147483647, None, None, None),
    ]
    cur = conn.cursor()
    await cur.execute("SELECT * FROM system.runtime.nodes")

    assert cur.description == expected
    _ = await cur.fetchone()
    assert cur.description == expected
