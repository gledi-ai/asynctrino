import math
import pickle
import re
import uuid
from datetime import date, datetime, time, timedelta, timezone
from decimal import Decimal
from zoneinfo import ZoneInfo

import pytest
from dateutil.relativedelta import relativedelta

from asynctrino._core.mapper import NamedRowTuple, RowMapper
from asynctrino._core.status import Column, TypeSignature
from asynctrino.exceptions import TrinoDataError

type JSON = dict[str, object]


def signature(raw_type: str, *arguments: int) -> TypeSignature:
    return {"rawType": raw_type, "arguments": [{"kind": "LONG", "value": a} for a in arguments]}


def of(*element_types: TypeSignature) -> TypeSignature:
    raw_type = "array" if len(element_types) == 1 else "map"
    return {
        "rawType": raw_type,
        "arguments": [{"kind": "TYPE", "value": t} for t in element_types],
    }


def row(*fields: tuple[str | None, TypeSignature]) -> TypeSignature:
    def field(name: str | None, field_type: TypeSignature) -> JSON:
        named: JSON = {"typeSignature": field_type}
        if name is not None:
            named["fieldName"] = {"name": name}
        return named

    return {
        "rawType": "row",
        "arguments": [{"kind": "NAMED_TYPE", "value": field(n, t)} for n, t in fields],
    }


def map_value(value_type: TypeSignature, value: object) -> object:
    column: Column = {"name": "c", "type": value_type["rawType"], "typeSignature": value_type}
    return RowMapper([column]).map([value])[0]


BIGINT = signature("bigint")
DOUBLE = signature("double")
VARCHAR = signature("varchar", 2147483647)


@pytest.mark.parametrize(
    "raw_type",
    ["boolean", "bigint", "double", "decimal", "varchar", "date", "timestamp", "uuid"],
)
def test_null_maps_to_none_for_every_type(raw_type: str) -> None:
    assert map_value(signature(raw_type), None) is None


@pytest.mark.parametrize(
    ("value", "expected"),
    [(True, True), (False, False), ("true", True), ("FALSE", False)],
)
def test_boolean(value: object, *, expected: bool) -> None:
    assert map_value(signature("boolean"), value) is expected


def test_unexpected_boolean_raises_trino_data_error() -> None:
    with pytest.raises(TrinoDataError, match="Could not convert 'maybe'"):
        _ = map_value(signature("boolean"), "maybe")


@pytest.mark.parametrize("raw_type", ["tinyint", "smallint", "integer", "bigint"])
def test_integers_including_map_keys_sent_as_strings(raw_type: str) -> None:
    assert map_value(signature(raw_type), -9223372036854775808) == -9223372036854775808
    assert map_value(signature(raw_type), "42") == 42


@pytest.mark.parametrize("raw_type", ["real", "double"])
@pytest.mark.parametrize(
    ("value", "expected"),
    [("Infinity", math.inf), ("-Infinity", -math.inf), (5e-324, 5e-324), (2, 2.0), ("1.5", 1.5)],
)
def test_floats(raw_type: str, value: object, expected: float) -> None:
    mapped = map_value(signature(raw_type), value)

    assert isinstance(mapped, float)
    assert mapped == expected


def test_nan() -> None:
    mapped = map_value(DOUBLE, "NaN")

    assert isinstance(mapped, float)
    assert math.isnan(mapped)


@pytest.mark.parametrize(
    ("value_type", "value", "expected"),
    [
        (signature("decimal", 18, 18), "0.123456789123456789", Decimal("0.123456789123456789")),
        (signature("decimal", 3, 2), "1.20", Decimal("1.20")),
        (signature("number"), "1.5", Decimal("1.5")),
        (signature("number"), "-1E+400", Decimal("-1E+400")),
    ],
)
def test_decimal_and_number(value_type: TypeSignature, value: str, expected: Decimal) -> None:
    mapped = map_value(value_type, value)

    assert isinstance(mapped, Decimal)
    assert mapped == expected
    assert str(mapped) == str(expected)


def test_number_nan_and_infinity() -> None:
    nan = map_value(signature("number"), "NaN")
    infinity = map_value(signature("number"), "-Infinity")

    assert isinstance(nan, Decimal)
    assert nan.is_nan()
    assert infinity == Decimal("-Infinity")


@pytest.mark.parametrize("raw_type", ["varchar", "char", "json", "ipaddress", "HyperLogLog"])
def test_text_types_stay_strings(raw_type: str) -> None:
    assert map_value(signature(raw_type), '"2001:db8::1"') == '"2001:db8::1"'


@pytest.mark.parametrize(("value", "expected"), [("ZWg/", b"eh?"), ("", b""), ("AA==", b"\x00")])
def test_varbinary_is_base64_decoded(value: str, expected: bytes) -> None:
    assert map_value(signature("varbinary"), value) == expected


def test_uuid() -> None:
    expected = uuid.UUID("12151fd2-7586-11e9-8f9e-2a86e4085a59")

    assert map_value(signature("uuid"), "12151fd2-7586-11e9-8f9e-2a86e4085a59") == expected


@pytest.mark.parametrize(
    ("value", "expected"),
    [("0001-01-01", date(1, 1, 1)), ("1582-10-05", date(1582, 10, 5)), ("9999-12-31", date.max)],
)
def test_date(value: str, expected: date) -> None:
    assert map_value(signature("date"), value) == expected


@pytest.mark.parametrize("value", ["-0001-01-01", "0000-01-01", "10000-01-01", "5000000-12-31"])
def test_dates_python_cannot_represent_raise_trino_data_error(value: str) -> None:
    with pytest.raises(TrinoDataError, match=value):
        _ = map_value(signature("date"), value)


@pytest.mark.parametrize(
    ("precision", "value", "expected"),
    [
        (0, "23:59:59", time(23, 59, 59)),
        (3, "12:34:56.123", time(12, 34, 56, 123000)),
        (6, "00:00:00.000001", time(0, 0, 0, 1)),
        (7, "12:34:56.1234561", time(12, 34, 56, 123456)),
        (9, "12:34:56.123456789", time(12, 34, 56, 123457)),
        (12, "00:00:00.000000000001", time(0, 0, 0, 0)),
        (7, "00:00:00.0000005", time(0, 0, 0, 0)),
        (8, "00:00:00.00000050", time(0, 0, 0, 0)),
        (12, "00:00:00.000000500001", time(0, 0, 0, 1)),
        (11, "00:00:00.00000099999", time(0, 0, 0, 1)),
        (10, "23:59:59.9999994999", time(23, 59, 59, 999999)),
        (7, "23:59:59.9999995", time(0, 0, 0)),
        (9, "23:59:59.999999999", time(0, 0, 0)),
    ],
)
def test_time_rounds_to_microseconds_half_even(precision: int, value: str, expected: time) -> None:
    assert map_value(signature("time", precision), value) == expected


def test_time_precision_defaults_to_milliseconds() -> None:
    assert map_value(signature("time"), "01:02:03.4567") == time(1, 2, 3, 457000)


@pytest.mark.parametrize(
    ("offset", "zone"),
    [
        ("-08:00", timezone(-timedelta(hours=8))),
        ("+08:00", timezone(timedelta(hours=8))),
        ("+05:30", timezone(timedelta(hours=5, minutes=30))),
    ],
)
def test_time_with_time_zone(offset: str, zone: timezone) -> None:
    time_type = signature("time with time zone", 9)

    assert map_value(time_type, f"01:02:03{offset}") == time(1, 2, 3, tzinfo=zone)
    assert map_value(time_type, f"23:59:59.999999999{offset}") == time(0, 0, 0, tzinfo=zone)
    assert map_value(time_type, f"12:34:56.123456789{offset}") == time(
        12, 34, 56, 123457, tzinfo=zone
    )


@pytest.mark.parametrize(
    ("precision", "value", "expected"),
    [
        (0, "2001-08-22 03:04:05", datetime(2001, 8, 22, 3, 4, 5)),
        (6, "2001-08-22 03:04:05.321000", datetime(2001, 8, 22, 3, 4, 5, 321000)),
        (9, "2001-08-22 12:34:56.123456789", datetime(2001, 8, 22, 12, 34, 56, 123457)),
        (7, "2001-08-22 00:00:00.0000005", datetime(2001, 8, 22)),
        (7, "2001-08-22 23:59:59.9999995", datetime(2001, 8, 23)),
        (12, "2001-12-31 23:59:59.999999999999", datetime(2002, 1, 1)),
        (3, "0001-01-01 01:23:45.123", datetime(1, 1, 1, 1, 23, 45, 123000)),
    ],
)
def test_timestamp_rounds_into_the_next_second_day_or_year(
    precision: int, value: str, expected: datetime
) -> None:
    assert map_value(signature("timestamp", precision), value) == expected


def test_timestamp_rounded_past_the_python_maximum_raises_trino_data_error() -> None:
    with pytest.raises(TrinoDataError):
        _ = map_value(signature("timestamp", 9), "9999-12-31 23:59:59.999999999")


@pytest.mark.parametrize(
    ("zone_name", "zone"),
    [
        ("-08:00", timezone(-timedelta(hours=8))),
        ("+05:30", timezone(timedelta(hours=5, minutes=30))),
        ("US/Eastern", ZoneInfo("US/Eastern")),
        ("Asia/Kolkata", ZoneInfo("Asia/Kolkata")),
        ("UTC", ZoneInfo("UTC")),
    ],
)
def test_timestamp_with_time_zone_offset_or_name(zone_name: str, zone: timezone) -> None:
    timestamp_type = signature("timestamp with time zone", 9)

    mapped = map_value(timestamp_type, f"2001-08-22 23:59:59.9999999 {zone_name}")

    assert mapped == datetime(2001, 8, 23, tzinfo=zone)
    assert isinstance(mapped, datetime)
    assert mapped.tzinfo == zone


def test_timestamp_with_time_zone_rounds_on_the_wall_clock_across_dst() -> None:
    brussels = ZoneInfo("Europe/Brussels")
    timestamp_type = signature("timestamp with time zone", 9)

    spring = map_value(timestamp_type, "2022-03-27 01:59:59.999999999 Europe/Brussels")
    autumn = map_value(timestamp_type, "2022-10-30 02:59:59.999999999 Europe/Brussels")

    assert spring == datetime(2022, 3, 27, 2, 0, 0, tzinfo=brussels)
    assert autumn == datetime(2022, 10, 30, 3, 0, 0, tzinfo=brussels)


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        ("10-0", relativedelta(years=10)),
        ("-5-0", relativedelta(years=-5)),
        ("0-3", relativedelta(months=3)),
        ("-1-6", relativedelta(years=-1, months=-6)),
        ("178956970-7", relativedelta(years=178956970, months=7)),
        ("-178956970-8", relativedelta(years=-178956970, months=-8)),
    ],
)
def test_interval_year_to_month(value: str, expected: relativedelta) -> None:
    assert map_value(signature("interval year to month"), value) == expected


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        ("2 00:00:00.000", timedelta(days=2)),
        ("-0 00:00:02.000", timedelta(seconds=-2)),
        ("-5 23:59:57.000", timedelta(days=-6, seconds=3)),
        ("1 11:11:11.116", timedelta(days=1, seconds=40271, microseconds=116000)),
        (
            "999999999 23:59:59.999",
            timedelta(days=999999999, hours=23, minutes=59, seconds=59, milliseconds=999),
        ),
        ("-999999999 00:00:00.000", timedelta(days=-999999999)),
    ],
)
def test_interval_day_to_second(value: str, expected: timedelta) -> None:
    assert map_value(signature("interval day to second"), value) == expected


def test_interval_day_to_second_beyond_timedelta_raises_trino_data_error() -> None:
    with pytest.raises(TrinoDataError, match=re.escape("-999999999 00:00:00.001")):
        _ = map_value(signature("interval day to second"), "-999999999 00:00:00.001")


def test_nested_arrays_and_maps_map_every_element() -> None:
    nested = of(of(VARCHAR, signature("timestamp with time zone", 9)))

    mapped = map_value(nested, [{"a": "2001-01-01 01:02:03.123456789 +05:30", "b": None}, None])

    zone = timezone(timedelta(hours=5, minutes=30))
    assert mapped == [{"a": datetime(2001, 1, 1, 1, 2, 3, 123457, tzinfo=zone), "b": None}, None]
    assert map_value(of(of(BIGINT)), [[1, None], [], None]) == [[1, None], [], None]


def test_map_keys_sent_as_strings_are_mapped() -> None:
    mapped = map_value(of(DOUBLE, BIGINT), {"NaN": 1, "-Infinity": None, "1.5": 2})

    assert isinstance(mapped, dict)
    keys: list[object] = list(mapped)
    assert isinstance(keys[0], float)
    assert math.isnan(keys[0])
    assert keys[1:] == [-math.inf, 1.5]
    assert list(mapped.values()) == [1, None, 2]


def test_map_keys_of_other_types() -> None:
    assert map_value(of(signature("boolean"), BIGINT), {"true": 1}) == {True: 1}
    assert map_value(of(signature("decimal", 2, 1), BIGINT), {"1.2": 1}) == {Decimal("1.2"): 1}
    assert map_value(of(signature("varbinary"), BIGINT), {"ZWg/": 1}) == {b"eh?": 1}


def test_row_is_a_tuple_with_named_fields() -> None:
    row_type = row(("x", BIGINT), ("y", DOUBLE), ("z", DOUBLE))

    mapped = map_value(row_type, [1, "Infinity", None])

    assert mapped == (1, math.inf, None)
    assert isinstance(mapped, NamedRowTuple)
    assert mapped.x == 1
    assert mapped.y == math.inf
    assert mapped.z is None
    assert mapped.__annotations__ == {
        "names": ["x", "y", "z"],
        "types": ["bigint", "double", "double"],
    }


def test_nested_named_row() -> None:
    inner = row(("x", BIGINT), ("y", VARCHAR))
    row_type = row(("x", signature("decimal", 3, 2)), ("y", inner))

    mapped = map_value(row_type, ["2.30", [1, "test"]])

    assert isinstance(mapped, NamedRowTuple)
    assert mapped == (Decimal("2.30"), (1, "test"))
    assert mapped.x == Decimal("2.30")
    nested = mapped.y
    assert isinstance(nested, NamedRowTuple)
    assert nested.x == 1
    assert nested.y == "test"
    assert mapped.__annotations__["types"] == ["decimal", "row"]
    assert repr(mapped) == "(x: Decimal('2.30'), y: (x: 1, y: 'test'))"


def test_row_with_duplicate_names_is_ambiguous_by_name() -> None:
    mapped = map_value(row(("x", BIGINT), ("x", DOUBLE)), [1, 2.0])

    assert mapped == (1, 2.0)
    with pytest.raises(ValueError, match="Ambiguous row field reference: x"):
        _ = getattr(mapped, "x")  # noqa: B009
    assert repr(mapped) == "(1, 2.0)"


def test_row_without_names_and_unknown_attributes() -> None:
    mapped = map_value(row((None, BIGINT), (None, VARCHAR)), [1, "a"])

    assert mapped == (1, "a")
    assert repr(mapped) == "(1, 'a')"
    assert isinstance(mapped, NamedRowTuple)
    assert mapped.__annotations__["names"] == [None, None]
    assert not hasattr(mapped, "x")


def test_named_row_survives_pickling() -> None:
    original = NamedRowTuple(["Alice", 38], ["name", "age"], ["varchar", "integer"])

    restored = pickle.loads(pickle.dumps(original))  # noqa: S301

    assert restored == original
    assert restored.name == "Alice"
    assert repr(restored) == "(name: 'Alice', age: 38)"
    assert restored.__annotations__ == original.__annotations__


def test_row_mapper_maps_each_column_by_its_own_type() -> None:
    columns: list[Column] = [
        {"name": "n", "type": "bigint", "typeSignature": BIGINT},
        {"name": "d", "type": "date", "typeSignature": signature("date")},
    ]

    assert RowMapper(columns).map(["7", "2001-08-22"]) == [7, date(2001, 8, 22)]
