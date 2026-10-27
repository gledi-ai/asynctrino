import io
import math
import struct
import uuid
from datetime import UTC, date, datetime, time, timedelta, timezone, tzinfo
from decimal import Decimal
from typing import override
from zoneinfo import ZoneInfo

import pytest

from asynctrino._core.parameters import execute_immediate, format_parameter
from asynctrino.exceptions import NotSupportedError

NOW = datetime(2026, 1, 15, tzinfo=UTC)


@pytest.mark.parametrize(
    ("value", "literal"),
    [
        (None, "NULL"),
        (True, "true"),
        (False, "false"),
        (0, "0"),
        (-42, "-42"),
        (2**70, "1180591620717411303424"),
        (1.5, "DOUBLE '1.5'"),
        (-2.0, "DOUBLE '-2.0'"),
        (1e-300, "DOUBLE '1e-300'"),
        (math.inf, "infinity()"),
        (-math.inf, "-infinity()"),
        (math.nan, "nan()"),
        (Decimal("1112.142857"), "DECIMAL '1112.142857'"),
        (Decimal("1E+3"), "DECIMAL '1000'"),
        (Decimal("-1.5E-7"), "DECIMAL '-0.00000015'"),
        ("", "''"),
        ("six'", "'six'''"),
        ("''", "''''''"),
        ("ünïcödé ✓", "'ünïcödé ✓'"),
    ],
)
def test_scalars_format_as_trino_literals(value: object, literal: str) -> None:
    assert format_parameter(value, now=NOW) == literal


@pytest.mark.parametrize(
    ("value", "literal"),
    [
        (b"", "X''"),
        (b"\x00\xffab", "X'00ff6162'"),
        (bytearray(b"\x0a\x0b"), "X'0a0b'"),
        (memoryview(b"\xde\xad"), "X'dead'"),
        (
            uuid.UUID("12151fd2-7586-11e9-8f9e-2a86e4085a59"),
            "UUID '12151fd2-7586-11e9-8f9e-2a86e4085a59'",
        ),
    ],
)
def test_binary_and_uuid_format_as_trino_literals(value: object, literal: str) -> None:
    assert format_parameter(value, now=NOW) == literal


BRUSSELS = ZoneInfo("Europe/Brussels")
KOLKATA = ZoneInfo("Asia/Kolkata")


@pytest.mark.parametrize(
    ("value", "literal"),
    [
        (date(2002, 3, 4), "DATE '2002-03-04'"),
        (date(1, 1, 1), "DATE '0001-01-01'"),
        (date(9999, 12, 31), "DATE '9999-12-31'"),
        (datetime(2020, 1, 2, 3, 4, 5, 6), "TIMESTAMP '2020-01-02 03:04:05.000006'"),
        (
            datetime(2020, 1, 2, 3, 4, 5, 123456, tzinfo=BRUSSELS),
            "TIMESTAMP '2020-01-02 03:04:05.123456 Europe/Brussels'",
        ),
        (
            datetime(2020, 1, 2, 3, 4, 5, tzinfo=UTC),
            "TIMESTAMP '2020-01-02 03:04:05.000000 +00:00'",
        ),
        (
            datetime(2020, 1, 2, 3, 4, 5, tzinfo=timezone(timedelta(hours=-3, minutes=-30))),
            "TIMESTAMP '2020-01-02 03:04:05.000000 -03:30'",
        ),
        (time(3, 4, 5, 6), "TIME '03:04:05.000006'"),
        (time(23, 59, 59, 999999), "TIME '23:59:59.999999'"),
        (time(3, 4, 5, tzinfo=KOLKATA), "TIME '03:04:05.000000 +05:30'"),
        (
            time(3, 4, 5, tzinfo=timezone(timedelta(hours=-3, minutes=-30))),
            "TIME '03:04:05.000000 -03:30'",
        ),
        (time(3, 4, 5, tzinfo=UTC), "TIME '03:04:05.000000 +00:00'"),
    ],
)
def test_dates_and_times_format_as_trino_literals(value: object, literal: str) -> None:
    assert format_parameter(value, now=NOW) == literal


class Hostile(tzinfo):
    """A fixed +01:00 zone whose name tries to break out of the literal."""

    def utcoffset(self, _: datetime | None) -> timedelta:
        return timedelta(hours=1)

    def tzname(self, _: datetime | None) -> str:
        return "x'; DROP TABLE t; --"

    def dst(self, _: datetime | None) -> timedelta:
        return timedelta()


def test_a_custom_zone_is_rendered_as_its_offset_never_its_name() -> None:
    literal = format_parameter(datetime(2020, 1, 2, 3, 4, 5, tzinfo=Hostile()), now=NOW)

    assert literal == "TIMESTAMP '2020-01-02 03:04:05.000000 +01:00'"


def test_a_zone_key_outside_the_safe_alphabet_is_not_supported() -> None:
    header = b"TZif" + bytes(16) + struct.pack(">6l", 0, 0, 0, 0, 1, 4)
    utc = header + struct.pack(">lBB", 0, 0, 0) + b"UTC\0"
    zone = ZoneInfo.from_file(io.BytesIO(utc), key="UTC' OR '1'='1")

    with pytest.raises(NotSupportedError, match="time zone"):
        _ = format_parameter(datetime(2020, 1, 2, tzinfo=zone), now=NOW)


@pytest.mark.parametrize(
    ("value", "literal"),
    [
        ([], "ARRAY[]"),
        ([1, None, 3], "ARRAY[1,NULL,3]"),
        ([[1.5], []], "ARRAY[ARRAY[DOUBLE '1.5'],ARRAY[]]"),
        (("a", 1, True), "ROW('a',1,true)"),
        (((1, [2]),), "ROW(ROW(1,ARRAY[2]))"),
        ({}, "MAP(ARRAY[], ARRAY[])"),
        ({"a": 1, "b's": None}, "MAP(ARRAY['a','b''s'], ARRAY[1,NULL])"),
        ({1: [date(2020, 1, 2)]}, "MAP(ARRAY[1], ARRAY[ARRAY[DATE '2020-01-02']])"),
    ],
)
def test_lists_tuples_and_dicts_format_as_array_row_and_map(value: object, literal: str) -> None:
    assert format_parameter(value, now=NOW) == literal


class Opaque:
    pass


@pytest.mark.parametrize(
    ("value", "type_name"),
    [
        (Opaque(), "Opaque"),
        ({1, 2}, "set"),
        (timedelta(days=1), "timedelta"),
        (1j, "complex"),
        ([1, object()], "object"),
    ],
)
def test_unsupported_types_raise_not_supported_error(value: object, type_name: str) -> None:
    expected = f"Query parameter of type '.*{type_name}.*' is not supported"
    with pytest.raises(NotSupportedError, match=expected):
        _ = format_parameter(value, now=NOW)


def test_execute_immediate_quotes_the_statement_and_joins_the_parameters() -> None:
    assert execute_immediate("SELECT ?, 'x' || ?", [None, "y'z"], now=NOW) == (
        "EXECUTE IMMEDIATE 'SELECT ?, ''x'' || ?' USING NULL,'y''z'"
    )


class SneakyStr(str):
    __slots__ = ()

    @override
    def replace(self, *_: object, **__: object) -> str:
        return str(self)

    @override
    def __format__(self, _: str) -> str:
        return "' OR 1=1 --"


class SneakyInt(int):
    @override
    def __format__(self, _: str) -> str:
        return "1 OR 1=1"

    @override
    def __repr__(self) -> str:
        return "1 OR 1=1"


def test_subclasses_cannot_change_how_their_values_are_escaped() -> None:
    assert format_parameter(SneakyStr("it's"), now=NOW) == "'it''s'"
    assert format_parameter(SneakyInt(7), now=NOW) == "7"


@pytest.mark.parametrize(
    ("now", "offset"),
    [(datetime(2026, 1, 15, tzinfo=UTC), "+01:00"), (datetime(2026, 7, 15, tzinfo=UTC), "+02:00")],
)
def test_a_time_in_a_named_zone_takes_the_offset_in_force_now(now: datetime, offset: str) -> None:
    literal = format_parameter(time(3, 4, 5, tzinfo=BRUSSELS), now=now)

    assert literal == f"TIME '03:04:05.000000 {offset}'"
