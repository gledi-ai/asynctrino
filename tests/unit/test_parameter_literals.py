import math
import uuid
from datetime import UTC, date, datetime, time, timedelta, timezone
from decimal import Decimal
from zoneinfo import ZoneInfo

import pytest

from asynctrino._core.parameters import execute_immediate, format_parameter
from asynctrino.exceptions import NotSupportedError


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
    assert format_parameter(value) == literal


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
    assert format_parameter(value) == literal


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
            "TIMESTAMP '2020-01-02 03:04:05.000000 UTC'",
        ),
        (
            datetime(2020, 1, 2, 3, 4, 5, tzinfo=timezone(timedelta(hours=-3, minutes=-30))),
            "TIMESTAMP '2020-01-02 03:04:05.000000 UTC-03:30'",
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
    assert format_parameter(value) == literal


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
    assert format_parameter(value) == literal


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
        _ = format_parameter(value)


def test_execute_immediate_quotes_the_statement_and_joins_the_parameters() -> None:
    assert execute_immediate("SELECT ?, 'x' || ?", [None, "y'z"]) == (
        "EXECUTE IMMEDIATE 'SELECT ?, ''x'' || ?' USING NULL,'y''z'"
    )
