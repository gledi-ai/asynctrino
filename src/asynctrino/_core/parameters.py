"""Query parameters bound by formatting them as Trino literals in an ``EXECUTE IMMEDIATE``."""

import math
import re
import uuid
from collections.abc import Sequence
from datetime import date, datetime, time, timedelta, tzinfo
from decimal import Decimal
from functools import singledispatch
from zoneinfo import ZoneInfo

from asynctrino.exceptions import NotSupportedError

_ZONE_PATTERN = re.compile(r"[A-Za-z0-9_+\-/:]+")


def execute_immediate(operation: str, parameters: Sequence[object]) -> str:
    """The ``EXECUTE IMMEDIATE`` statement that runs ``operation`` with ``parameters`` bound."""
    return f"EXECUTE IMMEDIATE {_quote(operation)} USING {_format_all(parameters)}"


@singledispatch
def format_parameter(value: object) -> str:
    """Format ``value`` as the Trino literal it binds to, following Upstream's rules.

    Raises:
        NotSupportedError: ``value``, or a value nested in it, has no Trino literal.
    """
    msg = f"Query parameter of type '{type(value)}' is not supported."
    raise NotSupportedError(msg)


@format_parameter.register(type(None))
def _format_null(_: None) -> str:
    return "NULL"


@format_parameter.register
def _format_boolean(value: bool) -> str:  # noqa: FBT001
    return "true" if value else "false"


@format_parameter.register
def _format_integer(value: int) -> str:
    return int.__repr__(value)


@format_parameter.register
def _format_double(value: float) -> str:
    if math.isnan(value):
        return "nan()"
    if math.isinf(value):
        return "infinity()" if value > 0 else "-infinity()"
    return f"DOUBLE '{float.__repr__(value)}'"


@format_parameter.register
def _format_decimal(value: Decimal) -> str:
    return f"DECIMAL '{Decimal.__format__(value, 'f')}'"


@format_parameter.register
def _quote(value: str) -> str:
    escaped = str.replace(value, "'", "''")
    return f"'{escaped}'"


@format_parameter.register(bytes)
@format_parameter.register(bytearray)
@format_parameter.register(memoryview)
def _format_varbinary(value: bytes | bytearray | memoryview) -> str:
    return f"X'{bytes(value).hex()}'"


@format_parameter.register
def _format_date(value: date) -> str:
    return f"DATE '{date.strftime(value, '%Y-%m-%d')}'"


@format_parameter.register
def _format_timestamp(value: datetime) -> str:
    text = datetime.strftime(value, "%Y-%m-%d %H:%M:%S.%f")
    zone = value.tzinfo
    offset = value.utcoffset()
    if zone is None or offset is None:
        return f"TIMESTAMP '{text}'"
    name = zone.key if isinstance(zone, ZoneInfo) else _format_offset(offset)
    return f"TIMESTAMP '{text} {_checked_zone(name)}'"


@format_parameter.register
def _format_time(value: time) -> str:
    text = time.strftime(value, "%H:%M:%S.%f")
    zone = value.tzinfo
    if zone is None:
        return f"TIME '{text}'"
    return f"TIME '{text} {_checked_zone(_format_offset(_current_offset(zone)))}'"


@format_parameter.register
def _format_uuid(value: uuid.UUID) -> str:
    return f"UUID '{uuid.UUID(str(value))}'"


@format_parameter.register(list)
def _format_array(value: list[object]) -> str:
    return f"ARRAY[{_format_all(value)}]"


@format_parameter.register(tuple)
def _format_row(value: tuple[object, ...]) -> str:
    return f"ROW({_format_all(value)})"


@format_parameter.register(dict)
def _format_map(value: dict[object, object]) -> str:
    keys = _format_all(list(value.keys()))
    values = _format_all(list(value.values()))
    return f"MAP(ARRAY[{keys}], ARRAY[{values}])"


def _format_all(values: Sequence[object]) -> str:
    return ",".join(map(format_parameter, values))


def _current_offset(zone: tzinfo) -> timedelta:
    offset = zone.utcoffset(None)
    if offset is None:
        # A time has no date, so a named zone's offset is taken as of now, like Upstream.
        offset = datetime.now(zone).utcoffset()
    return timedelta() if offset is None else offset


def _format_offset(offset: timedelta) -> str:
    sign = "-" if offset < timedelta() else "+"
    hours, minutes = divmod(abs(offset) // timedelta(minutes=1), 60)
    return f"{sign}{hours:02d}:{minutes:02d}"


def _checked_zone(name: str) -> str:
    if _ZONE_PATTERN.fullmatch(name) is None:
        msg = f"Query parameter has an unsupported time zone {name!r}."
        raise NotSupportedError(msg)
    return name
