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


def execute_immediate(operation: str, parameters: Sequence[object], *, now: datetime) -> str:
    """The ``EXECUTE IMMEDIATE`` statement that runs ``operation`` with ``parameters`` bound.

    ``now`` is as :func:`format_parameter` describes.
    """
    return f"EXECUTE IMMEDIATE {_quote(operation, now)} USING {_format_all(parameters, now)}"


def format_parameter(value: object, *, now: datetime) -> str:
    """Format ``value`` as the Trino literal it binds to, following Upstream's rules.

    Args:
        value: The parameter value.
        now: The current timezone-aware time. A ``time`` has no date, so one in a named zone
            takes the offset in force at ``now``, like Upstream.

    Raises:
        NotSupportedError: ``value``, or a value nested in it, has no Trino literal.
    """
    return _literal(value, now)


@singledispatch
def _literal(value: object, _now: datetime) -> str:
    msg = f"Query parameter of type '{type(value)}' is not supported."
    raise NotSupportedError(msg)


@_literal.register(type(None))
def _format_null(_: None, _now: datetime) -> str:
    return "NULL"


@_literal.register
def _format_boolean(value: bool, _now: datetime) -> str:  # noqa: FBT001  # singledispatch on the value
    return "true" if value else "false"


@_literal.register
def _format_integer(value: int, _now: datetime) -> str:
    return int.__repr__(value)


@_literal.register
def _format_double(value: float, _now: datetime) -> str:
    if math.isnan(value):
        return "nan()"
    if math.isinf(value):
        return "infinity()" if value > 0 else "-infinity()"
    return f"DOUBLE '{float.__repr__(value)}'"


@_literal.register
def _format_decimal(value: Decimal, _now: datetime) -> str:
    return f"DECIMAL '{Decimal.__format__(value, 'f')}'"


@_literal.register
def _quote(value: str, _now: datetime) -> str:
    escaped = str.replace(value, "'", "''")
    return f"'{escaped}'"


@_literal.register(bytes)
@_literal.register(bytearray)
@_literal.register(memoryview)
def _format_varbinary(value: bytes | bytearray | memoryview, _now: datetime) -> str:
    return f"X'{bytes(value).hex()}'"


@_literal.register
def _format_date(value: date, _now: datetime) -> str:
    return f"DATE '{date.strftime(value, '%Y-%m-%d')}'"


@_literal.register
def _format_timestamp(value: datetime, _now: datetime) -> str:
    text = datetime.strftime(value, "%Y-%m-%d %H:%M:%S.%f")
    zone = value.tzinfo
    offset = value.utcoffset()
    if zone is None or offset is None:
        return f"TIMESTAMP '{text}'"
    name = zone.key if isinstance(zone, ZoneInfo) else _format_offset(offset)
    return f"TIMESTAMP '{text} {_checked_zone(name)}'"


@_literal.register
def _format_time(value: time, now: datetime) -> str:
    text = time.strftime(value, "%H:%M:%S.%f")
    zone = value.tzinfo
    if zone is None:
        return f"TIME '{text}'"
    return f"TIME '{text} {_checked_zone(_format_offset(_current_offset(zone, now)))}'"


@_literal.register
def _format_uuid(value: uuid.UUID, _now: datetime) -> str:
    return f"UUID '{uuid.UUID(str(value))}'"


@_literal.register(list)
def _format_array(value: list[object], now: datetime) -> str:
    return f"ARRAY[{_format_all(value, now)}]"


@_literal.register(tuple)
def _format_row(value: tuple[object, ...], now: datetime) -> str:
    return f"ROW({_format_all(value, now)})"


@_literal.register(dict)
def _format_map(value: dict[object, object], now: datetime) -> str:
    keys = _format_all(list(value.keys()), now)
    values = _format_all(list(value.values()), now)
    return f"MAP(ARRAY[{keys}], ARRAY[{values}])"


def _format_all(values: Sequence[object], now: datetime) -> str:
    return ",".join(_literal(value, now) for value in values)


def _current_offset(zone: tzinfo, now: datetime) -> timedelta:
    offset = zone.utcoffset(None)
    if offset is None:
        offset = now.astimezone(zone).utcoffset()
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
