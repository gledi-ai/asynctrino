import base64
import uuid
from collections.abc import Callable, Iterable, Mapping, Sequence
from datetime import date, datetime, time, timedelta, timezone, tzinfo
from decimal import Decimal
from functools import partial
from typing import NotRequired, Self, TypedDict, cast
from zoneinfo import ZoneInfo

from dateutil.relativedelta import relativedelta

from asynctrino._core.status import Column, Row, TypeSignature
from asynctrino.exceptions import TrinoDataError

type ValueMapper = Callable[[object], object]
"""Maps one non-null value as decoded from JSON to its Python counterpart."""

DEFAULT_TEMPORAL_PRECISION = 3
_PYTHON_TEMPORAL_PRECISION = 6
_TIME_LENGTH = len("HH:MM:SS")
_TIMESTAMP_LENGTH = len("YYYY-MM-DD HH:MM:SS")
_OFFSET_LENGTH = len("+HH:MM")


class _FieldName(TypedDict):
    name: str


class _NamedType(TypedDict):
    fieldName: NotRequired[_FieldName]
    typeSignature: TypeSignature


class NamedRowTuple(tuple[object, ...]):  # noqa: SLOT001  # fields live in __dict__
    """A ROW value: a tuple whose uniquely named fields are also attributes.

    ``__annotations__["names"]`` and ``__annotations__["types"]`` hold each field's name (``None``
    when unnamed) and raw Trino type, as in Upstream. Reading a field name shared by several
    fields raises ``ValueError``.
    """

    _names: list[str | None]

    def __new__(
        cls, values: Iterable[object], names: Sequence[str | None], types: Sequence[str]
    ) -> Self:
        del names, types
        return super().__new__(cls, values)

    def __init__(
        self, values: Iterable[object], names: Sequence[str | None], types: Sequence[str]
    ) -> None:
        del values
        self._names = list(names)
        self.__annotations__ = {"names": list(names), "types": list(types)}
        for name, value in zip(names, self, strict=False):
            if name is not None and names.count(name) == 1:
                setattr(self, name, value)

    def __getattr__(self, name: str) -> object:
        if name in self.__dict__.get("_names", ()):
            msg = f"Ambiguous row field reference: {name}"
            raise ValueError(msg)
        msg = f"{type(self).__name__!r} object has no attribute {name!r}"
        raise AttributeError(msg)

    def __getnewargs__(self) -> tuple[tuple[object, ...], tuple[()], tuple[()]]:
        return (tuple(self), (), ())

    def __repr__(self) -> str:
        fields = (
            f"{name}: {value!r}"
            if name is not None and self._names.count(name) == 1
            else repr(value)
            for name, value in zip(self._names, self, strict=False)
        )
        return f"({', '.join(fields)})"


class RowMapper:
    """Maps every value of a row to the Python type of its column."""

    def __init__(self, columns: Sequence[Column]) -> None:
        self._mappers = [value_mapper(column["typeSignature"]) for column in columns]

    def map(self, row: Row) -> Row:
        """Map ``row``'s values, raising :class:`TrinoDataError` for one Python can't represent."""
        return [_map_value(mapper, value) for mapper, value in zip(self._mappers, row, strict=True)]


def _map_value(mapper: ValueMapper, value: object) -> object:
    if value is None:
        return None
    try:
        return mapper(value)
    except (ValueError, OverflowError) as e:
        msg = f"Could not convert '{value}' into the associated python type"
        raise TrinoDataError(msg) from e


def value_mapper(signature: TypeSignature) -> ValueMapper:
    """The mapper for values of the type described by ``signature``.

    Types without a mapping, such as ``ipaddress`` and the digests, keep their JSON value.
    """
    raw_type = signature["rawType"]
    arguments = [argument["value"] for argument in signature["arguments"]]
    if raw_type in _PRIMITIVE_MAPPERS:
        return _PRIMITIVE_MAPPERS[raw_type]
    if raw_type in _TEMPORAL_MAPPERS:
        precision = DEFAULT_TEMPORAL_PRECISION if len(arguments) == 0 else arguments[0]
        return _TEMPORAL_MAPPERS[raw_type](cast("int", precision))
    match raw_type:
        case "array":
            return _array_mapper(value_mapper(cast("TypeSignature", arguments[0])))
        case "map":
            key_signature, value_signature = cast("list[TypeSignature]", arguments)
            return _map_mapper(value_mapper(key_signature), value_mapper(value_signature))
        case "row":
            return _row_mapper(cast("list[_NamedType]", arguments))
        case _:
            return _identity


def _identity(value: object) -> object:
    return value


def _boolean(value: object) -> bool:
    if isinstance(value, bool):
        return value
    match str(value).lower():
        case "true":
            return True
        case "false":
            return False
        case _:
            msg = f"the Coordinator sent unexpected value {value} of type {type(value)} for boolean"
            raise ValueError(msg)


def _integer(value: object) -> int:
    return value if isinstance(value, int) else int(str(value))


def _double(value: object) -> float:
    return float(value) if isinstance(value, int | float) else float(str(value))


def _decimal(value: object) -> Decimal:
    return Decimal(str(value))


def _binary(value: object) -> bytes:
    return base64.b64decode(str(value))


def _date(value: object) -> date:
    return date.fromisoformat(str(value))


def _uuid(value: object) -> uuid.UUID:
    return uuid.UUID(str(value))


def _fraction(digits: str, precision: int) -> timedelta:
    """Fractional seconds rounded half-even to ``precision`` digits, at most microseconds."""
    if digits == "":
        return timedelta()
    quantum = Decimal(1).scaleb(-min(precision, _PYTHON_TEMPORAL_PRECISION))
    rounded = Decimal(f"0.{digits}").quantize(quantum)
    return timedelta(microseconds=int(rounded.scaleb(_PYTHON_TEMPORAL_PRECISION)))


def _tzinfo(zone: str) -> tzinfo:
    if zone[0] in "+-":
        offset = timedelta(hours=int(zone[1:3]), minutes=int(zone[4:6]))
        return timezone(-offset if zone[0] == "-" else offset)
    return ZoneInfo(zone)


def _time_mapper(precision: int, *, with_time_zone: bool) -> ValueMapper:
    def time_value(value: object) -> time:
        text = str(value)
        zone: tzinfo | None = None
        if with_time_zone:
            text, zone = text[:-_OFFSET_LENGTH], _tzinfo(text[-_OFFSET_LENGTH:])
        whole = time.fromisoformat(text[:_TIME_LENGTH])
        moment = datetime.combine(date.min, whole) + _fraction(text[_TIME_LENGTH + 1 :], precision)
        return moment.time().replace(tzinfo=zone)

    return time_value


def _timestamp_mapper(precision: int, *, with_time_zone: bool) -> ValueMapper:
    def timestamp_value(value: object) -> datetime:
        text = str(value)
        zone: tzinfo | None = None
        if with_time_zone:
            text, zone_name = text.rsplit(" ", 1)
            zone = _tzinfo(zone_name)
        whole = datetime.fromisoformat(text[:_TIMESTAMP_LENGTH]).replace(tzinfo=zone)
        return whole + _fraction(text[_TIMESTAMP_LENGTH + 1 :], precision)

    return timestamp_value


def _interval_year_to_month(value: object) -> relativedelta:
    text = str(value)
    sign = -1 if text.startswith("-") else 1
    years, months = text.removeprefix("-").split("-")
    return relativedelta(years=sign * int(years), months=sign * int(months))


def _interval_day_to_second(value: object) -> timedelta:
    text = str(value)
    sign = -1 if text.startswith("-") else 1
    days, clock = text.removeprefix("-").split(" ")
    hours, minutes, seconds = clock.split(":")
    whole_seconds, milliseconds = seconds.split(".")
    return sign * timedelta(
        days=int(days),
        hours=int(hours),
        minutes=int(minutes),
        seconds=int(whole_seconds),
        milliseconds=int(milliseconds),
    )


def _array_mapper(element: ValueMapper) -> ValueMapper:
    def array_value(value: object) -> list[object]:
        return [_nullable(element, v) for v in cast("list[object]", value)]

    return array_value


def _map_mapper(key: ValueMapper, element: ValueMapper) -> ValueMapper:
    def map_value(value: object) -> dict[object, object]:
        entries = cast("dict[str, object]", value)
        return {key(k): _nullable(element, v) for k, v in entries.items()}

    return map_value


def _row_mapper(fields: Sequence[_NamedType]) -> ValueMapper:
    signatures = [field["typeSignature"] for field in fields]
    mappers = [value_mapper(signature) for signature in signatures]
    names = [field["fieldName"]["name"] if "fieldName" in field else None for field in fields]
    types = [signature["rawType"] for signature in signatures]

    def row_value(value: object) -> NamedRowTuple:
        values = cast("list[object]", value)
        mapped = (_nullable(m, v) for m, v in zip(mappers, values, strict=True))
        return NamedRowTuple(mapped, names, types)

    return row_value


def _nullable(mapper: ValueMapper, value: object) -> object:
    return None if value is None else mapper(value)


_PRIMITIVE_MAPPERS: Mapping[str, ValueMapper] = {
    "boolean": _boolean,
    "tinyint": _integer,
    "smallint": _integer,
    "integer": _integer,
    "bigint": _integer,
    "real": _double,
    "double": _double,
    "decimal": _decimal,
    "number": _decimal,
    "varchar": str,
    "char": str,
    "json": str,
    "varbinary": _binary,
    "date": _date,
    "interval year to month": _interval_year_to_month,
    "interval day to second": _interval_day_to_second,
    "uuid": _uuid,
}

_TEMPORAL_MAPPERS: Mapping[str, Callable[[int], ValueMapper]] = {
    "time": partial(_time_mapper, with_time_zone=False),
    "time with time zone": partial(_time_mapper, with_time_zone=True),
    "timestamp": partial(_timestamp_mapper, with_time_zone=False),
    "timestamp with time zone": partial(_timestamp_mapper, with_time_zone=True),
}
