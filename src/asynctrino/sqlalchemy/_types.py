"""Trino-specific SQLAlchemy types, and the parser for Trino type strings."""

import json
import re
from collections.abc import Callable, Iterator, Sequence
from typing import Any, override

from sqlalchemy import exc, func, util
from sqlalchemy.engine.interfaces import Dialect
from sqlalchemy.sql import sqltypes
from sqlalchemy.sql.elements import BindParameter, ColumnElement
from sqlalchemy.sql.operators import OperatorClass
from sqlalchemy.sql.type_api import (
    TypeDecorator,
    TypeEngine,
    _BindProcessorType,
    _LiteralProcessorType,
    to_instance,
)
from sqlalchemy.types import DOUBLE

from asynctrino._core.parameters import format_parameter

type SQLType = TypeEngine[Any]  # pyrefly: ignore[explicit-any]
"""Any SQLAlchemy type instance."""

type SQLTypeArgument = SQLType | type[SQLType]
"""A SQLAlchemy type, as an instance or a class."""

__all__ = [
    "DOUBLE",
    "JSON",
    "MAP",
    "ROW",
    "TIME",
    "TIMESTAMP",
    "VARBINARY",
    "aware_split",
    "parse_sqltype",
]


class MAP(TypeEngine[dict[object, object]]):
    """Trino's ``MAP(key, value)``; values are :class:`dict`."""

    __visit_name__ = "MAP"
    operator_classes = OperatorClass.BASE | OperatorClass.COMPARISON

    def __init__(
        self,
        key_type: SQLTypeArgument,
        value_type: SQLTypeArgument,
    ) -> None:
        self.key_type: SQLType = to_instance(key_type)
        self.value_type: SQLType = to_instance(value_type)

    @property
    @override
    def python_type(self) -> type[dict[object, object]]:
        return dict


class ROW(TypeEngine[tuple[object, ...]]):
    """Trino's ``ROW(name type, ...)``; values are tuples."""

    __visit_name__ = "ROW"
    operator_classes = OperatorClass.BASE | OperatorClass.COMPARISON

    def __init__(self, attr_types: Sequence[tuple[str | None, SQLTypeArgument]]) -> None:
        self.attr_types: list[tuple[str | None, SQLType]] = [
            (name, to_instance(attr_type)) for name, attr_type in attr_types
        ]

    @property
    @override
    def python_type(self) -> type[tuple[object, ...]]:
        return tuple


class TIME(sqltypes.TIME):
    """Trino's ``TIME[(precision)] [WITH TIME ZONE]``; ``precision`` is 0 to 12."""

    __visit_name__ = "TIME"

    def __init__(self, precision: int | None = None, *, timezone: bool = False) -> None:
        super().__init__(timezone=timezone)
        self.precision: int | None = precision


class TIMESTAMP(sqltypes.TIMESTAMP):
    """Trino's ``TIMESTAMP[(precision)] [WITH TIME ZONE]``; ``precision`` is 0 to 12."""

    __visit_name__ = "TIMESTAMP"

    def __init__(self, precision: int | None = None, *, timezone: bool = False) -> None:
        super().__init__(timezone=timezone)
        self.precision: int | None = precision


class JSON(TypeDecorator[object]):
    """Trino's ``JSON``; bound values are serialized and parsed with ``JSON_PARSE``."""

    impl = sqltypes.JSON
    cache_ok = True

    @override
    def bind_expression(self, bindparam: BindParameter[object]) -> ColumnElement[object]:
        return func.JSON_PARSE(bindparam)


class VARBINARY(sqltypes.VARBINARY):
    """Trino's ``VARBINARY``; literals render as ``X'cafe'``."""

    __visit_name__ = "VARBINARY"

    @override
    def literal_processor(self, dialect: Dialect) -> _LiteralProcessorType[bytes]:
        return _varbinary_literal


def _varbinary_literal(value: object) -> str:
    if isinstance(value, bytes | bytearray | memoryview):
        return format_parameter(value)
    msg = f"VARBINARY literal must be bytes, bytearray or memoryview, got {type(value).__name__}"
    raise exc.CompileError(msg)


class FloatAsDouble(sqltypes.Float[float]):
    """Every float type as the dialect binds and renders it: as a ``DOUBLE``.

    Trino reads a bare ``1.5`` as a ``DECIMAL``, so literals render as ``DOUBLE '1.5'``, and
    bound values are converted to :class:`float`, which asynctrino binds as a ``DOUBLE``.
    """

    @override
    def bind_processor(self, dialect: Dialect) -> _BindProcessorType[float]:
        return _to_float

    @override
    def literal_processor(self, dialect: Dialect) -> _LiteralProcessorType[float]:
        return _double_literal


def _to_float(value: float | None) -> float | None:
    return None if value is None else float(value)


def _double_literal(value: float) -> str:
    return format_parameter(float(value))


class JSONIndexType(sqltypes.JSON.JSONIndexType):
    """A JSON index, ``column["key"]`` or ``column[0]``, as the path ``$["key"]`` or ``$[0]``."""

    @override
    def bind_processor(self, dialect: Dialect) -> _BindProcessorType[object]:
        return _index_path

    @override
    def literal_processor(self, dialect: Dialect) -> _LiteralProcessorType[object]:
        return _literal(_index_path)


class JSONPathType(sqltypes.JSON.JSONPathType):
    """A JSON path, ``column["a", 0]``, as the JSON path ``$["a"][0]``."""

    @override
    def bind_processor(self, dialect: Dialect) -> _BindProcessorType[object]:
        return _path

    @override
    def literal_processor(self, dialect: Dialect) -> _LiteralProcessorType[object]:
        return _literal(_path)


def _index_path(value: object) -> str:
    return "$" + _path_element(value)


def _path(value: object) -> str:
    if not isinstance(value, Sequence) or isinstance(value, str):
        msg = f"A JSON path must be a sequence of keys and indexes, got {value!r}"
        raise TypeError(msg)
    elements: Sequence[object] = value
    return "$" + "".join(_path_element(element) for element in elements)


def _path_element(value: object) -> str:
    if isinstance(value, int) and not isinstance(value, bool):
        return f"[{value:d}]"
    if isinstance(value, str):
        return f"[{json.dumps(value, ensure_ascii=False)}]"
    msg = f"A JSON path element must be a str key or an int index, got {value!r}"
    raise TypeError(msg)


def _literal(path: Callable[[object], str]) -> _LiteralProcessorType[object]:
    def process(value: object) -> str:
        return format_parameter(path(value))

    return process


_TYPES: dict[str, Callable[..., SQLType]] = {
    "boolean": sqltypes.BOOLEAN,
    "tinyint": sqltypes.SMALLINT,
    "smallint": sqltypes.SMALLINT,
    "int": sqltypes.INTEGER,
    "integer": sqltypes.INTEGER,
    "bigint": sqltypes.BIGINT,
    "real": sqltypes.REAL,
    "double": DOUBLE,
    "decimal": sqltypes.DECIMAL,
    "number": sqltypes.NUMERIC,
    "varchar": sqltypes.VARCHAR,
    "char": sqltypes.CHAR,
    "varbinary": VARBINARY,
    "json": JSON,
    "date": sqltypes.DATE,
    "uuid": sqltypes.Uuid,
}
"""Trino's scalar types by name; see https://trino.io/docs/current/language/types.html."""

_TYPE_STRING = re.compile(
    r"^(?P<name>\w+)\s*(?:\((?P<options>.*)\))?(?P<with_time_zone>\s+with time zone)?$",
    re.DOTALL | re.IGNORECASE,
)


def parse_sqltype(type_str: str) -> SQLType:  # noqa: PLR0911
    """The SQLAlchemy type of a Trino type string, such as ``map(varchar, array(integer))``.

    Names are case-insensitive, ``ROW`` field names may be double-quoted, and nested arrays
    become one :class:`~sqlalchemy.types.ARRAY` with ``dimensions``. A type with no SQLAlchemy
    equivalent warns and gives :data:`~sqlalchemy.types.NULLTYPE`.
    """
    match = _TYPE_STRING.match(type_str.strip())
    if match is None:
        util.warn(f"Could not parse type name '{type_str}'")
        return sqltypes.NULLTYPE
    name = str(match["name"]).lower()
    options: str | None = match["options"]
    with_time_zone = match["with_time_zone"] is not None
    match name, options:
        case "array", str():
            return _array(parse_sqltype(options))
        case "map", str():
            key_type, value_type = aware_split(options, maxsplit=1)
            return MAP(parse_sqltype(key_type), parse_sqltype(value_type))
        case "row", str():
            return ROW([_row_field(field) for field in aware_split(options)])
        case "time", _:
            return TIME(_precision(options), timezone=with_time_zone)
        case "timestamp", _:
            return TIMESTAMP(_precision(options), timezone=with_time_zone)
        case _ if name in _TYPES and not with_time_zone:
            arguments = [] if options is None else [int(option) for option in options.split(",")]
            return _TYPES[name](*arguments)
        case _:
            util.warn(f"Did not recognize type '{type_str}'")
            return sqltypes.NULLTYPE


def _array(item_type: SQLType) -> SQLType:
    if isinstance(item_type, sqltypes.ARRAY):
        nested: sqltypes.ARRAY[object] = item_type
        return sqltypes.ARRAY(
            nested.item_type, dimensions=(1 if nested.dimensions is None else nested.dimensions) + 1
        )
    return sqltypes.ARRAY(item_type)


def _row_field(field: str) -> tuple[str, SQLType]:
    name, type_str = aware_split(field.strip(), delimiter=" ", maxsplit=1)
    return unquote(name), parse_sqltype(type_str)


def _precision(options: str | None) -> int | None:
    return None if options is None else int(options)


def unquote(string: str) -> str:
    r"""``string`` without its enclosing double quotes, if it is quoted.

    Trino escapes a quote in a quoted name as ``""``; ``\"`` is also accepted.
    """
    if len(string) < 2 or not (string.startswith('"') and string.endswith('"')):  # noqa: PLR2004
        return string
    return string[1:-1].replace('\\"', '"').replace('""', '"')


def aware_split(string: str, *, delimiter: str = ",", maxsplit: int = -1) -> Iterator[str]:
    """Split ``string`` on ``delimiter``, except inside double quotes or parentheses.

    Args:
        string: The string to split.
        delimiter: Where to split, usually a comma or a space.
        maxsplit: The most splits to make; -1, the default, means no limit.

    Raises:
        ValueError: ``maxsplit`` is below -1.
    """
    if maxsplit < -1:
        msg = f"maxsplit must be >= -1, got {maxsplit}"
        raise ValueError(msg)
    depth = 0
    quoted = False
    start = 0
    for index, character in enumerate(string):
        if maxsplit == 0:
            break
        if character == '"':
            quoted = string[index - 1] == "\\" if quoted else True
        elif quoted:
            continue
        elif character == "(":
            depth += 1
        elif character == ")":
            depth -= 1
        elif depth == 0 and string.startswith(delimiter, index):
            yield string[start:index]
            start = index + len(delimiter)
            maxsplit -= 1
    yield string[start:]
