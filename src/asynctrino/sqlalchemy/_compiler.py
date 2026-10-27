"""Compilers that render SQLAlchemy statements, DDL and types as Trino SQL."""

from collections.abc import Mapping
from decimal import Decimal
from typing import Any, override

from sqlalchemy import util
from sqlalchemy.sql import compiler, sqltypes
from sqlalchemy.sql._typing import _ColumnExpressionArgument, _ColumnExpressionOrLiteralArgument
from sqlalchemy.sql.compiler import _ResultMapAppender
from sqlalchemy.sql.elements import BinaryExpression, FrameClause, TryCast
from sqlalchemy.sql.functions import Function, ReturnTypeFromArgs
from sqlalchemy.sql.operators import OperatorType
from sqlalchemy.sql.schema import (
    ForeignKeyConstraint,
    PrimaryKeyConstraint,
    Table,
    UniqueConstraint,
)
from sqlalchemy.sql.selectable import Alias, FromClause, GenerativeSelect, TableClause
from sqlalchemy.sql.visitors import InternalTraversal, _TraverseInternalsType

from asynctrino.sqlalchemy._types import MAP, ROW, TIME, TIMESTAMP

RESERVED_WORDS = frozenset(
    {
        "alter",
        "and",
        "as",
        "between",
        "by",
        "case",
        "cast",
        "constraint",
        "create",
        "cross",
        "cube",
        "current_catalog",
        "current_date",
        "current_path",
        "current_role",
        "current_schema",
        "current_time",
        "current_timestamp",
        "current_user",
        "deallocate",
        "delete",
        "describe",
        "distinct",
        "drop",
        "else",
        "end",
        "escape",
        "except",
        "execute",
        "exists",
        "extract",
        "false",
        "for",
        "from",
        "full",
        "group",
        "grouping",
        "having",
        "in",
        "inner",
        "insert",
        "intersect",
        "into",
        "is",
        "join",
        "left",
        "like",
        "localtime",
        "localtimestamp",
        "natural",
        "normalize",
        "not",
        "null",
        "on",
        "or",
        "order",
        "outer",
        "prepare",
        "recursive",
        "right",
        "rollup",
        "select",
        "skip",
        "table",
        "then",
        "true",
        "uescape",
        "union",
        "unnest",
        "using",
        "values",
        "when",
        "where",
        "with",
    }
)
"""Trino's reserved keywords, which are quoted as identifiers.

See https://trino.io/docs/current/language/reserved.html.
"""


class IgnoreNullsFunction[T](ReturnTypeFromArgs[T]):
    """A window function that skips nulls when called with ``ignore_nulls=True``."""

    _register = False
    inherit_cache = True
    _traverse_internals: _TraverseInternalsType = [  # noqa: RUF012
        *ReturnTypeFromArgs._traverse_internals,  # noqa: SLF001
        ("ignore_nulls", InternalTraversal.dp_boolean),
    ]

    def __init__(
        self,
        col: _ColumnExpressionArgument[T],
        *args: _ColumnExpressionOrLiteralArgument[object],
        ignore_nulls: bool = False,
        **kwargs: object,
    ) -> None:
        super().__init__(col, *args, **kwargs)
        self.ignore_nulls: bool = ignore_nulls


class first_value[T](IgnoreNullsFunction[T]):  # noqa: N801
    """``first_value(x)``, optionally ``IGNORE NULLS``."""

    inherit_cache = True


class last_value[T](IgnoreNullsFunction[T]):  # noqa: N801
    """``last_value(x)``, optionally ``IGNORE NULLS``."""

    inherit_cache = True


class nth_value[T](IgnoreNullsFunction[T]):  # noqa: N801
    """``nth_value(x, offset)``, optionally ``IGNORE NULLS``."""

    inherit_cache = True


class lead[T](IgnoreNullsFunction[T]):  # noqa: N801
    """``lead(x[, offset[, default]])``, optionally ``IGNORE NULLS``."""

    inherit_cache = True


class lag[T](IgnoreNullsFunction[T]):  # noqa: N801
    """``lag(x[, offset[, default]])``, optionally ``IGNORE NULLS``."""

    inherit_cache = True


class TrinoSQLCompiler(compiler.SQLCompiler):
    """Renders SQLAlchemy statements as Trino SQL."""

    @override
    def limit_clause(self, select: GenerativeSelect, **kw: object) -> str:
        """Trino accepts ``OFFSET`` only before ``LIMIT``."""
        text = ""
        if select._offset_clause is not None:  # noqa: SLF001
            text += "\nOFFSET " + self.process(select._offset_clause, **kw)  # noqa: SLF001
        if select._limit_clause is not None:  # noqa: SLF001
            text += "\nLIMIT " + self.process(select._limit_clause, **kw)  # noqa: SLF001
        return text

    @override
    def visit_table(
        self,
        table: TableClause,
        asfrom: bool = False,
        iscrud: bool = False,
        ashint: bool = False,
        fromhints: Mapping[FromClause, str] | None = None,
        use_schema: bool = True,
        from_linter: compiler.FromLinter | None = None,
        ambiguous_table_name_map: Mapping[str, str] | None = None,
        enclosing_alias: Alias | None = None,
        within_tstring: bool = False,
        **kwargs: object,
    ) -> str:
        sql = super().visit_table(
            table,
            asfrom,
            iscrud,
            ashint,
            fromhints,
            use_schema,
            from_linter,
            ambiguous_table_name_map,
            enclosing_alias,
            within_tstring,
            **kwargs,
        )
        return _with_catalog(sql, table, self.preparer)

    @override
    def visit_function(
        self,
        func: Function[Any],  # pyrefly: ignore[explicit-any]
        add_to_result_map: _ResultMapAppender | None = None,
        **kwargs: Any,  # pyrefly: ignore[explicit-any]
    ) -> str:
        sql = super().visit_function(func, add_to_result_map, **kwargs)
        if isinstance(func, IgnoreNullsFunction) and func.ignore_nulls:
            return f"{sql} IGNORE NULLS"
        return sql

    def visit_json_getitem_op_binary(
        self, binary: BinaryExpression[object], _operator: OperatorType, **kw: object
    ) -> str:
        """Render ``column["key"]`` as ``JSON_EXTRACT(column, '$["key"]')``."""
        return self._json_extract(binary, **kw)

    def visit_json_path_getitem_op_binary(
        self, binary: BinaryExpression[object], _operator: OperatorType, **kw: object
    ) -> str:
        """Render ``column["a", 0]`` as ``JSON_EXTRACT(column, '$["a"][0]')``."""
        return self._json_extract(binary, **kw)

    def _json_extract(self, binary: BinaryExpression[object], **kw: object) -> str:
        return (
            f"JSON_EXTRACT({self.process(binary.left, **kw)}, {self.process(binary.right, **kw)})"
        )

    @override
    def visit_frame_clause(self, frameclause: FrameClause, **kw: object) -> str:
        """Render frame offsets as literals, since Trino doesn't accept parameters there."""
        return super().visit_frame_clause(frameclause, **{**kw, "literal_binds": True})

    def visit_try_cast(self, cast: TryCast[object], **kw: object) -> str:
        """Render ``try_cast(x as type)``, which yields ``NULL`` where the cast fails."""
        return (
            f"try_cast({self.process(cast.clause, **kw)} as {self.process(cast.typeclause, **kw)})"
        )


class TrinoDDLCompiler(compiler.DDLCompiler):
    """Renders DDL for Trino, which has no table constraints."""

    @override
    def visit_primary_key_constraint(  # pyrefly: ignore[bad-override]
        self, constraint: PrimaryKeyConstraint, **kw: object
    ) -> str | None:
        return _ignored_constraint("PRIMARY KEY")

    @override
    def visit_foreign_key_constraint(  # pyrefly: ignore[bad-override]
        self, constraint: ForeignKeyConstraint, **kw: object
    ) -> str | None:
        return _ignored_constraint("FOREIGN KEY")

    @override
    def visit_unique_constraint(  # pyrefly: ignore[bad-override]
        self, constraint: UniqueConstraint, **kw: object
    ) -> str | None:
        return _ignored_constraint("UNIQUE")


def _ignored_constraint(kind: str) -> None:
    # SQLAlchemy leaves a constraint out of CREATE TABLE when its visitor returns None.
    util.warn(f"Trino does not support {kind} constraints. Constraint will be ignored.")


class TrinoTypeCompiler(compiler.GenericTypeCompiler):
    """Renders SQLAlchemy types as Trino types."""

    @override
    def visit_FLOAT(self, type_: sqltypes.Float[float], **kw: object) -> str:
        """``REAL`` up to 32 bits of precision, the default, and ``DOUBLE`` up to 64.

        Raises:
            ValueError: The precision is outside 0 to 64.
        """
        precision = 32 if type_.precision is None else type_.precision
        if 0 <= precision <= _REAL_PRECISION:
            return self.visit_REAL(sqltypes.REAL(precision), **kw)
        if precision <= _DOUBLE_PRECISION:
            return self.visit_DOUBLE(sqltypes.DOUBLE(precision), **kw)
        msg = f"type.precision must be in range [0, 64], got {type_.precision}"
        raise ValueError(msg)

    @override
    def visit_NUMERIC(self, type_: sqltypes.Numeric[Decimal], **kw: object) -> str:
        return self.visit_DECIMAL(sqltypes.DECIMAL(type_.precision, type_.scale), **kw)

    @override
    def visit_NCHAR(self, type_: sqltypes.NCHAR, **kw: object) -> str:
        return self.visit_CHAR(sqltypes.CHAR(type_.length, type_.collation), **kw)

    @override
    def visit_NVARCHAR(self, type_: sqltypes.NVARCHAR, **kw: object) -> str:
        return self.visit_VARCHAR(type_, **kw)

    @override
    def visit_TEXT(self, type_: sqltypes.Text, **kw: object) -> str:
        return self.visit_VARCHAR(type_, **kw)

    @override
    def visit_CLOB(self, type_: sqltypes.CLOB, **kw: object) -> str:
        return self.visit_VARCHAR(type_, **kw)

    @override
    def visit_NCLOB(self, type_: sqltypes.Text, **kw: object) -> str:
        return self.visit_VARCHAR(type_, **kw)

    @override
    def visit_BINARY(self, type_: sqltypes.BINARY, **kw: object) -> str:
        return "VARBINARY"

    @override
    def visit_BLOB(self, type_: sqltypes.LargeBinary, **kw: object) -> str:
        return "VARBINARY"

    @override
    def visit_VARBINARY(self, type_: sqltypes.VARBINARY, **kw: object) -> str:
        return "VARBINARY"

    @override
    def visit_DATETIME(self, type_: sqltypes.DateTime, **kw: object) -> str:
        return _datetime_type("TIMESTAMP", type_)

    @override
    def visit_TIMESTAMP(self, type_: sqltypes.TIMESTAMP, **kw: object) -> str:
        return _datetime_type("TIMESTAMP", type_)

    @override
    def visit_TIME(self, type_: sqltypes.Time, **kw: object) -> str:
        return _datetime_type("TIME", type_)

    def visit_JSON(self, _type: sqltypes.JSON, **_kw: object) -> str:  # noqa: N802
        """Trino's ``JSON``."""
        return "JSON"

    def visit_ARRAY(self, type_: sqltypes.ARRAY[object], **kw: object) -> str:  # noqa: N802
        """``ARRAY(item)``, nested once per dimension."""
        sql = self.process(type_.item_type, **kw)
        for _ in range(1 if type_.dimensions is None else type_.dimensions):
            sql = f"ARRAY({sql})"
        return sql

    def visit_MAP(self, type_: MAP, **kw: object) -> str:  # noqa: N802
        """``MAP(key, value)``."""
        return f"MAP({self.process(type_.key_type, **kw)}, {self.process(type_.value_type, **kw)})"

    def visit_ROW(self, type_: ROW, **kw: object) -> str:  # noqa: N802
        """``ROW(name type, ...)``, quoting names where needed."""
        quote = self.dialect.identifier_preparer.quote
        fields = (
            self.process(field_type, **kw)
            if name is None
            else f"{quote(name)} {self.process(field_type, **kw)}"
            for name, field_type in type_.attr_types
        )
        return f"ROW({', '.join(fields)})"


_REAL_PRECISION = 32
_DOUBLE_PRECISION = 64
_MAX_DATETIME_PRECISION = 12


def _datetime_type(name: str, type_: sqltypes.DateTime | sqltypes.Time) -> str:
    precision = type_.precision if isinstance(type_, TIME | TIMESTAMP) else None
    if precision is not None:
        if not 0 <= precision <= _MAX_DATETIME_PRECISION:
            msg = f"invalid precision={precision}, it must be from range 0-12"
            raise ValueError(msg)
        name += f"({precision})"
    if type_.timezone:
        name += " WITH TIME ZONE"
    return name


class TrinoIdentifierPreparer(compiler.IdentifierPreparer):
    """Quotes Trino's reserved words and prefixes tables with their ``trino_catalog``."""

    reserved_words = set(RESERVED_WORDS)  # noqa: RUF012  # SQLAlchemy declares it per instance

    @override
    def format_table(
        self, table: FromClause, use_schema: bool = True, name: str | None = None
    ) -> str:
        return _with_catalog(super().format_table(table, use_schema, name), table, self)


def _with_catalog(sql: str, table: FromClause, preparer: compiler.IdentifierPreparer) -> str:
    if not isinstance(table, Table):
        return sql
    catalog: object = table.dialect_options["trino"].get("catalog")
    if not isinstance(catalog, str):
        return sql
    return f"{preparer.quote_identifier(catalog)}.{sql}"
