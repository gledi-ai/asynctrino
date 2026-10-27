"""SQLAlchemy support: ``create_async_engine("asynctrino://user@host:port/catalog/schema")``.

Requires the ``asynctrino[sqlalchemy]`` extra.
"""

from asynctrino.sqlalchemy._dialect import TrinoDialect
from asynctrino.sqlalchemy._types import DOUBLE, JSON, MAP, ROW, TIME, TIMESTAMP, VARBINARY
from asynctrino.sqlalchemy._url import URL

__all__ = [
    "DOUBLE",
    "JSON",
    "MAP",
    "ROW",
    "TIME",
    "TIMESTAMP",
    "URL",
    "VARBINARY",
    "TrinoDialect",
]
