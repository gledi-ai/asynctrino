"""SQLAlchemy support: ``create_async_engine("asynctrino://user@host:port/catalog/schema")``.

Requires the ``asynctrino[sqlalchemy]`` extra.
"""

from asynctrino.sqlalchemy._url import URL
from asynctrino.sqlalchemy.dialect import TrinoDialect

__all__ = ["URL", "TrinoDialect"]
