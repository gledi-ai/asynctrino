"""Fully typed asyncio client for the Trino distributed SQL engine, built on httpx2."""

from importlib.metadata import version

from asynctrino import dbapi, exceptions
from asynctrino.dbapi import connect
from asynctrino.exceptions import (
    DatabaseError,
    DataError,
    Error,
    IntegrityError,
    InterfaceError,
    InternalError,
    NotSupportedError,
    OperationalError,
    ProgrammingError,
    TrinoConnectionError,
    TrinoDataError,
    TrinoQueryError,
    Warning,  # noqa: A004
)

__version__: str = version("asynctrino")

__all__ = [
    "DataError",
    "DatabaseError",
    "Error",
    "IntegrityError",
    "InterfaceError",
    "InternalError",
    "NotSupportedError",
    "OperationalError",
    "ProgrammingError",
    "TrinoConnectionError",
    "TrinoDataError",
    "TrinoQueryError",
    "Warning",
    "__version__",
    "connect",
    "dbapi",
    "exceptions",
]
