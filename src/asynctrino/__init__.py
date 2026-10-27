"""Fully typed asyncio client for the Trino distributed SQL engine, built on httpx2."""

from importlib.metadata import version

from asynctrino import auth, dbapi, exceptions
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
    TrinoAuthError,
    TrinoConnectionError,
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
    "TrinoAuthError",
    "TrinoConnectionError",
    "TrinoQueryError",
    "Warning",
    "__version__",
    "auth",
    "connect",
    "dbapi",
    "exceptions",
]
