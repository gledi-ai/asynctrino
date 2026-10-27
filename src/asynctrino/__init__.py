"""Fully typed asyncio client for the Trino distributed SQL engine, built on httpx2."""

from importlib.metadata import version

from asynctrino import dbapi, exceptions
from asynctrino.dbapi import connect
from asynctrino.exceptions import (
    DatabaseError,
    DataError,
    Error,
    Http502Error,
    Http503Error,
    Http504Error,
    HttpError,
    IntegrityError,
    InterfaceError,
    InternalError,
    NotSupportedError,
    OperationalError,
    ProgrammingError,
    TrinoConnectionError,
    TrinoExternalError,
    TrinoInternalError,
    TrinoQueryError,
    TrinoUserError,
    Warning,  # noqa: A004
)

__version__: str = version("asynctrino")

__all__ = [
    "DataError",
    "DatabaseError",
    "Error",
    "Http502Error",
    "Http503Error",
    "Http504Error",
    "HttpError",
    "IntegrityError",
    "InterfaceError",
    "InternalError",
    "NotSupportedError",
    "OperationalError",
    "ProgrammingError",
    "TrinoConnectionError",
    "TrinoExternalError",
    "TrinoInternalError",
    "TrinoQueryError",
    "TrinoUserError",
    "Warning",
    "__version__",
    "connect",
    "dbapi",
    "exceptions",
]
