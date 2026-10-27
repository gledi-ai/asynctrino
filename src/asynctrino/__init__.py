"""Fully typed asyncio client for the Trino distributed SQL engine, built on httpx2."""

from importlib.metadata import version

from asynctrino import auth, dbapi, exceptions
from asynctrino.dbapi import IsolationLevel, connect
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
    TrinoAuthError,
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
    "IsolationLevel",
    "NotSupportedError",
    "OperationalError",
    "ProgrammingError",
    "TrinoAuthError",
    "TrinoConnectionError",
    "TrinoExternalError",
    "TrinoInternalError",
    "TrinoQueryError",
    "TrinoUserError",
    "Warning",
    "__version__",
    "auth",
    "connect",
    "dbapi",
    "exceptions",
]
