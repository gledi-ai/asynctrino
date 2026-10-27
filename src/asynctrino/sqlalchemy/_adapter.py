"""asynctrino adapted to SQLAlchemy's synchronous DB-API interface through 2.1's async adapters."""

from collections.abc import Awaitable, Callable
from types import ModuleType
from typing import Any, cast

from sqlalchemy.connectors.asyncio import (
    AsyncAdapt_dbapi_connection,
    AsyncAdapt_dbapi_cursor,
    AsyncAdapt_dbapi_module,
    AsyncAdapt_dbapi_ss_cursor,
    AsyncAdapt_terminate,
    AsyncIODBAPIConnection,
)
from sqlalchemy.util.concurrency import await_

from asynctrino import dbapi
from asynctrino.dbapi import Connection

_EXCEPTIONS = (
    "Warning",
    "Error",
    "InterfaceError",
    "DatabaseError",
    "DataError",
    "OperationalError",
    "IntegrityError",
    "InternalError",
    "ProgrammingError",
    "NotSupportedError",
)


class AsyncAdaptCursor(AsyncAdapt_dbapi_cursor):
    """A cursor that buffers the whole result when it executes."""

    __slots__ = ()


class AsyncAdaptServerSideCursor(AsyncAdapt_dbapi_ss_cursor):
    """A cursor that fetches rows from the Coordinator as they are consumed."""

    __slots__ = ()


class AsyncAdaptConnection(AsyncAdapt_terminate, AsyncAdapt_dbapi_connection):
    """An asynctrino :class:`~asynctrino.dbapi.Connection` driven from SQLAlchemy's greenlets."""

    __slots__ = ()

    _cursor_cls = AsyncAdaptCursor
    _ss_cursor_cls = AsyncAdaptServerSideCursor


class AsyncAdaptDBAPI(AsyncAdapt_dbapi_module):
    """The :mod:`asynctrino.dbapi` module as SQLAlchemy's synchronous dialect code expects it."""

    def __init__(self, driver: ModuleType = dbapi) -> None:
        super().__init__(driver)
        self.paramstyle = dbapi.paramstyle
        for name in _EXCEPTIONS:
            setattr(self, name, getattr(driver, name))

    def connect(self, **kwargs: Any) -> AsyncAdaptConnection:  # noqa: ANN401  # pyrefly: ignore[explicit-any]
        """Create an asynctrino connection; ``async_creator_fn`` replaces :func:`connect`."""
        creator: Creator = kwargs.pop("async_creator_fn", _connect)
        return await_(AsyncAdaptConnection.create(self, creator(**kwargs)))


type Creator = Callable[..., Awaitable[AsyncIODBAPIConnection]]


async def _connect(**kwargs: Any) -> AsyncIODBAPIConnection:  # noqa: ANN401  # pyrefly: ignore[explicit-any]
    connection: Connection = dbapi.connect(**kwargs)
    return cast("AsyncIODBAPIConnection", connection)
