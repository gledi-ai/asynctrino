"""asynctrino adapted to SQLAlchemy's synchronous DB-API interface through 2.1's async adapters."""

from collections.abc import Awaitable, Callable
from typing import Any, cast, override

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
from asynctrino.dbapi import Connection, IsolationLevel

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

    @property
    @override
    def driver_connection(self) -> Connection:
        """The adapted asynctrino connection."""
        return cast("Connection", self._connection)

    @property
    def isolation_level(self) -> IsolationLevel:
        """The isolation level of the adapted connection."""
        return self.driver_connection.isolation_level

    def set_isolation_level(self, level: IsolationLevel) -> None:
        """Roll back any open transaction, then run the next statements at ``level``."""
        self.rollback()
        self.driver_connection.isolation_level = level

    @override
    async def _terminate_graceful_close(self) -> None:
        """Cancel the connection's unfinished queries and close its HTTP client."""
        await self.driver_connection.close()

    @override
    def _terminate_force_close(self) -> None:
        """Drop the connection without I/O, as garbage collection does.

        Trino keeps no session on a connection, so there is nothing to tear down; unfinished
        queries are left for the Coordinator to abandon once their ``nextUri`` goes unpolled.
        """


class AsyncAdaptDBAPI(AsyncAdapt_dbapi_module):
    """The :mod:`asynctrino.dbapi` module as SQLAlchemy's synchronous dialect code expects it."""

    def __init__(self) -> None:
        super().__init__(dbapi)
        self.paramstyle = dbapi.paramstyle
        for name in _EXCEPTIONS:
            setattr(self, name, getattr(dbapi, name))

    def connect(self, **kwargs: Any) -> AsyncAdaptConnection:  # noqa: ANN401  # pyrefly: ignore[explicit-any]  # SQLAlchemy's signature
        """Create an asynctrino connection; ``async_creator_fn`` replaces :func:`connect`."""
        creator: Creator = kwargs.pop("async_creator_fn", _connect)
        return await_(AsyncAdaptConnection.create(self, creator(**kwargs)))


type Creator = Callable[..., Awaitable[AsyncIODBAPIConnection]]


async def _connect(**kwargs: Any) -> AsyncIODBAPIConnection:  # noqa: ANN401  # pyrefly: ignore[explicit-any]  # connect()'s mixed kwargs
    connection: Connection = dbapi.connect(**kwargs)
    return cast("AsyncIODBAPIConnection", connection)
