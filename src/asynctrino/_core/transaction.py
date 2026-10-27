"""Isolation levels and the statements that start and end a Trino transaction."""

from enum import StrEnum

NO_TRANSACTION = "NONE"
COMMIT = "COMMIT"
ROLLBACK = "ROLLBACK"


class IsolationLevel(StrEnum):
    """How a connection runs its statements: each on its own, or inside a transaction.

    The values are the names SQL and SQLAlchemy use for them.
    """

    AUTOCOMMIT = "AUTOCOMMIT"
    READ_UNCOMMITTED = "READ UNCOMMITTED"
    READ_COMMITTED = "READ COMMITTED"
    REPEATABLE_READ = "REPEATABLE READ"
    SERIALIZABLE = "SERIALIZABLE"


def start_transaction(level: IsolationLevel) -> str:
    """The statement that starts a transaction at ``level``, which must not be autocommit."""
    return f"START TRANSACTION ISOLATION LEVEL {level.value}"
