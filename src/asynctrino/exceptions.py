"""The PEP 249 exception hierarchy plus Trino-specific errors."""

from collections.abc import Mapping

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
]


class Warning(Exception):  # noqa: A001, N818
    """Important warnings, such as data truncation (PEP 249)."""


class Error(Exception):
    """Base class of every asynctrino error (PEP 249)."""


class InterfaceError(Error):
    """An error in the database interface rather than the database (PEP 249)."""


class DatabaseError(Error):
    """An error related to the database (PEP 249)."""


class DataError(DatabaseError):
    """A problem with the processed data, such as division by zero (PEP 249)."""


class OperationalError(DatabaseError):
    """An error in the database's operation, not under the programmer's control (PEP 249)."""


class IntegrityError(DatabaseError):
    """The relational integrity of the database is affected (PEP 249)."""


class InternalError(DatabaseError):
    """The database encountered an internal error (PEP 249)."""


class ProgrammingError(DatabaseError):
    """A programming error, such as a syntax error or a misused cursor (PEP 249)."""


class NotSupportedError(DatabaseError):
    """A method or database API not supported by the database (PEP 249)."""


class TrinoAuthError(OperationalError):
    """Authentication failed, or credentials would have been sent without TLS."""


class TrinoConnectionError(OperationalError):
    """The Coordinator could not be reached or answered with an unexpected HTTP response."""


class TrinoQueryError(DatabaseError):
    """The Coordinator reported that a query failed.

    Attributes:
        error: The raw ``error`` object from the Coordinator's response.
        query_id: The id of the failed query.
    """

    def __init__(self, error: Mapping[str, object], query_id: str | None) -> None:
        self.error = error
        self.query_id = query_id
        super().__init__(f"{self.error_name}: {self.message} (query {query_id})")

    @property
    def message(self) -> str:
        """The human-readable error message."""
        return str(self.error.get("message", "unknown error"))

    @property
    def error_name(self) -> str | None:
        """The Trino error name, such as ``SYNTAX_ERROR``."""
        name = self.error.get("errorName")
        return name if isinstance(name, str) else None
