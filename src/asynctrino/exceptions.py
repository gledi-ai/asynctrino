"""The PEP 249 exception hierarchy plus Trino-specific errors."""

from collections.abc import Mapping
from typing import cast, override

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
    "TrinoAuthError",
    "TrinoConnectionError",
    "TrinoDataError",
    "TrinoExternalError",
    "TrinoInternalError",
    "TrinoQueryError",
    "TrinoUserError",
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
    """The Coordinator or Segment storage could not be reached, or answered unexpectedly."""


class TrinoDataError(NotSupportedError):
    """A value the Coordinator returned has no Python representation, like ``DATE '-0001-01-01'``.

    Pass ``legacy_primitive_types=True`` to receive such values as the Coordinator sent them.
    """


class HttpError(TrinoConnectionError):
    """The Coordinator answered with an unexpected HTTP status.

    Attributes:
        status_code: The response's HTTP status.
        body: The response body, decoded leniently.
    """

    def __init__(self, status_code: int, body: str) -> None:
        self.status_code = status_code
        self.body = body
        super().__init__(f"error {status_code}: {body}")


class Http502Error(HttpError):
    """The Coordinator, or a gateway in front of it, answered 502 Bad Gateway."""


class Http503Error(HttpError):
    """The Coordinator, or a gateway in front of it, answered 503 Service Unavailable."""


class Http504Error(HttpError):
    """The Coordinator, or a gateway in front of it, answered 504 Gateway Timeout."""


class TrinoQueryError(DatabaseError):
    """The Coordinator reported that a query failed.

    ``USER_ERROR``, ``EXTERNAL`` and ``INTERNAL_ERROR`` failures raise the subclasses
    :class:`TrinoUserError`, :class:`TrinoExternalError` and :class:`TrinoInternalError`.
    Other error types, such as ``INSUFFICIENT_RESOURCES``, raise this class itself.

    Attributes:
        error: The raw ``error`` object from the Coordinator's response.
        query_id: The id of the failed query.
    """

    def __init__(self, error: Mapping[str, object], query_id: str | None) -> None:
        self.error = error
        self.query_id = query_id
        super().__init__(f"{self.error_name}: {self.message} (query {query_id})")

    @property
    def error_code(self) -> int | None:
        """The numeric Trino error code."""
        return _int(self.error.get("errorCode"))

    @property
    def error_name(self) -> str | None:
        """The Trino error name, such as ``SYNTAX_ERROR``."""
        return _str(self.error.get("errorName"))

    @property
    def error_type(self) -> str | None:
        """The Trino error type: ``USER_ERROR``, ``INTERNAL_ERROR``, ``EXTERNAL``, and so on."""
        return _str(self.error.get("errorType"))

    @property
    def failure_info(self) -> Mapping[str, object] | None:
        """The server-side failure, with its Java exception ``type``, ``stack`` and causes."""
        return _mapping(self.error.get("failureInfo"))

    @property
    def error_exception(self) -> str | None:
        """The Java exception class that caused the failure."""
        info = self.failure_info
        return None if info is None else _str(info.get("type"))

    @property
    def message(self) -> str:
        """The human-readable error message."""
        message = _str(self.error.get("message"))
        return "Trino did not return an error message" if message is None else message

    @property
    def error_location(self) -> tuple[int, int] | None:
        """The ``(line, column)`` in the statement where the error was found."""
        location = _mapping(self.error.get("errorLocation"))
        if location is None:
            return None
        line, column = _int(location.get("lineNumber")), _int(location.get("columnNumber"))
        return None if line is None or column is None else (line, column)

    @override
    def __repr__(self) -> str:
        return (
            f"{type(self).__name__}(type={self.error_type}, name={self.error_name}, "
            f'message="{self.message}", query_id={self.query_id})'
        )


class TrinoUserError(TrinoQueryError, ProgrammingError):
    """A query failed because of the user, such as a syntax error or a missing table."""


class TrinoExternalError(TrinoQueryError, OperationalError):
    """A query failed because of a system outside Trino, such as a connector's data source."""


class TrinoInternalError(TrinoQueryError, InternalError):
    """A query failed because of an internal Trino error."""


def _str(value: object) -> str | None:
    return value if isinstance(value, str) else None


def _int(value: object) -> int | None:
    return value if isinstance(value, int) and not isinstance(value, bool) else None


def _mapping(value: object) -> Mapping[str, object] | None:
    return cast("Mapping[str, object]", value) if isinstance(value, Mapping) else None
