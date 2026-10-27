from dataclasses import dataclass
from datetime import UTC, datetime
from email.utils import parsedate_to_datetime
from http import HTTPStatus

import httpx2

from asynctrino._core.messages import HttpRequest, HttpResponse

DEFAULT_MAX_ATTEMPTS = 3
RETRYABLE_STATUSES = frozenset({429, 502, 503, 504})
RETRYABLE_ERRORS = (httpx2.TimeoutException, httpx2.NetworkError, httpx2.RemoteProtocolError)


@dataclass(frozen=True, slots=True)
class RetryPolicy:
    """How many times a request is attempted and how the waits between attempts grow.

    Attributes:
        max_attempts: Total attempts per request, including the first; 1 disables retries.
        base_delay: The backoff before jitter is ``base_delay * 2 ** attempt`` seconds.
        max_delay: The cap on any wait, including one asked for by ``Retry-After``.
    """

    max_attempts: int = DEFAULT_MAX_ATTEMPTS
    base_delay: float = 0.1
    max_delay: float = 1800.0

    def __post_init__(self) -> None:
        if self.max_attempts < 1:
            msg = f"max_attempts must be at least 1, got {self.max_attempts}"
            raise ValueError(msg)

    def delay(
        self,
        outcome: HttpResponse | Exception,
        request: HttpRequest,
        attempt: int,
        *,
        now: datetime,
        jitter: float,
    ) -> float | None:
        """Seconds to wait before attempting ``request`` again, or ``None`` to stop retrying.

        Args:
            outcome: The response to attempt number ``attempt``, or the error it raised.
            request: The request that was attempted.
            attempt: The attempt that produced ``outcome``, counting from 1.
            now: The current timezone-aware time, for ``Retry-After`` dates.
            jitter: A random factor in ``[0, 1)`` applied to the exponential backoff.

        Returns:
            ``None`` when ``outcome`` is final: a success, a non-transient failure, or the last
            attempt. Otherwise the ``Retry-After`` wait if the response has a valid one, else
            the jittered exponential backoff, capped at :attr:`max_delay`.
        """
        if attempt >= self.max_attempts or not _is_transient(outcome, request):
            return None
        wait = None
        if isinstance(outcome, HttpResponse) and "Retry-After" in outcome.headers:
            wait = parse_retry_after(outcome.headers["Retry-After"], now)
        if wait is None:
            wait = jitter * self.base_delay * 2**attempt
        return min(self.max_delay, wait)


def _is_transient(outcome: HttpResponse | Exception, request: HttpRequest) -> bool:
    # Retrying the statement POST is safe: Trino starts a query only on the first GET of nextUri.
    if isinstance(outcome, Exception):
        return isinstance(outcome, RETRYABLE_ERRORS)
    if outcome.status_code in RETRYABLE_STATUSES:
        return True
    empty = len(outcome.content.strip()) == 0
    return request.kind == "statement" and outcome.status_code == HTTPStatus.OK and empty


def parse_retry_after(value: str, now: datetime) -> float | None:
    """Seconds to wait according to a ``Retry-After`` header, or ``None`` if it is malformed.

    The value is either a non-negative number of seconds or an HTTP date, which is compared with
    the timezone-aware ``now``. A date in the past means no wait.
    """
    value = value.strip()
    if value.isascii() and value.isdigit():
        return float(value)
    try:
        when = parsedate_to_datetime(value)
    except TypeError, ValueError:
        return None
    if when.tzinfo is None:
        when = when.replace(tzinfo=UTC)
    return max(0.0, (when - now).total_seconds())
