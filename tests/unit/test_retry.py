from datetime import UTC, datetime

import httpx2
import pytest

from asynctrino._core.messages import HttpRequest, HttpResponse
from asynctrino._core.retry import RetryPolicy, parse_retry_after

NOW = datetime(2026, 10, 27, 12, 0, 0, tzinfo=UTC)


@pytest.mark.parametrize(
    ("value", "seconds"),
    [
        ("0", 0.0),
        ("7", 7.0),
        (" 120 ", 120.0),
        ("Tue, 27 Oct 2026 12:00:30 GMT", 30.0),
        ("Tue, 27 Oct 2026 13:00:05 +0100", 5.0),
        ("Tue, 27 Oct 2026 12:00:05 -0000", 5.0),
        ("Tue, 27 Oct 2026 11:59:00 GMT", 0.0),
    ],
)
def test_retry_after_is_parsed_as_seconds_or_an_http_date(value: str, seconds: float) -> None:
    assert parse_retry_after(value, NOW) == seconds


@pytest.mark.parametrize("value", ["", "soon", "-5", "1.5", "Tue, 99 Foo 2026"])
def test_an_unparseable_retry_after_is_ignored(value: str) -> None:
    assert parse_retry_after(value, NOW) is None


STATEMENT = HttpRequest(
    "GET", "http://coordinator:8080/v1/statement/executing/q/y/1", {}, kind="statement"
)
OTHER = HttpRequest("GET", "http://storage/segment/1", {})
POLICY = RetryPolicy(max_attempts=5)


def delay(
    outcome: HttpResponse | Exception,
    *,
    request: HttpRequest = STATEMENT,
    attempt: int = 1,
    policy: RetryPolicy = POLICY,
    jitter: float = 1.0,
) -> float | None:
    return policy.delay(outcome, request, attempt, now=NOW, jitter=jitter)


@pytest.mark.parametrize("status", [429, 502, 503, 504])
@pytest.mark.parametrize("request_", [STATEMENT, OTHER])
def test_throttling_and_gateway_statuses_are_retried(status: int, request_: HttpRequest) -> None:
    assert delay(HttpResponse(status, b"busy"), request=request_) == 0.2


@pytest.mark.parametrize("status", [200, 204, 400, 401, 404, 500, 501])
def test_other_statuses_are_not_retried(status: int) -> None:
    assert delay(HttpResponse(status, b'{"id": "q"}')) is None


@pytest.mark.parametrize("body", [b"", b" \n\t"])
def test_an_empty_200_is_retried_only_for_statement_requests(body: bytes) -> None:
    assert delay(HttpResponse(200, body), request=STATEMENT) == 0.2
    assert delay(HttpResponse(200, body), request=OTHER) is None


@pytest.mark.parametrize(
    "error",
    [
        httpx2.ConnectError("refused"),
        httpx2.ConnectTimeout("timed out"),
        httpx2.ReadError("reset"),
        httpx2.ReadTimeout("timed out"),
        httpx2.WriteTimeout("timed out"),
        httpx2.PoolTimeout("timed out"),
        httpx2.RemoteProtocolError("server disconnected"),
    ],
)
def test_connect_read_and_timeout_errors_are_retried(error: Exception) -> None:
    assert delay(error) == 0.2


@pytest.mark.parametrize(
    "error",
    [
        httpx2.UnsupportedProtocol("ftp"),
        httpx2.LocalProtocolError("bad header"),
        httpx2.InvalidURL("bad url"),
        ValueError("not a transport error"),
    ],
)
def test_other_errors_are_not_retried(error: Exception) -> None:
    assert delay(error) is None


def test_nothing_is_retried_on_the_last_attempt() -> None:
    policy = RetryPolicy(max_attempts=3)

    assert delay(HttpResponse(503, b""), attempt=2, policy=policy) is not None
    assert delay(HttpResponse(503, b""), attempt=3, policy=policy) is None
    assert delay(httpx2.ConnectError("refused"), attempt=3, policy=policy) is None


def test_a_single_attempt_policy_never_retries() -> None:
    assert delay(HttpResponse(503, b""), policy=RetryPolicy(max_attempts=1)) is None


@pytest.mark.parametrize(
    ("attempt", "jitter", "seconds"),
    [(1, 1.0, 0.2), (2, 1.0, 0.4), (3, 1.0, 0.8), (3, 0.5, 0.4), (2, 0.0, 0.0)],
)
def test_backoff_is_exponential_with_jitter(attempt: int, jitter: float, seconds: float) -> None:
    assert delay(httpx2.ReadError("reset"), attempt=attempt, jitter=jitter) == pytest.approx(
        seconds
    )


def test_backoff_is_capped_at_the_maximum_delay() -> None:
    policy = RetryPolicy(max_attempts=100, max_delay=10.0)

    assert delay(httpx2.ReadError("reset"), attempt=50, policy=policy) == 10.0


@pytest.mark.parametrize("status", [429, 503])
def test_retry_after_overrides_the_backoff(status: int) -> None:
    response = HttpResponse(status, b"", {"Retry-After": "3"})

    assert delay(response, jitter=0.0) == 3.0


def test_a_zero_retry_after_retries_immediately() -> None:
    assert delay(HttpResponse(503, b"", {"Retry-After": "0"})) == 0.0


def test_retry_after_as_an_http_date_is_honoured() -> None:
    response = HttpResponse(429, b"", {"Retry-After": "Tue, 27 Oct 2026 12:00:10 GMT"})

    assert delay(response) == 10.0


def test_retry_after_is_capped_at_the_maximum_delay() -> None:
    response = HttpResponse(429, b"", {"Retry-After": "86400"})

    assert delay(response, policy=RetryPolicy(max_attempts=3, max_delay=60.0)) == 60.0


def test_a_malformed_retry_after_falls_back_to_the_backoff() -> None:
    response = HttpResponse(429, b"", {"Retry-After": "soon"})

    assert delay(response, attempt=2) == 0.4


def test_the_policy_needs_at_least_one_attempt() -> None:
    with pytest.raises(ValueError, match="max_attempts must be at least 1"):
        _ = RetryPolicy(max_attempts=0)
