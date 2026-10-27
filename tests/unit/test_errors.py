import asyncio
from collections.abc import AsyncIterator, Callable

import httpx2
import pytest

from asynctrino.dbapi import connect
from asynctrino.exceptions import (
    DatabaseError,
    Http502Error,
    Http503Error,
    Http504Error,
    HttpError,
    InternalError,
    OperationalError,
    ProgrammingError,
    TrinoConnectionError,
    TrinoExternalError,
    TrinoInternalError,
    TrinoQueryError,
    TrinoUserError,
)
from tests.unit.coordinator import JSON, FakeCoordinator, column, statement_pages

SYNTAX_ERROR: JSON = {
    "errorCode": 1,
    "errorLocation": {"columnNumber": 15, "lineNumber": 1},
    "errorName": "SYNTAX_ERROR",
    "errorType": "USER_ERROR",
    "failureInfo": {
        "errorLocation": {"columnNumber": 15, "lineNumber": 1},
        "message": "line 1:15: Schema must be specified when session schema is not set",
        "stack": ["io.trino.sql.analyzer.SemanticExceptions.semanticException(...)"],
        "suppressed": [],
        "type": "io.trino.spi.TrinoException",
    },
    "message": "line 1:15: Schema must be specified when session schema is not set",
}


def failing_query(error: JSON) -> FakeCoordinator:
    pages = statement_pages(columns=None, batches=[])
    pages[-1]["error"] = error
    pages[-1]["stats"] = {"state": "FAILED"}
    return FakeCoordinator(pages)


async def test_a_failed_query_carries_every_error_detail() -> None:
    coordinator = failing_query(SYNTAX_ERROR)
    async with coordinator.client() as client:
        cur = connect(host="coordinator", http_client=client).cursor()

        with pytest.raises(TrinoUserError) as excinfo:
            await cur.execute("SELECT * FROM nation")

    error = excinfo.value
    assert error.error_code == 1
    assert error.error_name == "SYNTAX_ERROR"
    assert error.error_type == "USER_ERROR"
    assert error.error_exception == "io.trino.spi.TrinoException"
    assert error.failure_info is not None
    assert error.failure_info["suppressed"] == []
    assert error.message == "line 1:15: Schema must be specified when session schema is not set"
    assert error.error_location == (1, 15)
    assert error.query_id == "20261027_000000_00000_abcde"
    assert "SYNTAX_ERROR" in str(error)
    assert "Schema must be specified" in str(error)
    assert repr(error) == (
        "TrinoUserError(type=USER_ERROR, name=SYNTAX_ERROR, "
        'message="line 1:15: Schema must be specified when session schema is not set", '
        "query_id=20261027_000000_00000_abcde)"
    )


@pytest.mark.parametrize(
    ("error_type", "exception", "pep_249_class"),
    [
        ("USER_ERROR", TrinoUserError, ProgrammingError),
        ("EXTERNAL", TrinoExternalError, OperationalError),
        ("INTERNAL_ERROR", TrinoInternalError, InternalError),
        ("INSUFFICIENT_RESOURCES", TrinoQueryError, DatabaseError),
    ],
)
async def test_the_error_type_selects_the_pep_249_class(
    error_type: str, exception: type[TrinoQueryError], pep_249_class: type[DatabaseError]
) -> None:
    coordinator = failing_query(
        {"errorName": "SOME_ERROR", "errorType": error_type, "message": "x"}
    )
    async with coordinator.client() as client:
        cur = connect(host="coordinator", http_client=client).cursor()

        with pytest.raises(exception) as excinfo:
            await cur.execute("SELECT 1")

    assert type(excinfo.value) is exception
    assert isinstance(excinfo.value, pep_249_class)


async def test_a_sparse_error_has_no_location_or_failure_info() -> None:
    coordinator = failing_query({"errorLocation": {"lineNumber": 1}})
    async with coordinator.client() as client:
        cur = connect(host="coordinator", http_client=client).cursor()

        with pytest.raises(TrinoQueryError) as excinfo:
            await cur.execute("SELECT 1")

    error = excinfo.value
    assert error.error_location is None
    assert error.failure_info is None
    assert error.error_exception is None
    assert error.error_code is None
    assert error.error_type is None
    assert error.message == "Trino did not return an error message"


type Failure = httpx2.Response | Exception


def flaky(coordinator: FakeCoordinator, failures: list[Failure]) -> httpx2.AsyncClient:
    """A client whose first requests fail as scripted before ``coordinator`` answers."""

    def handler(request: httpx2.Request) -> httpx2.Response:
        if len(failures) == 0:
            return coordinator.handler(request)
        coordinator.requests.append(request)
        failure = failures.pop(0)
        if isinstance(failure, Exception):
            raise failure
        return failure

    return httpx2.AsyncClient(transport=httpx2.MockTransport(handler))


def select_1() -> FakeCoordinator:
    return FakeCoordinator(statement_pages(columns=[column("_col0", "integer")], batches=[[[1]]]))


def answer(status: int, body: str = "", *, retry_after: str | None = "0") -> httpx2.Response:
    headers = {} if retry_after is None else {"Retry-After": retry_after}
    return httpx2.Response(status, text=body, headers=headers)


@pytest.mark.parametrize("status", [429, 502, 503, 504])
async def test_throttling_and_gateway_errors_are_retried(status: int) -> None:
    coordinator = select_1()
    async with flaky(coordinator, [answer(status), answer(status)]) as client:
        cur = connect(host="coordinator", max_attempts=3, http_client=client).cursor()
        await cur.execute("SELECT 1")

        assert await cur.fetchall() == [[1]]

    assert [r.method for r in coordinator.requests] == ["POST", "POST", "POST", "GET", "GET"]


@pytest.mark.parametrize(
    ("status", "exception"),
    [(429, HttpError), (502, Http502Error), (503, Http503Error), (504, Http504Error)],
)
async def test_retries_stop_after_max_attempts_and_raise_the_last_status(
    status: int, exception: type[HttpError]
) -> None:
    coordinator = select_1()
    failures: list[Failure] = [answer(status, "try later") for _ in range(5)]
    async with flaky(coordinator, failures) as client:
        cur = connect(host="coordinator", max_attempts=4, http_client=client).cursor()

        with pytest.raises(exception, match=f"error {status}: try later") as excinfo:
            await cur.execute("SELECT 1")

    assert len(coordinator.requests) == 4
    assert excinfo.value.status_code == status
    assert excinfo.value.body == "try later"
    assert isinstance(excinfo.value, TrinoConnectionError)


@pytest.mark.parametrize("status", [400, 404, 500, 501])
async def test_other_error_statuses_raise_without_retrying(status: int) -> None:
    coordinator = select_1()
    async with flaky(coordinator, [answer(status, "nope")]) as client:
        cur = connect(host="coordinator", http_client=client).cursor()

        with pytest.raises(HttpError, match=f"error {status}: nope") as excinfo:
            await cur.execute("SELECT 1")

    assert len(coordinator.requests) == 1
    assert excinfo.value.status_code == status


async def test_a_single_attempt_disables_retries() -> None:
    coordinator = select_1()
    async with flaky(coordinator, [answer(503)]) as client:
        cur = connect(host="coordinator", max_attempts=1, http_client=client).cursor()

        with pytest.raises(Http503Error):
            await cur.execute("SELECT 1")

    assert len(coordinator.requests) == 1


def test_max_attempts_must_be_positive() -> None:
    with pytest.raises(ValueError, match="max_attempts must be at least 1"):
        _ = connect(host="coordinator", max_attempts=0)


async def test_an_empty_200_to_a_poll_is_retried() -> None:
    coordinator = select_1()

    def handler(request: httpx2.Request) -> httpx2.Response:
        polls = [r for r in coordinator.requests if r.method == "GET"]
        if request.method == "GET" and len(polls) == 0:
            coordinator.requests.append(request)
            return httpx2.Response(200, content=b"  ", headers={"Retry-After": "0"})
        return coordinator.handler(request)

    async with httpx2.AsyncClient(transport=httpx2.MockTransport(handler)) as client:
        cur = connect(host="coordinator", http_client=client).cursor()
        await cur.execute("SELECT 1")

        assert await cur.fetchall() == [[1]]

    assert [r.method for r in coordinator.requests] == ["POST", "GET", "GET", "GET"]


async def test_an_empty_200_to_the_statement_post_is_retried_then_raises() -> None:
    coordinator = select_1()
    failures: list[Failure] = [answer(200) for _ in range(3)]
    async with flaky(coordinator, failures) as client:
        cur = connect(host="coordinator", max_attempts=3, http_client=client).cursor()

        with pytest.raises(TrinoConnectionError, match="empty response"):
            await cur.execute("SELECT 1")

    assert len(coordinator.requests) == 3


@pytest.mark.parametrize(
    "error",
    [
        lambda: httpx2.ConnectError("connection refused"),
        lambda: httpx2.ReadError("connection reset"),
        lambda: httpx2.ReadTimeout("timed out"),
    ],
)
async def test_connect_read_and_timeout_errors_are_retried(error: Callable[[], Exception]) -> None:
    coordinator = select_1()
    async with flaky(coordinator, [error()]) as client:
        cur = connect(host="coordinator", max_attempts=2, http_client=client).cursor()
        await cur.execute("SELECT 1")

        assert await cur.fetchall() == [[1]]

    assert [r.method for r in coordinator.requests] == ["POST", "POST", "GET", "GET"]


async def test_a_persistent_transport_error_raises_trino_connection_error() -> None:
    coordinator = select_1()
    failures: list[Failure] = [httpx2.ConnectError("connection refused") for _ in range(2)]
    async with flaky(coordinator, failures) as client:
        cur = connect(host="coordinator", max_attempts=2, http_client=client).cursor()

        with pytest.raises(TrinoConnectionError, match="connection refused") as excinfo:
            await cur.execute("SELECT 1")

    assert len(coordinator.requests) == 2
    assert isinstance(excinfo.value.__cause__, httpx2.ConnectError)


@pytest.mark.parametrize(
    "error", [httpx2.UnsupportedProtocol("no ftp"), httpx2.InvalidURL("bad nextUri")]
)
async def test_other_httpx2_errors_raise_trino_connection_error_without_retrying(
    error: Exception,
) -> None:
    coordinator = select_1()
    async with flaky(coordinator, [error]) as client:
        cur = connect(host="coordinator", http_client=client).cursor()

        with pytest.raises(TrinoConnectionError):
            await cur.execute("SELECT 1")

    assert len(coordinator.requests) == 1


async def test_a_body_that_is_not_json_raises_trino_connection_error() -> None:
    coordinator = select_1()
    async with flaky(coordinator, [answer(200, "<html>gateway</html>")]) as client:
        cur = connect(host="coordinator", http_client=client).cursor()

        with pytest.raises(TrinoConnectionError, match="invalid JSON"):
            await cur.execute("SELECT 1")


@pytest.fixture
async def silent_port() -> AsyncIterator[int]:
    """A local port that accepts connections but never answers."""

    async def hang(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        await reader.read()
        writer.close()

    server = await asyncio.start_server(hang, "127.0.0.1", 0)
    async with server:
        yield int(server.sockets[0].getsockname()[1])


@pytest.mark.parametrize("request_timeout", [0.05, httpx2.Timeout(5.0, read=0.05)])
async def test_request_timeout_applies_to_the_client_asynctrino_creates(
    silent_port: int, request_timeout: float | httpx2.Timeout
) -> None:
    async with connect(
        host="127.0.0.1", port=silent_port, max_attempts=1, request_timeout=request_timeout
    ) as conn:
        async with asyncio.timeout(2):
            with pytest.raises(TrinoConnectionError, match="ReadTimeout") as excinfo:
                await conn.cursor().execute("SELECT 1")

    assert not isinstance(excinfo.value, httpx2.HTTPError)


async def test_request_timeout_is_ignored_for_an_injected_client() -> None:
    timeout = httpx2.Timeout(12.0, connect=3.0)
    coordinator = select_1()
    async with httpx2.AsyncClient(
        transport=httpx2.MockTransport(coordinator.handler), timeout=timeout
    ) as client:
        conn = connect(host="coordinator", request_timeout=0.001, http_client=client)
        await conn.cursor().execute("SELECT 1")

        assert client.timeout == timeout
