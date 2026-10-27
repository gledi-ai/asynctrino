import socket
from datetime import date
from decimal import Decimal
from typing import Literal
from zoneinfo import ZoneInfoNotFoundError

import httpx2
import pytest
import tzlocal

import asynctrino.dbapi
from asynctrino.dbapi import connect
from asynctrino.exceptions import (
    ProgrammingError,
    TrinoConnectionError,
    TrinoDataError,
    TrinoQueryError,
)
from tests.unit.coordinator import JSON, FakeCoordinator, column, statement_pages


async def test_select_1_returns_its_single_row() -> None:
    coordinator = FakeCoordinator(
        statement_pages(columns=[column("_col0", "integer")], batches=[[[1]]])
    )
    async with coordinator.client() as client:
        conn = asynctrino.dbapi.connect(host="coordinator", user="alice", http_client=client)
        cur = conn.cursor()
        await cur.execute("SELECT 1")

        assert await cur.fetchall() == [[1]]
        assert cur.connection is conn

    statement = coordinator.requests[0]
    assert statement.method == "POST"
    assert str(statement.url) == "http://coordinator:8080/v1/statement"
    assert statement.content == b"SELECT 1"
    assert statement.headers["X-Trino-User"] == "alice"


async def test_execute_polls_until_columns_are_known_and_describes_them() -> None:
    columns = [column("n", "integer"), column("s", "varchar", 3)]
    coordinator = FakeCoordinator(
        statement_pages(columns=columns, batches=[[[1, "one"]]], queued_pages=3)
    )
    async with coordinator.client() as client:
        cur = asynctrino.dbapi.connect(host="coordinator", http_client=client).cursor()
        await cur.execute("SELECT n, s FROM t")

        assert [r.method for r in coordinator.requests] == ["POST", "GET", "GET", "GET"]
        assert cur.description == [
            ("n", "integer", None, None, None, None, None),
            ("s", "varchar(3)", None, 3, None, None, None),
        ]


def three_pages_of_rows() -> FakeCoordinator:
    return FakeCoordinator(
        statement_pages(
            columns=[column("n", "integer"), column("s", "varchar", 5)],
            batches=[[[1, "one"], [2, "two"]], [[3, "three"]], [], [[4, "four"], [5, "five"]]],
        )
    )


async def test_fetchone_walks_every_page_then_returns_none() -> None:
    coordinator = three_pages_of_rows()
    async with coordinator.client() as client:
        cur = asynctrino.dbapi.connect(host="coordinator", http_client=client).cursor()
        await cur.execute("SELECT n, s FROM t")

        rows = [await cur.fetchone() for _ in range(6)]

    assert rows == [[1, "one"], [2, "two"], [3, "three"], [4, "four"], [5, "five"], None]


async def test_fetchmany_spans_pages_and_defaults_to_arraysize() -> None:
    coordinator = three_pages_of_rows()
    async with coordinator.client() as client:
        cur = asynctrino.dbapi.connect(host="coordinator", http_client=client).cursor()
        await cur.execute("SELECT n, s FROM t")

        assert await cur.fetchmany() == [[1, "one"]]
        assert await cur.fetchmany(3) == [[2, "two"], [3, "three"], [4, "four"]]
        cur.arraysize = 10
        assert await cur.fetchmany() == [[5, "five"]]
        assert await cur.fetchmany() == []


async def test_async_for_streams_every_row() -> None:
    coordinator = three_pages_of_rows()
    async with coordinator.client() as client:
        cur = asynctrino.dbapi.connect(host="coordinator", http_client=client).cursor()
        await cur.execute("SELECT n, s FROM t")

        assert [row[0] async for row in cur] == [1, 2, 3, 4, 5]


async def test_fetchall_returns_remaining_rows_after_fetchone() -> None:
    coordinator = three_pages_of_rows()
    async with coordinator.client() as client:
        cur = asynctrino.dbapi.connect(host="coordinator", http_client=client).cursor()
        await cur.execute("SELECT n, s FROM t")

        await cur.fetchone()
        assert [row[0] for row in await cur.fetchall()] == [2, 3, 4, 5]
        assert await cur.fetchall() == []


@pytest.mark.parametrize(
    ("host", "port", "http_scheme", "statement_url"),
    [
        ("trino.example", None, None, "http://trino.example:8080/v1/statement"),
        ("trino.example", 443, None, "https://trino.example/v1/statement"),
        ("trino.example", None, "https", "https://trino.example/v1/statement"),
        ("trino.example", 8443, "https", "https://trino.example:8443/v1/statement"),
        ("https://trino.example:8443", None, None, "https://trino.example:8443/v1/statement"),
        ("https://trino.example", None, "http", "https://trino.example/v1/statement"),
        ("http://trino.example", 9000, None, "http://trino.example:9000/v1/statement"),
        ("http://trino.example:9000", 1234, None, "http://trino.example:9000/v1/statement"),
        ("trino.example:9443", None, "https", "https://trino.example:9443/v1/statement"),
        ("http://gw.example/trino/", None, None, "http://gw.example:8080/trino/v1/statement"),
    ],
)
async def test_statement_is_posted_to_the_url_resolved_from_host(
    host: str, port: int | None, http_scheme: Literal["http", "https"] | None, statement_url: str
) -> None:
    base = statement_url.removesuffix("/v1/statement")
    coordinator = FakeCoordinator(statement_pages(columns=None, batches=[], base_url=base))
    async with coordinator.client() as client:
        conn = connect(host=host, port=port, http_scheme=http_scheme, http_client=client)
        await conn.cursor().execute("SELECT 1")

    assert str(coordinator.requests[0].url) == statement_url


def test_connect_rejects_an_unknown_scheme() -> None:
    with pytest.raises(ValueError, match="unsupported http_scheme 'ftp'"):
        _ = connect(host="ftp://trino.example")


def test_connect_and_cursor_need_no_event_loop_and_do_no_io() -> None:
    coordinator = FakeCoordinator(statement_pages(columns=None, batches=[]))

    cur = connect(host="coordinator", http_client=coordinator.client()).cursor()

    assert cur.description is None
    assert coordinator.requests == []


async def test_injected_client_is_left_open_and_unmodified() -> None:
    coordinator = FakeCoordinator(
        statement_pages(columns=[column("_col0", "integer")], batches=[[[1]]])
    )
    async with coordinator.client() as client:
        defaults = dict(client.headers)
        async with (
            connect(host="coordinator", user="alice", http_client=client) as conn,
            conn.cursor() as cur,
        ):
            await cur.execute("SELECT 1")
            await cur.fetchall()

        assert not client.is_closed
        assert dict(client.headers) == defaults


async def test_owned_client_is_created_on_first_request() -> None:
    async with connect(host="127.0.0.1", port=unused_port()) as conn:
        with pytest.raises(TrinoConnectionError, match="failed to reach the Coordinator"):
            await conn.cursor().execute("SELECT 1")


async def test_closing_a_connection_that_never_sent_a_request_is_harmless() -> None:
    conn = connect(host="coordinator")
    await conn.close()
    await conn.close()


def unused_port() -> int:
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])


async def test_a_failed_query_raises_trino_query_error() -> None:
    pages = statement_pages(columns=None, batches=[])
    pages[-1]["error"] = {
        "message": "line 1:1: mismatched input 'SELEC'",
        "errorName": "SYNTAX_ERROR",
        "errorType": "USER_ERROR",
    }
    coordinator = FakeCoordinator(pages)
    async with coordinator.client() as client:
        cur = connect(host="coordinator", http_client=client).cursor()

        with pytest.raises(TrinoQueryError, match="SYNTAX_ERROR: line 1:1") as excinfo:
            await cur.execute("SELEC 1")

    assert excinfo.value.query_id == pages[0]["id"]
    assert excinfo.value.error_name == "SYNTAX_ERROR"


async def test_an_http_error_from_the_coordinator_raises_trino_connection_error() -> None:
    def handler(_: httpx2.Request) -> httpx2.Response:
        return httpx2.Response(500, text="internal failure")

    async with httpx2.AsyncClient(transport=httpx2.MockTransport(handler)) as client:
        cur = connect(host="coordinator", http_client=client).cursor()

        with pytest.raises(TrinoConnectionError, match="error 500: internal failure"):
            await cur.execute("SELECT 1")


async def test_fetching_before_execute_is_a_programming_error() -> None:
    cur = connect(host="coordinator").cursor()

    with pytest.raises(ProgrammingError, match="execute"):
        await cur.fetchone()


async def test_a_closed_cursor_cannot_be_used() -> None:
    coordinator = FakeCoordinator(
        statement_pages(columns=[column("_col0", "integer")], batches=[[[1]]])
    )
    async with coordinator.client() as client:
        async with connect(host="coordinator", http_client=client).cursor() as cur:
            await cur.execute("SELECT 1")

        with pytest.raises(ProgrammingError, match="closed"):
            await cur.fetchall()
        with pytest.raises(ProgrammingError, match="closed"):
            await cur.execute("SELECT 1")


async def test_every_coordinator_request_describes_the_session() -> None:
    coordinator = FakeCoordinator(
        statement_pages(columns=[column("_col0", "integer")], batches=[[[1]]])
    )
    async with coordinator.client() as client:
        conn = connect(
            host="coordinator", user="alice", catalog="tpch", schema="tiny", http_client=client
        )
        await conn.cursor().execute("SELECT 1")

    for request in coordinator.requests:
        assert request.headers["X-Trino-User"] == "alice"
        assert request.headers["X-Trino-Source"] == "asynctrino"
        assert request.headers["X-Trino-Catalog"] == "tpch"
        assert request.headers["X-Trino-Schema"] == "tiny"
        assert request.headers["User-Agent"].startswith("asynctrino/")
    assert coordinator.requests[0].headers["Content-Type"] == "text/plain; charset=utf-8"


async def test_unset_session_fields_send_no_header() -> None:
    coordinator = FakeCoordinator(statement_pages(columns=None, batches=[]))
    async with coordinator.client() as client:
        await connect(host="coordinator", http_client=client).cursor().execute("SELECT 1")

    for header in ("X-Trino-User", "X-Trino-Catalog", "X-Trino-Schema"):
        assert header not in coordinator.requests[0].headers


def test_module_declares_pep_249_globals() -> None:
    assert asynctrino.dbapi.apilevel == "2.0"
    assert asynctrino.dbapi.threadsafety == 1
    assert asynctrino.dbapi.paramstyle == "qmark"


async def test_every_request_advertises_the_client_capabilities() -> None:
    coordinator = FakeCoordinator(
        statement_pages(columns=[column("_col0", "integer")], batches=[[[1]]])
    )
    async with coordinator.client() as client:
        cur = connect(host="coordinator", http_client=client).cursor()
        await cur.execute("SELECT 1")
        await cur.fetchall()

    assert len(coordinator.requests) > 1
    for request in coordinator.requests:
        assert (
            request.headers["X-Trino-Client-Capabilities"]
            == "NUMBER,PARAMETRIC_DATETIME,SESSION_AUTHORIZATION"
        )


async def test_session_time_zone_defaults_to_the_local_zone() -> None:
    coordinator = FakeCoordinator(statement_pages(columns=None, batches=[]))
    async with coordinator.client() as client:
        await connect(host="coordinator", http_client=client).cursor().execute("SELECT 1")

    for request in coordinator.requests:
        assert request.headers["X-Trino-Time-Zone"] == tzlocal.get_localzone_name()


async def test_session_time_zone_can_be_overridden() -> None:
    coordinator = FakeCoordinator(statement_pages(columns=None, batches=[]))
    async with coordinator.client() as client:
        conn = connect(host="coordinator", timezone="Europe/Brussels", http_client=client)
        await conn.cursor().execute("SELECT 1")

    for request in coordinator.requests:
        assert request.headers["X-Trino-Time-Zone"] == "Europe/Brussels"


def test_connect_rejects_an_unknown_time_zone() -> None:
    with pytest.raises(ZoneInfoNotFoundError):
        _ = connect(host="coordinator", timezone="INVALID_TIMEZONE")


def decimal_and_date_rows() -> FakeCoordinator:
    return FakeCoordinator(
        statement_pages(
            columns=[column("d", "decimal", 3, 2), column("day", "date")],
            batches=[[["1.20", "2001-08-22"]], [[None, None]]],
        )
    )


async def test_values_are_mapped_to_python_types() -> None:
    coordinator = decimal_and_date_rows()
    async with coordinator.client() as client:
        cur = connect(host="coordinator", http_client=client).cursor()
        await cur.execute("SELECT d, day FROM t")

        assert await cur.fetchall() == [[Decimal("1.20"), date(2001, 8, 22)], [None, None]]


@pytest.mark.parametrize(
    ("on_connection", "on_cursor", "legacy"),
    [
        (False, None, False),
        (True, None, True),
        (True, False, False),
        (False, True, True),
    ],
)
async def test_legacy_primitive_types_returns_values_as_sent(
    *, on_connection: bool, on_cursor: bool | None, legacy: bool
) -> None:
    coordinator = decimal_and_date_rows()
    async with coordinator.client() as client:
        conn = connect(host="coordinator", legacy_primitive_types=on_connection, http_client=client)
        cur = conn.cursor(legacy_primitive_types=on_cursor)
        await cur.execute("SELECT d, day FROM t")
        rows = await cur.fetchall()

    expected = ["1.20", "2001-08-22"] if legacy else [Decimal("1.20"), date(2001, 8, 22)]
    assert rows == [expected, [None, None]]


async def test_a_value_python_cannot_represent_raises_trino_data_error() -> None:
    coordinator = FakeCoordinator(
        statement_pages(columns=[column("day", "date")], batches=[[["-2001-08-22"]]])
    )
    async with coordinator.client() as client:
        cur = connect(host="coordinator", http_client=client).cursor()

        with pytest.raises(TrinoDataError, match="-2001-08-22"):
            await cur.execute("SELECT DATE '-2001-08-22'")


async def test_description_reports_length_precision_and_scale_by_type() -> None:
    def described(name: str, type_code: str, raw_type: str, *arguments: int) -> JSON:
        described_column = column(name, raw_type, *arguments)
        described_column["type"] = type_code
        return described_column

    columns = [
        described("d", "decimal(38, 0)", "decimal", 38, 0),
        described("c", "char(5)", "char", 5),
        described("v", "varchar", "varchar", 2147483647),
        described("t", "time(3)", "time", 3),
        described("tz", "time(9) with time zone", "time with time zone", 9),
        described("ts", "timestamp(6)", "timestamp", 6),
        described("tstz", "timestamp(3) with time zone", "timestamp with time zone", 3),
        described("day", "date", "date"),
        described("n", "number", "number"),
    ]
    coordinator = FakeCoordinator(statement_pages(columns=columns, batches=[]))
    async with coordinator.client() as client:
        cur = connect(host="coordinator", http_client=client).cursor()
        await cur.execute("SELECT ...")

    assert cur.description == [
        ("d", "decimal(38, 0)", None, None, 38, 0, None),
        ("c", "char(5)", None, 5, None, None, None),
        ("v", "varchar", None, 2147483647, None, None, None),
        ("t", "time(3)", None, None, 3, None, None),
        ("tz", "time(9) with time zone", None, None, 9, None, None),
        ("ts", "timestamp(6)", None, None, 6, None, None),
        ("tstz", "timestamp(3) with time zone", None, None, 3, None, None),
        ("day", "date", None, None, None, None, None),
        ("n", "number", None, None, None, None, None),
    ]


async def test_a_closed_connection_and_its_cursors_cannot_be_used() -> None:
    coordinator = FakeCoordinator(
        statement_pages(columns=[column("_col0", "integer")], batches=[[[1]]])
    )
    async with coordinator.client() as client:
        conn = connect(host="coordinator", http_client=client)
        cur = conn.cursor()
        await cur.execute("SELECT 1")
        await conn.close()
        sent = len(coordinator.requests)

        with pytest.raises(ProgrammingError, match="connection is closed"):
            _ = conn.cursor()
        with pytest.raises(ProgrammingError, match="connection is closed"):
            await cur.execute("SELECT 1")
        with pytest.raises(ProgrammingError, match="connection is closed"):
            await cur.fetchall()
        with pytest.raises(ProgrammingError, match="connection is closed"):
            await conn.commit()
        with pytest.raises(ProgrammingError, match="connection is closed"):
            await conn.rollback()
        await conn.close()

    assert len(coordinator.requests) == sent


async def test_closing_a_connection_inside_its_async_with_block_is_harmless() -> None:
    async with connect(host="coordinator") as conn:
        await conn.close()
