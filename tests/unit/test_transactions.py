import asyncio
from typing import cast

import httpx2
import pytest

from asynctrino.dbapi import Connection, IsolationLevel, ProgrammingError, connect
from asynctrino.exceptions import TrinoUserError
from tests.unit.coordinator import BASE_URL, JSON, TransactionCoordinator


async def test_transaction_starts_on_first_execute_and_its_id_is_carried_on() -> None:
    coordinator = TransactionCoordinator()
    async with coordinator.client() as client:
        conn = connect(
            host="coordinator", isolation_level=IsolationLevel.READ_COMMITTED, http_client=client
        )
        cur = conn.cursor()
        assert coordinator.requests == []

        await cur.execute("SELECT 1")
        await cur.execute("SELECT 2")

    assert coordinator.statements() == [
        ("START TRANSACTION ISOLATION LEVEL READ COMMITTED", "NONE"),
        ("SELECT 1", "tx-1"),
        ("SELECT 2", "tx-1"),
    ]


async def test_transaction_id_is_read_from_a_later_page_of_start_transaction() -> None:
    sent: list[httpx2.Request] = []

    def handler(request: httpx2.Request) -> httpx2.Response:
        sent.append(request)
        page: JSON = {"id": "q", "infoUri": f"{BASE_URL}/ui", "stats": {"state": "FINISHED"}}
        if request.content.startswith(b"START"):
            page |= {"stats": {"state": "QUEUED"}, "nextUri": f"{BASE_URL}/v1/statement/q/1"}
            return httpx2.Response(200, json=page)
        if request.method == "GET":
            return httpx2.Response(200, json=page, headers={"X-Trino-Started-Transaction-Id": "t"})
        return httpx2.Response(200, json=page)

    async with httpx2.AsyncClient(transport=httpx2.MockTransport(handler)) as client:
        await transactional(client).cursor().execute("SELECT 1")

    assert [(r.method, r.headers["X-Trino-Transaction-Id"]) for r in sent] == [
        ("POST", "NONE"),
        ("GET", "NONE"),
        ("POST", "t"),
    ]


def test_isolation_level_accepts_its_sql_name() -> None:
    conn = connect(host="coordinator", isolation_level=cast("IsolationLevel", "READ COMMITTED"))

    assert conn.isolation_level is IsolationLevel.READ_COMMITTED


def test_unknown_isolation_level_is_rejected() -> None:
    with pytest.raises(ValueError, match="CHAOS"):
        _ = connect(host="coordinator", isolation_level=cast("IsolationLevel", "CHAOS"))


def transactional(client: httpx2.AsyncClient) -> Connection:
    return connect(
        host="coordinator", isolation_level=IsolationLevel.SERIALIZABLE, http_client=client
    )


async def test_changing_the_isolation_level_applies_to_the_next_transaction() -> None:
    coordinator = TransactionCoordinator()
    async with coordinator.client() as client:
        conn = connect(host="coordinator", http_client=client)
        cur = conn.cursor()
        await cur.execute("SELECT 1")
        conn.isolation_level = IsolationLevel.REPEATABLE_READ
        await cur.execute("SELECT 2")
        await conn.commit()
        conn.isolation_level = cast("IsolationLevel", "AUTOCOMMIT")
        await cur.execute("SELECT 3")

        assert conn.isolation_level is IsolationLevel.AUTOCOMMIT
    assert coordinator.statements() == [
        ("SELECT 1", None),
        ("START TRANSACTION ISOLATION LEVEL REPEATABLE READ", "NONE"),
        ("SELECT 2", "tx-1"),
        ("COMMIT", "tx-1"),
        ("SELECT 3", None),
    ]


async def test_isolation_level_cannot_change_while_a_transaction_is_open() -> None:
    coordinator = TransactionCoordinator()
    async with coordinator.client() as client:
        conn = transactional(client)
        await conn.cursor().execute("SELECT 1")

        with pytest.raises(ProgrammingError, match="transaction is open"):
            conn.isolation_level = IsolationLevel.AUTOCOMMIT

        assert conn.isolation_level is IsolationLevel.SERIALIZABLE


async def test_autocommit_is_the_default_and_sends_no_transaction_header() -> None:
    coordinator = TransactionCoordinator()
    async with coordinator.client() as client:
        conn = connect(host="coordinator", http_client=client)
        await conn.cursor().execute("SELECT 1")

        assert conn.isolation_level is IsolationLevel.AUTOCOMMIT
    assert coordinator.statements() == [("SELECT 1", None)]


@pytest.mark.parametrize("end", ["commit", "rollback"])
async def test_ending_a_transaction_makes_the_next_execute_start_another(end: str) -> None:
    coordinator = TransactionCoordinator()
    async with coordinator.client() as client:
        conn = transactional(client)
        cur = conn.cursor()
        await cur.execute("SELECT 1")
        await getattr(conn, end)()
        await cur.execute("SELECT 2")

    assert coordinator.statements() == [
        ("START TRANSACTION ISOLATION LEVEL SERIALIZABLE", "NONE"),
        ("SELECT 1", "tx-1"),
        (end.upper(), "tx-1"),
        ("START TRANSACTION ISOLATION LEVEL SERIALIZABLE", "NONE"),
        ("SELECT 2", "tx-2"),
    ]


@pytest.mark.parametrize(
    "isolation_level", [IsolationLevel.AUTOCOMMIT, IsolationLevel.SERIALIZABLE]
)
@pytest.mark.parametrize("end", ["commit", "rollback"])
async def test_commit_and_rollback_without_a_transaction_do_nothing(
    isolation_level: IsolationLevel, end: str
) -> None:
    coordinator = TransactionCoordinator()
    async with coordinator.client() as client:
        conn = connect(host="coordinator", isolation_level=isolation_level, http_client=client)
        await getattr(conn, end)()

    assert coordinator.requests == []


def statement_sent_before_close(
    conn: Connection, coordinator: TransactionCoordinator, monkeypatch: pytest.MonkeyPatch
) -> list[str]:
    """Spy on ``conn.close``, recording the last statement sent before each call."""
    seen: list[str] = []
    close = conn.close

    async def recording_close() -> None:
        seen.append(coordinator.statements()[-1][0])
        await close()

    monkeypatch.setattr(conn, "close", recording_close)
    return seen


async def test_async_with_commits_on_clean_exit_then_closes(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    coordinator = TransactionCoordinator()
    async with coordinator.client() as client:
        conn = transactional(client)
        closed_after = statement_sent_before_close(conn, coordinator, monkeypatch)
        async with conn:
            await conn.cursor().execute("SELECT 1")

    assert closed_after == ["COMMIT"]


async def test_async_with_rolls_back_on_error_then_closes(monkeypatch: pytest.MonkeyPatch) -> None:
    coordinator = TransactionCoordinator()
    async with coordinator.client() as client:
        conn = transactional(client)
        closed_after = statement_sent_before_close(conn, coordinator, monkeypatch)

        async def fail_inside_a_transaction() -> None:
            async with conn:
                await conn.cursor().execute("SELECT 1")
                raise LookupError

        with pytest.raises(LookupError):
            await fail_inside_a_transaction()

    assert closed_after == ["ROLLBACK"]


async def test_async_with_closes_even_when_commit_fails(monkeypatch: pytest.MonkeyPatch) -> None:
    coordinator = TransactionCoordinator(failing_statements={"COMMIT"})
    async with coordinator.client() as client:
        conn = transactional(client)
        closed_after = statement_sent_before_close(conn, coordinator, monkeypatch)
        with pytest.raises(TrinoUserError):
            async with conn:
                await conn.cursor().execute("SELECT 1")

    assert closed_after == ["COMMIT"]


async def test_commit_reads_its_statement_to_the_end() -> None:
    coordinator = TransactionCoordinator(open_statements={"COMMIT"})
    async with coordinator.client() as client:
        conn = transactional(client)
        await conn.cursor().execute("SELECT 1")
        await conn.commit()

    assert [r.method for r in coordinator.requests][-2:] == ["POST", "GET"]


async def test_a_failed_commit_still_ends_the_transaction() -> None:
    coordinator = TransactionCoordinator(failing_statements={"COMMIT"})
    async with coordinator.client() as client:
        conn = transactional(client)
        await conn.cursor().execute("SELECT 1")
        with pytest.raises(TrinoUserError):
            await conn.commit()
        await conn.cursor().execute("SELECT 2")

    assert coordinator.statements()[-2:] == [
        ("START TRANSACTION ISOLATION LEVEL SERIALIZABLE", "NONE"),
        ("SELECT 2", "tx-2"),
    ]


async def test_a_second_statement_in_flight_in_a_transaction_is_refused() -> None:
    coordinator = TransactionCoordinator(open_statements={"SELECT 1"})
    async with coordinator.client() as client:
        conn = transactional(client)
        first, second = conn.cursor(), conn.cursor()
        await first.execute("SELECT 1")
        sent = len(coordinator.requests)

        with pytest.raises(ProgrammingError, match="in flight"):
            await second.execute("SELECT 2")
        with pytest.raises(ProgrammingError, match="in flight"):
            await first.execute("SELECT 2")

        assert len(coordinator.requests) == sent


@pytest.mark.parametrize("finish", ["fetchall", "cancel", "close", "commit", "rollback"])
async def test_the_next_statement_may_run_once_the_one_in_flight_is_done(finish: str) -> None:
    coordinator = TransactionCoordinator(open_statements={"SELECT 1"})
    async with coordinator.client() as client:
        conn = transactional(client)
        first = conn.cursor()
        await first.execute("SELECT 1")
        target = conn if finish in {"commit", "rollback"} else first
        await getattr(target, finish)()

        await conn.cursor().execute("SELECT 2")

    assert coordinator.statements()[-1][0] == "SELECT 2"


async def test_a_failed_statement_is_no_longer_in_flight() -> None:
    coordinator = TransactionCoordinator(failing_statements={"SELECT 1/0"})
    async with coordinator.client() as client:
        conn = transactional(client)
        with pytest.raises(TrinoUserError):
            await conn.cursor().execute("SELECT 1/0")
        await conn.cursor().execute("SELECT 2")

    assert coordinator.statements()[-1] == ("SELECT 2", "tx-1")


async def test_a_statement_failing_while_fetching_is_no_longer_in_flight() -> None:
    coordinator = TransactionCoordinator(
        open_statements={"SELECT 1/0"}, failing_statements={"SELECT 1/0"}
    )
    async with coordinator.client() as client:
        conn = transactional(client)
        cur = conn.cursor()
        await cur.execute("SELECT 1/0")
        with pytest.raises(TrinoUserError):
            await cur.fetchall()
        await conn.cursor().execute("SELECT 2")

    assert coordinator.statements()[-1] == ("SELECT 2", "tx-1")


async def test_a_transaction_that_fails_to_start_runs_no_statement() -> None:
    coordinator = TransactionCoordinator(
        failing_statements={"START TRANSACTION ISOLATION LEVEL SERIALIZABLE"}
    )
    async with coordinator.client() as client:
        cur = transactional(client).cursor()
        for _ in range(2):
            with pytest.raises(TrinoUserError):
                await cur.execute("SELECT 1")

    assert [sql for sql, _ in coordinator.statements()] == [
        "START TRANSACTION ISOLATION LEVEL SERIALIZABLE"
    ] * 2


async def test_concurrent_executes_in_a_transaction_fail_fast() -> None:
    coordinator = TransactionCoordinator()
    async with coordinator.client() as client:
        conn = transactional(client)
        results = await asyncio.gather(
            conn.cursor().execute("SELECT 1"),
            conn.cursor().execute("SELECT 2"),
            return_exceptions=True,
        )

    assert isinstance(results[1], ProgrammingError)
    assert [sql for sql, _ in coordinator.statements()] == [
        "START TRANSACTION ISOLATION LEVEL SERIALIZABLE",
        "SELECT 1",
    ]


async def test_autocommit_cursors_may_overlap() -> None:
    coordinator = TransactionCoordinator(open_statements={"SELECT 1"})
    async with coordinator.client() as client:
        conn = connect(host="coordinator", http_client=client)
        await conn.cursor().execute("SELECT 1")
        await conn.cursor().execute("SELECT 2")

    assert [sql for sql, _ in coordinator.statements()] == ["SELECT 1", "SELECT 2"]
