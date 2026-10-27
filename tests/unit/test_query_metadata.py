from asynctrino.dbapi import connect
from tests.unit.coordinator import FakeCoordinator, column, statement_pages


async def test_metadata_is_unset_before_execute() -> None:
    cur = connect(host="coordinator").cursor()

    assert cur.rowcount == -1
    assert cur.query_id is None
    assert cur.query is None
    assert cur.stats is None
    assert cur.warnings is None
    assert cur.info_uri is None
    assert cur.update_type is None


async def test_select_exposes_its_query_metadata() -> None:
    pages = statement_pages(columns=[column("_col0", "integer")], batches=[[[1]]])
    warning = {"warningCode": {"code": 1, "name": "DEPRECATED"}, "message": "old"}
    pages[0]["warnings"] = [warning]
    coordinator = FakeCoordinator(pages)
    async with coordinator.client() as client:
        cur = connect(host="coordinator", http_client=client).cursor()
        await cur.execute("SELECT 1")

        assert cur.query == "SELECT 1"
        assert cur.query_id == "20261027_000000_00000_abcde"
        assert cur.info_uri == "http://coordinator:8080/ui/query.html?20261027_000000_00000_abcde"
        assert cur.stats == {"queryId": "20261027_000000_00000_abcde", "state": "RUNNING"}
        assert cur.warnings == [warning]
        assert cur.update_type is None
        assert cur.rowcount == -1


async def test_an_update_statement_is_drained_by_execute_and_reports_its_rowcount() -> None:
    pages = statement_pages(columns=[column("rows", "bigint")], batches=[[[3]]], queued_pages=1)
    for page in pages[1:]:
        page["updateType"] = "INSERT"
    pages[-1]["updateCount"] = 3
    coordinator = FakeCoordinator(pages)
    async with coordinator.client() as client:
        cur = connect(host="coordinator", http_client=client).cursor()
        await cur.execute("INSERT INTO t VALUES 1, 2, 3")

        assert len(coordinator.requests) == len(pages)
        assert cur.update_type == "INSERT"
        assert cur.rowcount == 3
        assert await cur.fetchall() == [[3]]


async def test_stats_callback_receives_stats_after_every_response() -> None:
    pages = statement_pages(columns=[column("_col0", "integer")], batches=[[[1]], [[2]]])
    for index, page in enumerate(pages):
        page["stats"] = {"state": "RUNNING", "completedSplits": index}
    coordinator = FakeCoordinator(pages)
    received: list[dict[str, object]] = []
    async with coordinator.client() as client:
        cur = connect(host="coordinator", http_client=client).cursor(stats_callback=received.append)
        await cur.execute("SELECT 1")
        assert [s["completedSplits"] for s in received] == [0, 1]

        await cur.fetchall()

    assert [s["completedSplits"] for s in received] == [0, 1, 2, 3]
    assert all(s["queryId"] == "20261027_000000_00000_abcde" for s in received)


async def test_stats_callback_cannot_mutate_the_cursor_stats() -> None:
    pages = statement_pages(columns=None, batches=[], queued_pages=0)
    pages[0]["stats"] = {"state": "FINISHED", "rootStage": {"state": "FINISHED"}}
    coordinator = FakeCoordinator(pages)

    def tamper(stats: dict[str, object]) -> None:
        stats["state"] = "TAMPERED"
        root_stage = stats["rootStage"]
        assert isinstance(root_stage, dict)
        root_stage["state"] = "TAMPERED"

    async with coordinator.client() as client:
        cur = connect(host="coordinator", http_client=client).cursor(stats_callback=tamper)
        await cur.execute("SELECT 1")

    assert cur.stats == {
        "queryId": "20261027_000000_00000_abcde",
        "state": "FINISHED",
        "rootStage": {"state": "FINISHED"},
    }
