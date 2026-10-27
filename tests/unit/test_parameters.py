import pytest

import asynctrino.dbapi
from asynctrino.exceptions import NotSupportedError, ProgrammingError
from tests.unit.coordinator import FakeCoordinator, column, statement_pages


async def test_parameters_are_bound_with_execute_immediate() -> None:
    coordinator = FakeCoordinator(
        statement_pages(columns=[column("_col0", "integer")], batches=[[[1]]])
    )
    async with coordinator.client() as client:
        cur = asynctrino.dbapi.connect(host="coordinator", http_client=client).cursor()
        await cur.execute("SELECT ? WHERE 'a' = ?", [1, "it's"])

        assert await cur.fetchall() == [[1]]

    statement = coordinator.requests[0]
    assert statement.content == b"EXECUTE IMMEDIATE 'SELECT ? WHERE ''a'' = ?' USING 1,'it''s'"
    assert not any("prepared" in name.lower() for name in statement.headers)


async def test_empty_parameters_run_the_statement_as_is() -> None:
    coordinator = FakeCoordinator(statement_pages(columns=None, batches=[], queued_pages=0))
    async with coordinator.client() as client:
        cur = asynctrino.dbapi.connect(host="coordinator", http_client=client).cursor()
        await cur.execute("SELECT '?'", [])

    assert coordinator.requests[0].content == b"SELECT '?'"


@pytest.mark.parametrize("params", ["ab", {"a": 1}, {1, 2}, object])
async def test_parameters_must_be_a_list_or_tuple(params: object) -> None:
    coordinator = FakeCoordinator(statement_pages(columns=None, batches=[], queued_pages=0))
    async with coordinator.client() as client:
        cur = asynctrino.dbapi.connect(host="coordinator", http_client=client).cursor()

        with pytest.raises(ProgrammingError, match="list or tuple"):
            await cur.execute("SELECT ?", params)  # pyrefly: ignore[bad-argument-type]

    assert coordinator.requests == []


async def test_an_unsupported_parameter_raises_before_anything_is_sent() -> None:
    coordinator = FakeCoordinator(statement_pages(columns=None, batches=[], queued_pages=0))
    async with coordinator.client() as client:
        cur = asynctrino.dbapi.connect(host="coordinator", http_client=client).cursor()

        with pytest.raises(NotSupportedError, match="not supported"):
            await cur.execute("SELECT ?", [object()])

    assert coordinator.requests == []


def inserts(count: int) -> FakeCoordinator:
    pages = statement_pages(columns=[column("rows", "bigint")], batches=[[[count]]])
    for page in pages[1:]:
        page["updateType"] = "INSERT"
    pages[-1]["updateCount"] = count
    return FakeCoordinator(pages)


async def test_executemany_runs_once_per_parameter_set_and_totals_the_rowcount() -> None:
    coordinator = inserts(2)
    async with coordinator.client() as client:
        cur = asynctrino.dbapi.connect(host="coordinator", http_client=client).cursor()
        await cur.executemany("INSERT INTO t VALUES (?, ?)", [(1, "a"), (2, "b"), (3, "c")])

        assert cur.update_type == "INSERT"
        assert cur.rowcount == 6

        await cur.execute("INSERT INTO t VALUES (?, ?)", (4, "d"))
        assert cur.rowcount == 2

    posted = [r.content for r in coordinator.requests if r.method == "POST"]
    assert posted == [
        b"EXECUTE IMMEDIATE 'INSERT INTO t VALUES (?, ?)' USING 1,'a'",
        b"EXECUTE IMMEDIATE 'INSERT INTO t VALUES (?, ?)' USING 2,'b'",
        b"EXECUTE IMMEDIATE 'INSERT INTO t VALUES (?, ?)' USING 3,'c'",
        b"EXECUTE IMMEDIATE 'INSERT INTO t VALUES (?, ?)' USING 4,'d'",
    ]


async def test_executemany_without_parameter_sets_runs_the_statement_once_as_is() -> None:
    coordinator = inserts(1)
    async with coordinator.client() as client:
        cur = asynctrino.dbapi.connect(host="coordinator", http_client=client).cursor()
        await cur.executemany("INSERT INTO t VALUES (1)", [])

        assert cur.rowcount == 1

    assert [r.content for r in coordinator.requests if r.method == "POST"] == [
        b"INSERT INTO t VALUES (1)"
    ]


async def test_executemany_of_a_query_returning_rows_raises_not_supported_error() -> None:
    coordinator = FakeCoordinator(
        statement_pages(columns=[column("_col0", "integer")], batches=[[[1]]])
    )
    async with coordinator.client() as client:
        cur = asynctrino.dbapi.connect(host="coordinator", http_client=client).cursor()

        with pytest.raises(NotSupportedError, match="Query must return update type"):
            await cur.executemany("SELECT ?", [(1,), (2,)])
