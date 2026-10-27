import uuid
from collections.abc import AsyncIterator

import httpx2
import pytest

from asynctrino.dbapi import Connection, connect
from tests.integration.trino import SegmentStorageRoute, segment_storage_route

LINEITEM = """
    SELECT n, orderkey, linenumber, comment
    FROM tpch.tiny.lineitem, TABLE(sequence(start => 1, stop => 5, step => 1)) AS t (n)
"""
LINEITEM_ROWS = 5 * 60175


@pytest.fixture
async def route() -> AsyncIterator[SegmentStorageRoute]:
    route = segment_storage_route()
    yield route
    await route.aclose()


def spooling_connection(
    trino_address: tuple[str, int], route: SegmentStorageRoute, encoding: str
) -> tuple[httpx2.AsyncClient, Connection]:
    host, port = trino_address
    client = httpx2.AsyncClient(
        transport=route, headers={"X-Client-Default": "d"}, cookies={"c": "v"}, timeout=60
    )
    conn = connect(
        host=host,
        port=port,
        user="test",
        encoding=encoding,
        http_headers={"X-Custom": "c"},
        http_client=client,
    )
    return client, conn


@pytest.mark.parametrize("encoding", ["json+zstd", "json+lz4", "json"])
async def test_spooled_segments_are_downloaded_from_storage_without_client_headers(
    trino_address: tuple[str, int], route: SegmentStorageRoute, encoding: str
) -> None:
    client, conn = spooling_connection(trino_address, route, encoding)
    async with client, conn:
        cur = conn.cursor()
        await cur.execute(LINEITEM)
        first = await cur.fetchmany(10)
        rows = [*first, *[row async for row in cur]]

    assert len(rows) == LINEITEM_ROWS
    assert len({(n, orderkey, line) for n, orderkey, line, _ in rows}) == LINEITEM_ROWS
    assert len(route.requests) > 0
    for request in route.requests:
        assert request.method == "GET"
        assert not any(
            name.lower().startswith("x-trino-")
            or name.lower() in {"authorization", "cookie", "x-custom", "x-client-default"}
            for name in request.headers
        ), request.headers


async def test_a_segment_cursor_yields_segments_whose_rows_make_up_the_result(
    trino_address: tuple[str, int], route: SegmentStorageRoute
) -> None:
    client, conn = spooling_connection(trino_address, route, "json+zstd")
    async with client, conn:
        cur = conn.cursor(cursor_style="segment")
        await cur.execute(LINEITEM)
        segments = await cur.fetchall()
        assert route.requests == []

        rows = [row for segment in segments for row in await segment.rows()]
        for segment in segments:
            await segment.acknowledge()

    assert cur.description is not None
    assert [column.name for column in cur.description] == ["n", "orderkey", "linenumber", "comment"]
    assert len(rows) == LINEITEM_ROWS
    assert [s.metadata.row_offset for s in segments] == sorted(
        s.metadata.row_offset for s in segments
    )
    assert sum(s.metadata.rows_count for s in segments) == LINEITEM_ROWS
    assert any(s.uri is not None for s in segments)
    assert all(s.encoding == "json+zstd" for s in segments)


async def test_an_update_statement_runs_to_completion_over_spooling(
    encoded_conn: Connection,
) -> None:
    table = f"memory.default.spooled_{uuid.uuid4().hex}"
    cur = encoded_conn.cursor()
    await cur.execute(f"CREATE TABLE {table} AS SELECT 1 AS n")
    try:
        await cur.execute(f"INSERT INTO {table} VALUES 2, 3")  # noqa: S608
        assert cur.rowcount == 2

        await cur.execute(f"SELECT n FROM {table} ORDER BY n")  # noqa: S608
        assert await cur.fetchall() == [[1], [2], [3]]
    finally:
        await cur.execute(f"DROP TABLE {table}")
