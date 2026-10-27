import httpx2
import pytest

from asynctrino.dbapi import Connection, connect
from asynctrino.exceptions import TrinoQueryError


@pytest.fixture
def conn(encoded_conn: Connection) -> Connection:
    """Run every test here over the Direct protocol and the Spooling protocol's encodings."""
    return encoded_conn


async def test_select_1(conn: Connection) -> None:
    cur = conn.cursor()
    await cur.execute("SELECT 1")

    assert cur.description == [("_col0", "integer", None, None, None, None, None)]
    assert await cur.fetchall() == [[1]]


async def test_integer_and_varchar_columns(conn: Connection) -> None:
    async with conn.cursor() as cur:
        await cur.execute("SELECT * FROM (VALUES (1, 'a'), (2, 'bc')) AS t (n, s)")

        assert cur.description == [
            ("n", "integer", None, None, None, None, None),
            ("s", "varchar(2)", None, 2, None, None, None),
        ]
        assert await cur.fetchone() == [1, "a"]
        assert await cur.fetchone() == [2, "bc"]
        assert await cur.fetchone() is None


async def test_rows_stream_across_many_pages(trino_address: tuple[str, int]) -> None:
    host, port = trino_address
    pages: list[httpx2.Request] = []

    async def count_page(request: httpx2.Request) -> None:
        if request.method == "GET":
            pages.append(request)

    async with (
        httpx2.AsyncClient(event_hooks={"request": [count_page]}) as client,
        connect(host=host, port=port, user="test", http_client=client) as conn,
    ):
        cur = conn.cursor()
        await cur.execute("SELECT count(*) FROM tpch.tiny.lineitem")
        [[expected]] = await cur.fetchall()
        pages.clear()

        await cur.execute("SELECT orderkey, linenumber FROM tpch.tiny.lineitem")
        first = await cur.fetchmany(3)
        streamed = len(first) + len([row async for row in cur])

    assert streamed == expected
    assert len(pages) > 2


async def test_query_against_the_default_catalog_and_schema(conn: Connection) -> None:
    cur = conn.cursor()
    await cur.execute("SELECT name FROM nation ORDER BY nationkey")

    rows = await cur.fetchall()

    assert len(rows) == 25
    assert rows[0] == ["ALGERIA"]


async def test_a_syntax_error_raises_trino_query_error(conn: Connection) -> None:
    with pytest.raises(TrinoQueryError, match="SYNTAX_ERROR") as excinfo:
        await conn.cursor().execute("SELEC 1")

    assert excinfo.value.query_id is not None


async def test_host_url_with_scheme_and_port_and_an_injected_client(
    trino_address: tuple[str, int],
) -> None:
    host, port = trino_address
    async with httpx2.AsyncClient() as client:
        async with connect(host=f"http://{host}:{port}", user="test", http_client=client) as conn:
            cur = conn.cursor()
            await cur.execute("SELECT 'ok'")
            assert await cur.fetchall() == [["ok"]]

        assert not client.is_closed
