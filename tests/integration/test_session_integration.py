import uuid
from collections.abc import AsyncGenerator, Mapping, Sequence
from contextlib import asynccontextmanager
from dataclasses import dataclass, field
from typing import TypedDict, Unpack

import httpx2

from asynctrino.dbapi import Connection, Cursor, Row, connect


@dataclass
class RecordedRequests:
    requests: list[httpx2.Request] = field(default_factory=list[httpx2.Request])

    async def record(self, request: httpx2.Request) -> None:
        self.requests.append(request)

    @property
    def last_statement(self) -> httpx2.Request:
        return next(r for r in reversed(self.requests) if r.method == "POST")


class SessionOptions(TypedDict, total=False):
    catalog: str
    session_properties: Mapping[str, str]
    roles: Mapping[str, str] | str
    timezone: str
    client_tags: Sequence[str]


@asynccontextmanager
async def recorded_connection(
    trino_address: tuple[str, int], **options: Unpack[SessionOptions]
) -> AsyncGenerator[tuple[Connection, RecordedRequests]]:
    host, port = trino_address
    recorded = RecordedRequests()
    async with (
        httpx2.AsyncClient(event_hooks={"request": [recorded.record]}) as client,
        connect(
            host=host,
            port=port,
            user="test",
            catalog=options.get("catalog"),
            session_properties=options.get("session_properties"),
            roles=options.get("roles"),
            timezone=options.get("timezone"),
            client_tags=options.get("client_tags", ()),
            http_client=client,
        ) as conn,
    ):
        yield conn, recorded


async def run(conn: Connection, sql: str) -> list[Row]:
    async with conn.cursor() as cur:
        await cur.execute(sql)
        return await cur.fetchall()


async def session_property(conn: Connection, name: str) -> str:
    rows = await run(conn, f"SHOW SESSION LIKE '{name}'")
    return str(rows[0][1])


async def test_use_changes_the_catalog_and_schema(trino_address: tuple[str, int]) -> None:
    async with recorded_connection(trino_address) as (conn, _):
        assert await run(conn, "SELECT current_catalog, current_schema") == [[None, None]]

        await run(conn, "USE tpch.tiny")
        assert await run(conn, "SELECT current_catalog, current_schema") == [["tpch", "tiny"]]

        await run(conn, "USE sf1")
        assert await run(conn, "SELECT current_catalog, current_schema") == [["tpch", "sf1"]]


async def test_session_properties_from_connect(trino_address: tuple[str, int]) -> None:
    properties = {"query_max_run_time": "10m", "query_priority": "3"}
    async with recorded_connection(trino_address, session_properties=properties) as (conn, _):
        assert await session_property(conn, "query_max_run_time") == "10m"
        assert await session_property(conn, "query_priority") == "3"


async def test_set_and_reset_session(trino_address: tuple[str, int]) -> None:
    async with recorded_connection(trino_address) as (conn, recorded):
        default = await session_property(conn, "query_max_run_time")

        await run(conn, "SET SESSION query_max_run_time = '7m'")
        assert await session_property(conn, "query_max_run_time") == "7m"
        assert recorded.last_statement.headers["X-Trino-Session"] == "query_max_run_time=7m"

        await run(conn, "RESET SESSION query_max_run_time")
        assert await session_property(conn, "query_max_run_time") == default
        assert recorded.last_statement.headers["X-Trino-Session"] == ""


async def test_set_role_is_sent_with_later_statements(trino_address: tuple[str, int]) -> None:
    async with recorded_connection(trino_address, catalog="tpch") as (conn, recorded):
        await run(conn, "SHOW TABLES FROM information_schema")
        assert "X-Trino-Role" not in recorded.last_statement.headers

        await run(conn, "SET ROLE ALL")
        await run(conn, "SHOW TABLES FROM information_schema")

        assert recorded.last_statement.headers["X-Trino-Role"] == "system=ALL"


async def test_roles_from_connect(trino_address: tuple[str, int]) -> None:
    for roles in ({"system": "ALL"}, "ALL"):
        async with recorded_connection(trino_address, catalog="tpch", roles=roles) as (conn, rec):
            await run(conn, "SHOW TABLES FROM information_schema")

            assert rec.last_statement.headers["X-Trino-Role"] == "system=ALL"


async def test_timezone_from_connect(trino_address: tuple[str, int]) -> None:
    async with recorded_connection(trino_address, timezone="Europe/Brussels") as (conn, _):
        assert await run(conn, "SELECT current_timezone()") == [["Europe/Brussels"]]


async def test_client_tags_reach_the_query_session(trino_address: tuple[str, int]) -> None:
    client_tags = ["foo %20", "bar=test"]
    async with (
        recorded_connection(trino_address, client_tags=client_tags) as (conn, _),
        conn.cursor() as cur,
    ):
        await cur.execute("SELECT 1")
        await cur.fetchall()
        query_id = cur.query_id

    host, port = trino_address
    async with httpx2.AsyncClient(base_url=f"http://{host}:{port}") as ui:
        login = await ui.post("/ui/auth/login", json={"username": "admin", "password": ""})
        assert login.is_success
        query_info = (await ui.get(f"/ui/api/query/{query_id}")).json()

    assert query_info["session"]["clientTags"] == client_tags


async def test_execute_returns_the_cursor(conn: Connection) -> None:
    async with conn.cursor() as cur:
        assert await (await cur.execute("SELECT 1")).fetchone() == [1]


async def test_select_query_stats_progress(conn: Connection) -> None:
    async with conn.cursor() as cur:
        await cur.execute("SELECT * FROM tpch.sf1.customer LIMIT 1000")
        stats = cur.stats
        assert stats is not None
        query_id = stats["queryId"]
        assert query_id == cur.query_id
        completed_splits = int(str(stats["completedSplits"]))

        while await cur.fetchone() is not None:
            assert cur.stats is not None
            assert cur.stats["queryId"] == query_id
            assert completed_splits <= int(str(cur.stats["completedSplits"]))

        assert cur.stats is not None
        assert cur.stats["state"] == "FINISHED"


async def test_stats_callback_follows_the_query(conn: Connection) -> None:
    received: list[dict[str, object]] = []
    async with conn.cursor(stats_callback=received.append) as cur:
        await cur.execute("SELECT * FROM tpch.tiny.nation")
        await cur.fetchall()

    assert len(received) > 1
    assert received[-1]["state"] == "FINISHED"


async def test_query_metadata(conn: Connection) -> None:
    async with conn.cursor() as cur:
        assert cur.info_uri is None
        await cur.execute("SELECT * FROM system.runtime.nodes")

        assert cur.query == "SELECT * FROM system.runtime.nodes"
        assert cur.query_id is not None
        assert cur.info_uri is not None
        assert cur.query_id in cur.info_uri
        assert cur.update_type is None
        assert cur.warnings == []
        await cur.fetchall()
        assert cur.rowcount == -1


@asynccontextmanager
async def memory_table(conn: Connection, definition: str) -> AsyncGenerator[tuple[str, Cursor]]:
    name = f"memory.default.test_{uuid.uuid4().hex}"
    async with conn.cursor() as cur:
        await cur.execute(f"CREATE TABLE {name} {definition}")
        try:
            yield name, cur
        finally:
            await run(conn, f"DROP TABLE {name}")


async def test_rowcount_of_create_table(conn: Connection) -> None:
    async with memory_table(conn, "(a varchar)") as (_, cur):
        assert cur.update_type == "CREATE TABLE"
        assert cur.rowcount == -1


async def test_rowcount_of_create_table_as_select(conn: Connection) -> None:
    async with memory_table(conn, "AS SELECT 1 a UNION ALL SELECT 2") as (_, cur):
        assert cur.update_type == "CREATE TABLE"
        assert cur.rowcount == 2


async def test_rowcount_of_insert(conn: Connection) -> None:
    async with memory_table(conn, "(a varchar)") as (name, cur):
        await cur.execute(f"INSERT INTO {name} (a) VALUES ('x'), ('y'), ('z')")  # noqa: S608

        assert cur.update_type == "INSERT"
        assert cur.rowcount == 3
