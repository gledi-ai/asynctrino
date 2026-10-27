import asyncio
from collections.abc import Sequence
from dataclasses import dataclass, field
from zoneinfo import ZoneInfoNotFoundError

import httpx2
import pytest

from asynctrino.dbapi import Connection, connect
from tests.unit.coordinator import BASE_URL

type ResponseHeaders = Sequence[tuple[str, str]]


@dataclass
class SessionCoordinator:
    """Finishes every statement in its POST, answering with the next scripted response headers."""

    response_headers: list[ResponseHeaders] = field(default_factory=list[ResponseHeaders])
    requests: list[httpx2.Request] = field(default_factory=list[httpx2.Request])

    def handler(self, request: httpx2.Request) -> httpx2.Response:
        self.requests.append(request)
        index = len(self.requests)
        headers = self.response_headers.pop(0) if len(self.response_headers) > 0 else []
        query_id = f"20261027_000000_{index:05}_abcde"
        page = {
            "id": query_id,
            "infoUri": f"{BASE_URL}/ui/query.html?{query_id}",
            "stats": {"state": "FINISHED"},
        }
        return httpx2.Response(200, json=page, headers=headers)

    def client(self) -> httpx2.AsyncClient:
        return httpx2.AsyncClient(transport=httpx2.MockTransport(self.handler))


async def statement_headers(conn: Connection, coordinator: SessionCoordinator) -> httpx2.Headers:
    await conn.cursor().execute("SELECT 1")
    return coordinator.requests[-1].headers


async def test_session_properties_are_sent_url_quoted() -> None:
    coordinator = SessionCoordinator()
    async with coordinator.client() as client:
        conn = connect(
            host="coordinator",
            session_properties={"query_max_run_time": "10m", "comment": "a b,c=d"},
            http_client=client,
        )

        headers = await statement_headers(conn, coordinator)

    assert headers["X-Trino-Session"] == "query_max_run_time=10m,comment=a%20b%2Cc%3Dd"


async def test_session_header_is_empty_without_properties() -> None:
    coordinator = SessionCoordinator()
    async with coordinator.client() as client:
        headers = await statement_headers(
            connect(host="coordinator", http_client=client), coordinator
        )

    assert headers["X-Trino-Session"] == ""


async def test_client_tags_are_sent_comma_separated() -> None:
    coordinator = SessionCoordinator()
    async with coordinator.client() as client:
        conn = connect(host="coordinator", client_tags=["foo", "bar"], http_client=client)

        headers = await statement_headers(conn, coordinator)

    assert headers["X-Trino-Client-Tags"] == "foo,bar"


async def test_no_client_tags_send_no_header() -> None:
    coordinator = SessionCoordinator()
    async with coordinator.client() as client:
        conn = connect(host="coordinator", client_tags=[], http_client=client)

        headers = await statement_headers(conn, coordinator)

    assert "X-Trino-Client-Tags" not in headers


@pytest.mark.parametrize(
    ("roles", "expected"),
    [
        ("analyst", "system=ROLE%7Banalyst%7D"),
        ("ALL", "system=ALL"),
        (
            {"hive": "ALL", "system": "analyst", "tpch": "NONE"},
            "hive=ALL,system=ROLE%7Banalyst%7D,tpch=NONE",
        ),
    ],
)
async def test_roles_are_sent_per_catalog(roles: str | dict[str, str], expected: str) -> None:
    coordinator = SessionCoordinator()
    async with coordinator.client() as client:
        conn = connect(host="coordinator", roles=roles, http_client=client)

        headers = await statement_headers(conn, coordinator)

    assert headers["X-Trino-Role"] == expected


async def test_legacy_wrapped_role_is_sent_verbatim_with_a_deprecation_warning() -> None:
    coordinator = SessionCoordinator()
    async with coordinator.client() as client:
        with pytest.warns(DeprecationWarning, match="legacy ROLE{} format"):
            conn = connect(host="coordinator", roles={"hive": "ROLE{analyst}"}, http_client=client)

        headers = await statement_headers(conn, coordinator)

    assert headers["X-Trino-Role"] == "hive=ROLE%7Banalyst%7D"


async def test_no_roles_send_no_header() -> None:
    coordinator = SessionCoordinator()
    async with coordinator.client() as client:
        headers = await statement_headers(
            connect(host="coordinator", http_client=client), coordinator
        )

    assert "X-Trino-Role" not in headers


async def test_extra_credentials_are_sent_form_encoded() -> None:
    coordinator = SessionCoordinator()
    async with coordinator.client() as client:
        conn = connect(
            host="coordinator",
            extra_credential=[("a.username", "foo"), ("b.password", "bar baz=+")],
            http_client=client,
        )

        headers = await statement_headers(conn, coordinator)

    assert headers["X-Trino-Extra-Credential"] == "a.username=foo, b.password=bar+baz%3D%2B"


@pytest.mark.parametrize("key", ["", "with space", "a=b", "\tkey", "naïve"])
def test_extra_credential_keys_must_be_ascii_without_whitespace_or_equals(key: str) -> None:
    with pytest.raises(ValueError, match="extra credential"):
        _ = connect(host="coordinator", extra_credential=[(key, "value")])


async def test_timezone_is_sent() -> None:
    coordinator = SessionCoordinator()
    async with coordinator.client() as client:
        conn = connect(host="coordinator", timezone="Europe/Brussels", http_client=client)

        headers = await statement_headers(conn, coordinator)

    assert headers["X-Trino-Time-Zone"] == "Europe/Brussels"


def test_an_unknown_timezone_is_rejected() -> None:
    with pytest.raises(ZoneInfoNotFoundError):
        _ = connect(host="coordinator", timezone="Not/AZone")


async def test_custom_http_headers_go_on_every_coordinator_request() -> None:
    coordinator = SessionCoordinator()
    async with coordinator.client() as client:
        conn = connect(
            host="coordinator",
            http_headers={"X-Trino-Client-Info": "nightly-batch", "X-Request-Id": "42"},
            http_client=client,
        )

        headers = await statement_headers(conn, coordinator)

    assert headers["X-Trino-Client-Info"] == "nightly-batch"
    assert headers["X-Request-Id"] == "42"


@pytest.mark.parametrize(
    "header",
    [
        "X-Trino-User",
        "x-trino-catalog",
        "X-Trino-Schema",
        "X-Trino-Source",
        "X-Trino-Session",
        "X-Trino-Role",
        "X-Trino-Path",
        "X-Trino-Client-Tags",
        "X-Trino-Time-Zone",
        "X-Trino-Extra-Credential",
        "X-Trino-Original-User",
        "X-Trino-Transaction-Id",
        "X-Trino-Client-Capabilities",
        "X-Trino-Query-Data-Encoding",
        "User-Agent",
    ],
)
def test_custom_http_headers_cannot_override_protocol_headers(header: str) -> None:
    with pytest.raises(ValueError, match="cannot override reserved HTTP header"):
        _ = connect(host="coordinator", http_headers={header: "spoofed"})


async def test_use_changes_the_catalog_and_schema_of_later_statements() -> None:
    coordinator = SessionCoordinator(
        [[("X-Trino-Set-Catalog", "tpch"), ("X-Trino-Set-Schema", "tiny")]]
    )
    async with coordinator.client() as client:
        conn = connect(host="coordinator", catalog="memory", schema="default", http_client=client)
        await conn.cursor().execute("USE tpch.tiny")

        headers = await statement_headers(conn, coordinator)

    assert headers["X-Trino-Catalog"] == "tpch"
    assert headers["X-Trino-Schema"] == "tiny"


async def test_set_session_adds_properties_to_later_statements() -> None:
    coordinator = SessionCoordinator(
        [
            [("X-Trino-Set-Session", "query_priority=2")],
            [("X-Trino-Set-Session", "comment=a+b%2Cc"), ("X-Trino-Set-Session", "x=1, y=2")],
        ]
    )
    async with coordinator.client() as client:
        conn = connect(
            host="coordinator", session_properties={"query_priority": "1"}, http_client=client
        )
        await conn.cursor().execute("SET SESSION query_priority = 2")
        await conn.cursor().execute("SET SESSION comment = 'a b,c'")

        headers = await statement_headers(conn, coordinator)

    assert headers["X-Trino-Session"] == "query_priority=2,comment=a%20b%2Cc,x=1,y=2"


async def test_reset_session_clears_properties_from_later_statements() -> None:
    coordinator = SessionCoordinator(
        [[("X-Trino-Clear-Session", "query_priority"), ("X-Trino-Clear-Session", "unknown, x")]]
    )
    async with coordinator.client() as client:
        conn = connect(
            host="coordinator",
            session_properties={"query_priority": "1", "x": "1", "y": "2"},
            http_client=client,
        )
        await conn.cursor().execute("RESET SESSION query_priority")

        headers = await statement_headers(conn, coordinator)

    assert headers["X-Trino-Session"] == "y=2"


async def test_set_role_changes_the_roles_of_later_statements() -> None:
    coordinator = SessionCoordinator([[("X-Trino-Set-Role", "system=ROLE%7Banalyst%7D")]])
    async with coordinator.client() as client:
        conn = connect(host="coordinator", roles={"hive": "ALL"}, http_client=client)
        await conn.cursor().execute("SET ROLE analyst")

        headers = await statement_headers(conn, coordinator)

    assert headers["X-Trino-Role"] == "hive=ALL,system=ROLE%7Banalyst%7D"


async def test_set_path_changes_the_path_of_later_statements() -> None:
    coordinator = SessionCoordinator([[("X-Trino-Set-Path", "memory.default, tpch.tiny")]])
    async with coordinator.client() as client:
        conn = connect(host="coordinator", http_client=client)
        first = await statement_headers(conn, coordinator)

        headers = await statement_headers(conn, coordinator)

    assert "X-Trino-Path" not in first
    assert headers["X-Trino-Path"] == "memory.default, tpch.tiny"


async def test_session_authorization_impersonates_until_reset() -> None:
    coordinator = SessionCoordinator(
        [
            [("X-Trino-Set-Authorization-User", "bob")],
            [],
            [("X-Trino-Reset-Authorization-User", "true")],
        ]
    )
    async with coordinator.client() as client:
        conn = connect(host="coordinator", user="alice", http_client=client)
        await conn.cursor().execute("SET SESSION AUTHORIZATION bob")

        impersonating = await statement_headers(conn, coordinator)
        await conn.cursor().execute("RESET SESSION AUTHORIZATION")
        reset = await statement_headers(conn, coordinator)

    assert impersonating["X-Trino-User"] == "bob"
    assert impersonating["X-Trino-Original-User"] == "alice"
    assert reset["X-Trino-User"] == "alice"
    assert "X-Trino-Original-User" not in reset


async def test_concurrent_statements_each_apply_their_own_session_update() -> None:
    statements = ["USE tpch.tiny", "SET SESSION query_priority = 2"]
    updates = [
        [("X-Trino-Set-Catalog", "tpch"), ("X-Trino-Set-Schema", "tiny")],
        [("X-Trino-Set-Session", "query_priority=2")],
    ]
    both_submitted = asyncio.Barrier(len(statements))
    requests: list[httpx2.Request] = []

    async def handler(request: httpx2.Request) -> httpx2.Response:
        requests.append(request)
        page: dict[str, object] = {"id": "q", "infoUri": f"{BASE_URL}/ui", "stats": {}}
        if request.method == "GET":
            index = int(request.url.path.rsplit("/", 1)[1])
            return httpx2.Response(200, json=page, headers=updates[index])
        sql = request.content.decode()
        if sql in statements:
            _ = await both_submitted.wait()
            page["nextUri"] = f"{BASE_URL}/v1/statement/executing/{statements.index(sql)}"
        return httpx2.Response(200, json=page)

    async with httpx2.AsyncClient(transport=httpx2.MockTransport(handler)) as client:
        conn = connect(host="coordinator", http_client=client)
        async with asyncio.TaskGroup() as tasks:
            for statement in statements:
                _ = tasks.create_task(conn.cursor().execute(statement))
        await conn.cursor().execute("SELECT 1")

    headers = requests[-1].headers
    assert headers["X-Trino-Catalog"] == "tpch"
    assert headers["X-Trino-Session"] == "query_priority=2"


async def test_prepared_statements_the_coordinator_adds_are_sent_until_deallocated() -> None:
    coordinator = SessionCoordinator(
        [
            [("X-Trino-Added-Prepare", "pick=SELECT+%3F%2C+%27a%3Db%27")],
            [("X-Trino-Added-Prepare", "two=SELECT+2")],
            [],
            [("X-Trino-Deallocated-Prepare", "pick")],
            [],
            [("X-Trino-Deallocated-Prepare", "two")],
        ]
    )
    async with coordinator.client() as client:
        conn = connect(host="coordinator", http_client=client)
        await conn.cursor().execute("PREPARE pick FROM SELECT ?, 'a=b'")
        await conn.cursor().execute("PREPARE two FROM SELECT 2")

        both = await statement_headers(conn, coordinator)
        await conn.cursor().execute("DEALLOCATE PREPARE pick")
        one = await statement_headers(conn, coordinator)
        await conn.cursor().execute("DEALLOCATE PREPARE two")
        none = await statement_headers(conn, coordinator)

    assert "X-Trino-Prepared-Statement" not in coordinator.requests[0].headers
    assert both["X-Trino-Prepared-Statement"] == "pick=SELECT+%3F%2C+%27a%3Db%27,two=SELECT+2"
    assert one["X-Trino-Prepared-Statement"] == "two=SELECT+2"
    assert "X-Trino-Prepared-Statement" not in none


def test_the_prepared_statement_header_cannot_be_overridden() -> None:
    with pytest.raises(ValueError, match="X-Trino-Prepared-Statement"):
        _ = connect(host="coordinator", http_headers={"X-Trino-Prepared-Statement": "x=y"})
