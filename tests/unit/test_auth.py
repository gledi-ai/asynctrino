import asyncio
from collections.abc import AsyncGenerator, Callable
from typing import Literal, override

import httpx2
import pytest

from asynctrino.auth import BasicAuthentication, JWTAuthentication
from asynctrino.dbapi import Row, connect
from asynctrino.exceptions import TrinoAuthError
from tests.unit.coordinator import FakeCoordinator, column, statement_pages

type HttpScheme = Literal["http", "https"]

TLS_BASE_URL = "https://coordinator:8443"


def secure_coordinator() -> FakeCoordinator:
    return FakeCoordinator(
        statement_pages(
            columns=[column("_col0", "integer")],
            batches=[[[1]], [[2]]],
            base_url=TLS_BASE_URL,
        )
    )


async def run_query(
    handler: Callable[[httpx2.Request], httpx2.Response], auth: httpx2.Auth
) -> list[Row]:
    async with httpx2.AsyncClient(transport=httpx2.MockTransport(handler)) as client:
        cur = connect(host=TLS_BASE_URL, auth=auth, http_client=client).cursor()
        await cur.execute("SELECT 1")
        return await cur.fetchall()


async def test_basic_authentication_is_sent_with_every_coordinator_request() -> None:
    coordinator = secure_coordinator()

    await run_query(coordinator.handler, BasicAuthentication("alice", "s3cret"))

    assert len(coordinator.requests) > 2
    for request in coordinator.requests:
        assert request.headers["Authorization"] == "Basic YWxpY2U6czNjcmV0"


async def test_jwt_authentication_sends_the_token_as_a_bearer() -> None:
    coordinator = secure_coordinator()

    await run_query(coordinator.handler, JWTAuthentication("eyJhbGciOi.payload.sig"))

    for request in coordinator.requests:
        assert request.headers["Authorization"] == "Bearer eyJhbGciOi.payload.sig"


@pytest.mark.parametrize(
    ("host", "port", "http_scheme"),
    [
        ("coordinator", None, None),
        ("coordinator", None, "http"),
        ("http://coordinator", None, "https"),
        ("coordinator", 8080, None),
    ],
)
def test_credentials_over_plain_http_are_refused(
    host: str, port: int | None, http_scheme: HttpScheme | None
) -> None:
    with pytest.raises(TrinoAuthError, match="TLS/SSL is required") as excinfo:
        _ = connect(
            host=host, port=port, http_scheme=http_scheme, auth=BasicAuthentication("a", "b")
        )

    assert "allow_insecure_auth=True" in str(excinfo.value)
    assert "http-server.authentication.allow-insecure-over-http=true" in str(excinfo.value)


@pytest.mark.parametrize(
    ("host", "port", "http_scheme"),
    [
        ("coordinator", None, "https"),
        ("https://coordinator", None, None),
        ("coordinator", 443, None),
    ],
)
def test_credentials_over_https_are_accepted(
    host: str, port: int | None, http_scheme: HttpScheme | None
) -> None:
    _ = connect(host=host, port=port, http_scheme=http_scheme, auth=BasicAuthentication("a", "b"))


async def test_allow_insecure_auth_sends_credentials_over_plain_http() -> None:
    coordinator = FakeCoordinator(statement_pages(columns=None, batches=[]))
    async with coordinator.client() as client:
        conn = connect(
            host="coordinator",
            auth=JWTAuthentication("t"),
            allow_insecure_auth=True,
            http_client=client,
        )
        await conn.cursor().execute("SELECT 1")

    assert str(coordinator.requests[0].url).startswith("http://")
    assert coordinator.requests[0].headers["Authorization"] == "Bearer t"


@pytest.mark.parametrize("rejected_request", [0, 2])
async def test_rejected_credentials_raise_trino_auth_error(rejected_request: int) -> None:
    coordinator = secure_coordinator()

    def rejecting(request: httpx2.Request) -> httpx2.Response:
        if len(coordinator.requests) == rejected_request:
            coordinator.requests.append(request)
            return httpx2.Response(401, text="Invalid credentials")
        return coordinator.handler(request)

    with pytest.raises(TrinoAuthError, match="error 401: Invalid credentials"):
        await run_query(rejecting, BasicAuthentication("a", "b"))


class NegotiateAuth(httpx2.Auth):
    def __init__(self) -> None:
        self.challenges = 0

    @override
    async def async_auth_flow(
        self, request: httpx2.Request
    ) -> AsyncGenerator[httpx2.Request, httpx2.Response]:
        response = yield request
        if response.status_code == 401 and response.headers["WWW-Authenticate"] == "Negotiate":
            self.challenges += 1
            await asyncio.sleep(0)
            request.headers["Authorization"] = "Negotiate dG9rZW4="
            yield request


async def test_any_httpx2_auth_with_an_async_challenge_flow_is_honoured() -> None:
    coordinator = secure_coordinator()

    def challenging(request: httpx2.Request) -> httpx2.Response:
        if request.headers.get("Authorization") != "Negotiate dG9rZW4=":
            return httpx2.Response(401, headers={"WWW-Authenticate": "Negotiate"})
        return coordinator.handler(request)

    auth = NegotiateAuth()

    assert await run_query(challenging, auth) == [[1], [2]]
    assert auth.challenges == len(coordinator.requests)


async def test_auth_never_becomes_a_default_of_an_injected_client() -> None:
    coordinator = secure_coordinator()
    async with coordinator.client() as client:
        conn = connect(host=TLS_BASE_URL, auth=JWTAuthentication("t"), http_client=client)
        await conn.cursor().execute("SELECT 1")

        assert "Authorization" not in client.headers
        await client.get(f"{TLS_BASE_URL}/v1/info")

    assert "Authorization" not in coordinator.requests[-1].headers


def test_built_in_schemes_compare_by_value_and_hide_secrets() -> None:
    assert BasicAuthentication("alice", "s3cret") == BasicAuthentication("alice", "s3cret")
    assert BasicAuthentication("alice", "s3cret") != BasicAuthentication("alice", "other")
    assert JWTAuthentication("token") == JWTAuthentication("token")
    assert "s3cret" not in repr(BasicAuthentication("alice", "s3cret"))
    assert "token" not in repr(JWTAuthentication("token"))


def redirecting_coordinator(next_uri: str) -> FakeCoordinator:
    coordinator = secure_coordinator()
    coordinator.pages[0]["nextUri"] = next_uri
    return coordinator


@pytest.mark.parametrize(
    "next_uri",
    [
        "https://attacker:8443/v1/statement/executing/q/y/1",
        "https://coordinator:9443/v1/statement/executing/q/y/1",
        "http://coordinator:8443/v1/statement/executing/q/y/1",
        "https://coordinator:port/v1/statement/executing/q/y/1",
    ],
    ids=["other-host", "other-port", "downgraded-to-http", "invalid-port"],
)
@pytest.mark.parametrize("auth", [BasicAuthentication("a", "b"), None], ids=["auth", "no-auth"])
async def test_coordinator_requests_are_refused_for_a_next_uri_on_another_origin(
    next_uri: str, auth: httpx2.Auth | None
) -> None:
    coordinator = redirecting_coordinator(next_uri)
    async with coordinator.client() as client:
        conn = connect(host=TLS_BASE_URL, auth=auth, http_client=client)

        with pytest.raises(TrinoAuthError, match="another origin"):
            await conn.cursor().execute("SELECT 1")
        await conn.close()

    assert [str(r.url) for r in coordinator.requests if str(r.url).startswith(next_uri)] == []


async def test_allow_insecure_auth_follows_next_uris_downgraded_to_http() -> None:
    coordinator = FakeCoordinator(
        statement_pages(
            columns=[column("_col0", "integer")],
            batches=[[[1]], [[2]]],
            base_url="http://coordinator:8443",
        )
    )
    async with coordinator.client() as client:
        conn = connect(
            host=TLS_BASE_URL,
            auth=JWTAuthentication("t"),
            allow_insecure_auth=True,
            http_client=client,
        )
        cur = conn.cursor()
        await cur.execute("SELECT 1")

        assert await cur.fetchall() == [[1], [2]]

    assert coordinator.requests[1].url.scheme == "http"
    assert coordinator.requests[1].headers["Authorization"] == "Bearer t"


async def test_a_next_uri_with_the_default_port_left_implicit_is_followed() -> None:
    coordinator = FakeCoordinator(
        statement_pages(
            columns=[column("_col0", "integer")], batches=[[[1]]], base_url="https://coordinator"
        )
    )
    async with coordinator.client() as client:
        cur = connect(
            host="https://coordinator:443", auth=JWTAuthentication("t"), http_client=client
        ).cursor()
        await cur.execute("SELECT 1")

        assert await cur.fetchall() == [[1]]
