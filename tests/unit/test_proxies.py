import httpx2
import pytest

from asynctrino.dbapi import connect
from tests.unit.server import OneRowServer, unused_port

pytestmark = pytest.mark.usefixtures("without_env_proxies")


async def test_the_created_client_sends_through_the_environment_proxy(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async with OneRowServer() as proxy:
        monkeypatch.setenv("HTTP_PROXY", proxy.url)

        async with connect(host="coordinator.invalid", user="alice") as conn:
            cur = await conn.cursor().execute("SELECT 1")
            assert await cur.fetchall() == [[1]]

    [request] = proxy.requests
    assert request.method == "POST"
    assert request.target == "http://coordinator.invalid:8080/v1/statement"
    assert request.headers["x-trino-user"] == "alice"


async def test_no_proxy_bypasses_the_environment_proxy(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("HTTP_PROXY", f"http://127.0.0.1:{unused_port()}")
    monkeypatch.setenv("NO_PROXY", "127.0.0.1")

    async with OneRowServer() as server, connect(host=server.url) as conn:
        cur = await conn.cursor().execute("SELECT 1")
        assert await cur.fetchall() == [[1]]

    assert server.requests[0].target == "/v1/statement"


async def test_an_injected_client_keeps_its_own_proxy(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("HTTP_PROXY", f"http://127.0.0.1:{unused_port()}")

    async with (
        OneRowServer() as proxy,
        httpx2.AsyncClient(proxy=proxy.url) as client,
        connect(host="coordinator.invalid", http_client=client) as conn,
    ):
        cur = await conn.cursor().execute("SELECT 1")
        assert await cur.fetchall() == [[1]]

    assert proxy.requests[0].target == "http://coordinator.invalid:8080/v1/statement"
