import ssl
from collections.abc import AsyncIterator

import pytest
import trustme

from asynctrino.auth import BasicAuthentication
from asynctrino.dbapi import connect
from asynctrino.exceptions import TrinoConnectionError
from tests.unit.server import OneRowServer

pytestmark = pytest.mark.usefixtures("without_env_proxies")


@pytest.fixture(scope="module")
def ca() -> trustme.CA:
    return trustme.CA()


def server_context(ca: trustme.CA, *, require_client_cert: bool = False) -> ssl.SSLContext:
    context = ssl.create_default_context(ssl.Purpose.CLIENT_AUTH)
    ca.issue_cert("127.0.0.1").configure_cert(context)
    if require_client_cert:
        ca.configure_trust(context)
        context.verify_mode = ssl.CERT_REQUIRED
    return context


def trusting(ca: trustme.CA) -> ssl.SSLContext:
    context = ssl.create_default_context()
    ca.configure_trust(context)
    return context


@pytest.fixture
async def tls_server(ca: trustme.CA) -> AsyncIterator[OneRowServer]:
    async with OneRowServer(server_context(ca)) as server:
        yield server


async def test_default_verification_rejects_a_certificate_the_os_does_not_trust(
    tls_server: OneRowServer,
) -> None:
    async with connect(host=tls_server.url) as conn:
        with pytest.raises(TrinoConnectionError, match="CERTIFICATE_VERIFY_FAILED"):
            await conn.cursor().execute("SELECT 1")

    assert tls_server.requests == []


async def test_verify_false_skips_certificate_verification(tls_server: OneRowServer) -> None:
    async with connect(host=tls_server.url, verify=False) as conn:
        cur = await conn.cursor().execute("SELECT 1")

        assert await cur.fetchall() == [[1]]


async def test_an_ssl_context_can_trust_a_private_ca(
    ca: trustme.CA, tls_server: OneRowServer
) -> None:
    auth = BasicAuthentication("alice", "s3cret")
    async with connect(host=tls_server.url, auth=auth, verify=trusting(ca)) as conn:
        cur = await conn.cursor().execute("SELECT 1")

        assert await cur.fetchall() == [[1]]
    assert tls_server.requests[0].headers["authorization"] == "Basic YWxpY2U6czNjcmV0"


async def test_an_ssl_context_can_present_a_client_certificate(ca: trustme.CA) -> None:
    with_certificate = trusting(ca)
    ca.issue_cert("alice@example.com").configure_cert(with_certificate)

    async with OneRowServer(server_context(ca, require_client_cert=True)) as server:
        async with connect(host=server.url, verify=with_certificate) as conn:
            cur = await conn.cursor().execute("SELECT 1")
            assert await cur.fetchall() == [[1]]

        async with connect(host=server.url, verify=trusting(ca)) as conn:
            with pytest.raises(TrinoConnectionError):
                await conn.cursor().execute("SELECT 1")
