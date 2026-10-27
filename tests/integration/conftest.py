from collections.abc import AsyncIterator, Generator

import httpx2
import pytest

from asynctrino.dbapi import Connection, connect
from tests.integration.trino import segment_storage_route, trino

ENCODINGS = [None, "json+zstd", "json+lz4", "json"]


@pytest.fixture(scope="session")
def trino_address() -> Generator[tuple[str, int]]:
    with trino() as address:
        yield address


@pytest.fixture
async def conn(trino_address: tuple[str, int]) -> AsyncIterator[Connection]:
    host, port = trino_address
    async with connect(host=host, port=port, user="test", catalog="tpch", schema="tiny") as conn:
        yield conn


@pytest.fixture(params=ENCODINGS, ids=lambda encoding: encoding or "direct")
async def encoded_conn(
    trino_address: tuple[str, int], request: pytest.FixtureRequest
) -> AsyncIterator[Connection]:
    """A connection using the Direct protocol, then the Spooling protocol with each encoding."""
    host, port = trino_address
    encoding: object = request.param
    assert encoding is None or isinstance(encoding, str)
    async with (
        httpx2.AsyncClient(transport=segment_storage_route(), timeout=60) as client,
        connect(
            host=host,
            port=port,
            user="test",
            catalog="tpch",
            schema="tiny",
            encoding=encoding,
            http_client=client,
        ) as conn,
    ):
        yield conn
