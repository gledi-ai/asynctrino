from collections.abc import AsyncIterator, Generator

import pytest

from asynctrino.dbapi import Connection, connect
from tests.integration.trino import trino


@pytest.fixture(scope="session")
def trino_address() -> Generator[tuple[str, int]]:
    with trino() as address:
        yield address


@pytest.fixture
async def conn(trino_address: tuple[str, int]) -> AsyncIterator[Connection]:
    host, port = trino_address
    async with connect(host=host, port=port, user="test", catalog="tpch", schema="tiny") as conn:
        yield conn
