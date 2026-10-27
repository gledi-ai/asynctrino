import pytest

from asynctrino.dbapi import Connection, ProgrammingError, connect
from asynctrino.exceptions import TrinoConnectionError, TrinoUserError


async def test_select_failed_query(conn: Connection) -> None:
    async with conn.cursor() as cur:
        with pytest.raises(TrinoUserError) as excinfo:
            await cur.execute("SELECT * FROM catalog.schema.do_not_exist")

    error = excinfo.value
    assert isinstance(error, ProgrammingError)
    assert error.error_name == "CATALOG_NOT_FOUND"
    assert error.error_type == "USER_ERROR"
    assert error.query_id is not None


async def test_invalid_query_throws_correct_error(conn: Connection) -> None:
    async with conn.cursor() as cur:
        with pytest.raises(TrinoUserError) as excinfo:
            await cur.execute("SELECT * FRMO foo WHERE x = 3")

    error = excinfo.value
    assert error.error_name == "SYNTAX_ERROR"
    assert error.error_code == 1
    assert error.error_location == (1, 10)
    assert error.failure_info is not None
    assert error.error_exception is not None
    assert "mismatched input" in error.message


async def test_a_failure_during_execution_carries_its_error(conn: Connection) -> None:
    async with conn.cursor() as cur:

        async def run() -> None:
            await cur.execute("SELECT 1 / 0")
            await cur.fetchall()

        with pytest.raises(TrinoUserError) as excinfo:
            await run()

    assert excinfo.value.error_name == "DIVISION_BY_ZERO"


async def test_a_request_timeout_surfaces_as_trino_connection_error(
    trino_address: tuple[str, int],
) -> None:
    host, port = trino_address
    async with connect(
        host=host, port=port, user="test", max_attempts=1, request_timeout=0.000001
    ) as conn:
        with pytest.raises(TrinoConnectionError, match="Timeout"):
            await conn.cursor().execute("SELECT 1")
