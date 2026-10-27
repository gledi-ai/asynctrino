import math
import uuid
from collections.abc import AsyncIterator, Sequence
from datetime import UTC, date, datetime, time, timedelta, timezone
from decimal import Decimal
from zoneinfo import ZoneInfo

import httpx2
import pytest

from asynctrino.dbapi import ColumnDescription, Connection, connect
from asynctrino.exceptions import NotSupportedError, ProgrammingError, TrinoUserError

MAX_RESPONSE_HEADER_BYTES = 100 * 1024


async def select(conn: Connection, value: object, sql: str = "SELECT ?") -> object:
    cur = conn.cursor()
    await cur.execute(sql, [value])
    [[selected]] = await cur.fetchall()
    return selected


async def describe(conn: Connection, value: object) -> tuple[object, ColumnDescription]:
    cur = conn.cursor()
    await cur.execute("SELECT ?", [value])
    [[selected]] = await cur.fetchall()
    assert cur.description is not None
    return selected, cur.description[0]


async def test_select_with_a_parameter_in_the_where_clause(conn: Connection) -> None:
    cur = conn.cursor()
    await cur.execute(
        """
        SELECT * FROM (
            VALUES (1, 'one', 'a'), (2, 'two', 'b'), (3, 'three', 'c'), (4, 'four', 'd')
        ) x (id, name, letter)
        WHERE id >= ?
        ORDER BY id
        """,
        (3,),
    )

    assert await cur.fetchall() == [[3, "three", "c"], [4, "four", "d"]]


async def test_none(conn: Connection) -> None:
    selected, column = await describe(conn, None)

    assert selected is None
    assert column.type_code == "unknown"


@pytest.mark.parametrize("value", ["six'", "", "''", "ünïcödé ✓", "a\nb"])
async def test_string(conn: Connection, value: str) -> None:
    assert await select(conn, value) == value


async def test_string_column_type(conn: Connection) -> None:
    _, column = await describe(conn, "six'")

    assert column.type_code == "varchar(4)"
    assert column.internal_size == 4


@pytest.mark.parametrize("value", [True, False])
async def test_boolean(conn: Connection, value: bool) -> None:  # noqa: FBT001
    assert await select(conn, value) is value


@pytest.mark.parametrize(
    ("value", "trino_type"),
    [(3, "integer"), (-2147483648, "integer"), (9223372036854775807, "bigint")],
)
async def test_int(conn: Connection, value: int, trino_type: str) -> None:
    selected, column = await describe(conn, value)

    assert selected == value
    assert column.type_code == trino_type


@pytest.mark.parametrize("value", [1.1, -0.0, 1e-300, 1.7976931348623157e308, math.inf, -math.inf])
async def test_float(conn: Connection, value: float) -> None:
    selected, column = await describe(conn, value)

    assert selected == value
    assert column.type_code == "double"


async def test_float_nan(conn: Connection) -> None:
    selected, column = await describe(conn, math.nan)

    assert isinstance(selected, float)
    assert math.isnan(selected)
    assert column.type_code == "double"


@pytest.mark.parametrize(
    ("value", "precision", "scale"),
    [
        (Decimal("1112.142857"), 10, 6),
        (Decimal("0E-10"), 10, 10),
        (Decimal("1E-1"), 1, 1),
        (Decimal(99999999999999999999999999999999999999), 38, 0),
        (Decimal(-99999999999999999999999999999999999999), 38, 0),
        (Decimal("0.99999999999999999999999999999999999999"), 38, 38),
    ],
)
async def test_decimal(conn: Connection, value: Decimal, precision: int, scale: int) -> None:
    selected, column = await describe(conn, value)

    assert selected == value
    assert isinstance(selected, Decimal)
    assert column.type_code == f"decimal({precision}, {scale})"
    assert (column.precision, column.scale) == (precision, scale)


@pytest.mark.parametrize(
    "value",
    [b"", b"a", b"\x00\x00\x00\x00", bytes(range(256)), bytearray([1, 2, 3]), memoryview(b"\xff")],
)
async def test_binary(conn: Connection, value: bytes | bytearray | memoryview) -> None:
    assert await select(conn, value) == bytes(value)


async def test_uuid(conn: Connection) -> None:
    value = uuid.UUID("12151fd2-7586-11e9-8f9e-2a86e4085a59")

    selected, column = await describe(conn, value)

    assert selected == value
    assert column.type_code == "uuid"


@pytest.mark.parametrize(
    "value",
    [
        date(1, 1, 1),
        date(1500, 1, 1),
        date(1752, 9, 4),
        date(1952, 4, 3),
        date(1970, 1, 1),
        date(2017, 7, 1),
        date(2017, 12, 31),
        date(9999, 12, 31),
    ],
)
async def test_date(conn: Connection, value: date) -> None:
    selected, column = await describe(conn, value)

    assert selected == value
    assert column.type_code == "date"


async def test_naive_datetime(conn: Connection) -> None:
    value = datetime(2020, 1, 1, 16, 43, 22, 320000)

    selected, column = await describe(conn, value)

    assert selected == value
    assert column.type_code == "timestamp(6)"


@pytest.mark.parametrize(
    "zone",
    [
        ZoneInfo("UTC"),
        ZoneInfo("America/Los_Angeles"),
        UTC,
        timezone(-timedelta(hours=5, minutes=30)),
    ],
    ids=str,
)
async def test_aware_datetime(conn: Connection, zone: timezone | ZoneInfo) -> None:
    value = datetime(2020, 1, 1, 16, 43, 22, 320000, tzinfo=zone)

    selected, column = await describe(conn, value)

    assert selected == value
    assert column.type_code == "timestamp(6) with time zone"


async def test_datetime_in_a_dst_gap_is_rejected_by_trino(conn: Connection) -> None:
    value = datetime(2021, 3, 28, 2, 30, 0, tzinfo=ZoneInfo("Europe/Brussels"))

    with pytest.raises(TrinoUserError):
        _ = await select(conn, value)


@pytest.mark.parametrize("fold", [0, 1])
async def test_doubled_datetime_in_a_dst_overlap(conn: Connection, fold: int) -> None:
    eastern = ZoneInfo("US/Eastern")
    value = datetime(2002, 10, 27, 1, 30, 0, tzinfo=eastern, fold=fold)

    assert await select(conn, value) == datetime(2002, 10, 27, 1, 30, 0, tzinfo=eastern)


async def test_naive_time(conn: Connection) -> None:
    value = time(12, 3, 44, 333000)

    selected, column = await describe(conn, value)

    assert selected == value
    assert column.type_code == "time(6)"


@pytest.mark.parametrize(
    "zone", [timezone(-timedelta(hours=8)), timezone(timedelta(hours=5, minutes=30)), UTC]
)
async def test_time_with_an_offset(conn: Connection, zone: timezone) -> None:
    value = time(16, 43, 22, 320000, tzinfo=zone)

    selected, column = await describe(conn, value)

    assert selected == value
    assert column.type_code == "time(6) with time zone"


async def test_time_with_a_named_zone_takes_its_current_offset(conn: Connection) -> None:
    selected = await select(conn, time(16, 43, 22, 320000, tzinfo=ZoneInfo("Asia/Shanghai")))

    assert selected == time(16, 43, 22, 320000, tzinfo=timezone(timedelta(hours=8)))


@pytest.mark.parametrize(
    ("value", "trino_type"),
    [
        ([1, 2, 3], "array(integer)"),
        ([[1, 2, 3], [4, 5, 6]], "array(array(integer))"),
        ([None, None], "array(unknown)"),
        ([None, 1], "array(integer)"),
        ([datetime(2020, 1, 1), datetime(2020, 1, 2)], "array(timestamp(6))"),
        (
            [
                datetime(2020, 1, 1, tzinfo=ZoneInfo("UTC")),
                datetime(2020, 1, 2, tzinfo=ZoneInfo("UTC")),
            ],
            "array(timestamp(6) with time zone)",
        ),
        ([], "array(unknown)"),
    ],
)
async def test_array(conn: Connection, value: list[object], trino_type: str) -> None:
    assert await select(conn, value) == value
    assert await select(conn, value, "SELECT typeof(?)") == trino_type


@pytest.mark.parametrize(
    ("value", "trino_type"),
    [
        ({"foo": "bar"}, "map(varchar(3), varchar(3))"),
        ({"foo": datetime(2020, 1, 1, 16, 43, 22, 320000)}, "map(varchar(3), timestamp(6))"),
        ({1: [Decimal("1.5")], 2: []}, "map(integer, array(decimal(2,1)))"),
    ],
)
async def test_map(conn: Connection, value: dict[object, object], trino_type: str) -> None:
    assert await select(conn, value) == value
    assert await select(conn, value, "SELECT typeof(?)") == trino_type


@pytest.mark.parametrize(
    "value",
    [
        (1, Decimal("2.0"), datetime(2020, 1, 1)),
        ((1, "test", Decimal("3.1")), Decimal("2.0"), datetime(2020, 1, 1)),
        (b"\x01", uuid.UUID(int=1), [True, None], {"k": (1,)}),
    ],
)
async def test_row(conn: Connection, value: tuple[object, ...]) -> None:
    assert await select(conn, value) == value


async def test_several_parameters_of_different_types(conn: Connection) -> None:
    cur = conn.cursor()
    await cur.execute("SELECT ?, ?, ?", [1, "two", date(2003, 3, 3)])

    assert await cur.fetchall() == [[1, "two", date(2003, 3, 3)]]


@pytest.mark.parametrize("params", ["NOT A LIST OR TUPLE", {"invalid", "params"}, object])
async def test_params_must_be_a_list_or_tuple(conn: Connection, params: object) -> None:
    with pytest.raises(ProgrammingError):
        _ = await conn.cursor().execute("SELECT ?", params)  # pyrefly: ignore[bad-argument-type]


async def test_unsupported_parameter_type(conn: Connection) -> None:
    with pytest.raises(NotSupportedError):
        _ = await select(conn, timedelta(days=1))


@pytest.fixture
async def table(conn: Connection) -> AsyncIterator[str]:
    name = f"memory.default.test_parameters_{uuid.uuid4().hex}"
    cur = conn.cursor()
    await cur.execute(f"CREATE TABLE {name} (key integer, value varchar)")
    yield name
    await cur.execute(f"DROP TABLE IF EXISTS {name}")


async def rows_of(conn: Connection, table: str) -> Sequence[object]:
    cur = conn.cursor()
    await cur.execute(f"SELECT * FROM {table} ORDER BY key")  # noqa: S608
    return await cur.fetchall()


async def test_executemany_inserts_every_parameter_set(conn: Connection, table: str) -> None:
    cur = conn.cursor()
    operation = f"INSERT INTO {table} (key, value) VALUES (?, ?)"  # noqa: S608

    await cur.executemany(operation, [(1, "value1")])
    assert cur.rowcount == 1
    assert await rows_of(conn, table) == [[1, "value1"]]

    await cur.executemany(operation, [(2, "value2"), (3, "value3"), (4, None)])
    assert cur.rowcount == 3
    assert await rows_of(conn, table) == [[1, "value1"], [2, "value2"], [3, "value3"], [4, None]]


async def test_executemany_without_parameter_sets_runs_without_parameters(
    conn: Connection, table: str
) -> None:
    with pytest.raises(
        TrinoUserError, match="Incorrect number of parameters: expected 1 but found 0"
    ):
        _ = await conn.cursor().executemany(f"INSERT INTO {table} (value) VALUES (?)", [])  # noqa: S608


async def test_executemany_of_a_select_is_not_supported(conn: Connection) -> None:
    with pytest.raises(NotSupportedError, match="Query must return update type"):
        _ = await conn.cursor().executemany("SELECT ?, ?", [(1, "value1"), (2, "value2")])


async def test_responses_carry_no_prepared_statements_and_fit_the_header_limit(
    trino_address: tuple[str, int],
) -> None:
    host, port = trino_address
    responses: list[httpx2.Response] = []
    requests: list[httpx2.Request] = []

    async def record_request(request: httpx2.Request) -> None:
        requests.append(request)

    async def record_response(response: httpx2.Response) -> None:
        responses.append(response)

    big = "x" * 50_000
    async with (
        httpx2.AsyncClient(
            event_hooks={"request": [record_request], "response": [record_response]}
        ) as client,
        connect(host=host, port=port, user="test", http_client=client) as conn,
    ):
        for _ in range(3):
            assert await select(conn, big, "SELECT length(?)") == len(big)

    assert len(responses) > 0
    for response in responses:
        assert "X-Trino-Added-Prepare" not in response.headers
        size = sum(len(name) + len(value) + 4 for name, value in response.headers.raw)
        assert size < MAX_RESPONSE_HEADER_BYTES
    for request in requests:
        assert "X-Trino-Prepared-Statement" not in request.headers
