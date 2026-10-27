import base64
from collections.abc import Callable
from typing import cast

import httpx2
import pytest

from asynctrino.auth import BasicAuthentication
from asynctrino.dbapi import Row, connect
from asynctrino.exceptions import HttpError, NotSupportedError, TrinoConnectionError
from tests.unit.coordinator import JSON, FakeCoordinator, column, statement_pages
from tests.unit.segments import SegmentStorage, inline, spooled_pages

ENCODINGS = ["json", "json+lz4", "json+zstd"]
TWO_COLUMNS = [column("n", "integer"), column("s", "varchar")]


def one_row_coordinator() -> FakeCoordinator:
    return FakeCoordinator(statement_pages(columns=[column("_col0", "integer")], batches=[[[1]]]))


async def statement_headers(encoding: str | list[str] | None) -> list[httpx2.Headers]:
    coordinator = one_row_coordinator()
    async with coordinator.client() as client:
        cur = connect(host="coordinator", encoding=encoding, http_client=client).cursor()
        await cur.execute("SELECT 1")
        _ = await cur.fetchall()
    return [request.headers for request in coordinator.requests]


async def test_direct_protocol_is_used_unless_an_encoding_is_set() -> None:
    for headers in await statement_headers(None):
        assert "X-Trino-Query-Data-Encoding" not in headers


@pytest.mark.parametrize(
    ("encoding", "expected"),
    [
        ("json+zstd", "json+zstd"),
        ("json+zstd,json+lz4,json", "json+zstd,json+lz4,json"),
        (["json+lz4", "json"], "json+lz4,json"),
    ],
)
async def test_encoding_is_requested_in_preference_order(
    encoding: str | list[str], expected: str
) -> None:
    for headers in await statement_headers(encoding):
        assert headers["X-Trino-Query-Data-Encoding"] == expected


@pytest.mark.parametrize("encoding", ["json+gzip", ["json", "arrow"], "", []])
def test_an_unknown_encoding_is_rejected(encoding: str | list[str]) -> None:
    with pytest.raises(ValueError, match="encoding"):
        _ = connect(host="coordinator", encoding=encoding)


async def test_inline_segments_are_decoded_into_rows() -> None:
    coordinator = FakeCoordinator(
        spooled_pages(
            columns=[column("n", "integer"), column("s", "varchar")],
            encoding="json",
            pages=[[inline([[1, "one"], [2, "two"]]), inline([[3, "three"]], offset=2)]],
        )
    )
    async with coordinator.client() as client:
        cur = connect(host="coordinator", encoding="json", http_client=client).cursor()
        await cur.execute("SELECT n, s FROM t")

        assert await cur.fetchall() == [[1, "one"], [2, "two"], [3, "three"]]


def spooled_query(
    script: Callable[[SegmentStorage], list[list[JSON]]], *, encoding: str = "json"
) -> SegmentStorage:
    """A query whose data pages hold the Segments ``script`` builds, spooling into storage."""
    coordinator = FakeCoordinator([])
    storage = SegmentStorage(coordinator)
    coordinator.pages = spooled_pages(columns=TWO_COLUMNS, encoding=encoding, pages=script(storage))
    return storage


@pytest.mark.parametrize("encoding", ENCODINGS)
@pytest.mark.parametrize("compress", [True, False])
async def test_every_encoding_decodes_inline_and_spooled_segments(
    encoding: str, *, compress: bool
) -> None:
    rows = [[n, f"value {n}" * 20] for n in range(50)]
    storage = spooled_query(
        lambda s: [
            [
                inline(rows[:10], encoding=encoding, compress=compress),
                s.spooled("a", rows[10:30], encoding=encoding, offset=10, compress=compress),
            ],
            [s.spooled("b", rows[30:], encoding=encoding, offset=30, compress=compress)],
        ],
        encoding=encoding,
    )
    async with storage.client() as client:
        cur = connect(host="coordinator", encoding=encoding, http_client=client).cursor()
        await cur.execute("SELECT n, s FROM t")

        assert await cur.fetchall() == rows


async def test_spooled_segments_are_fetched_lazily_in_order_and_acked_after_decoding() -> None:
    storage = spooled_query(
        lambda s: [
            [s.spooled("a", [[1, "one"], [2, "two"]]), inline([[3, "three"]], offset=2)],
            [s.spooled("b", [[4, "four"]], offset=3)],
        ]
    )
    async with storage.client() as client:
        cur = connect(host="coordinator", encoding="json", http_client=client).cursor()
        await cur.execute("SELECT n, s FROM t")
        assert storage.segment_requests() == []

        assert await cur.fetchone() == [1, "one"]
        assert storage.segment_requests() == ["GET /segments/a", "GET /v1/spooled/ack/a"]

        assert await cur.fetchmany(2) == [[2, "two"], [3, "three"]]
        assert len(storage.segment_requests()) == 2

        assert await cur.fetchone() == [4, "four"]
        assert storage.segment_requests()[2:] == ["GET /segments/b", "GET /v1/spooled/ack/b"]
        assert await cur.fetchone() is None


async def test_segment_requests_carry_only_the_segments_own_headers() -> None:
    storage = spooled_query(
        lambda s: [
            [
                s.spooled(
                    "a",
                    [[1, "one"]],
                    headers={"X-Trino-Spooling-Token": ["t0k3n"], "x-amz-meta": ["m"]},
                )
            ]
        ]
    )
    async with httpx2.AsyncClient(
        transport=httpx2.MockTransport(storage.handler),
        headers={"X-Client-Default": "d"},
        cookies={"session": "c"},
    ) as client:
        conn = connect(
            host="coordinator",
            user="alice",
            auth=BasicAuthentication("alice", "s3cret"),
            allow_insecure_auth=True,
            http_headers={"X-Gateway": "g"},
            encoding="json",
            http_client=client,
        )
        cur = conn.cursor()
        await cur.execute("SELECT n, s FROM t")
        assert await cur.fetchall() == [[1, "one"]]

    statement = storage.requests[0]
    assert statement.headers["Authorization"].startswith("Basic ")
    assert statement.headers["X-Gateway"] == "g"
    segment_requests = [
        r for r in storage.requests if r.url.path.startswith(("/segments/", "/v1/spooled/"))
    ]
    assert [str(r.url) for r in segment_requests] == [
        SegmentStorage.uri("a"),
        SegmentStorage.ack_uri("a"),
    ]
    for request in segment_requests:
        assert dict(request.headers) == {
            "host": request.url.netloc.decode(),
            "x-trino-spooling-token": "t0k3n",
            "x-amz-meta": "m",
        }


async def fetch_all(storage: SegmentStorage) -> list[Row]:
    async with storage.client() as client:
        cur = connect(
            host="coordinator", encoding="json", max_attempts=1, http_client=client
        ).cursor()
        await cur.execute("SELECT n, s FROM t")
        return await cur.fetchall()


async def test_a_failed_download_raises_and_the_segment_is_retried_not_skipped() -> None:
    storage = spooled_query(
        lambda s: [[s.spooled("a", [[1, "one"]]), s.spooled("b", [[2, "two"]], offset=1)]]
    )
    storage.failures[SegmentStorage.uri("a")] = [httpx2.Response(500, text="boom")]
    async with storage.client() as client:
        cur = connect(
            host="coordinator", encoding="json", max_attempts=1, http_client=client
        ).cursor()
        await cur.execute("SELECT n, s FROM t")

        with pytest.raises(HttpError, match="500"):
            _ = await cur.fetchone()
        assert storage.segment_requests() == ["GET /segments/a"]

        assert await cur.fetchall() == [[1, "one"], [2, "two"]]


async def test_a_transient_download_failure_is_retried() -> None:
    storage = spooled_query(lambda s: [[s.spooled("a", [[1, "one"]])]])
    storage.failures[SegmentStorage.uri("a")] = [httpx2.Response(503), httpx2.ConnectError("x")]
    async with storage.client() as client:
        cur = connect(host="coordinator", encoding="json", http_client=client).cursor()
        await cur.execute("SELECT n, s FROM t")

        assert await cur.fetchall() == [[1, "one"]]


async def test_an_unreachable_segment_storage_raises_a_connection_error() -> None:
    storage = spooled_query(lambda s: [[s.spooled("a", [[1, "one"]])]])
    storage.failures[SegmentStorage.uri("a")] = [httpx2.ConnectError("refused")]

    with pytest.raises(TrinoConnectionError, match="Segment storage"):
        _ = await fetch_all(storage)


@pytest.mark.parametrize("encoding", ENCODINGS)
async def test_a_truncated_segment_raises_instead_of_returning_fewer_rows(encoding: str) -> None:
    storage = spooled_query(
        lambda s: [[s.spooled("a", [[n, "x" * 50] for n in range(20)], encoding=encoding)]],
        encoding=encoding,
    )
    uri = SegmentStorage.uri("a")
    storage.segments[uri] = storage.segments[uri][:-3]

    with pytest.raises(TrinoConnectionError, match="expected"):
        _ = await fetch_all(storage)


@pytest.mark.parametrize("encoding", ["json+lz4", "json+zstd"])
async def test_corrupt_compressed_data_raises(encoding: str) -> None:
    storage = spooled_query(
        lambda s: [[s.spooled("a", [[n, "x" * 50] for n in range(20)], encoding=encoding)]],
        encoding=encoding,
    )
    uri = SegmentStorage.uri("a")
    storage.segments[uri] = bytes(len(storage.segments[uri]))

    with pytest.raises(TrinoConnectionError, match="decompress"):
        _ = await fetch_all(storage)


async def test_a_failed_ack_is_logged_and_rows_are_still_returned(
    caplog: pytest.LogCaptureFixture,
) -> None:
    storage = spooled_query(
        lambda s: [[s.spooled("a", [[1, "one"]]), s.spooled("b", [[2, "two"]], offset=1)]]
    )
    storage.failures[SegmentStorage.ack_uri("a")] = [httpx2.Response(500)]
    storage.failures[SegmentStorage.ack_uri("b")] = [httpx2.ConnectError("refused")]

    assert await fetch_all(storage) == [[1, "one"], [2, "two"]]
    assert [r.getMessage() for r in caplog.records] == [
        f"failed to acknowledge Segment {SegmentStorage.ack_uri('a')}: HTTP 500",
        (
            f"failed to acknowledge Segment {SegmentStorage.ack_uri('b')}: failed to reach "
            f"Segment storage at {SegmentStorage.ack_uri('b')}: ConnectError: refused"
        ),
    ]


@pytest.mark.parametrize(
    ("data", "match"),
    [
        ({"encoding": "arrow", "segments": []}, "encoding 'arrow'"),
        (
            {"encoding": "json", "segments": [{"type": "remote", "metadata": {}}]},
            "Segment type 'remote'",
        ),
        (
            {
                "encoding": "json",
                "segments": [
                    {
                        "type": "inline",
                        "data": "not base64!",
                        "metadata": {"rowOffset": 0, "rowsCount": 1, "segmentSize": 8},
                    }
                ],
            },
            "base64",
        ),
    ],
)
async def test_an_unsupported_spooled_response_raises(data: JSON, match: str) -> None:
    pages = spooled_pages(columns=TWO_COLUMNS, encoding="json", pages=[[]])
    pages[1]["data"] = data
    coordinator = FakeCoordinator(pages)
    async with coordinator.client() as client:
        cur = connect(host="coordinator", encoding="json", http_client=client).cursor()

        with pytest.raises(TrinoConnectionError, match=match):
            await cur.execute("SELECT n, s FROM t")


async def test_segment_data_that_is_not_json_raises() -> None:
    segment = inline([[1, "one"]])
    segment["data"] = base64.b64encode(b"[[1, 'one']]").decode()
    pages = spooled_pages(columns=TWO_COLUMNS, encoding="json", pages=[[segment]])
    coordinator = FakeCoordinator(pages)
    async with coordinator.client() as client:
        cur = connect(host="coordinator", encoding="json", http_client=client).cursor()
        await cur.execute("SELECT n, s FROM t")

        with pytest.raises(TrinoConnectionError, match="not valid JSON"):
            _ = await cur.fetchall()


async def test_a_segment_header_with_several_values_raises() -> None:
    storage = spooled_query(
        lambda s: [[s.spooled("a", [[1, "one"]], headers={"X-Token": ["a", "b"]})]]
    )

    with pytest.raises(TrinoConnectionError, match="X-Token"):
        _ = await fetch_all(storage)


async def test_a_segment_with_fewer_rows_than_announced_raises() -> None:
    storage = spooled_query(lambda s: [[s.spooled("a", [[1, "one"]])]])
    data = storage.coordinator.pages[1]["data"]
    assert isinstance(data, dict)
    [segment] = cast("list[JSON]", data["segments"])
    cast("JSON", segment["metadata"])["rowsCount"] = 2

    with pytest.raises(TrinoConnectionError, match="expected 2 rows"):
        _ = await fetch_all(storage)


async def test_the_direct_protocol_is_handled_when_the_server_does_not_spool() -> None:
    coordinator = FakeCoordinator(
        statement_pages(columns=TWO_COLUMNS, batches=[[[1, "one"]], [[2, "two"]]])
    )
    async with coordinator.client() as client:
        cur = connect(host="coordinator", encoding="json+zstd", http_client=client).cursor()
        await cur.execute("SELECT n, s FROM t")

        assert await cur.fetchall() == [[1, "one"], [2, "two"]]


async def test_a_segment_cursor_yields_segments_without_downloading_them() -> None:
    storage = spooled_query(
        lambda s: [
            [inline([[1, "one"]]), s.spooled("a", [[2, "two"]], offset=1)],
            [s.spooled("b", [[3, "three"]], offset=2, headers={"X-Token": ["t"]})],
        ],
        encoding="json+zstd",
    )
    async with storage.client() as client:
        conn = connect(
            host="coordinator",
            auth=BasicAuthentication("alice", "s3cret"),
            allow_insecure_auth=True,
            encoding="json+zstd",
            http_client=client,
        )
        cur = conn.cursor(cursor_style="segment")
        await cur.execute("SELECT n, s FROM t")

        first = await cur.fetchone()
        rest = await cur.fetchall()
        assert storage.segment_requests() == []
        assert first is not None
        assert [(s.encoding, s.metadata.row_offset, s.uri) for s in [first, *rest]] == [
            ("json+zstd", 0, None),
            ("json+zstd", 1, SegmentStorage.uri("a")),
            ("json+zstd", 2, SegmentStorage.uri("b")),
        ]
        assert rest[0].ack_uri == SegmentStorage.ack_uri("a")
        assert rest[1].headers == {"X-Token": "t"}
        assert cur.description is not None
        assert repr(rest[0]) == (
            "Segment(encoding='json+zstd', metadata=SegmentMetadata(row_offset=1, rows_count=1, "
            f"segment_size={rest[0].metadata.segment_size}, uncompressed_size=None, "
            "expires_at=datetime.datetime(2026, 10, 28, 12, 0)), "
            f"uri={SegmentStorage.uri('a')!r})"
        )

        assert [await s.rows() for s in [first, *rest]] == [
            [[1, "one"]],
            [[2, "two"]],
            [[3, "three"]],
        ]
        assert await rest[0].data() == storage.segments[SegmentStorage.uri("a")]
        await first.acknowledge()
        await rest[1].acknowledge()

    assert storage.segment_requests() == [
        "GET /segments/a",
        "GET /segments/b",
        "GET /segments/a",
        "GET /v1/spooled/ack/b",
    ]
    for request in storage.requests:
        if request.url.path.startswith(("/segments/", "/v1/spooled/")):
            assert "Authorization" not in request.headers
            assert "X-Trino-User" not in request.headers


async def test_a_segment_cursor_iterates_segments_and_maps_their_rows() -> None:
    storage = spooled_query(lambda s: [[s.spooled("a", [[1, "one"]])], [inline([[2, "two"]])]])
    async with storage.client() as client:
        conn = connect(host="coordinator", encoding="json", http_client=client)
        async with conn.cursor(cursor_style="segment", legacy_primitive_types=True) as cur:
            await cur.execute("SELECT n, s FROM t")

            rows = [row async for segment in cur for row in await segment.rows()]

    assert rows == [[1, "one"], [2, "two"]]


async def test_a_failed_segment_download_raises_from_the_segment() -> None:
    storage = spooled_query(lambda s: [[s.spooled("a", [[1, "one"]])]])
    storage.failures[SegmentStorage.uri("a")] = [httpx2.Response(404)]
    async with storage.client() as client:
        conn = connect(host="coordinator", encoding="json", max_attempts=1, http_client=client)
        cur = conn.cursor(cursor_style="segment")
        await cur.execute("SELECT n, s FROM t")
        [segment] = await cur.fetchmany(5)

        with pytest.raises(HttpError, match="404"):
            _ = await segment.rows()


def test_a_segment_cursor_needs_an_encoding() -> None:
    with pytest.raises(ValueError, match="encoding"):
        _ = connect(host="coordinator").cursor(cursor_style="segment")


async def test_a_segment_cursor_rejects_a_direct_protocol_result() -> None:
    coordinator = FakeCoordinator(statement_pages(columns=TWO_COLUMNS, batches=[[[1, "one"]]]))
    async with coordinator.client() as client:
        conn = connect(host="coordinator", encoding="json", http_client=client)
        cur = conn.cursor(cursor_style="segment")

        with pytest.raises(NotSupportedError, match="Direct protocol"):
            await cur.execute("SELECT n, s FROM t")
