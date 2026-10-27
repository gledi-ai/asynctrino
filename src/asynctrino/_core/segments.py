"""Spooling protocol Segments: parsed from Coordinator responses and decoded into rows."""

import base64
import binascii
import json
from collections.abc import Mapping, Sequence
from compression import zstd
from dataclasses import dataclass, field
from datetime import datetime
from typing import Literal, NotRequired, TypedDict, cast, get_args

import lz4.block

from asynctrino._core.messages import HttpRequest
from asynctrino.exceptions import TrinoConnectionError

type Encoding = Literal["json", "json+lz4", "json+zstd"]

ENCODINGS: frozenset[str] = frozenset(get_args(Encoding.__value__))

MAX_UNCOMPRESSED_SIZE = 1 << 30
"""The largest decompressed Segment accepted, far above Trino's default maximum of 16 MB."""


def format_encoding(encoding: str | Sequence[str] | None) -> str | None:
    """The ``X-Trino-Query-Data-Encoding`` value for ``connect(encoding=...)``, if any.

    Raises:
        ValueError: No encoding is given, or one isn't ``json``, ``json+lz4`` or ``json+zstd``.
    """
    if encoding is None:
        return None
    names = encoding.split(",") if isinstance(encoding, str) else list(encoding)
    names = [name.strip() for name in names]
    unknown = [name for name in names if name not in ENCODINGS]
    if len(names) == 0 or len(unknown) > 0:
        msg = f"unsupported encoding {encoding!r}; choose from {', '.join(sorted(ENCODINGS))}"
        raise ValueError(msg)
    return ",".join(names)


class _MetadataJSON(TypedDict):
    rowOffset: int
    rowsCount: int
    segmentSize: int
    uncompressedSize: NotRequired[int]
    expiresAt: NotRequired[str]


class _SegmentJSON(TypedDict):
    type: str
    metadata: _MetadataJSON


class _InlineSegmentJSON(_SegmentJSON):
    data: str


class _SpooledSegmentJSON(_SegmentJSON):
    uri: str
    ackUri: str
    headers: NotRequired[dict[str, list[str]]]


class SpooledDataJSON(TypedDict):
    """The ``data`` of a Spooling-protocol response."""

    encoding: str
    segments: list[_SegmentJSON]


@dataclass(frozen=True, slots=True)
class SegmentMetadata:
    """What the Coordinator says about a Segment.

    Attributes:
        row_offset: The position of the Segment's first row in the whole result.
        rows_count: How many rows the Segment holds.
        segment_size: The size of the Segment's data in bytes, as sent.
        uncompressed_size: The size of the data once decompressed, or ``None`` when the data
            was too small to be worth compressing and is sent as plain JSON.
        expires_at: When a spooled Segment is deleted from Segment storage.
    """

    row_offset: int
    rows_count: int
    segment_size: int
    uncompressed_size: int | None = None
    expires_at: datetime | None = None


@dataclass(frozen=True, slots=True)
class InlineSegment:
    """A Segment whose data is embedded in the Coordinator's response."""

    encoding: Encoding
    metadata: SegmentMetadata
    data: bytes

    def decode(self) -> list[list[object]]:
        """The Segment's rows, as the Coordinator encoded them."""
        return decode(self.encoding, self.metadata, self.data)


@dataclass(frozen=True, slots=True)
class SpooledSegment:
    """A Segment whose data is fetched from ``uri`` and then acknowledged at ``ack_uri``.

    The URIs and headers may be credentials, such as pre-signed URLs, so they stay out of the
    ``repr``.

    Attributes:
        headers: Opaque headers the Coordinator requires on both requests; nothing else,
            credentials included, is ever sent with them.
    """

    encoding: Encoding
    metadata: SegmentMetadata
    uri: str = field(repr=False)
    ack_uri: str = field(repr=False)
    headers: Mapping[str, str] = field(default_factory=dict[str, str], repr=False)

    def download_request(self) -> HttpRequest:
        """The request that retrieves the Segment's data."""
        return HttpRequest("GET", self.uri, self.headers, segment=True)

    def ack_request(self) -> HttpRequest:
        """The request that tells the server the Segment can be deleted."""
        return HttpRequest("GET", self.ack_uri, self.headers, segment=True)

    def decode(self, data: bytes) -> list[list[object]]:
        """The Segment's rows, from the data :meth:`download_request` retrieved."""
        return decode(self.encoding, self.metadata, data)


type Segment = InlineSegment | SpooledSegment


def parse_segments(data: SpooledDataJSON) -> list[Segment]:
    """Parse a Spooling-protocol response's ``data`` into Segments.

    Raises:
        TrinoConnectionError: The encoding or a Segment type is unknown.
    """
    encoding = data["encoding"]
    if encoding not in ENCODINGS:
        msg = f"the Coordinator answered with unsupported encoding {encoding!r}"
        raise TrinoConnectionError(msg)
    return [_segment(cast("Encoding", encoding), segment) for segment in data["segments"]]


def _segment(encoding: Encoding, segment: _SegmentJSON) -> Segment:
    match segment["type"]:
        case "inline":
            inline = cast("_InlineSegmentJSON", segment)
            try:
                data = base64.b64decode(inline["data"], validate=True)
            except binascii.Error as e:
                msg = f"inline Segment data is not valid base64: {e}"
                raise TrinoConnectionError(msg) from e
            return InlineSegment(encoding, _metadata(inline["metadata"]), data)
        case "spooled":
            spooled = cast("_SpooledSegmentJSON", segment)
            return SpooledSegment(
                encoding,
                _metadata(spooled["metadata"]),
                spooled["uri"],
                spooled["ackUri"],
                _headers(spooled.get("headers", {})),
            )
        case other:
            msg = f"the Coordinator answered with unsupported Segment type {other!r}"
            raise TrinoConnectionError(msg)


def _metadata(metadata: _MetadataJSON) -> SegmentMetadata:
    expires_at = metadata.get("expiresAt")
    return SegmentMetadata(
        row_offset=metadata["rowOffset"],
        rows_count=metadata["rowsCount"],
        segment_size=metadata["segmentSize"],
        uncompressed_size=metadata.get("uncompressedSize"),
        expires_at=None if expires_at is None else datetime.fromisoformat(expires_at),
    )


def _headers(headers: Mapping[str, list[str]]) -> dict[str, str]:
    single: dict[str, str] = {}
    for name, values in headers.items():
        if len(values) != 1:
            msg = f"Segment header {name!r} must have exactly one value, got {values!r}"
            raise TrinoConnectionError(msg)
        single[name] = values[0]
    return single


def decode(encoding: Encoding, metadata: SegmentMetadata, data: bytes) -> list[list[object]]:
    """Decompress and parse a Segment's data into rows.

    Raises:
        TrinoConnectionError: The data doesn't match its metadata or can't be decoded.
    """
    data = _sized(data, metadata.segment_size, "Segment data")
    if metadata.uncompressed_size is not None and encoding != "json":
        data = _decompress(encoding, metadata.uncompressed_size, data)
        data = _sized(data, metadata.uncompressed_size, "decompressed Segment data")
    try:
        rows = cast("list[list[object]]", json.loads(data))
    except ValueError as e:
        msg = f"Segment data at row {metadata.row_offset} is not valid JSON: {e}"
        raise TrinoConnectionError(msg) from e
    if len(rows) != metadata.rows_count:
        msg = f"expected {metadata.rows_count} rows in the Segment, got {len(rows)}"
        raise TrinoConnectionError(msg)
    return rows


def _decompress(encoding: Encoding, size: int, data: bytes) -> bytes:
    if size > MAX_UNCOMPRESSED_SIZE:
        msg = f"Segment is too large: {size} bytes once decompressed"
        raise TrinoConnectionError(msg)
    try:
        if encoding == "json+lz4":
            return lz4.block.decompress(data, uncompressed_size=size)
        return _decompress_zstd(size, data)
    except (lz4.block.LZ4BlockError, zstd.ZstdError) as e:
        msg = f"cannot decompress {encoding} Segment data: {e}"
        raise TrinoConnectionError(msg) from e


def _decompress_zstd(size: int, data: bytes) -> bytes:
    """Decompress every frame of ``data``, never producing more than ``size`` + 1 bytes."""
    decompressed = bytearray()
    while len(data) > 0:
        decompressor = zstd.ZstdDecompressor()
        decompressed += decompressor.decompress(data, max_length=size + 1 - len(decompressed))
        if len(decompressed) > size:
            msg = f"Segment data decompresses beyond its announced {size} bytes"
            raise TrinoConnectionError(msg)
        if not decompressor.eof:
            msg = "cannot decompress json+zstd Segment data: the frame is truncated"
            raise TrinoConnectionError(msg)
        data = decompressor.unused_data
    return bytes(decompressed)


def _sized(data: bytes, expected: int, what: str) -> bytes:
    if len(data) != expected:
        msg = f"expected {expected} bytes of {what}, got {len(data)}"
        raise TrinoConnectionError(msg)
    return data
