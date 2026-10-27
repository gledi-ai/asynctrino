import json
from compression import zstd

import pytest

from asynctrino._core.messages import redact
from asynctrino._core.segments import Encoding, SegmentMetadata, decode
from asynctrino.exceptions import TrinoDataError

COMPRESSED: list[Encoding] = ["json+zstd", "json+lz4"]


@pytest.mark.parametrize("encoding", COMPRESSED)
def test_negative_uncompressed_size_is_rejected_before_decompressing(encoding: Encoding) -> None:
    data = zstd.compress(json.dumps([[1]] * 1000).encode())
    metadata = SegmentMetadata(
        row_offset=0, rows_count=1000, segment_size=len(data), uncompressed_size=-1
    )

    with pytest.raises(TrinoDataError, match="-1"):
        _ = decode(encoding, metadata, data)


def test_redact_drops_userinfo_query_and_fragment() -> None:
    url = "https://key:secret@[::1]:9000/bucket/segment?X-Amz-Signature=abc#frag"

    assert redact(url) == "https://[::1]:9000/bucket/segment"
