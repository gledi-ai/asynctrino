from collections.abc import Buffer
from typing import Literal

class LZ4BlockError(Exception): ...

def compress(
    source: Buffer | str,
    mode: Literal["default", "fast", "high_compression"] = "default",
    acceleration: int = 1,
    compression: int = 9,
    store_size: bool = True,
    return_bytearray: bool = False,
    dict: Buffer | None = None,
) -> bytes: ...
def decompress(
    source: Buffer | str,
    uncompressed_size: int = -1,
    return_bytearray: bool = False,
    dict: Buffer | None = None,
) -> bytes: ...
