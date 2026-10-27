"""HTTP messages as plain data, exchanged between the protocol core and the I/O layer."""

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from typing import Literal

type Method = Literal["GET", "POST", "DELETE", "HEAD"]


@dataclass(frozen=True, slots=True)
class HttpRequest:
    """A request the I/O layer should send verbatim."""

    method: Method
    url: str
    headers: Mapping[str, str]
    content: bytes | None = None


@dataclass(frozen=True, slots=True)
class HttpResponse:
    """The parts of a received response the protocol core needs."""

    status_code: int
    content: bytes
    headers: Sequence[tuple[str, str]] = ()
