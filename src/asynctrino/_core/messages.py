"""HTTP messages as plain data, exchanged between the protocol core and the I/O layer."""

from collections.abc import Mapping
from dataclasses import dataclass, field
from typing import Literal

type Method = Literal["GET", "POST", "DELETE", "HEAD"]


@dataclass(frozen=True, slots=True)
class HttpRequest:
    """A request the I/O layer should send verbatim.

    Attributes:
        statement: Whether this is a statement POST or ``nextUri`` GET, whose empty 200
            responses are retried.
    """

    method: Method
    url: str
    headers: Mapping[str, str]
    content: bytes | None = None
    statement: bool = False


@dataclass(frozen=True, slots=True)
class HttpResponse:
    """The parts of a received response the protocol core needs."""

    status_code: int
    content: bytes
    headers: Mapping[str, str] = field(default_factory=dict[str, str])
