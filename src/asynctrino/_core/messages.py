"""HTTP messages as plain data, exchanged between the protocol core and the I/O layer."""

from collections.abc import Mapping
from dataclasses import dataclass, field
from typing import Literal
from urllib.parse import urlsplit, urlunsplit

type Method = Literal["GET", "POST", "DELETE", "HEAD"]


@dataclass(frozen=True, slots=True)
class HttpRequest:
    """A request the I/O layer should send verbatim.

    Attributes:
        statement: Whether this is a statement POST or ``nextUri`` GET, whose empty 200
            responses are retried.
        segment: Whether this is a Segment download or acknowledgement, sent with ``headers``
            only: no auth, client default headers or cookies.
    """

    method: Method
    url: str
    headers: Mapping[str, str]
    content: bytes | None = None
    statement: bool = False
    segment: bool = False


@dataclass(frozen=True, slots=True)
class HttpResponse:
    """The parts of a received response the protocol core needs."""

    status_code: int
    content: bytes
    headers: Mapping[str, str] = field(default_factory=dict[str, str])


def redact(url: str) -> str:
    """``url`` without userinfo, query and fragment, which may hold credentials."""
    parts = urlsplit(url)
    host = "" if parts.hostname is None else parts.hostname
    if ":" in host:
        host = f"[{host}]"
    netloc = host if parts.port is None else f"{host}:{parts.port}"
    return urlunsplit((parts.scheme, netloc, parts.path, "", ""))
