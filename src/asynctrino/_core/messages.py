from collections.abc import Mapping
from dataclasses import dataclass, field
from http import HTTPStatus
from typing import Literal
from urllib.parse import urlsplit, urlunsplit

type Method = Literal["GET", "POST", "DELETE", "HEAD"]
type RequestKind = Literal["coordinator", "statement", "segment"]
"""Who a request goes to and how it is sent.

``coordinator`` requests carry auth; ``statement`` ones are the Coordinator requests whose
empty 200 responses are retried: the statement POST and ``nextUri`` GETs. ``segment`` requests
are Segment downloads and acknowledgements, sent with their ``headers`` only: no auth, client
default headers or cookies.
"""


@dataclass(frozen=True, slots=True)
class HttpRequest:
    """A request the I/O layer should send verbatim, as its :data:`RequestKind` says."""

    method: Method
    url: str
    headers: Mapping[str, str]
    content: bytes | None = None
    kind: RequestKind = "coordinator"


@dataclass(frozen=True, slots=True)
class HttpResponse:
    """The parts of a received response the protocol core needs."""

    status_code: int
    content: bytes
    headers: Mapping[str, str] = field(default_factory=dict[str, str])

    @property
    def is_success(self) -> bool:
        """Whether the status is 2xx."""
        return HTTPStatus.OK <= self.status_code < HTTPStatus.MULTIPLE_CHOICES


def redact(url: str) -> str:
    """``url`` without userinfo, query and fragment, which may hold credentials."""
    parts = urlsplit(url)
    host = "" if parts.hostname is None else parts.hostname
    if ":" in host:
        host = f"[{host}]"
    netloc = host if parts.port is None else f"{host}:{parts.port}"
    return urlunsplit((parts.scheme, netloc, parts.path, "", ""))
