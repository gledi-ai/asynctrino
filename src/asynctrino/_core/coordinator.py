"""Where the Coordinator lives and what the client tells it about the session."""

from dataclasses import dataclass
from typing import Literal
from urllib.parse import urlsplit

type HttpScheme = Literal["http", "https"]

DEFAULT_PORT = 8080
DEFAULT_TLS_PORT = 443
DEFAULT_SOURCE = "asynctrino"
STATEMENT_PATH = "/v1/statement"


@dataclass(frozen=True, slots=True)
class Coordinator:
    """The Coordinator's base URL, resolved from ``connect()``'s host, port and scheme."""

    base_url: str

    @classmethod
    def resolve(cls, host: str, port: int | None, http_scheme: HttpScheme | None) -> Coordinator:
        """Resolve the base URL like Upstream.

        A scheme or port inside ``host`` wins over the arguments. Without a scheme, port 443
        means ``https``; without a port, the scheme's default Trino port is used.
        """
        parts = urlsplit(host if "//" in host else f"//{host}")
        explicit_port = parts.port if parts.port is not None else port
        if parts.scheme != "":
            scheme = parts.scheme
        elif http_scheme is not None:
            scheme = http_scheme
        else:
            scheme = "https" if explicit_port == DEFAULT_TLS_PORT else "http"
        if scheme not in {"http", "https"}:
            msg = f"unsupported http_scheme {scheme!r}; expected 'http' or 'https'"
            raise ValueError(msg)
        if explicit_port is None:
            explicit_port = DEFAULT_TLS_PORT if scheme == "https" else DEFAULT_PORT
        path = parts.path.rstrip("/")
        return cls(f"{scheme}://{parts.hostname}:{explicit_port}{path}")

    @property
    def uses_tls(self) -> bool:
        """Whether requests to the Coordinator are sent over ``https``."""
        return self.base_url.startswith("https://")

    @property
    def statement_url(self) -> str:
        """The URL statements are POSTed to."""
        return f"{self.base_url}{STATEMENT_PATH}"


@dataclass(frozen=True, slots=True)
class ClientSession:
    """The session the client announces to the Coordinator with every statement."""

    user: str | None
    source: str = DEFAULT_SOURCE
    catalog: str | None = None
    schema: str | None = None

    def headers(self) -> dict[str, str]:
        """The ``X-Trino-*`` headers describing this session."""
        values = {
            "X-Trino-User": self.user,
            "X-Trino-Source": self.source,
            "X-Trino-Catalog": self.catalog,
            "X-Trino-Schema": self.schema,
        }
        return {name: value for name, value in values.items() if value is not None}
