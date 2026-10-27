from dataclasses import dataclass
from typing import Literal
from urllib.parse import urlsplit

from asynctrino.exceptions import TrinoAuthError

type HttpScheme = Literal["http", "https"]

DEFAULT_PORT = 8080
DEFAULT_TLS_PORT = 443
STATEMENT_PATH = "/v1/statement"
_SCHEME_PORTS = {"http": 80, "https": 443}


@dataclass(frozen=True, slots=True)
class Coordinator:
    """The Coordinator's base URL, resolved from ``connect()``'s host, port and scheme.

    Attributes:
        allow_insecure_auth: Whether credentials may also go to the Coordinator's host and port
            over plain ``http`` when its base URL is ``https``.
    """

    base_url: str
    allow_insecure_auth: bool = False

    @classmethod
    def resolve(
        cls,
        host: str,
        port: int | None,
        http_scheme: HttpScheme | None,
        *,
        allow_insecure_auth: bool = False,
    ) -> Coordinator:
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
        return cls(f"{scheme}://{parts.hostname}:{explicit_port}{path}", allow_insecure_auth)

    @property
    def uses_tls(self) -> bool:
        """Whether requests to the Coordinator are sent over ``https``."""
        return self.base_url.startswith("https://")

    @property
    def statement_url(self) -> str:
        """The URL statements are POSTed to."""
        return f"{self.base_url}{STATEMENT_PATH}"

    def check_origin(self, uri: str) -> None:
        """Refuse a Coordinator-supplied ``uri`` that credentials and session headers can't go to.

        Raises:
            TrinoAuthError: ``uri``'s host or port differs from the Coordinator's, or its scheme
                does, unless it is ``http`` for an ``https`` Coordinator with
                ``allow_insecure_auth``.
        """
        scheme, host, port = _origin(self.base_url)
        uri_scheme, uri_host, uri_port = _origin(uri)
        downgrade_allowed = self.allow_insecure_auth and (scheme, uri_scheme) == ("https", "http")
        if (uri_host, uri_port) != (host, port) or (uri_scheme != scheme and not downgrade_allowed):
            msg = (
                f"the Coordinator at {scheme}://{host}:{port} pointed the query at another origin, "
                f"{uri_scheme}://{uri_host}:{uri_port}; "
                "credentials are only sent to the Coordinator"
            )
            raise TrinoAuthError(msg)


def _origin(url: str) -> tuple[str, str | None, int | None]:
    parts = urlsplit(url)
    try:
        port = parts.port
    except ValueError:
        return parts.scheme, parts.hostname, None
    return parts.scheme, parts.hostname, _SCHEME_PORTS.get(parts.scheme) if port is None else port
