"""Authentication schemes for Coordinator requests.

Pass one as ``connect(auth=...)``. Any ``httpx2.Auth`` works too, including ones with async
flows such as Kerberos. asynctrino applies it to each Coordinator request only: it never
becomes an HTTP client default and never reaches Segment storage.
"""

from abc import ABC, abstractmethod
from base64 import b64encode
from collections.abc import Generator
from dataclasses import dataclass, field
from typing import override

import httpx2

__all__ = ["Authentication", "BasicAuthentication", "JWTAuthentication"]


class Authentication(httpx2.Auth, ABC):
    """Base of the built-in schemes, which each set one ``Authorization`` header per request."""

    @abstractmethod
    def authorization(self) -> str:
        """The value of the ``Authorization`` header."""

    @override
    def auth_flow(self, request: httpx2.Request) -> Generator[httpx2.Request, httpx2.Response]:
        request.headers["Authorization"] = self.authorization()
        yield request


@dataclass(frozen=True)
class BasicAuthentication(Authentication):
    """HTTP Basic authentication with a user name and password."""

    username: str
    password: str = field(repr=False)

    @override
    def authorization(self) -> str:
        credentials = b64encode(f"{self.username}:{self.password}".encode()).decode("ascii")
        return f"Basic {credentials}"


@dataclass(frozen=True)
class JWTAuthentication(Authentication):
    """JSON Web Token authentication, sending the token as a bearer token."""

    token: str = field(repr=False)

    @override
    def authorization(self) -> str:
        return f"Bearer {self.token}"
