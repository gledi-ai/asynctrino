"""The client session: what the client tells the Coordinator about itself with every request."""

import re
import warnings
from collections.abc import Mapping
from dataclasses import dataclass, field, replace
from urllib.parse import quote, quote_plus, unquote_plus
from zoneinfo import ZoneInfo

from asynctrino._core.transaction import NO_TRANSACTION

DEFAULT_SOURCE = "asynctrino"

_LEGACY_ROLE = re.compile(r"^ROLE\{.*\}$")
_EXTRA_CREDENTIAL_KEY = re.compile(r"[^\s=]+")
RESERVED_HEADERS = frozenset(
    {
        "user-agent",
        "x-trino-catalog",
        "x-trino-client-capabilities",
        "x-trino-client-tags",
        "x-trino-extra-credential",
        "x-trino-original-user",
        "x-trino-path",
        "x-trino-query-data-encoding",
        "x-trino-role",
        "x-trino-schema",
        "x-trino-session",
        "x-trino-source",
        "x-trino-time-zone",
        "x-trino-transaction-id",
        "x-trino-user",
    }
)


def format_roles(roles: Mapping[str, str] | str | None) -> dict[str, str]:
    """Normalise ``connect(roles=...)`` into the per-catalog role selections Trino expects.

    A plain string is the ``system`` role. ``ALL`` and ``NONE`` are kept; any other role name is
    wrapped as ``ROLE{name}``. Already-wrapped names are accepted with a ``DeprecationWarning``.
    """
    if roles is None:
        return {}
    if isinstance(roles, str):
        roles = {"system": roles}
    formatted: dict[str, str] = {}
    for catalog, role in roles.items():
        if role in {"ALL", "NONE"}:
            formatted[catalog] = role
        elif _LEGACY_ROLE.match(role) is not None:
            warnings.warn(
                f"role {role!r} uses the legacy ROLE{{}} format; pass the bare role name instead",
                DeprecationWarning,
                stacklevel=3,
            )
            formatted[catalog] = role
        else:
            formatted[catalog] = f"ROLE{{{role}}}"
    return formatted


@dataclass(frozen=True, slots=True)
class ClientSession:
    """The session the client announces to the Coordinator with every statement."""

    user: str | None
    source: str = DEFAULT_SOURCE
    catalog: str | None = None
    schema: str | None = None
    path: str | None = None
    authorization_user: str | None = None
    properties: Mapping[str, str] = field(default_factory=dict[str, str])
    client_tags: tuple[str, ...] = ()
    roles: Mapping[str, str] = field(default_factory=dict[str, str])
    extra_credential: tuple[tuple[str, str], ...] = ()
    timezone: str | None = None
    http_headers: Mapping[str, str] = field(default_factory=dict[str, str])
    transaction_id: str | None = None
    """``None`` in autocommit; otherwise the open transaction's id, or ``"NONE"`` between them."""

    def __post_init__(self) -> None:
        for name in self.http_headers:
            if name.lower() in RESERVED_HEADERS:
                msg = f"cannot override reserved HTTP header {name}"
                raise ValueError(msg)
        if self.timezone is not None:
            _ = ZoneInfo(self.timezone)
        for key, _ in self.extra_credential:
            if _EXTRA_CREDENTIAL_KEY.fullmatch(key) is None or not key.isascii():
                msg = f"extra credential key {key!r} must be ASCII, without whitespace or '='"
                raise ValueError(msg)

    def headers(self) -> dict[str, str]:
        """The ``X-Trino-*`` headers describing this session, plus the user's custom headers."""
        impersonating = self.authorization_user is not None
        values = {
            "X-Trino-User": self.authorization_user if impersonating else self.user,
            "X-Trino-Original-User": self.user if impersonating else None,
            "X-Trino-Source": self.source,
            "X-Trino-Catalog": self.catalog,
            "X-Trino-Schema": self.schema,
            "X-Trino-Path": self.path,
            "X-Trino-Session": ",".join(f"{k}={quote(v)}" for k, v in self.properties.items()),
            "X-Trino-Client-Tags": _nonempty(",".join(self.client_tags)),
            "X-Trino-Role": _nonempty(",".join(f"{c}={quote(r)}" for c, r in self.roles.items())),
            "X-Trino-Extra-Credential": _nonempty(
                ", ".join(f"{k}={quote_plus(v)}" for k, v in self.extra_credential)
            ),
            "X-Trino-Time-Zone": self.timezone,
            "X-Trino-Transaction-Id": self.transaction_id,
        }
        present = {name: value for name, value in values.items() if value is not None}
        return {**present, **self.http_headers}

    def updated(self, response_headers: Mapping[str, str]) -> ClientSession:
        """The session after applying a response's ``X-Trino-Set-*`` and ``Clear-*`` headers."""
        updates = _ResponseHeaders(response_headers)
        cleared = set(updates.listed("x-trino-clear-session"))
        properties = {k: v for k, v in self.properties.items() if k not in cleared}
        properties.update(updates.assignments("x-trino-set-session"))
        authorization_user = updates.last("x-trino-set-authorization-user", self.authorization_user)
        if len(updates.values("x-trino-reset-authorization-user")) > 0:
            authorization_user = None
        return replace(
            self,
            catalog=updates.last("x-trino-set-catalog", self.catalog),
            schema=updates.last("x-trino-set-schema", self.schema),
            path=updates.last("x-trino-set-path", self.path),
            authorization_user=authorization_user,
            properties=properties,
            roles={**self.roles, **updates.assignments("x-trino-set-role")},
            transaction_id=self._transaction_id_after(updates),
        )

    @property
    def in_transaction(self) -> bool:
        """Whether a transaction is open, so statements run inside it."""
        return self.transaction_id not in {None, NO_TRANSACTION}

    def _transaction_id_after(self, updates: _ResponseHeaders) -> str | None:
        if self.transaction_id is None:
            return None
        if len(updates.values("x-trino-clear-transaction-id")) > 0:
            return NO_TRANSACTION
        started = updates.last("x-trino-started-transaction-id", None)
        return self.transaction_id if started in {None, "", NO_TRANSACTION} else started


@dataclass(frozen=True, slots=True)
class _ResponseHeaders:
    items: Mapping[str, str]

    def values(self, name: str) -> list[str]:
        return [value for key, value in self.items.items() if key.lower() == name]

    def last(self, name: str, default: str | None) -> str | None:
        values = self.values(name)
        return values[-1] if len(values) > 0 else default

    def listed(self, name: str) -> list[str]:
        items = (item.strip() for value in self.values(name) for item in value.split(","))
        return [item for item in items if item != ""]

    def assignments(self, name: str) -> dict[str, str]:
        pairs = (item.partition("=") for item in self.listed(name))
        return {key.strip(): unquote_plus(value.strip()) for key, _, value in pairs}


def _nonempty(value: str) -> str | None:
    return value if value != "" else None


class SessionState:
    """A connection's current :class:`ClientSession`, shared by all of its queries.

    Each Coordinator response replaces the session in one synchronous step, so concurrent
    queries never observe a half-applied update.
    """

    def __init__(self, session: ClientSession) -> None:
        self.current = session

    def apply(self, response_headers: Mapping[str, str]) -> None:
        """Apply a Coordinator response's session updates."""
        self.current = self.current.updated(response_headers)

    def end_transaction(self) -> None:
        """Forget the open transaction, so the next statement starts another."""
        self.current = replace(self.current, transaction_id=NO_TRANSACTION)
