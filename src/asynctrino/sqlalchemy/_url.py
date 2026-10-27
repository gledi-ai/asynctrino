import json
import re
from collections.abc import Mapping, Sequence
from urllib.parse import quote, urlencode, urlparse

from sqlalchemy import exc

DEFAULT_SOURCE = "asynctrino-sqlalchemy"


def URL(  # noqa: N802, PLR0913  # named after the sqlalchemy.URL it builds
    host: str,
    *,
    port: int | None = 8080,
    user: str | None = None,
    password: str | None = None,
    catalog: str | None = None,
    schema: str | None = None,
    source: str = DEFAULT_SOURCE,
    session_properties: Mapping[str, str] | None = None,
    http_headers: Mapping[str, str] | None = None,
    extra_credential: Sequence[tuple[str, str]] | None = None,
    client_tags: Sequence[str] | None = None,
    roles: Mapping[str, str] | str | None = None,
    timezone: str | None = None,
    encoding: str | Sequence[str] | None = None,
    legacy_primitive_types: bool | None = None,
    verify: bool | None = None,
    access_token: str | None = None,
    allow_insecure_auth: bool | None = None,
) -> str:
    """Compose an ``asynctrino://`` URL for ``create_async_engine``, quoting every part.

    The options mean what they mean for :func:`asynctrino.connect`. A ``password`` selects Basic
    authentication and an ``access_token`` JWT authentication.

    Warning:
        SQLAlchemy masks only the password when it renders a URL, so an ``access_token`` in the
        query string appears in ``str(engine.url)``, reprs, and logs. For secrets, pass
        ``connect_args={"auth": JWTAuthentication(token)}`` to ``create_async_engine`` instead.

    Raises:
        sqlalchemy.exc.ArgumentError: ``host`` is not a bare host name, or ``password`` is given
            without ``user`` or ``schema`` without ``catalog``.
    """
    _assert_valid_host(host)
    query: dict[str, str | None] = {
        "source": source,
        "session_properties": _json(session_properties),
        "http_headers": _json(http_headers),
        "extra_credential": _json(extra_credential),
        "client_tags": _json(client_tags),
        "roles": _json(roles),
        "timezone": timezone,
        "encoding": encoding if isinstance(encoding, str) else _json(encoding),
        "legacy_primitive_types": _json(legacy_primitive_types),
        "verify": _json(verify),
        "access_token": access_token,
        "allow_insecure_auth": _json(allow_insecure_auth),
    }
    authority = _userinfo(user, password) + host + ("" if port is None else f":{port}")
    rendered_query = urlencode({k: v for k, v in query.items() if v is not None})
    return f"asynctrino://{authority}/{_database(catalog, schema)}?{rendered_query}"


def _userinfo(user: str | None, password: str | None) -> str:
    if user is None:
        if password is not None:
            msg = "user must be specified when specifying a password."
            raise exc.ArgumentError(msg)
        return ""
    if password is None:
        return f"{_quote_userinfo(user)}@"
    return f"{_quote_userinfo(user)}:{_quote_userinfo(password)}@"


def _database(catalog: str | None, schema: str | None) -> str:
    if catalog is None:
        if schema is not None:
            msg = "catalog must be specified when specifying a default schema."
            raise exc.ArgumentError(msg)
        return ""
    if schema is None:
        return quote(catalog, safe="")
    return f"{quote(catalog, safe='')}/{quote(schema, safe='')}"


def _json(value: object) -> str | None:
    return None if value is None else json.dumps(value)


def _quote_userinfo(text: str) -> str:
    return re.sub(r"[:@/%]", lambda m: f"%{ord(m.group(0)):X}", text)


def _assert_valid_host(host: str) -> None:
    try:
        parsed = urlparse("//" + host)
        invalid = (
            host == ""
            or urlparse(host).scheme != ""
            or parsed.port is not None
            or parsed.path != ""
            or parsed.username is not None
            or parsed.password is not None
        )
    except ValueError:
        invalid = True
    if invalid:
        msg = (
            f"host must be a hostname only, without a scheme, port, path, or credentials. "
            f"Got {host!r}. For example, use 'example.com' instead of 'https://example.com' "
            "and pass the port via the 'port' argument."
        )
        raise exc.ArgumentError(msg)
