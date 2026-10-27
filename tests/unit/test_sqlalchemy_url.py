import pytest
from sqlalchemy import exc
from sqlalchemy.engine import make_url

from asynctrino.auth import BasicAuthentication, JWTAuthentication
from asynctrino.sqlalchemy import URL, TrinoDialect


def connect_args(url: str) -> dict[str, object]:
    args, kwargs = TrinoDialect().create_connect_args(make_url(url))
    assert args == []
    return dict(kwargs)


@pytest.mark.parametrize(
    ("url", "rendered", "expected"),
    [
        (
            URL("localhost", user="user"),
            "asynctrino://user@localhost:8080/?source=asynctrino-sqlalchemy",
            {
                "host": "localhost",
                "port": 8080,
                "catalog": "system",
                "user": "user",
                "source": "asynctrino-sqlalchemy",
            },
        ),
        (
            URL("localhost", port=443, user="user", catalog="tpch", schema="tiny"),
            "asynctrino://user@localhost:443/tpch/tiny?source=asynctrino-sqlalchemy",
            {
                "host": "localhost",
                "port": 443,
                "catalog": "tpch",
                "schema": "tiny",
                "user": "user",
                "source": "asynctrino-sqlalchemy",
            },
        ),
        (
            URL(
                "localhost",
                user="user",
                session_properties={"query_max_run_time": "1d"},
                http_headers={"X-Custom": "1"},
                extra_credential=[("a", "b"), ("c", "d")],
                client_tags=["1", "sql"],
                legacy_primitive_types=False,
            ),
            (
                "asynctrino://user@localhost:8080/"
                "?client_tags=%5B%221%22%2C+%22sql%22%5D"
                "&extra_credential=%5B%5B%22a%22%2C+%22b%22%5D%2C+%5B%22c%22%2C+%22d%22%5D%5D"
                "&http_headers=%7B%22X-Custom%22%3A+%221%22%7D"
                "&legacy_primitive_types=false"
                "&session_properties=%7B%22query_max_run_time%22%3A+%221d%22%7D"
                "&source=asynctrino-sqlalchemy"
            ),
            {
                "host": "localhost",
                "port": 8080,
                "catalog": "system",
                "user": "user",
                "source": "asynctrino-sqlalchemy",
                "session_properties": {"query_max_run_time": "1d"},
                "http_headers": {"X-Custom": "1"},
                "extra_credential": [("a", "b"), ("c", "d")],
                "client_tags": ["1", "sql"],
                "legacy_primitive_types": False,
            },
        ),
        (
            URL(
                "localhost",
                user="user@test.org/my_role",
                password="pass /*&",
                extra_credential=[("user1@test.org/my_role", "user2@test.org/my_role")],
                client_tags=['1 @& /"', "sql"],
                verify=False,
            ),
            (
                "asynctrino://user%40test.org%2Fmy_role:***@localhost:8080/"
                "?client_tags=%5B%221+%40%26+%2F%5C%22%22%2C+%22sql%22%5D"
                "&extra_credential=%5B%5B%22user1%40test.org%2Fmy_role%22%2C+"
                "%22user2%40test.org%2Fmy_role%22%5D%5D"
                "&source=asynctrino-sqlalchemy"
                "&verify=false"
            ),
            {
                "host": "localhost",
                "port": 8080,
                "catalog": "system",
                "user": "user@test.org/my_role",
                "auth": BasicAuthentication("user@test.org/my_role", "pass /*&"),
                "http_scheme": "https",
                "source": "asynctrino-sqlalchemy",
                "extra_credential": [("user1@test.org/my_role", "user2@test.org/my_role")],
                "client_tags": ['1 @& /"', "sql"],
                "verify": False,
            },
        ),
        (
            URL("localhost", user="user", roles={"hive": "finance", "system": "analyst"}),
            (
                "asynctrino://user@localhost:8080/"
                "?roles=%7B%22hive%22%3A+%22finance%22%2C+%22system%22%3A+%22analyst%22%7D"
                "&source=asynctrino-sqlalchemy"
            ),
            {
                "host": "localhost",
                "port": 8080,
                "catalog": "system",
                "user": "user",
                "roles": {"hive": "finance", "system": "analyst"},
                "source": "asynctrino-sqlalchemy",
            },
        ),
        (
            URL(
                "localhost",
                user="user",
                source="etl",
                timezone="Europe/Brussels",
                encoding=["json+zstd", "json"],
                access_token="tok",
                allow_insecure_auth=True,
            ),
            (
                "asynctrino://user@localhost:8080/"
                "?access_token=tok"
                "&allow_insecure_auth=true"
                "&encoding=%5B%22json%2Bzstd%22%2C+%22json%22%5D"
                "&source=etl"
                "&timezone=Europe%2FBrussels"
            ),
            {
                "host": "localhost",
                "port": 8080,
                "catalog": "system",
                "user": "user",
                "auth": JWTAuthentication("tok"),
                "allow_insecure_auth": True,
                "source": "etl",
                "timezone": "Europe/Brussels",
                "encoding": ["json+zstd", "json"],
            },
        ),
    ],
)
def test_url_options_map_onto_connect(url: str, rendered: str, expected: dict[str, object]) -> None:
    assert repr(make_url(url)) == rendered
    assert connect_args(url) == expected


def test_single_encoding_is_passed_as_a_string() -> None:
    assert connect_args(URL("localhost", encoding="json+lz4"))["encoding"] == "json+lz4"


def test_access_token_overrides_the_password() -> None:
    kwargs = connect_args("asynctrino://user:pass@host/?access_token=tok%2Ben")

    assert kwargs["auth"] == JWTAuthentication("tok+en")
    assert kwargs["http_scheme"] == "https"


def test_password_is_decoded_once() -> None:
    kwargs = connect_args("asynctrino://user:pass%2Bword@localhost:8080/system")

    assert kwargs["user"] == "user"
    assert kwargs["auth"] == BasicAuthentication("user", "pass+word")


def test_password_without_user_is_rejected() -> None:
    with pytest.raises(ValueError, match="Username is required when specifying a password"):
        _ = connect_args("asynctrino://:pass@localhost")


def test_more_than_catalog_and_schema_is_rejected() -> None:
    with pytest.raises(ValueError, match="Unexpected database format catalog/schema/foobar"):
        _ = connect_args("asynctrino://abc@localhost/catalog/schema/foobar")


def test_query_values_keep_their_plus_signs() -> None:
    kwargs = connect_args(
        URL(
            "host",
            user="us+er",
            password="pa+ss",
            catalog="ca+t",
            schema="sc+h",
            source="so+urce",
            session_properties={"prop": "1+1"},
            http_headers={"x-custom": "a+b"},
            extra_credential=[("cred", "va+lue")],
            client_tags=["ta+g"],
            roles={"system": "ro+le"},
        )
    )

    assert kwargs["user"] == "us+er"
    assert kwargs["auth"] == BasicAuthentication("us+er", "pa+ss")
    assert kwargs["catalog"] == "ca+t"
    assert kwargs["schema"] == "sc+h"
    assert kwargs["source"] == "so+urce"
    assert kwargs["session_properties"] == {"prop": "1+1"}
    assert kwargs["http_headers"] == {"x-custom": "a+b"}
    assert kwargs["extra_credential"] == [("cred", "va+lue")]
    assert kwargs["client_tags"] == ["ta+g"]
    assert kwargs["roles"] == {"system": "ro+le"}


@pytest.mark.parametrize(
    ("url", "catalog", "schema"),
    [
        (URL("localhost", catalog="ca t", schema="sc+h"), "ca t", "sc+h"),
        (URL("localhost", catalog="ca%2Bt", schema="sc%h"), "ca%2Bt", "sc%h"),
        ("asynctrino://localhost/ca%2Bt/sc%20h", "ca+t", "sc h"),
    ],
)
def test_catalog_and_schema_are_decoded_once(url: str, catalog: str, schema: str) -> None:
    kwargs = connect_args(url)

    assert kwargs["catalog"] == catalog
    assert kwargs["schema"] == schema


def test_insecure_auth_keeps_the_scheme_of_the_port() -> None:
    kwargs = connect_args("asynctrino://user:pass@host:8080/?allow_insecure_auth=true")

    assert "http_scheme" not in kwargs


@pytest.mark.parametrize(
    "host",
    ["https://example.com", "example.com:8080", "example.com/path", "user:pw@example.com", "[::1"],
)
def test_url_rejects_a_host_that_is_not_a_bare_hostname(host: str) -> None:
    with pytest.raises(exc.ArgumentError, match="host must be a hostname only"):
        _ = URL(host)


def test_url_requires_a_catalog_for_a_schema() -> None:
    with pytest.raises(exc.ArgumentError, match="catalog must be specified"):
        _ = URL("localhost", schema="tiny")


def test_url_requires_a_user_for_a_password() -> None:
    with pytest.raises(exc.ArgumentError, match="user must be specified"):
        _ = URL("localhost", password="secret")
