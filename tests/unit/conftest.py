import pytest

PROXY_VARIABLES = ("ALL_PROXY", "HTTP_PROXY", "HTTPS_PROXY", "NO_PROXY")


@pytest.fixture
def without_env_proxies(monkeypatch: pytest.MonkeyPatch) -> None:
    for name in PROXY_VARIABLES:
        monkeypatch.delenv(name, raising=False)
        monkeypatch.delenv(name.lower(), raising=False)
