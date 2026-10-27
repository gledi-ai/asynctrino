import os
from contextlib import ExitStack
from pathlib import Path

import pytest

from tests.integration.trino import RUNNING_HOST, RUNNING_PORT, running_trino, trino_container

INTEGRATION_TESTS = Path(__file__).parent / "integration"


def pytest_configure(config: pytest.Config) -> None:
    """Under xdist, start one Trino in the controller and point every worker at it."""
    if hasattr(config, "workerinput") or config.pluginmanager.getplugin("xdist") is None:
        return
    numprocesses: object = config.getoption("numprocesses", None)
    if numprocesses in {None, 0} or running_trino() is not None:
        return
    if not any(_selects_integration_tests(arg) for arg in config.args):
        return
    stack = ExitStack()
    config.add_cleanup(stack.close)
    host, port = stack.enter_context(trino_container())
    os.environ[RUNNING_HOST] = host
    os.environ[RUNNING_PORT] = str(port)


def _selects_integration_tests(arg: str) -> bool:
    path = Path(arg.split("::", maxsplit=1)[0]).resolve()
    return path.is_relative_to(INTEGRATION_TESTS) or INTEGRATION_TESTS.is_relative_to(path)
