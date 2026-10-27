"""The Trino that integration tests run against: reused if already running, else a container.

The supported Trino releases are pinned here and nowhere else (ADR-0003). ``TRINO_VERSION``
selects one by alias (``previous`` or ``latest``, the default) or by number. Set
``TRINO_RUNNING_HOST`` and ``TRINO_RUNNING_PORT`` to reuse a Trino that is already running.
"""

import os
from collections.abc import Generator
from contextlib import contextmanager
from pathlib import Path

from testcontainers.core.container import DockerContainer
from testcontainers.core.wait_strategies import HealthcheckWaitStrategy

TRINO_VERSIONS = {"previous": "483", "latest": "484"}

TRINO_PORT = 8080
ETC = Path(__file__).parents[2] / "etc" / "trino"
RUNNING_HOST = "TRINO_RUNNING_HOST"
RUNNING_PORT = "TRINO_RUNNING_PORT"


def trino_version() -> str:
    """The Trino release selected by ``TRINO_VERSION``."""
    selected = os.environ.get("TRINO_VERSION", "latest")
    return TRINO_VERSIONS.get(selected, selected)


def running_trino() -> tuple[str, int] | None:
    """The already-running Trino named by the environment, if any."""
    port = os.environ.get(RUNNING_PORT)
    if port is None:
        return None
    return os.environ.get(RUNNING_HOST, "localhost"), int(port)


@contextmanager
def trino_container() -> Generator[tuple[str, int]]:
    """Start Trino in a container and yield its host and port until the context exits."""
    container = (
        DockerContainer(f"trinodb/trino:{trino_version()}")
        .with_exposed_ports(TRINO_PORT)
        .with_volume_mapping(str(ETC / "catalog"), "/etc/trino/catalog")
        .with_volume_mapping(str(ETC / "jvm.config"), "/etc/trino/jvm.config")
        .with_volume_mapping(str(ETC / "config.properties"), "/etc/trino/config.properties")
        .waiting_for(HealthcheckWaitStrategy().with_startup_timeout(180))
    )
    with container:
        yield container.get_container_host_ip(), container.get_exposed_port(TRINO_PORT)


@contextmanager
def trino() -> Generator[tuple[str, int]]:
    """Yield the host and port of the running Trino, starting a container if none is named."""
    running = running_trino()
    if running is not None:
        yield running
        return
    with trino_container() as address:
        yield address
