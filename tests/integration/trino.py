"""The Trino that integration tests run against: reused if already running, else a container.

The supported Trino releases are pinned here and nowhere else (ADR-0003). ``TRINO_VERSION``
selects one by alias (``previous`` or ``latest``, the default) or by number. Set
``TRINO_RUNNING_HOST`` and ``TRINO_RUNNING_PORT`` to reuse a Trino that is already running.

The container spools results to Garage, an S3-compatible Segment storage that clients download
from directly. Its address inside the container network (``garage:3900``) is unknown to the
host, so :func:`segment_storage_client` routes those requests to its published port, named by
``TRINO_SEGMENT_STORAGE``.
"""

import os
from collections.abc import Generator
from contextlib import ExitStack, contextmanager
from dataclasses import dataclass, field
from pathlib import Path
from typing import override

import httpx2
from testcontainers.core.container import DockerContainer
from testcontainers.core.network import Network
from testcontainers.core.wait_strategies import HealthcheckWaitStrategy, LogMessageWaitStrategy

TRINO_VERSIONS = {"previous": "483", "latest": "484"}

TRINO_PORT = 8080
ETC = Path(__file__).parents[2] / "etc"
RUNNING_HOST = "TRINO_RUNNING_HOST"
RUNNING_PORT = "TRINO_RUNNING_PORT"
SEGMENT_STORAGE = "TRINO_SEGMENT_STORAGE"

# Keep in sync with etc/trino/spooling-manager.properties.
GARAGE_IMAGE = "dxflrs/garage:v2.5.0"
GARAGE_HOST = "garage"
GARAGE_PORT = 3900
GARAGE_ACCESS_KEY = "GK3f44937995da33e6ff995bbe"
GARAGE_SECRET_KEY = "18d166ef0c360047393ea66e7a2c0f484f2069428a4ca8cacd050af8debb4256"
GARAGE_BUCKET = "spooling"


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
    """Start Trino and its Segment storage in containers and yield Trino's host and port.

    ``TRINO_SEGMENT_STORAGE`` names the storage's published address until the context exits.
    """
    with ExitStack() as stack:
        network = stack.enter_context(Network())
        garage = stack.enter_context(
            DockerContainer(GARAGE_IMAGE)
            .with_network(network)
            .with_network_aliases(GARAGE_HOST)
            .with_exposed_ports(GARAGE_PORT)
            .with_env("GARAGE_DEFAULT_ACCESS_KEY", GARAGE_ACCESS_KEY)
            .with_env("GARAGE_DEFAULT_SECRET_KEY", GARAGE_SECRET_KEY)
            .with_env("GARAGE_DEFAULT_BUCKET", GARAGE_BUCKET)
            .with_command("/garage server --single-node --default-bucket")
            .with_volume_mapping(str(ETC / "garage" / "garage.toml"), "/etc/garage.toml", "ro")
            .waiting_for(
                LogMessageWaitStrategy("S3 API server listening on").with_startup_timeout(60)
            )
        )
        trino = stack.enter_context(
            DockerContainer(f"trinodb/trino:{trino_version()}")
            .with_network(network)
            .with_exposed_ports(TRINO_PORT)
            .with_volume_mapping(str(ETC / "trino" / "catalog"), "/etc/trino/catalog")
            .with_volume_mapping(str(ETC / "trino" / "jvm.config"), "/etc/trino/jvm.config")
            .with_volume_mapping(
                str(ETC / "trino" / "config.properties"), "/etc/trino/config.properties"
            )
            .with_volume_mapping(
                str(ETC / "trino" / "spooling-manager.properties"),
                "/etc/trino/spooling-manager.properties",
            )
            .waiting_for(HealthcheckWaitStrategy().with_startup_timeout(180))
        )
        storage = f"{garage.get_container_host_ip()}:{garage.get_exposed_port(GARAGE_PORT)}"
        previous = os.environ.get(SEGMENT_STORAGE)
        os.environ[SEGMENT_STORAGE] = storage
        try:
            yield trino.get_container_host_ip(), trino.get_exposed_port(TRINO_PORT)
        finally:
            if previous is None:
                del os.environ[SEGMENT_STORAGE]
            else:
                os.environ[SEGMENT_STORAGE] = previous


@contextmanager
def trino() -> Generator[tuple[str, int]]:
    """Yield the host and port of the running Trino, starting a container if none is named."""
    running = running_trino()
    if running is not None:
        yield running
        return
    with trino_container() as address:
        yield address


@dataclass
class SegmentStorageRoute(httpx2.AsyncBaseTransport):
    """Sends requests for ``garage:3900`` to the storage's published address, recording them.

    The ``Host`` header is left as ``garage:3900``, so presigned URLs stay valid.
    """

    published: str | None
    requests: list[httpx2.Request] = field(default_factory=list[httpx2.Request])
    _transport: httpx2.AsyncHTTPTransport = field(default_factory=httpx2.AsyncHTTPTransport)

    @override
    async def handle_async_request(self, request: httpx2.Request) -> httpx2.Response:
        if (request.url.host, request.url.port) == (GARAGE_HOST, GARAGE_PORT):
            self.requests.append(request)
            if self.published is not None:
                host, _, port = self.published.rpartition(":")
                request.url = request.url.copy_with(host=host, port=int(port))
        return await self._transport.handle_async_request(request)

    @override
    async def aclose(self) -> None:
        await self._transport.aclose()


def segment_storage_route() -> SegmentStorageRoute:
    """A transport that reaches the Segment storage of the Trino containers, if they're used."""
    return SegmentStorageRoute(os.environ.get(SEGMENT_STORAGE))
