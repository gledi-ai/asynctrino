from collections.abc import Generator

import pytest
from sqlalchemy import Column, Integer, MetaData, Table
from sqlalchemy.dialects import registry

import asynctrino.sqlalchemy  # noqa: F401


@pytest.fixture
def other_trino_dialect_installed() -> Generator[None]:
    """Register a ``trino`` dialect that fails to load, like an Upstream install would shadow."""
    registry.register("trino", "tests.unit.no_such_module", "TrinoDialect")
    try:
        yield
    finally:
        registry.deregister("trino")


@pytest.mark.usefixtures("other_trino_dialect_installed")
def test_trino_keyword_arguments_are_accepted_without_loading_another_dialect() -> None:
    table = Table("t", MetaData(), Column("x", Integer), trino_format="ORC")

    assert table.dialect_kwargs["trino_format"] == "ORC"
