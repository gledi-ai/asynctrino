# pyright: reportMissingImports=false
import os
from pathlib import Path

import nox
from nox_uv import session

nox.options.default_venv_backend = "uv"
nox.options.sessions = ["lint", "format", "typecheck", "test"]

PYTHONS = ["3.14", "3.15"]
SOURCES = ["src", "tests", "noxfile.py"]
DIST_CHECK = """
from importlib.resources import files

from sqlalchemy.ext.asyncio import create_async_engine

import asynctrino
import asynctrino.sqlalchemy

assert files("asynctrino").joinpath("py.typed").is_file()
engine = create_async_engine("asynctrino://me@localhost/tpch")
assert (engine.dialect.name, engine.dialect.driver) == ("trino", "asynctrino")
print("asynctrino", asynctrino.__version__)
"""


@session(uv_only_groups=["lint"])
def lint(s: nox.Session) -> None:
    """Check for lint issues."""
    _ = s.run("ruff", "check", *SOURCES)


@session(uv_only_groups=["lint"])
def format(s: nox.Session) -> None:  # noqa: A001
    """Check formatting."""
    _ = s.run("ruff", "format", "--check", *SOURCES)


@session(uv_groups=["lint", "test"], uv_extras=["sqlalchemy"])
def typecheck(s: nox.Session) -> None:
    """Type check the package and the tests at maximum strictness."""
    _ = s.run("pyrefly", "check")


@session(python=PYTHONS, uv_groups=["test"], uv_extras=["sqlalchemy"])
def test(s: nox.Session) -> None:
    """Run the tests with branch coverage; pass pytest arguments after ``--``."""
    suffix = f"{s.python}-trino-{os.environ.get('TRINO_VERSION', 'latest')}"
    _ = s.run(
        "pytest",
        "--cov",
        "--cov-report=term-missing",
        f"--cov-report=xml:coverage-{suffix}.xml",
        f"--junitxml=junit-{suffix}.xml",
        *s.posargs,
    )
    summary = os.environ.get("GITHUB_STEP_SUMMARY")
    if summary is not None:
        report = f'coverage report --format=markdown --fail-under=0 >> "{summary}"'
        _ = s.run("bash", "-c", report, external=True)


@nox.session(python=PYTHONS[0], venv_backend="uv")
def test_lower(s: nox.Session) -> None:
    """Run the tests against the lowest versions the runtime and sqlalchemy specifiers allow."""
    s.install("--resolution", "lowest-direct", "-e", ".[sqlalchemy]", "--group", "test")
    _ = s.run("pytest", *s.posargs)


@nox.session(python=PYTHONS, venv_backend="uv")
def dist(s: nox.Session) -> None:
    """Install each wheel and sdist in ``dist/`` with the sqlalchemy extra, then import it."""
    artifacts = sorted(Path("dist").glob("asynctrino-*"))
    if len(artifacts) == 0:
        s.error("dist/ holds no asynctrino wheel or sdist; run `uv build` first")
    for artifact in artifacts:
        s.log(f"Checking {artifact.name}")
        s.install("--reinstall", f"asynctrino[sqlalchemy] @ {artifact.resolve().as_uri()}")
        _ = s.run("python", "-I", "-c", DIST_CHECK)
