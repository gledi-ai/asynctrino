"""Nox sessions for linting, formatting, type checking, and the test matrix."""

import os

import nox
from nox_uv import session

nox.options.default_venv_backend = "uv"
nox.options.sessions = ["lint", "format", "typecheck", "test"]

PYTHONS = ["3.14", "3.15"]
SOURCES = ["src", "tests", "noxfile.py"]


@session(uv_groups=["dev"])
def lint(s: nox.Session) -> None:
    """Check for lint issues."""
    _ = s.run("ruff", "check", *SOURCES)


@session(uv_groups=["dev"])
def format(s: nox.Session) -> None:  # noqa: A001
    """Check formatting."""
    _ = s.run("ruff", "format", "--check", *SOURCES)


@session(uv_groups=["dev"])
def typecheck(s: nox.Session) -> None:
    """Type check the package and the tests at maximum strictness."""
    _ = s.run("pyrefly", "check")


@session(python=PYTHONS, uv_groups=["dev"])
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
        _ = s.run("bash", "-c", f'coverage report --format=markdown >> "{summary}"', external=True)
