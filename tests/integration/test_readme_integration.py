"""The README's Python examples, run against a real Coordinator.

Examples address Trino as ``localhost`` on port 8080; both are swapped for the integration
Trino's address. An example preceded by a ``<!-- not run: reason -->`` line is only compiled.
Each example may ``await`` at the top level, as in ``python -m asyncio``.
"""

import ast
import asyncio
import re
from collections.abc import Coroutine
from dataclasses import dataclass
from pathlib import Path

import pytest

README = Path(__file__).parents[2] / "README.md"
BLOCK = re.compile(
    r"(?P<marker><!-- not run: [^>]+ -->\n+)?```python\n(?P<source>.*?)```", re.DOTALL
)


@dataclass(frozen=True)
class Example:
    line: int
    source: str
    runs: bool


def examples() -> list[Example]:
    text = README.read_text()
    return [
        Example(
            line=text.count("\n", 0, match.start("source")) + 1,
            source=match["source"],
            runs=match["marker"] is None,
        )
        for match in BLOCK.finditer(text)
    ]


def run(example: Example, host: str, port: int) -> None:
    source = example.source.replace('"localhost"', f'"{host}"').replace("localhost:", f"{host}:")
    source = source.replace("8080", str(port))
    code = compile(
        "\n" * (example.line - 1) + source,
        str(README),
        "exec",
        flags=ast.PyCF_ALLOW_TOP_LEVEL_AWAIT,
    )
    namespace: dict[str, object] = {"__name__": "__readme__"}
    # The README is this repository's own documentation, not untrusted input.
    result: object = eval(code, namespace)  # noqa: S307
    if isinstance(result, Coroutine):
        coroutine: Coroutine[object, object, object] = result
        _ = asyncio.run(coroutine)


def test_the_readme_has_runnable_examples() -> None:
    assert sum(example.runs for example in examples()) >= 5


@pytest.mark.parametrize("example", examples(), ids=lambda example: f"README.md:{example.line}")
def test_readme_example(example: Example, trino_address: tuple[str, int]) -> None:
    if example.runs:
        run(example, *trino_address)
    else:
        _ = compile(example.source, str(README), "exec", flags=ast.PyCF_ALLOW_TOP_LEVEL_AWAIT)
