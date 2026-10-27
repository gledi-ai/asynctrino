"""Coordinator responses parsed into typed query status."""

import json
from dataclasses import dataclass
from http import HTTPStatus
from typing import Any, NotRequired, TypedDict, cast

from asynctrino._core.messages import HttpResponse
from asynctrino.exceptions import TrinoAuthError, TrinoConnectionError, TrinoQueryError

type Row = list[Any]  # pyrefly: ignore[explicit-any]  # row values are dynamic by nature


class TypeArgument(TypedDict):
    kind: str
    value: object


class TypeSignature(TypedDict):
    rawType: str
    arguments: list[TypeArgument]


class Column(TypedDict):
    name: str
    type: str
    typeSignature: TypeSignature


class _QueryResults(TypedDict):
    id: str
    infoUri: str
    stats: dict[str, object]
    nextUri: NotRequired[str]
    columns: NotRequired[list[Column]]
    data: NotRequired[list[Row]]
    error: NotRequired[dict[str, object]]


@dataclass(frozen=True, slots=True)
class QueryStatus:
    """One Coordinator response to a statement POST or a ``nextUri`` GET."""

    id: str
    info_uri: str
    next_uri: str | None
    stats: dict[str, object]
    columns: list[Column] | None
    rows: list[Row]


def parse_status(response: HttpResponse) -> QueryStatus:
    """Parse a Coordinator response, raising if it reports a failure."""
    if not 200 <= response.status_code < 300:  # noqa: PLR2004
        body = response.content.decode(errors="replace")
        msg = f"error {response.status_code}: {body}"
        if response.status_code == HTTPStatus.UNAUTHORIZED:
            raise TrinoAuthError(msg)
        raise TrinoConnectionError(msg)
    results = cast("_QueryResults", json.loads(response.content))
    if "error" in results:
        raise TrinoQueryError(results["error"], results.get("id"))
    return QueryStatus(
        id=results["id"],
        info_uri=results["infoUri"],
        next_uri=results.get("nextUri"),
        stats=results["stats"],
        columns=results.get("columns"),
        rows=results.get("data", []),
    )
