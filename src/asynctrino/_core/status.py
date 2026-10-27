import json
from dataclasses import dataclass
from http import HTTPStatus
from typing import Any, NotRequired, TypedDict, cast

from asynctrino._core.messages import HttpResponse
from asynctrino._core.segments import Segment, SpooledDataJSON, parse_segments
from asynctrino.exceptions import (
    Http502Error,
    Http503Error,
    Http504Error,
    HttpError,
    TrinoAuthError,
    TrinoConnectionError,
    TrinoExternalError,
    TrinoInternalError,
    TrinoQueryError,
    TrinoUserError,
)

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
    data: NotRequired[list[Row] | SpooledDataJSON]
    error: NotRequired[dict[str, object]]
    warnings: NotRequired[list[dict[str, object]]]
    updateType: NotRequired[str]
    updateCount: NotRequired[int]


@dataclass(frozen=True, slots=True)
class QueryStatus:
    """One Coordinator response to a statement POST or a ``nextUri`` GET."""

    id: str
    info_uri: str
    next_uri: str | None
    stats: dict[str, object]
    columns: list[Column] | None
    rows: list[Row]
    segments: list[Segment]
    warnings: list[dict[str, object]]
    update_type: str | None
    update_count: int | None


def check_status(response: HttpResponse) -> None:
    """Raise if the Coordinator answered with an HTTP error status."""
    if not response.is_success:
        raise _http_error(response)


def parse_status(response: HttpResponse) -> QueryStatus:
    """Parse a Coordinator response, raising if it reports a failure."""
    check_status(response)
    if len(response.content.strip()) == 0:
        msg = f"received an empty response from the Coordinator (status {response.status_code})"
        raise TrinoConnectionError(msg)
    try:
        results = cast("_QueryResults", json.loads(response.content))
    except ValueError as e:
        msg = f"received invalid JSON from the Coordinator: {e}"
        raise TrinoConnectionError(msg) from e
    if "error" in results:
        raise _query_error(results["error"], results.get("id"))
    data = results.get("data", [])
    return QueryStatus(
        id=results["id"],
        info_uri=results["infoUri"],
        next_uri=results.get("nextUri"),
        stats=results["stats"],
        columns=results.get("columns"),
        rows=data if isinstance(data, list) else [],
        segments=[] if isinstance(data, list) else parse_segments(data),
        warnings=results.get("warnings", []),
        update_type=results.get("updateType"),
        update_count=results.get("updateCount"),
    )


def _query_error(error: dict[str, object], query_id: str | None) -> TrinoQueryError:
    match error.get("errorType"):
        case "USER_ERROR":
            return TrinoUserError(error, query_id)
        case "EXTERNAL":
            return TrinoExternalError(error, query_id)
        case "INTERNAL_ERROR":
            return TrinoInternalError(error, query_id)
        case _:
            return TrinoQueryError(error, query_id)


_HTTP_ERRORS: dict[int, type[HttpError]] = {502: Http502Error, 503: Http503Error, 504: Http504Error}


def _http_error(response: HttpResponse) -> TrinoAuthError | HttpError:
    body = response.content.decode(errors="replace")
    if response.status_code == HTTPStatus.UNAUTHORIZED:
        return TrinoAuthError(f"error {response.status_code}: {body}")
    error = _HTTP_ERRORS.get(response.status_code, HttpError)
    return error(response.status_code, body)
