# asynctrino

A fully typed asyncio client for the [Trino](https://trino.io/) distributed SQL engine, built on
[httpx2](https://pypi.org/project/httpx2/), with a PEP 249 style DB-API and a SQLAlchemy 2.1
async dialect.

> **Status:** pre-release. The API may change before 1.0.

asynctrino is a sibling of [aiotrino](https://github.com/gledi-ai/aiotrino), not its successor.
It follows the behaviour of
[trino-python-client](https://github.com/trinodb/trino-python-client) 0.340.0, minus the
[deliberate differences](#differences-from-aiotrino-and-upstream) listed below.

## Supported versions

| Component  | Versions   |
| ---------- | ---------- |
| Python     | 3.14, 3.15 |
| Trino      | 483, 484   |
| SQLAlchemy | 2.1        |

Only the latest two Trino releases are supported and tested. Older servers may work, but there
are no version checks or fallbacks for them.

## Install

```shell
pip install asynctrino
pip install "asynctrino[sqlalchemy]"   # adds SQLAlchemy 2.1 with its asyncio extra
```

## DB-API quick start

The examples use top-level `await`, as in the `python -m asyncio` REPL. In a script, put them
in an `async def main()` and call `asyncio.run(main())`.

```python
import asynctrino

async with asynctrino.connect(
    host="localhost", port=8080, user="me", catalog="tpch", schema="tiny"
) as conn:
    async with conn.cursor() as cur:
        await cur.execute("SELECT nationkey, name FROM nation WHERE regionkey = ?", [1])
        print([column.name for column in cur.description])
        async for row in cur:
            print(row)
```

- `connect()` and `cursor()` are plain calls and do no I/O, so connections can be built at
  import or configuration time. `host` also accepts a URL such as
  `https://trino.example.com:8443`.
- `await cur.execute(...)` returns once the first rows are available, the columns are known, or
  the query has finished, so `cur.description` and, for updates, `cur.rowcount` are ready
  right after it.
- Rows are lists of Python values: `Decimal`, `datetime` with time zones, `uuid.UUID`,
  `relativedelta` and `timedelta` intervals, lists, dicts, and named tuples for `ROW`. Pass
  `legacy_primitive_types=True` to `connect()` or `cursor()` to get values as Trino sent them.
- Parameters use the `qmark` style and are bound with `EXECUTE IMMEDIATE`.
- `cur.query_id`, `cur.stats`, `cur.warnings`, `cur.info_uri` and `cur.update_type` describe
  the running query, and `conn.cursor(stats_callback=...)` reports its progress.

```python
import asynctrino

async with asynctrino.connect(
    host="localhost", port=8080, user="me", catalog="tpch", schema="tiny"
) as conn:
    cur = conn.cursor()
    await cur.execute("SELECT name FROM region ORDER BY name")
    first = await cur.fetchone()
    next_two = await cur.fetchmany(2)
    rest = await cur.fetchall()

    await cur.execute("CREATE TABLE memory.default.events (id integer, name varchar)")
    await cur.executemany(
        "INSERT INTO memory.default.events VALUES (?, ?)", [(1, "start"), (2, "stop")]
    )
    print(cur.rowcount)
    await cur.execute("DROP TABLE memory.default.events")
```

### Transactions

Connections run in autocommit by default. Any other isolation level runs statements in a
transaction that starts with the first `execute`. `async with` commits on a clean exit and rolls
back on an error; `commit()` and `rollback()` end it explicitly. Writes inside a transaction
need a connector that supports them.

```python
import asynctrino
from asynctrino import IsolationLevel

async with asynctrino.connect(
    host="localhost",
    port=8080,
    user="me",
    catalog="tpch",
    schema="tiny",
    isolation_level=IsolationLevel.REPEATABLE_READ,
) as conn:
    cur = conn.cursor()
    await cur.execute("SELECT count(*) FROM nation")
    print(await cur.fetchall())
    await conn.commit()
```

### Concurrency

One autocommit connection can run queries from many tasks at once, each with its own cursor.
Inside a transaction, a second statement while one is still running raises `ProgrammingError`.
Connections must not be shared across threads (`threadsafety = 1`).

```python
import asyncio
import asynctrino

async with asynctrino.connect(
    host="localhost", port=8080, user="me", catalog="tpch", schema="tiny"
) as conn:

    async def count(table: str) -> int:
        async with conn.cursor() as cur:
            await cur.execute(f"SELECT count(*) FROM {table}")
            [[rows]] = await cur.fetchall()
            return rows

    async with asyncio.TaskGroup() as tg:
        nations = tg.create_task(count("nation"))
        regions = tg.create_task(count("region"))
    print(nations.result(), regions.result())
```

## SQLAlchemy quick start

Install the `sqlalchemy` extra and use the `asynctrino://` URL scheme with SQLAlchemy's async
engine. The URL is `asynctrino://user[:password]@host:port/catalog[/schema]`.

```python
from sqlalchemy import text
from sqlalchemy.ext.asyncio import create_async_engine

engine = create_async_engine("asynctrino://me@localhost:8080/tpch/tiny")
async with engine.connect() as conn:
    result = await conn.execute(
        text("SELECT name FROM nation WHERE regionkey = :region"), {"region": 1}
    )
    print(result.scalars().all())

    async for row in await conn.stream(text("SELECT name FROM region")):
        print(row)
await engine.dispose()
```

`asynctrino.sqlalchemy.URL` builds the URL and quotes every part. Query options map onto
`connect()`: `source`, `session_properties`, `http_headers`, `extra_credential`, `client_tags`,
`roles`, `timezone`, `encoding`, `legacy_primitive_types`, `verify`, `access_token` and
`allow_insecure_auth`. Pass anything else, such as `auth` or `http_client`, through
`connect_args`.

```python
from sqlalchemy import MetaData
from sqlalchemy.ext.asyncio import create_async_engine

from asynctrino.sqlalchemy import URL

url = URL("localhost", port=8080, user="me", catalog="tpch", schema="tiny", timezone="UTC")
engine = create_async_engine(url, connect_args={"max_attempts": 5})
metadata = MetaData()
async with engine.connect() as conn:
    await conn.run_sync(metadata.reflect, only=["nation", "region"])
print(sorted(metadata.tables))
await engine.dispose()
```

The dialect's `name` is `trino` and its `driver` is `asynctrino`. It compiles Core and ORM
statements to Trino SQL, reflects schemas, tables, views, columns with comments, and partition
columns as an index, and exports the Trino types `MAP`, `ROW`, `DOUBLE`, `TIME`, `TIMESTAMP`,
`JSON` and `VARBINARY` from `asynctrino.sqlalchemy`. Table properties are
`trino_<property>=value` arguments on `Table`, rendered as `CREATE TABLE ... WITH (...)`.

## Authentication

Authentication applies to Coordinator requests only. It never becomes an HTTP client default
and never reaches Segment storage.

```python
import asynctrino
from asynctrino.auth import BasicAuthentication, JWTAuthentication

basic = asynctrino.connect(
    host="https://trino.example.com",
    user="me",
    auth=BasicAuthentication("me", "secret"),
)
jwt = asynctrino.connect(
    host="https://trino.example.com",
    user="me",
    auth=JWTAuthentication("eyJhbGciOi..."),
)
```

`auth=` accepts any `httpx2.Auth`, including ones with async flows, so you can bring
authentication asynctrino doesn't ship, such as Kerberos or a token that refreshes:

```python
from collections.abc import AsyncGenerator, Awaitable, Callable

import httpx2

import asynctrino


class RefreshingToken(httpx2.Auth):
    def __init__(self, fetch_token: Callable[[], Awaitable[str]]) -> None:
        self.fetch_token = fetch_token

    async def async_auth_flow(
        self, request: httpx2.Request
    ) -> AsyncGenerator[httpx2.Request, httpx2.Response]:
        request.headers["Authorization"] = f"Bearer {await self.fetch_token()}"
        yield request


async def fetch_token() -> str:
    return "token from your identity provider"


conn = asynctrino.connect(host="https://trino.example.com", auth=RefreshingToken(fetch_token))
```

In SQLAlchemy URLs, a password selects Basic authentication and the `access_token` query option
JWT authentication. SQLAlchemy masks only the password when it prints a URL, so pass tokens as
`connect_args={"auth": JWTAuthentication(token)}` instead.

asynctrino refuses to send credentials over plain `http` and raises `TrinoAuthError`. It also
refuses when the Coordinator points a follow-up request at another origin or downgrades it to
`http`. When the transport is already encrypted below HTTP, for example by an mTLS service mesh,
opt out with `allow_insecure_auth=True`. The Coordinator may also need
`http-server.authentication.allow-insecure-over-http=true`.

```python
import asynctrino
from asynctrino.auth import BasicAuthentication

conn = asynctrino.connect(
    host="localhost",
    port=8080,
    user="me",
    auth=BasicAuthentication("me", "secret"),
    allow_insecure_auth=True,
)
```

## TLS, proxies and your own HTTP client

`verify` takes `True` (the default, trusting the operating system's certificate store), `False`,
or an `ssl.SSLContext`, which can trust a private CA or present a client certificate.
`request_timeout` takes seconds or an `httpx2.Timeout`.

<!-- not run: needs certificate files -->

```python
import ssl

import httpx2

import asynctrino

context = ssl.create_default_context(cafile="company-ca.pem")
context.load_cert_chain("client.pem", "client.key")
conn = asynctrino.connect(
    host="https://trino.example.com",
    user="me",
    verify=context,
    request_timeout=httpx2.Timeout(60, connect=5),
)
```

The HTTP client asynctrino creates honours the proxy environment variables (`HTTPS_PROXY`,
`HTTP_PROXY`, `ALL_PROXY`, `NO_PROXY`). For anything else, such as an explicit proxy, HTTP/2,
connection pool limits or event hooks, pass your own `httpx2.AsyncClient` as `http_client`:

```python
import httpx2

import asynctrino

async with httpx2.AsyncClient(proxy="http://proxy.example.com:3128", timeout=60) as client:
    conn = asynctrino.connect(host="https://trino.example.com", user="me", http_client=client)
```

asynctrino never closes a client you pass in and never changes its defaults: headers and
authentication are set per request. `verify` and `request_timeout` are ignored with
`http_client`; configure the client itself instead. Without one, asynctrino creates a client on
the first request and closes it with the connection.

## Spooling protocol

Results use the Direct protocol by default, which works even when Segment storage isn't
reachable from your network. To opt into the Spooling protocol, pass the encodings you accept,
most preferred first, as a comma-separated string or a list. `json+zstd` uses the standard
library's zstd, and `json+lz4` uses `lz4`.

```python
import asynctrino

async with asynctrino.connect(
    host="localhost",
    port=8080,
    user="me",
    catalog="tpch",
    schema="tiny",
    encoding="json+zstd,json+lz4,json",
) as conn:
    cur = conn.cursor()
    await cur.execute("SELECT * FROM nation")
    print(len(await cur.fetchall()))
```

The server falls back to the Direct protocol when it can't spool, and asynctrino handles both.
Spooled Segments are downloaded one at a time as you consume rows, with only their own headers:
no credentials, no `X-Trino-*` headers, and none of your `http_headers`. Each Segment is
acknowledged after it's decoded. While one downloads, a heartbeat keeps the query alive on the
Coordinator every `heartbeat_interval` seconds. A failed download raises instead of truncating
the result.

To process Segments yourself, use a Segment cursor:

```python
import asynctrino

async with asynctrino.connect(
    host="localhost", port=8080, user="me", catalog="tpch", schema="tiny", encoding="json+zstd"
) as conn:
    cur = conn.cursor(cursor_style="segment")
    await cur.execute("SELECT * FROM nation")
    for segment in await cur.fetchall():
        print(segment.encoding, segment.metadata, len(await segment.rows()))
        await segment.acknowledge()
```

## Cancellation

`await cur.cancel()` cancels the running query on the Coordinator. When the task awaiting
`execute` or a fetch is cancelled, by `asyncio.timeout`, a failing `TaskGroup` or Ctrl-C,
asynctrino sends a best-effort, cancellation-shielded `DELETE` for the query with a short
timeout before `CancelledError` propagates, so abandoned queries stop using cluster resources.
Closing a cursor or connection with an unfinished query cancels it the same way.

```python
import asyncio

import asynctrino

async with asynctrino.connect(host="localhost", port=8080, user="me", catalog="tpch") as conn:
    cur = conn.cursor()
    try:
        async with asyncio.timeout(1):
            await cur.execute("SELECT count(*) FROM sf100000.lineitem")
            await cur.fetchall()
    except TimeoutError:
        print(f"query {cur.query_id} was cancelled on the cluster")
```

## Errors

Every failure is an `asynctrino.exceptions` error, part of the PEP 249 hierarchy; httpx2
exceptions never escape. Failed queries raise `TrinoUserError`, `TrinoExternalError` or
`TrinoInternalError`, all `TrinoQueryError`s carrying Trino's `error_code`, `error_name`,
`error_type`, `error_location` and `query_id`. Transport failures raise `TrinoConnectionError`,
and error statuses `HttpError`. Segment data that can't be decoded, and values with no Python
representation, raise `TrinoDataError`. Requests are retried up to `max_attempts` times on 429, 502,
503 and 504, on empty responses to statement requests, and on connect, read and timeout errors,
honouring `Retry-After` and otherwise backing off exponentially with jitter.

## Differences from aiotrino and Upstream

"Upstream" is trino-python-client 0.340.0; "aiotrino" is the
[aiotrino](https://github.com/gledi-ai/aiotrino) sibling. These breaks are deliberate.

### API shape

- `conn.cursor()` is synchronous (aiotrino: `await conn.cursor()`).
- `cur.description` is a property, as in PEP 249 (aiotrino: `await cur.get_description()`).
- `connect()` takes keyword arguments only and does no I/O; the HTTP client is created lazily
  on the first request inside the running event loop.
- `threadsafety = 1`: connections must not be shared across threads (Upstream and aiotrino
  declare 2). One connection can be shared across asyncio tasks instead.
- asyncio only, no synchronous API, and only Python 3.14 and 3.15.

### Transactions

- A transaction starts lazily on the first `execute`, not when the cursor is created, so
  `cursor()` never does I/O.
- The transaction starts with `START TRANSACTION ISOLATION LEVEL <level>` (Upstream sends a bare
  `START TRANSACTION`).
- Autocommit sends no `X-Trino-Transaction-Id` header (Upstream sends `NONE`), so Trino refuses
  a manual `START TRANSACTION` statement instead of opening a transaction asynctrino can't
  track. Use `isolation_level` instead.

### Statements and parameters

- No legacy prepared statements: parameters are always bound with `EXECUTE IMMEDIATE`, and the
  `legacy_prepared_statements` option is gone. `PREPARE`, `EXECUTE` and `DEALLOCATE PREPARE`
  written in SQL still work: statements the Coordinator registers are sent back in
  `X-Trino-Prepared-Statement` until deallocated. Response headers are capped at 100 KB by
  httpcore2.
- `executemany` reports the total `rowcount` across all parameter sets (Upstream: the last
  set's).
- An aware `time`, or a `datetime` whose `tzinfo` isn't a `ZoneInfo`, is sent with its numeric
  UTC offset (`+02:00`) rather than its `tzname()`.
- `http_headers` can't override a header asynctrino sends itself; that raises `ValueError`.

### Results and types

- `X-Trino-Query-Data-Encoding` is sent only when you set `encoding`; otherwise results use the
  Direct protocol. Upstream and aiotrino request every installed codec by default, opting into
  the Spooling protocol unless you pass `encoding=None`.
- All three Client capabilities are advertised: `NUMBER`, `PARAMETRIC_DATETIME` and
  `SESSION_AUTHORIZATION` (aiotrino sends two).
- A named `ROW` value raises `AttributeError` for a field it doesn't have (Upstream returns
  `None`).
- A timestamp that rounds past year 9999 raises `TrinoDataError`.
- Segments announcing more than 1 GiB decompressed are refused, and a failed Segment download
  raises instead of returning a partial result.
- Time zones are `zoneinfo` zones; `pytz` is not used.

### HTTP, TLS and authentication

- `verify` accepts an `ssl.SSLContext` as well as `True` and `False` (aiotrino: bool only;
  Upstream: bool or a CA bundle path), and `True` uses the operating system's trust store.
- `http_client=` takes your own `httpx2.AsyncClient`. asynctrino never closes it and never
  mutates its defaults; `verify` and `request_timeout` are ignored with it.
- Authentication and `X-Trino-*` headers go to the Coordinator only. Segment downloads and
  acknowledgements carry only the Segment's own headers, and your `http_headers` are never
  forwarded to Segment URIs, even ones the Coordinator hosts (Upstream 0.339 forwards them).
- Credentials and `X-Trino-*` headers only go to the Coordinator's own scheme, host and port. A
  `nextUri` pointing anywhere else raises `TrinoAuthError` before any request is sent to it; with
  `allow_insecure_auth`, an `https` Coordinator may hand out `http` URIs on the same host and port.
- Built-in authentication is Basic and JWT only. There is no OAuth2, Kerberos, certificate
  authentication or keyring support; pass any `httpx2.Auth` instead.
- httpx2 exceptions are mapped to `TrinoConnectionError` and the rest of the asynctrino
  hierarchy.

### Cancellation

- Cancelling the task that awaits `execute` or a fetch cancels the query on the Coordinator, as
  does closing a cursor or connection with an unfinished query (aiotrino leaves it running).

### SQLAlchemy

- SQLAlchemy 2.1 only, through its async engine; there are no 2.0 shims or sync dialect.
- The URL scheme is `asynctrino://` (aiotrino: `aiotrino://`; Upstream: `trino://`). The
  dialect's `name` is `trino` (aiotrino: `default`), so `trino_*` keyword arguments and
  name-based dialect checks work, and its `driver` is `asynctrino`. `trino_*` arguments are
  validated against this dialect and are only accepted by `Table` and `CreateTableAs`.
- `pre_ping`, connection termination and `set_isolation_level` work, fixing aiotrino's adapter.
- Float literals and binds are sent as `DOUBLE`; JSON path keys are quoted so a key can't
  rewrite the path; window frame offsets render as literals; table constraints are left out of
  `CREATE TABLE` with a warning.

## Development

The workflow uses [uv](https://docs.astral.sh/uv/), nox and a Makefile; run `make help` for the
targets. `make dev` sets up the environment, and `make check` runs lint, format, type checking
and the tests with the 90% branch-coverage gate. Integration tests start Trino in Docker with
testcontainers, or reuse one named by `TRINO_RUNNING_HOST` and `TRINO_RUNNING_PORT`. Commits
follow [Conventional Commits](https://www.conventionalcommits.org/). Releases and the Upstream
Catch-up process are described in [MAINTAINING.md][maintaining].

## License

Apache-2.0. See [LICENSE][license] and [NOTICE][notice] for attribution to trino-python-client,
the original aiotrino authors, and the aiotrino sibling.

[maintaining]: https://github.com/gledi-ai/asynctrino/blob/main/MAINTAINING.md
[license]: https://github.com/gledi-ai/asynctrino/blob/main/LICENSE
[notice]: https://github.com/gledi-ai/asynctrino/blob/main/NOTICE
