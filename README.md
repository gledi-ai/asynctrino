# asynctrino

A fully typed asyncio client for the [Trino](https://trino.io/) distributed SQL engine, built on
[httpx2](https://pypi.org/project/httpx2/), with a PEP 249 style DB-API.

> **Status:** pre-release. The API may change before 1.0.

## Supported versions

| Component | Versions   |
| --------- | ---------- |
| Python    | 3.14, 3.15 |
| Trino     | 483, 484   |

Only the latest two Trino releases are supported and tested.

## Quick start

```python
import asynctrino

async with asynctrino.connect(
    host="localhost", port=8080, user="me", catalog="tpch", schema="tiny"
) as conn:
    async with conn.cursor() as cur:
        await cur.execute("SELECT name FROM nation")
        print(cur.description)
        async for row in cur:
            print(row)
```

`connect()` and `cursor()` do no I/O. Pass `http_client=httpx2.AsyncClient(...)` to reuse your
own client; asynctrino never closes it or changes its defaults.

## License

Apache-2.0. See [LICENSE](LICENSE) and [NOTICE](NOTICE) for attribution to trino-python-client
and aiotrino.
