# OMERO.DataQueryWorker

OMERO.DataQueryWorker is a private, OMERO-agnostic service for bounded SQL
queries over immutable DuckDB, SQLite, and CSV files. It is designed to run as
a separate container beside an authenticated application such as
OMERO.Analysis. It does not connect to OMERO or authorize end users.

The worker stores source files and bounded CSV query results in its own
persistent cache. Every query runs in a disposable subprocess with a timeout,
row and byte limits, disabled DuckDB external access, and a restrictive SQL
policy. SQLite uses the standard-library engine in immutable, query-only mode.
A CSV source is imported once and exposed as the table `data`.

## Run locally

```bash
python -m venv .venv
. .venv/bin/activate
python -m pip install -e '.[test]'
export DQW_API_TOKEN='replace-with-at-least-16-characters'
export DQW_CACHE_DIR="$PWD/cache"
omero-data-query-worker
```

OpenAPI is available at `http://localhost:8080/docs`. All `/v1/*` requests
require `Authorization: Bearer <DQW_API_TOKEN>`. `/health/live` and
`/health/ready` intentionally return only minimal status.

## Source and query flow

1. Call `POST /v1/sources/resolve` with an opaque scope and immutable source
   reference.
2. On a miss, stream the file to `POST /v1/sources` as multipart form data.
3. Inspect `GET /v1/sources/{source_id}/schema`.
4. Submit one parameterized `SELECT` to
   `POST /v1/sources/{source_id}/query`.
5. Download the complete bounded result from
   `GET /v1/results/{result_id}/download`.

Named parameters use `$name` placeholders and typed values:

```json
{
  "sql": "SELECT * FROM measurements WHERE area >= $minimum",
  "parameters": {
    "minimum": {"type": "float", "value": 12.5}
  }
}
```

Deterministic results are cached by source digest, normalized SQL, typed
parameters, engine/policy version, and configured limits. Queries containing
volatile time, random, sequence, or UUID functions bypass the result cache.

## Configuration

| Variable | Default |
| --- | --- |
| `DQW_API_TOKEN` | required, at least 16 characters |
| `DQW_CACHE_DIR` | `/var/lib/omero-data-query-worker` |
| `TMPDIR` | `/var/lib/omero-data-query-worker/tmp` in the container |
| `DQW_SOURCE_CACHE_MAX_BYTES` | 100 GiB |
| `DQW_RESULT_CACHE_MAX_BYTES` | 10 GiB |
| `DQW_SOURCE_TTL_SECONDS` | 7 days |
| `DQW_RESULT_TTL_SECONDS` | 24 hours |
| `DQW_MAX_SOURCE_BYTES` | 20 GiB |
| `DQW_QUERY_TIMEOUT_SECONDS` | 30 |
| `DQW_MAX_RESULT_ROWS` | 100,000 |
| `DQW_MAX_RESULT_BYTES` | 64 MiB |
| `DQW_MAX_CONCURRENT_QUERIES` | 4 |
| `DQW_DUCKDB_MEMORY_LIMIT` | `1GB` |
| `DQW_DUCKDB_THREADS` | 2 |

The cache volume contains source data and query results in plaintext and must
be treated as trusted server storage. Run one worker replica per cache volume.
Container deployments should keep `TMPDIR` under that writable volume. The
multipart parser spools large streamed uploads there before atomic ingestion;
the separate `/tmp` tmpfs can therefore remain small and constrained.

## Verification

```bash
pytest
ruff check .
mypy src
python scripts/smoke_container.py
```

The smoke test builds the container, queries fixtures for all three formats,
checks result caching and unsafe-SQL rejection, restarts the service, and
confirms that its cache survives.

## License

Copyright NL-BioImaging contributors. Licensed under
AGPL-3.0-or-later.
