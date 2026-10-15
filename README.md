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

See [the coordinated production foundations guide](docs/production-foundations.md)
for compatibility fixtures, leases and quota behavior, token rotation, mTLS,
administrative purge, joint testing and rollback.

```bash
pytest
ruff check .
mypy src
python scripts/smoke_container.py
```

The smoke test builds the container, queries fixtures for all three formats,
checks result caching and unsafe-SQL rejection, restarts the service, and
confirms that its cache survives.

## Releases

The published container is
[`cellularimagingcf/omero-data-query-worker`](https://hub.docker.com/r/cellularimagingcf/omero-data-query-worker)
on Docker Hub. For example:

```console
docker pull cellularimagingcf/omero-data-query-worker:0.2.2
```

Update both `pyproject.toml` and `src/omero_data_query_worker/__init__.py` before
creating a release. Run `python scripts/check_version.py --tag vX.Y.Z`, then
create an SSH-signed tag using a signer listed in `.github/release-signers`.
The publication workflow checks both versions and the tag signature before
publishing the multi-platform container to Docker Hub with SBOM and provenance
attestations. CI keeps its separate SPDX SBOM as a workflow artifact and does
not modify GitHub releases.

Configure repository Actions secrets `DOCKERHUB_USERNAME` and `DOCKERHUB_TOKEN`
for the `cellularimagingcf` namespace. Publication no longer uses GHCR or
`GITHUB_TOKEN` as the container-registry credential.

The initial workflow selected GHCR on 10 August 2026. Version 0.2.2 is copied
unchanged to Docker Hub by the manually dispatched
`migrate-0.2.2-to-dockerhub.yml` workflow: it preserves the complete OCI index,
amd64/arm64 images and embedded attestations, and refuses to replace a different
existing target tag. Its index digest is
`sha256:96620294ab7be707952cc94184a70bea86220c2ca257db7e14d489741bfc8c1b`.
The historical GHCR copy remains available to avoid breaking existing installs.
Future signed release tags publish directly to Docker Hub.

Version 0.2.1 accidentally tagged the unchanged 0.2.0 source and did not publish
a container. Version 0.2.2 corrects the release metadata and CI artifact handling;
it does not change the worker API or query behavior. Existing published tags
are preserved rather than moved to different commits.

## License

Copyright NL-BioImaging contributors. Licensed under
AGPL-3.0-or-later.
