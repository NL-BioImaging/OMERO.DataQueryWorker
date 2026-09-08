# Coordinated release: worker 0.2.0 / Analysis 0.14.0

Analysis is the OMERO-aware gateway. The worker remains read-only and has no OMERO credentials or write API. This release retains `omero-data-query-worker-v1`, existing routes, typed `$name` parameters, preview rules and CSV bytes. Analysis retains `omero-data-query-v1`, result tokens, local/remote policy, notebook `ctx.query()` and saved Method bindings v1/v2. Saving is additive and depends on `features.result_provenance_v1`.

## Compatibility contract

The frozen worker source is commit `2115594`; the frozen Analysis broker is from `4db7f8b` on `analysis_integration`. `tests/fixtures/compatibility` contains the contract and original broker. The Analysis HTTP harness runs both brokers against both worker implementations, exercising DuckDB, SQLite, SQLite3, CSV, schema, typed parameters, Unicode/quoting, empty results, downloads, hit/miss/bypass and reuse after rollback. It starts real HTTP processes and separate caches, using the installed dependency set. It is not a substitute for testing every historical engine build.

Tested engines: DuckDB 1.5.5, sqlglot 28.10.1, SQLite 3.45.3 (Windows) and 3.46.1 (Linux container). The retained local worker 0.1.0 image also contains DuckDB 1.5.5/sqlglot 28.10.1/SQLite 3.46.1. SQLite3 is a filename alias for SQLite. The existing browser runtime uses DuckDB 1.5.1. Supported files are standalone databases readable by the selected engine and UTF-8 CSV accepted by the existing ingestion rules; external files, extensions, attached databases and executable database features are outside the supported format. Copying an uncheckpointed database without its journal is unsupported. Original source bytes are never rewritten.

Actual engine, parser, worker and policy versions, effective query limits, source/schema digests and typed parameters participate in result identity. Legacy manifests are readable; missing execution provenance causes query recomputation. Startup discards legacy entries lacking the digest needed for recovery validation, including converted CSV sources without a converted-database digest. Rollback can reuse readable sources and recompute results under the previous key; do not downgrade DuckDB separately without testing its file compatibility.

## Resource and trust boundaries

Use exactly one service process per cache volume. An OS lock rejects a second process. Source leases span execution; result leases span sending the file, including failures/disconnects. Cleanup, quota eviction and administrative purge skip leases. Duplicate deterministic queries and exhausted ingestion/query slots return 429 instead of accumulating lock waiters.

Upload admission happens before multipart parsing. Spools, source copies, CSV conversion output, manifests and result data count toward storage admission. Source and result quotas remain separate. A source may require its spool, immutable copy and converted database simultaneously. If safe eviction cannot make space, the request fails with 507. Result rows are serialized with an exact UTF-8 byte check before each write, including headers and individual oversized values. Subprocess conversion files are observed periodically; use a dedicated filesystem quota in production to cap transient conversion growth and protect unrelated services from a native-engine failure.

Ingestion runs off the async loop and defaults to one simultaneous ingestion; query concurrency remains four. Subprocess limits, cancellation monitoring and `finally` cleanup release pipes, processes, uploads, staging paths and slots. Engine children receive credential-free settings and an allowlisted environment before processing sources or SQL. They still run under the service UID: native-code compromise is contained by the container/host boundaries, not by a separate per-query security identity. Keep secrets and cache inaccessible to other users, worker egress blocked, root filesystem read-only, capabilities dropped, no-new-privileges enabled and cgroup memory/PID limits enforced.

Cache storage contains plaintext scientific data. It is disposable, not an audit store or durable result archive. Persist promoted results and provenance in OMERO. No SQL or parameter values are included in metrics. The Analysis audit records identity, context, hashes, counts, duration and outcome; the full recipe is deliberately stored only in the protected provenance annotation.

Source and result publication now fsyncs the staged files and directories, renames the entry, then fsyncs both parents before returning. On restart, a service/engine lock fences surviving work, incomplete staging is removed immediately and committed file checksums/lengths are verified before readiness. Corrupt entries are discarded for recomputation. Linux execution children install a parent-death signal. Each DuckDB query has a private spill directory inside its quota-accounted staging path, preventing concurrent queries from colliding in a source-adjacent spill directory.

## Optional 10M export profile

The [completed joint release evidence](https://github.com/NL-BioImaging/OMERO.Analysis/blob/297f5c44ed1b0501a6c19444a5eab7f9e8d0f686/docs/testing/query-large-export-2026-09-08.json) validates 540 10M requests: 468 successes, 72 expected admission rejections and zero unexpected failures. All 72 single-client 1M/4M regression requests also pass. Normal runs have no OOM kills and stay within cache quotas. Separate engine-OOM, exact row/byte boundary, live OMERO save and VM power-cut gates pass. See the [tested implementation pair](https://github.com/NL-BioImaging/OMERO.Analysis/blob/297f5c44ed1b0501a6c19444a5eab7f9e8d0f686/docs/testing/query-version-pair.json) for exact commits, engine versions and image/test scope.

`deploy/compose.large-export.yaml` raises the row ceiling to 10,000,000, CSV byte limit to 2 GiB, query timeout to 900 seconds and ingestion timeout to 1,800 seconds. Normal defaults remain unchanged. Apply the matching Analysis profile, including its dedicated CSV promotion limit and web/proxy timeouts. The tested two-CPU, 2 GiB, four-query container uses 256 MB per DuckDB engine; four 512 MB engines exceeded the container memory in testing. Wide results must still fit the byte/time/storage limits or fail explicitly.

The coordinated Analysis [large-export/recovery runbook](https://github.com/NL-BioImaging/OMERO.Analysis/blob/297f5c44ed1b0501a6c19444a5eab7f9e8d0f686/docs/data-query-large-export-recovery.md) provides reproducible 1M/4M/10M fixtures, real HTTP capacity tests, exact 10M/2-GiB boundary checks, real DuckDB/SQLite/CSV OOM, whole-worker OOM, active-query restart and isolated Hyper-V power-cut gates. Real OMERO saved the same verified 609,383,573-byte 10M CSV for all three formats. Analysis journals and an explicit administrator command reconcile interrupted promotions; the worker remains read-only and OMERO-agnostic.

## Operator controls

Additional environment settings:

| Setting | Default / purpose |
| --- | --- |
| `DQW_MAX_CONCURRENT_INGESTIONS` | `1`, includes early upload admission |
| `DQW_CLEANUP_INTERVAL_SECONDS` | `60` |
| `DQW_TOKEN_KEYRING_FILE` | optional atomically replaced JSON keyring |

`GET /health/live` is independent of storage. Readiness checks writable temporary storage, physical free space, shutdown state and headroom after safe eviction. `GET /v1/metrics` requires bearer authentication and exposes request durations, admission rejection, active operations, cache counters/bytes, failures and subprocess exits. Counters reset on restart; scrape externally. Periodic cleanup removes expired inactive entries and abandoned temporary directories older than twice the larger execution timeout.

Run the local administrative command inside the running worker container:

```sh
omero-data-query-worker purge --source-id src_<digest>
omero-data-query-worker purge --scope-id <opaque-scope> --apply
omero-data-query-worker purge --result-id res_<digest> --apply
```

The default is dry-run. Exactly one selector is required. Source/scope selection includes associated results. A busy selection is reported without partial deletion. This command contacts the live lease manager over loopback; do not manipulate active cache directories with shell deletion tools.

## Transport and overlapping credentials

Legacy private-network `DQW_API_TOKEN` remains supported. TLS is optional and must not be switched on automatically during a mixed-version upgrade. For rotation, mount a **directory**, set `DQW_TOKEN_KEYRING_FILE=/run/query-secrets/tokens.json`, and atomically rename a new JSON file within that directory. Format: `{"tokens":["old credential at least 16 characters","new credential at least 16 characters"]}`. Never commit real values. Mounting a single file may pin its old inode across host-side atomic replacement. Malformed keyrings fail closed. The environment token is accepted alongside the ring; remove it from deployment configuration when retiring that credential.

`deploy/compose.mtls.yaml` and `deploy/nginx-mtls.conf` supply an optional private mTLS reverse proxy. Combine with `compose.yaml` as a starting profile and replace the smoke token/image with deployment secrets and a pinned candidate image. Mount server.crt, server.key and ca.crt; the server SAN must cover `query-tls`. Attach Analysis only to the broker-facing network. Neither service publishes a host port. Analysis supplies a CA bundle and client certificate/key. Retain bearer authentication behind mTLS. The base compose file is a smoke fixture, not production credential provisioning.

## Reproducible gates and rollout

```sh
python -m pip install -e '.[test]'
python -m pytest
ruff check .
ruff format --check .
mypy src
python scripts/smoke_container.py
python scripts/security_container.py
# In the adjacent Analysis checkout (its test dependencies installed):
DQW_RUN_INTEGRATION=1 python -m pytest tests/test_worker_contract.py
python scripts/test_local_omero_queries.py
python scripts/test_query_mtls.py
python scripts/benchmark_query_capacity.py --rows 100000 --repeats 3
```

PowerShell: `$env:DQW_RUN_INTEGRATION='1'` before pytest. Set `DQW_REPO`/`DQW_PYTHON` for non-adjacent checkouts. The Linux gate uses disposable containers for regression, read-only filesystem, blocked egress, PID pressure and OOM probes; tests also cover real query timeout/crash/disconnect, corrupt sources, storage exhaustion and leased eviction. The allocator OOM probe validates the cgroup boundary; it does not prove recovery for every possible engine OOM. See the Analysis release document for live permission coverage and remaining acceptance boundaries.

1. Retain both previous images/configuration and back up deployment configuration. Record exact candidate image digests and engine versions. Keep audit/state volumes separate from cache.
2. Upgrade only the worker. Exercise existing Analysis queries, schema and downloads.
3. Upgrade Analysis from `analysis_integration`. Check legacy notebooks and Methods, then verify exact CSV promotion and provenance.
4. Enable the chosen saving/transport deployment configuration after the permission, container and compatibility gates pass on the target environment. Test overlapping credentials before removing the old credential.
5. To roll back, restore the previous Analysis and/or worker image with its matching transport configuration. Keep OMERO annotations, audit/state and cache volumes. Do not delete caches to force rollback; allow safe reuse/recomputation. Old Analysis ignores additive metadata and offers its existing manual result saving.

Production rollout remains gated on target-environment acceptance, including realistic large exports and restart/fault behavior. These changes do not introduce asynchronous jobs, cancellation APIs, Arrow/Parquet, multi-worker routing, OMERO.Tables materialization or grid discovery.
