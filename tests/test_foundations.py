from __future__ import annotations

import asyncio
import hashlib
import io
import json
import threading
from dataclasses import replace

import pytest
from fastapi import UploadFile
from fastapi.testclient import TestClient

from omero_data_query_worker.app import LeasedFileResponse, create_app
from omero_data_query_worker.cache import CacheManager, ResultRecord
from omero_data_query_worker.engines import _write_result, engine_versions
from omero_data_query_worker.errors import (
    AuthenticationError,
    CacheCapacityExceeded,
    QueryLimitExceeded,
)
from omero_data_query_worker.models import QueryRequest, SourceFormat
from omero_data_query_worker.operations import (
    Credentials,
    sanitize_execution_environment,
    volume_lock,
)
from omero_data_query_worker.service import QueryService

from .conftest import upload_source


def test_provenance_survives_hit_and_legacy_manifest(settings, auth):
    with TestClient(create_app(settings)) as client:
        source = upload_source(
            client,
            auth,
            source_ref="receipt",
            source_format="csv",
            filename="test.csv",
            content=b"id,label\n1,alpha\n",
        ).json()
        endpoint = f"/v1/sources/{source['source_id']}/query"
        payload = {"sql": "SELECT * FROM data", "parameters": {}}
        first = client.post(endpoint, headers=auth, json=payload).json()
        csv = client.get(f"/v1/results/{first['result_id']}/download", headers=auth).content
        assert first["execution"]["result_sha256"] == hashlib.sha256(csv).hexdigest()
        assert first["execution"]["versions"]["duckdb"] == engine_versions()["duckdb"]
        second = client.post(endpoint, headers=auth, json=payload).json()
        assert second["cache_status"] == "hit"
        assert second["execution"] == first["execution"]
        manifest = settings.results_dir / first["result_id"] / "manifest.json"
        old = json.loads(manifest.read_text())
        old.pop("execution")
        assert ResultRecord.from_dict(old).execution is None
        manifest.write_text(json.dumps(old))
        recomputed = client.post(endpoint, headers=auth, json=payload).json()
        assert recomputed["cache_status"] == "miss"
        assert recomputed["execution"]["result_sha256"] == hashlib.sha256(csv).hexdigest()


@pytest.mark.parametrize("rows,limit", [([], 1), ([("a" * 5000,)], 100)])
def test_byte_limit_includes_header_and_single_value(tmp_path, rows, limit):
    class Cursor:
        description = [("column", "VARCHAR")]

        def fetchmany(self, size):
            nonlocal rows
            result, rows = rows, []
            return result

    output = tmp_path / "bounded.csv"
    with pytest.raises(QueryLimitExceeded):
        _write_result(Cursor(), output, 100, limit, False)
    assert output.stat().st_size <= limit


def test_exact_utf8_csv_limit_and_digest(tmp_path):
    class Cursor:
        description = [("label", "VARCHAR")]
        calls = 0

        def fetchmany(self, size):
            self.calls += 1
            return [('é,"x"',)] if self.calls == 1 else []

    expected = 'label\n"é,""x"""\n'.encode()
    result = _write_result(Cursor(), tmp_path / "out", 1, len(expected), False)
    assert (tmp_path / "out").read_bytes() == expected
    assert result["byte_count"] == len(expected)


def test_leases_prevent_eviction_purge_and_expiry(client, settings, auth):
    source = upload_source(
        client,
        auth,
        source_ref="lease",
        source_format="csv",
        filename="test.csv",
        content=b"id\n1\n",
    ).json()
    service = client.app.state.query_service
    cache = service.cache
    with cache.lease_source(source["source_id"]):
        cache._cleanup(settings.sources_dir, 0, set())
        assert cache.source_path(source["source_id"]).exists()
        assert cache.purge(source_id=source["source_id"], dry_run=False)["busy"]
        result = service.query(source["source_id"], QueryRequest(sql="SELECT * FROM data"))
        cache.acquire_result(result.result_id)
        cache._cleanup(settings.results_dir, 0, set())
        assert cache.result_path(result.result_id).exists()
        cache.release(result.result_id)
    dry = cache.purge(scope_id="test-scope")
    assert len(dry["selected"]) == 2 and not dry["removed"]
    assert len(cache.purge(scope_id="test-scope", dry_run=False)["removed"]) == 2


def test_concurrent_staging_capacity_is_accounted(settings):
    settings = replace(settings, result_cache_max_bytes=100)
    settings.prepare()
    cache = CacheManager(settings)
    a, b = cache.staging_path("result"), cache.staging_path("result")
    cache.reserve(a, 60)
    with pytest.raises(CacheCapacityExceeded):
        cache.reserve(b, 41)
    cache.discard_staging(a)
    cache.reserve(b, 100)
    cache.discard_staging(b)


def test_single_entry_including_manifest_must_fit(settings, auth):
    with TestClient(create_app(replace(settings, source_cache_max_bytes=100))) as client:
        response = upload_source(
            client,
            auth,
            source_ref="too-large",
            source_format="csv",
            filename="test.csv",
            content=b"id\n1\n",
        )
        assert response.status_code == 507
        assert not list(settings.sources_dir.iterdir())
        assert not list(settings.tmp_dir.glob("source-*"))


def test_keyring_rotation_and_fail_closed(settings, tmp_path):
    path = tmp_path / "keys.json"
    old, new = "a" * 24, "b" * 24
    path.write_text(json.dumps({"tokens": [old]}))
    credentials = Credentials(replace(settings, api_token="", token_keyring_file=str(path)))
    credentials.authenticate(f"Bearer {old}")
    next_path = path.with_suffix(".new")
    next_path.write_text(json.dumps({"tokens": [old, new]}))
    next_path.replace(path)
    credentials.authenticate(f"Bearer {old}")
    credentials.authenticate(f"Bearer {new}")
    path.write_text(json.dumps({"tokens": [new]}))
    with pytest.raises(AuthenticationError):
        credentials.authenticate(f"Bearer {old}")
    path.write_text("broken")
    with pytest.raises(AuthenticationError):
        credentials.authenticate(f"Bearer {new}")


def test_no_credentials_in_execution_configuration_or_environment(settings, monkeypatch):
    safe = replace(settings, token_keyring_file="secret-path").execution_settings()
    assert not safe.api_token and not safe.token_keyring_file
    # Restore the whole environment after exercising the allowlist.
    import os

    with monkeypatch.context() as scoped:
        for name, value in list(os.environ.items()):
            scoped.setenv(name, value)
        scoped.setenv("OMERO_SESSION", "secret")
        scoped.setenv("DQW_API_TOKEN", "secret")
        sanitize_execution_environment()
        assert "OMERO_SESSION" not in os.environ and "DQW_API_TOKEN" not in os.environ


def test_volume_owner_is_exclusive(tmp_path):
    with (
        volume_lock(tmp_path),
        pytest.raises(RuntimeError, match="Another worker"),
        volume_lock(tmp_path),
    ):
        pass
    with volume_lock(tmp_path):
        pass


def test_busy_ingestion_closes_upload_and_lock_wait_is_bounded(settings):
    settings.prepare()
    service = QueryService(settings)
    upload = UploadFile(io.BytesIO(b"id\n1\n"), filename="test.csv")
    with (
        service._admit(service._ingest_capacity, "ingestion"),
        pytest.raises(Exception, match="slots are busy"),
    ):
        service.ingest_source(
            scope_id="s",
            source_ref="r",
            source_format=SourceFormat.csv,
            declared_size=5,
            expected_sha256=None,
            upload=upload,
        )
    assert upload.file.closed
    lock = threading.Lock()
    with lock, pytest.raises(Exception, match="already running"), service._try_lock(lock):
        pass


def test_download_lease_released_if_send_fails(tmp_path):
    path = tmp_path / "result.csv"
    path.write_bytes(b"id\n1\n")
    released = []
    response = LeasedFileResponse(
        path, release=lambda: released.append(True), media_type="text/csv", filename="result.csv"
    )

    async def send(message):
        raise OSError("disconnected")

    async def receive():
        return {"type": "http.disconnect"}

    with pytest.raises(OSError):
        asyncio.run(response({"type": "http", "method": "GET", "headers": []}, receive, send))
    assert released == [True]


def test_metrics_private_and_correlation_validated(client, auth):
    assert client.get("/v1/metrics").status_code == 401
    response = client.get("/v1/metrics", headers={**auth, "X-Request-ID": "bad/id"})
    assert response.status_code == 200
    assert response.headers["X-Request-ID"] != "bad/id"
    assert "dqw_source_count" in response.text


def test_upload_spool_capacity_and_unauthorized_body_not_consumed(settings, auth):
    limited = replace(settings, source_cache_max_bytes=100)
    with TestClient(create_app(limited)) as client:
        response = upload_source(
            client,
            auth,
            source_ref="spool",
            source_format="csv",
            filename="test.csv",
            content=b"id\n1\n",
        )
        assert response.status_code == 507
        assert not client.app.state.query_service.cache._staging
        assert not list(limited.tmp_dir.iterdir())


def test_real_timeout_and_disconnect_leave_no_children_or_leases(settings, tmp_path):
    import multiprocessing
    import sqlite3

    from omero_data_query_worker.errors import QueryExecutionError, QueryTimedOut
    from omero_data_query_worker.executor import execute_in_subprocess

    path = tmp_path / "source.sqlite"
    with sqlite3.connect(path) as connection:
        connection.execute("CREATE TABLE data(id INTEGER)")
    sql = (
        "WITH RECURSIVE t(x) AS (SELECT 1 UNION ALL SELECT x+1 FROM t "
        "WHERE x<1000000000) SELECT sum(x) FROM t"
    )
    before = {p.pid for p in multiprocessing.active_children()}
    with pytest.raises(QueryTimedOut):
        execute_in_subprocess(
            path,
            SourceFormat.sqlite,
            sql,
            {},
            tmp_path / "out.csv",
            replace(settings, query_timeout_seconds=1),
        )
    service = QueryService(settings)
    cancelled = threading.Event()
    cancelled.set()
    with pytest.raises(QueryExecutionError, match="interrupted"):
        execute_in_subprocess(
            path,
            SourceFormat.sqlite,
            sql,
            {},
            tmp_path / "out.csv",
            settings,
            monitor=lambda: service._check_cancelled(cancelled),
        )
    assert {p.pid for p in multiprocessing.active_children()} == before


def _crash_child(*args):
    import os

    os._exit(17)


def test_subprocess_crash_is_reported_and_reaped(settings, monkeypatch, tmp_path):
    from omero_data_query_worker import executor
    from omero_data_query_worker.errors import QueryExecutionError

    monkeypatch.setattr(executor, "_child_execute", _crash_child)
    with pytest.raises(QueryExecutionError, match="exited unexpectedly"):
        executor.execute_in_subprocess(
            tmp_path / "unused", SourceFormat.sqlite, "SELECT 1", {}, tmp_path / "out", settings
        )
