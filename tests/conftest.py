from __future__ import annotations

import sqlite3
from collections.abc import Iterator
from pathlib import Path

import duckdb
import pytest
from fastapi.testclient import TestClient

from omero_data_query_worker.app import create_app
from omero_data_query_worker.config import MIB, Settings

TOKEN = "test-token-at-least-16-characters"


@pytest.fixture
def settings(tmp_path: Path) -> Settings:
    return Settings(
        api_token=TOKEN,
        cache_dir=tmp_path / "cache",
        source_cache_max_bytes=10 * MIB,
        result_cache_max_bytes=10 * MIB,
        source_ttl_seconds=3600,
        result_ttl_seconds=3600,
        max_source_bytes=5 * MIB,
        query_timeout_seconds=5,
        ingestion_timeout_seconds=30,
        max_result_rows=1000,
        max_result_bytes=MIB,
        max_concurrent_queries=2,
        duckdb_memory_limit="128MB",
        duckdb_threads=1,
    )


@pytest.fixture
def client(settings: Settings) -> Iterator[TestClient]:
    with TestClient(create_app(settings)) as value:
        yield value


@pytest.fixture
def auth() -> dict[str, str]:
    return {"Authorization": f"Bearer {TOKEN}"}


def duckdb_bytes(tmp_path: Path) -> bytes:
    path = tmp_path / "measurements.duckdb"
    connection = duckdb.connect(str(path))
    connection.execute("CREATE TABLE measurements(id INTEGER, area DOUBLE, label VARCHAR)")
    connection.execute("INSERT INTO measurements VALUES (1, 10.5, 'alpha'), (2, 20.5, 'beta')")
    connection.close()
    return path.read_bytes()


def sqlite_bytes(tmp_path: Path) -> bytes:
    path = tmp_path / "measurements.sqlite"
    connection = sqlite3.connect(path)
    connection.execute("CREATE TABLE measurements(id INTEGER, area REAL, label TEXT)")
    connection.executemany(
        "INSERT INTO measurements VALUES (?, ?, ?)",
        [(1, 10.5, "alpha"), (2, 20.5, "beta")],
    )
    connection.commit()
    connection.close()
    return path.read_bytes()


def upload_source(
    client: TestClient,
    auth: dict[str, str],
    *,
    source_ref: str,
    source_format: str,
    filename: str,
    content: bytes,
    expected_sha256: str | None = None,
):
    data = {
        "scope_id": "test-scope",
        "source_ref": source_ref,
        "format": source_format,
        "size": str(len(content)),
    }
    if expected_sha256 is not None:
        data["expected_sha256"] = expected_sha256
    return client.post(
        "/v1/sources",
        headers=auth,
        data=data,
        files={"file": (filename, content, "application/octet-stream")},
    )
