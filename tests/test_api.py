from __future__ import annotations

import hashlib
from dataclasses import replace
from pathlib import Path

from fastapi.testclient import TestClient

from omero_data_query_worker.app import create_app

from .conftest import TOKEN, duckdb_bytes, sqlite_bytes, upload_source


def test_health_is_public_but_v1_requires_auth(client: TestClient) -> None:
    assert client.get("/health/live").json() == {"status": "live"}
    assert client.get("/health/ready").json() == {"status": "ready"}
    response = client.get("/v1/capabilities")
    assert response.status_code == 401
    assert response.json()["code"] == "authentication_required"


def test_capabilities(client: TestClient, auth: dict[str, str]) -> None:
    response = client.get("/v1/capabilities", headers=auth)
    assert response.status_code == 200
    assert response.json()["formats"] == ["duckdb", "sqlite", "csv"]
    assert response.json()["csv_table"] == "data"


def test_invalid_upload_metadata_is_a_client_error(client, auth):
    response = upload_source(
        client,
        auth,
        source_ref="invalid:reference",
        source_format="csv",
        filename="source.csv",
        content=b"id\n1\n",
    )
    assert response.status_code == 422
    assert response.json()["code"] == "invalid_source"
    assert client.get("/health/ready").status_code == 200


def test_duckdb_ingest_schema_query_cache_and_download(
    client: TestClient,
    auth: dict[str, str],
    tmp_path: Path,
) -> None:
    content = duckdb_bytes(tmp_path)
    digest = hashlib.sha256(content).hexdigest()
    uploaded = upload_source(
        client,
        auth,
        source_ref="duckdb-source",
        source_format="duckdb",
        filename="measurements.duckdb",
        content=content,
        expected_sha256=digest,
    )
    assert uploaded.status_code == 201, uploaded.text
    source = uploaded.json()
    assert source["cache_status"] == "created"

    resolved = client.post(
        "/v1/sources/resolve",
        headers=auth,
        json={
            "scope_id": "test-scope",
            "source_ref": "duckdb-source",
            "format": "duckdb",
            "size": len(content),
            "expected_sha256": digest,
        },
    )
    assert resolved.json()["cached"] is True

    schema = client.get(f"/v1/sources/{source['source_id']}/schema", headers=auth)
    assert schema.status_code == 200
    assert schema.json()["tables"][0]["name"] == "measurements"

    payload = {
        "sql": "SELECT id, area FROM measurements WHERE area >= $minimum ORDER BY id",
        "parameters": {"minimum": {"type": "float", "value": 10.0}},
    }
    first = client.post(f"/v1/sources/{source['source_id']}/query", headers=auth, json=payload)
    assert first.status_code == 200, first.text
    assert first.json()["cache_status"] == "miss"
    assert first.json()["row_count"] == 2
    second = client.post(f"/v1/sources/{source['source_id']}/query", headers=auth, json=payload)
    assert second.json()["cache_status"] == "hit"
    assert second.json()["result_id"] == first.json()["result_id"]

    download = client.get(f"/v1/results/{first.json()['result_id']}/download", headers=auth)
    assert download.status_code == 200
    assert download.text == "id,area\n1,10.5\n2,20.5\n"


def test_sqlite_and_csv_sources(
    client: TestClient,
    auth: dict[str, str],
    tmp_path: Path,
) -> None:
    sources = [
        ("sqlite-source", "sqlite", "measurements.sqlite", sqlite_bytes(tmp_path), "measurements"),
        (
            "csv-source",
            "csv",
            "measurements.csv",
            b"id,area,label\n1,10.5,alpha\n2,20.5,beta\n",
            "data",
        ),
    ]
    for source_ref, source_format, filename, content, table in sources:
        uploaded = upload_source(
            client,
            auth,
            source_ref=source_ref,
            source_format=source_format,
            filename=filename,
            content=content,
        )
        assert uploaded.status_code == 201, uploaded.text
        source_id = uploaded.json()["source_id"]
        schema = client.get(f"/v1/sources/{source_id}/schema", headers=auth).json()
        assert schema["tables"][0]["name"] == table
        result = client.post(
            f"/v1/sources/{source_id}/query",
            headers=auth,
            json={"sql": f"SELECT count(*) AS count FROM {table}", "parameters": {}},
        )
        assert result.status_code == 200, result.text
        assert result.json()["preview"] == [[2]]


def test_volatile_query_bypasses_cache(
    client: TestClient,
    auth: dict[str, str],
    tmp_path: Path,
) -> None:
    uploaded = upload_source(
        client,
        auth,
        source_ref="volatile-source",
        source_format="duckdb",
        filename="measurements.duckdb",
        content=duckdb_bytes(tmp_path),
    )
    source_id = uploaded.json()["source_id"]
    ids = []
    for _ in range(2):
        response = client.post(
            f"/v1/sources/{source_id}/query",
            headers=auth,
            json={"sql": "SELECT random() AS value", "parameters": {}},
        )
        assert response.json()["cache_status"] == "bypass"
        ids.append(response.json()["result_id"])
    assert ids[0] != ids[1]


def test_rejects_unsafe_query_and_bad_parameter(
    client: TestClient,
    auth: dict[str, str],
) -> None:
    content = b"id,value\n1,x\n"
    source_id = upload_source(
        client,
        auth,
        source_ref="security-source",
        source_format="csv",
        filename="data.csv",
        content=content,
    ).json()["source_id"]
    unsafe = client.post(
        f"/v1/sources/{source_id}/query",
        headers=auth,
        json={"sql": "COPY data TO '/tmp/stolen.csv'", "parameters": {}},
    )
    assert unsafe.status_code == 422
    invalid = client.post(
        f"/v1/sources/{source_id}/query",
        headers=auth,
        json={
            "sql": "SELECT * FROM data WHERE id = $value",
            "parameters": {"value": {"type": "integer", "value": "not-an-integer"}},
        },
    )
    assert invalid.status_code == 422


def test_rejects_corrupt_hash_size_and_reference_conflict(
    client: TestClient,
    auth: dict[str, str],
) -> None:
    wrong_hash = upload_source(
        client,
        auth,
        source_ref="bad-hash",
        source_format="csv",
        filename="data.csv",
        content=b"x\n1\n",
        expected_sha256="0" * 64,
    )
    assert wrong_hash.status_code == 422

    corrupt = upload_source(
        client,
        auth,
        source_ref="corrupt-db",
        source_format="duckdb",
        filename="data.duckdb",
        content=b"not a database",
    )
    assert corrupt.status_code == 422

    original = upload_source(
        client,
        auth,
        source_ref="immutable-ref",
        source_format="csv",
        filename="data.csv",
        content=b"x\n1\n",
    )
    assert original.status_code == 201
    conflict = upload_source(
        client,
        auth,
        source_ref="immutable-ref",
        source_format="csv",
        filename="data.csv",
        content=b"x\n123\n",
    )
    assert conflict.status_code == 409


def test_cache_status_contains_no_data_values(
    client: TestClient,
    auth: dict[str, str],
) -> None:
    status = client.get("/v1/cache/status", headers=auth)
    assert status.status_code == 200
    assert set(status.json()) == {
        "source_count",
        "source_bytes",
        "result_count",
        "result_bytes",
        "hits",
        "misses",
        "bypasses",
        "evictions",
        "active_queries",
    }
    assert client.get("/v1/sources/not-a-source/schema", headers=auth).status_code == 404
    assert client.get("/v1/results/not-a-result/download", headers=auth).status_code == 404


def test_sqlite_typed_parameters(
    client: TestClient,
    auth: dict[str, str],
    tmp_path: Path,
) -> None:
    source_id = upload_source(
        client,
        auth,
        source_ref="sqlite-parameters",
        source_format="sqlite",
        filename="measurements.sqlite",
        content=sqlite_bytes(tmp_path),
    ).json()["source_id"]
    response = client.post(
        f"/v1/sources/{source_id}/query",
        headers=auth,
        json={
            "sql": "SELECT label FROM measurements WHERE id = $identifier",
            "parameters": {"identifier": {"type": "integer", "value": 2}},
        },
    )
    assert response.status_code == 200, response.text
    assert response.json()["preview"] == [["beta"]]


def test_result_row_limit_returns_413(settings, auth: dict[str, str]) -> None:
    limited = replace(settings, max_result_rows=1)
    with TestClient(create_app(limited)) as local_client:
        source_id = upload_source(
            local_client,
            auth,
            source_ref="limited-results",
            source_format="csv",
            filename="data.csv",
            content=b"id\n1\n2\n",
        ).json()["source_id"]
        response = local_client.post(
            f"/v1/sources/{source_id}/query",
            headers=auth,
            json={"sql": "SELECT * FROM data", "parameters": {}},
        )
        assert response.status_code == 413
        assert response.json()["code"] == "query_limit_exceeded"


def test_query_timeout_returns_504(settings, tmp_path: Path) -> None:
    timed = replace(settings, cache_dir=tmp_path / "timeout-cache", query_timeout_seconds=1)
    headers = {"Authorization": f"Bearer {TOKEN}"}
    with TestClient(create_app(timed)) as local_client:
        source_id = upload_source(
            local_client,
            headers,
            source_ref="timeout-source",
            source_format="csv",
            filename="data.csv",
            content=b"id\n1\n",
        ).json()["source_id"]
        response = local_client.post(
            f"/v1/sources/{source_id}/query",
            headers=headers,
            json={
                "sql": (
                    "WITH RECURSIVE counts(value) AS ("
                    "SELECT 1 UNION ALL SELECT value + 1 FROM counts"
                    ") SELECT sum(value) FROM counts"
                ),
                "parameters": {},
            },
        )
        assert response.status_code == 504
        assert response.json()["code"] == "query_timeout"
