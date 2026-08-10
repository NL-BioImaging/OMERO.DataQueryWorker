from __future__ import annotations

import argparse
import hashlib
import json
import sqlite3
import tempfile
import urllib.error
import urllib.request
import uuid
from pathlib import Path
from typing import Any

import duckdb

BASE_URL = "http://127.0.0.1:8080"
TOKEN = "smoke-test-token-at-least-16-characters"
STATE_PATH = Path("/var/lib/omero-data-query-worker/smoke-state.json")


def request(
    method: str,
    path: str,
    *,
    body: bytes | None = None,
    content_type: str | None = "application/json",
    expected: int = 200,
) -> tuple[dict[str, Any] | None, bytes]:
    headers = {"Authorization": f"Bearer {TOKEN}"}
    if content_type:
        headers["Content-Type"] = content_type
    incoming = urllib.request.Request(
        f"{BASE_URL}{path}",
        data=body,
        headers=headers,
        method=method,
    )
    try:
        with urllib.request.urlopen(incoming, timeout=60) as response:
            status = response.status
            raw = response.read()
            response_type = response.headers.get_content_type()
    except urllib.error.HTTPError as exc:
        status = exc.code
        raw = exc.read()
        response_type = exc.headers.get_content_type()
    if status != expected:
        raise AssertionError(f"{method} {path}: expected {expected}, got {status}: {raw!r}")
    value = json.loads(raw) if response_type == "application/json" else None
    return value, raw


def json_request(
    method: str,
    path: str,
    payload: dict[str, Any],
    expected: int = 200,
) -> dict[str, Any]:
    value, _ = request(
        method,
        path,
        body=json.dumps(payload).encode(),
        expected=expected,
    )
    assert isinstance(value, dict)
    return value


def multipart(fields: dict[str, str], filename: str, content: bytes) -> tuple[bytes, str]:
    boundary = f"----data-query-smoke-{uuid.uuid4().hex}"
    pieces: list[bytes] = []
    for name, value in fields.items():
        pieces.extend(
            [
                f"--{boundary}\r\n".encode(),
                f'Content-Disposition: form-data; name="{name}"\r\n\r\n'.encode(),
                value.encode(),
                b"\r\n",
            ]
        )
    pieces.extend(
        [
            f"--{boundary}\r\n".encode(),
            (f'Content-Disposition: form-data; name="file"; filename="{filename}"\r\n').encode(),
            b"Content-Type: application/octet-stream\r\n\r\n",
            content,
            b"\r\n",
            f"--{boundary}--\r\n".encode(),
        ]
    )
    return b"".join(pieces), f"multipart/form-data; boundary={boundary}"


def upload(source_ref: str, source_format: str, path: Path) -> dict[str, Any]:
    content = path.read_bytes()
    body, content_type = multipart(
        {
            "scope_id": "smoke-scope",
            "source_ref": source_ref,
            "format": source_format,
            "size": str(len(content)),
            "expected_sha256": hashlib.sha256(content).hexdigest(),
        },
        path.name,
        content,
    )
    value, _ = request(
        "POST",
        "/v1/sources",
        body=body,
        content_type=content_type,
        expected=201,
    )
    assert isinstance(value, dict)
    return value


def create_fixtures(directory: Path) -> list[tuple[str, str, Path, str]]:
    duckdb_path = directory / "measurements.duckdb"
    connection = duckdb.connect(str(duckdb_path))
    connection.execute("CREATE TABLE measurements(id INTEGER, area DOUBLE, label VARCHAR)")
    connection.execute("INSERT INTO measurements VALUES (1, 10.5, 'alpha'), (2, 20.5, 'beta')")
    connection.close()

    sqlite_path = directory / "measurements.sqlite"
    sqlite_connection = sqlite3.connect(sqlite_path)
    sqlite_connection.execute("CREATE TABLE measurements(id INTEGER, area REAL, label TEXT)")
    sqlite_connection.executemany(
        "INSERT INTO measurements VALUES (?, ?, ?)",
        [(1, 10.5, "alpha"), (2, 20.5, "beta")],
    )
    sqlite_connection.commit()
    sqlite_connection.close()

    csv_path = directory / "measurements.csv"
    csv_path.write_text(
        "id,area,label\n1,10.5,alpha\n2,20.5,beta\n",
        encoding="utf-8",
        newline="\n",
    )
    return [
        ("smoke-duckdb", "duckdb", duckdb_path, "measurements"),
        ("smoke-sqlite", "sqlite", sqlite_path, "measurements"),
        ("smoke-csv", "csv", csv_path, "data"),
    ]


def initial() -> None:
    capabilities, _ = request("GET", "/v1/capabilities", content_type=None)
    assert capabilities and capabilities["formats"] == ["duckdb", "sqlite", "csv"]
    with tempfile.TemporaryDirectory(prefix="data-query-smoke-") as temporary:
        uploaded: dict[str, dict[str, Any]] = {}
        for source_ref, source_format, path, table in create_fixtures(Path(temporary)):
            source = upload(source_ref, source_format, path)
            uploaded[source_ref] = source
            schema, _ = request(
                "GET", f"/v1/sources/{source['source_id']}/schema", content_type=None
            )
            assert schema and schema["tables"][0]["name"] == table
            result = json_request(
                "POST",
                f"/v1/sources/{source['source_id']}/query",
                {"sql": f"SELECT count(*) AS count FROM {table}", "parameters": {}},
            )
            assert result["preview"] == [[2]]

        source = uploaded["smoke-duckdb"]
        query = {
            "sql": "SELECT id, area FROM measurements ORDER BY id",
            "parameters": {},
        }
        first = json_request("POST", f"/v1/sources/{source['source_id']}/query", query)
        second = json_request("POST", f"/v1/sources/{source['source_id']}/query", query)
        assert second["cache_status"] == "hit"
        assert first["result_id"] == second["result_id"]
        _, downloaded = request(
            "GET", f"/v1/results/{first['result_id']}/download", content_type=None
        )
        assert downloaded == b"id,area\n1,10.5\n2,20.5\n"
        volatile = json_request(
            "POST",
            f"/v1/sources/{source['source_id']}/query",
            {"sql": "SELECT random()", "parameters": {}},
        )
        assert volatile["cache_status"] == "bypass"
        json_request(
            "POST",
            f"/v1/sources/{source['source_id']}/query",
            {"sql": "COPY measurements TO '/tmp/stolen.csv'", "parameters": {}},
            expected=422,
        )
        STATE_PATH.write_text(json.dumps({"source": source, "query": query}), encoding="utf-8")


def restart() -> None:
    state = json.loads(STATE_PATH.read_text(encoding="utf-8"))
    source = state["source"]
    resolved = json_request(
        "POST",
        "/v1/sources/resolve",
        {
            "scope_id": "smoke-scope",
            "source_ref": "smoke-duckdb",
            "format": "duckdb",
            "size": source["size"],
            "expected_sha256": source["sha256"],
        },
    )
    assert resolved["cached"] is True
    result = json_request("POST", f"/v1/sources/{source['source_id']}/query", state["query"])
    assert result["cache_status"] == "hit"


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("phase", choices=["initial", "restart"])
    args = parser.parse_args()
    if args.phase == "initial":
        initial()
    else:
        restart()


if __name__ == "__main__":
    main()
