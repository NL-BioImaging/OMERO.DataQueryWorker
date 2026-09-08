from __future__ import annotations

import base64
import csv
import hashlib
import json
import os
import sqlite3
from collections.abc import Iterable
from datetime import date, datetime, time
from decimal import Decimal
from importlib.metadata import version
from pathlib import Path
from typing import Any

import duckdb

from .config import Settings
from .errors import InvalidSource, QueryExecutionError, QueryLimitExceeded, QueryTimedOut
from .models import SourceFormat

DUCKDB_EXTENSION = ".duckdb"
SQLITE_EXTENSIONS = {".sqlite", ".sqlite3"}
CSV_EXTENSION = ".csv"


def engine_versions() -> dict[str, str]:
    return {
        "duckdb": duckdb.__version__,
        "sqlite": sqlite3.sqlite_version,
        "sqlglot": version("sqlglot"),
    }


class BoundedCSVOutput:
    def __init__(self, handle: Any, maximum: int):
        self.handle = handle
        self.maximum = maximum
        self.size = 0
        self.digest = hashlib.sha256()

    def write(self, text: str) -> int:
        content = text.encode("utf-8")
        if self.size + len(content) > self.maximum:
            raise QueryLimitExceeded(f"Query exceeds the {self.maximum} byte limit")
        self.handle.write(content)
        self.digest.update(content)
        self.size += len(content)
        return len(text)


def safe_filename(filename: str) -> str:
    name = Path(filename.replace("\\", "/")).name.strip()
    if not name or name in {".", ".."} or "\x00" in name:
        raise InvalidSource("A valid source filename is required")
    return name[:255]


def validate_filename(filename: str, source_format: SourceFormat) -> str:
    name = safe_filename(filename)
    suffix = Path(name).suffix.lower()
    valid = (
        (source_format is SourceFormat.duckdb and suffix == DUCKDB_EXTENSION)
        or (source_format is SourceFormat.sqlite and suffix in SQLITE_EXTENSIONS)
        or (source_format is SourceFormat.csv and suffix == CSV_EXTENSION)
    )
    if not valid:
        raise InvalidSource(
            f"Filename extension does not match declared {source_format.value} format"
        )
    return name


def configure_duckdb(connection: duckdb.DuckDBPyConnection, settings: Settings) -> None:
    connection.execute("SET autoinstall_known_extensions = false")
    connection.execute("SET autoload_known_extensions = false")
    connection.execute("SET allow_unsigned_extensions = false")
    connection.execute("SET allow_community_extensions = false")
    connection.execute(f"SET threads = {int(settings.duckdb_threads)}")
    memory_limit = settings.duckdb_memory_limit.replace("'", "")
    connection.execute(f"SET memory_limit = '{memory_limit}'")
    connection.execute("SET enable_external_access = false")
    connection.execute("SET lock_configuration = true")


def _duckdb_schema(path: Path, settings: Settings) -> dict[str, Any]:
    try:
        connection = duckdb.connect(str(path), read_only=True)
        configure_duckdb(connection, settings)
        tables = connection.execute(
            """
            SELECT table_schema, table_name, table_type
            FROM information_schema.tables
            WHERE table_schema NOT IN ('information_schema', 'pg_catalog')
            ORDER BY table_schema, table_name
            """
        ).fetchall()
        columns = connection.execute(
            """
            SELECT table_schema, table_name, column_name, data_type, is_nullable,
                   ordinal_position
            FROM information_schema.columns
            WHERE table_schema NOT IN ('information_schema', 'pg_catalog')
            ORDER BY table_schema, table_name, ordinal_position
            """
        ).fetchall()
    except Exception as exc:
        raise InvalidSource(f"DuckDB source could not be opened safely: {exc}") from exc
    finally:
        if "connection" in locals():
            connection.close()
    grouped: dict[tuple[str, str], list[dict[str, Any]]] = {}
    for schema_name, table_name, column_name, data_type, nullable, _ in columns:
        grouped.setdefault((str(schema_name), str(table_name)), []).append(
            {
                "name": str(column_name),
                "type": str(data_type),
                "nullable": str(nullable).upper() == "YES",
            }
        )
    normalized = []
    for schema_name, table_name, table_type in tables:
        display = str(table_name) if schema_name == "main" else f"{schema_name}.{table_name}"
        normalized.append(
            {
                "name": display,
                "kind": "view" if "VIEW" in str(table_type).upper() else "table",
                "columns": grouped.get((str(schema_name), str(table_name)), []),
            }
        )
    return {"tables": normalized}


def _sqlite_uri(path: Path) -> str:
    return f"{path.resolve().as_uri()}?mode=ro&immutable=1"


def _sqlite_schema(path: Path) -> dict[str, Any]:
    try:
        connection = sqlite3.connect(_sqlite_uri(path), uri=True, timeout=1)
        connection.execute("PRAGMA query_only = ON")
        tables = connection.execute(
            """
            SELECT name, type FROM sqlite_master
            WHERE type IN ('table', 'view') AND name NOT LIKE 'sqlite_%'
            ORDER BY name
            """
        ).fetchall()
        normalized = []
        for table_name, table_type in tables:
            escaped = str(table_name).replace('"', '""')
            columns = connection.execute(f'PRAGMA table_info("{escaped}")').fetchall()
            normalized.append(
                {
                    "name": str(table_name),
                    "kind": "view" if table_type == "view" else "table",
                    "columns": [
                        {
                            "name": str(column[1]),
                            "type": str(column[2] or "UNKNOWN"),
                            "nullable": not bool(column[3]),
                        }
                        for column in columns
                    ],
                }
            )
    except sqlite3.Error as exc:
        raise InvalidSource(f"SQLite source could not be opened safely: {exc}") from exc
    finally:
        if "connection" in locals():
            connection.close()
    return {"tables": normalized}


def convert_csv(raw_path: Path, converted_path: Path, settings: Settings) -> None:
    try:
        connection = duckdb.connect(str(converted_path))
        connection.execute("SET autoinstall_known_extensions = false")
        connection.execute("SET autoload_known_extensions = false")
        connection.execute("SET allow_unsigned_extensions = false")
        connection.execute("SET allow_community_extensions = false")
        connection.execute(f"SET threads = {int(settings.duckdb_threads)}")
        memory_limit = settings.duckdb_memory_limit.replace("'", "")
        connection.execute(f"SET memory_limit = '{memory_limit}'")
        escaped_path = str(raw_path.resolve()).replace("'", "''")
        connection.execute(
            f"CREATE TABLE data AS SELECT * FROM read_csv('{escaped_path}', sample_size=-1)"
        )
        connection.execute("CHECKPOINT")
    except Exception as exc:
        if converted_path.exists():
            converted_path.unlink()
        raise InvalidSource(f"CSV source could not be parsed: {exc}") from exc
    finally:
        if "connection" in locals():
            connection.close()


def inspect_source(
    path: Path,
    source_format: SourceFormat,
    settings: Settings,
) -> tuple[dict[str, Any], str]:
    if source_format is SourceFormat.sqlite:
        schema = _sqlite_schema(path)
    else:
        schema = _duckdb_schema(path, settings)
    canonical = json.dumps(schema, sort_keys=True, separators=(",", ":"))
    return schema, hashlib.sha256(canonical.encode()).hexdigest()


def sqlite_query_authorizer(
    action: int,
    _arg1: str | None,
    _arg2: str | None,
    _database: str | None,
    _trigger: str | None,
) -> int:
    denied = {
        sqlite3.SQLITE_ALTER_TABLE,
        sqlite3.SQLITE_ANALYZE,
        sqlite3.SQLITE_ATTACH,
        sqlite3.SQLITE_CREATE_INDEX,
        sqlite3.SQLITE_CREATE_TABLE,
        sqlite3.SQLITE_CREATE_TEMP_INDEX,
        sqlite3.SQLITE_CREATE_TEMP_TABLE,
        sqlite3.SQLITE_CREATE_TEMP_TRIGGER,
        sqlite3.SQLITE_CREATE_TEMP_VIEW,
        sqlite3.SQLITE_CREATE_TRIGGER,
        sqlite3.SQLITE_CREATE_VIEW,
        sqlite3.SQLITE_DELETE,
        sqlite3.SQLITE_DETACH,
        sqlite3.SQLITE_DROP_INDEX,
        sqlite3.SQLITE_DROP_TABLE,
        sqlite3.SQLITE_DROP_TEMP_INDEX,
        sqlite3.SQLITE_DROP_TEMP_TABLE,
        sqlite3.SQLITE_DROP_TEMP_TRIGGER,
        sqlite3.SQLITE_DROP_TEMP_VIEW,
        sqlite3.SQLITE_DROP_TRIGGER,
        sqlite3.SQLITE_DROP_VIEW,
        sqlite3.SQLITE_INSERT,
        sqlite3.SQLITE_PRAGMA,
        sqlite3.SQLITE_REINDEX,
        sqlite3.SQLITE_TRANSACTION,
        sqlite3.SQLITE_UPDATE,
    }
    return sqlite3.SQLITE_DENY if action in denied else sqlite3.SQLITE_OK


def _json_value(value: Any) -> Any:
    if value is None or isinstance(value, (bool, int, float, str)):
        return value
    if isinstance(value, (date, time, datetime)):
        return value.isoformat()
    if isinstance(value, Decimal):
        return str(value)
    if isinstance(value, (bytes, bytearray, memoryview)):
        return {"base64": base64.b64encode(bytes(value)).decode("ascii")}
    if isinstance(value, (list, tuple)):
        return [_json_value(item) for item in value]
    if isinstance(value, dict):
        return {str(key): _json_value(item) for key, item in value.items()}
    return str(value)


def _csv_value(value: Any) -> Any:
    normalized = _json_value(value)
    if isinstance(normalized, (dict, list)):
        return json.dumps(normalized, sort_keys=True, separators=(",", ":"))
    return normalized


def _sqlite_runtime_type(value: Any) -> str:
    if value is None:
        return "UNKNOWN"
    if isinstance(value, bool):
        return "BOOLEAN"
    if isinstance(value, int):
        return "INTEGER"
    if isinstance(value, float):
        return "REAL"
    if isinstance(value, (bytes, bytearray, memoryview)):
        return "BLOB"
    return "TEXT"


def _write_result(
    cursor: Any,
    output_path: Path,
    max_rows: int,
    max_bytes: int,
    sqlite_types: bool,
) -> dict[str, Any]:
    description = cursor.description or []
    columns = [
        {
            "name": str(item[0]),
            "type": "UNKNOWN" if sqlite_types else str(item[1]),
        }
        for item in description
    ]
    preview: list[list[Any]] = []
    row_count = 0
    with output_path.open("wb") as text_handle:
        bounded = BoundedCSVOutput(text_handle, max_bytes)
        writer = csv.writer(bounded, lineterminator="\n")
        writer.writerow([column["name"] for column in columns])
        while True:
            rows: Iterable[tuple[Any, ...]] = cursor.fetchmany(1000)
            rows = list(rows)
            if not rows:
                break
            for row in rows:
                row_count += 1
                if row_count > max_rows:
                    raise QueryLimitExceeded(f"Query exceeds the {max_rows} row limit")
                if sqlite_types:
                    for index, value in enumerate(row):
                        if columns[index]["type"] == "UNKNOWN" and value is not None:
                            columns[index]["type"] = _sqlite_runtime_type(value)
                if len(preview) < 100:
                    preview.append([_json_value(value) for value in row])
                writer.writerow([_csv_value(value) for value in row])
            text_handle.flush()
        text_handle.flush()
        os.fsync(text_handle.fileno())
        byte_count = os.fstat(text_handle.fileno()).st_size
    return {
        "columns": columns,
        "row_count": row_count,
        "byte_count": byte_count,
        "preview": preview,
        "result_sha256": bounded.digest.hexdigest(),
    }


def execute_duckdb_query(
    source_path: Path,
    sql: str,
    parameters: dict[str, Any],
    output_path: Path,
    settings: Settings,
) -> dict[str, Any]:
    try:
        connection = duckdb.connect(str(source_path), read_only=True)
        configure_duckdb(connection, settings)
        cursor = connection.execute(sql, parameters)
        return _write_result(
            cursor,
            output_path,
            settings.max_result_rows,
            settings.max_result_bytes,
            sqlite_types=False,
        )
    except QueryLimitExceeded:
        raise
    except Exception as exc:
        raise QueryExecutionError(f"DuckDB rejected the query: {exc}") from exc
    finally:
        if "connection" in locals():
            connection.close()


def execute_sqlite_query(
    source_path: Path,
    sql: str,
    parameters: dict[str, Any],
    output_path: Path,
    settings: Settings,
) -> dict[str, Any]:
    import time as time_module

    deadline = time_module.monotonic() + settings.query_timeout_seconds

    def progress() -> int:
        return 1 if time_module.monotonic() >= deadline else 0

    try:
        connection = sqlite3.connect(_sqlite_uri(source_path), uri=True, timeout=1)
        connection.execute("PRAGMA query_only = ON")
        connection.set_authorizer(sqlite_query_authorizer)
        connection.set_progress_handler(progress, 1000)
        cursor = connection.execute(sql, parameters)
        return _write_result(
            cursor,
            output_path,
            settings.max_result_rows,
            settings.max_result_bytes,
            sqlite_types=True,
        )
    except QueryLimitExceeded:
        raise
    except sqlite3.Error as exc:
        if "interrupted" in str(exc).lower():
            raise QueryTimedOut(
                f"Query exceeded the {settings.query_timeout_seconds} second timeout"
            ) from exc
        raise QueryExecutionError(f"SQLite rejected the query: {exc}") from exc
    finally:
        if "connection" in locals():
            connection.close()
