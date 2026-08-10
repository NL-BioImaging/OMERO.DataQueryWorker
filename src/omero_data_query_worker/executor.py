from __future__ import annotations

import importlib
import multiprocessing
import os
import time
from multiprocessing.connection import Connection
from pathlib import Path
from typing import Any

from .config import Settings
from .engines import execute_duckdb_query, execute_sqlite_query
from .errors import QueryExecutionError, QueryLimitExceeded, QueryTimedOut, WorkerError
from .models import SourceFormat


def apply_process_limits(settings: Settings, timeout_seconds: int) -> None:
    if os.name != "posix":
        return
    resource: Any = importlib.import_module("resource")

    cpu_limit = max(1, timeout_seconds + 2)
    resource.setrlimit(resource.RLIMIT_CPU, (cpu_limit, cpu_limit))
    resource.setrlimit(resource.RLIMIT_CORE, (0, 0))
    resource.setrlimit(resource.RLIMIT_NOFILE, (64, 64))


def _child_execute(
    child: Connection,
    source_path: str,
    source_format: str,
    sql: str,
    parameters: dict[str, Any],
    output_path: str,
    settings: Settings,
) -> None:
    try:
        os.environ.pop("DQW_API_TOKEN", None)
        apply_process_limits(settings, settings.query_timeout_seconds)
        started = time.monotonic()
        if SourceFormat(source_format) is SourceFormat.sqlite:
            result = execute_sqlite_query(
                Path(source_path), sql, parameters, Path(output_path), settings
            )
        else:
            result = execute_duckdb_query(
                Path(source_path), sql, parameters, Path(output_path), settings
            )
        result["duration_ms"] = int((time.monotonic() - started) * 1000)
        child.send({"ok": True, "result": result})
    except WorkerError as exc:
        child.send({"ok": False, "code": exc.code, "message": str(exc)})
    except BaseException as exc:
        child.send({"ok": False, "code": "query_execution_failed", "message": str(exc)})
    finally:
        child.close()


def execute_in_subprocess(
    source_path: Path,
    source_format: SourceFormat,
    sql: str,
    parameters: dict[str, Any],
    output_path: Path,
    settings: Settings,
) -> dict[str, Any]:
    context = multiprocessing.get_context("spawn")
    parent, child = context.Pipe(duplex=False)
    process = context.Process(
        target=_child_execute,
        args=(
            child,
            str(source_path),
            source_format.value,
            sql,
            parameters,
            str(output_path),
            settings,
        ),
        daemon=True,
    )
    process.start()
    child.close()
    try:
        if not parent.poll(settings.query_timeout_seconds):
            process.terminate()
            process.join(2)
            if process.is_alive():
                process.kill()
                process.join(2)
            raise QueryTimedOut(
                f"Query exceeded the {settings.query_timeout_seconds} second timeout"
            )
        payload = parent.recv()
    except EOFError as exc:
        raise QueryExecutionError("The disposable query process exited unexpectedly") from exc
    finally:
        parent.close()
        process.join(2)
        if process.is_alive():
            process.terminate()
            process.join(2)
    if payload.get("ok"):
        return dict(payload["result"])
    if payload.get("code") == "query_limit_exceeded":
        raise QueryLimitExceeded(str(payload.get("message") or "Query limit exceeded"))
    if payload.get("code") == "query_timeout":
        raise QueryTimedOut(str(payload.get("message") or "Query timed out"))
    raise QueryExecutionError(str(payload.get("message") or "Query execution failed"))
