from __future__ import annotations

import importlib
import multiprocessing
import os
import time
from collections.abc import Callable
from multiprocessing.connection import Connection
from multiprocessing.process import BaseProcess
from pathlib import Path
from typing import Any

from .config import Settings
from .engines import execute_duckdb_query, execute_sqlite_query
from .errors import QueryExecutionError, QueryLimitExceeded, QueryTimedOut, WorkerError
from .models import SourceFormat
from .operations import sanitize_execution_environment


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
        sanitize_execution_environment()
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
    monitor: Callable[[], None] | None = None,
    on_exit: Callable[[int | None], None] | None = None,
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
            settings.execution_settings(),
        ),
        daemon=True,
    )
    try:
        process.start()
        child.close()
        deadline = time.monotonic() + settings.query_timeout_seconds
        while not parent.poll(0.1):
            if monitor:
                monitor()
            if time.monotonic() >= deadline:
                raise QueryTimedOut(
                    f"Query exceeded the {settings.query_timeout_seconds} second timeout"
                )
        payload = parent.recv()
        if monitor:
            monitor()
    except EOFError as exc:
        raise QueryExecutionError("The disposable query process exited unexpectedly") from exc
    finally:
        parent.close()
        child.close()
        exit_code = stop_process(process)
        if on_exit:
            on_exit(exit_code)
    if payload.get("ok"):
        return dict(payload["result"])
    if payload.get("code") == "query_limit_exceeded":
        raise QueryLimitExceeded(str(payload.get("message") or "Query limit exceeded"))
    if payload.get("code") == "query_timeout":
        raise QueryTimedOut(str(payload.get("message") or "Query timed out"))
    raise QueryExecutionError(str(payload.get("message") or "Query execution failed"))


def stop_process(process: BaseProcess) -> int | None:
    if process.pid is None:
        return None
    process.join(0.1)
    if process.is_alive():
        process.terminate()
        process.join(2)
    if process.is_alive():
        process.kill()
        process.join(2)
    exit_code = process.exitcode
    process.close()
    return exit_code
