from __future__ import annotations

import multiprocessing
import os
import time
from collections.abc import Callable
from multiprocessing.connection import Connection
from pathlib import Path
from typing import Any

from .config import Settings
from .engines import convert_csv, inspect_source
from .errors import IngestionTimedOut, InvalidSource, WorkerError
from .executor import apply_process_limits, stop_process
from .models import SourceFormat
from .operations import execution_guard, sanitize_execution_environment


def _child_ingest(
    child: Connection,
    raw_path: str,
    source_format: str,
    settings: Settings,
    parent_pid: int,
) -> None:
    guard = None
    try:
        guard = execution_guard(parent_pid, settings.cache_dir)
        sanitize_execution_environment()
        apply_process_limits(settings, settings.ingestion_timeout_seconds)
        raw = Path(raw_path)
        selected = SourceFormat(source_format)
        if selected is SourceFormat.csv:
            data = raw.parent / "data.duckdb"
            convert_csv(raw, data, settings)
        else:
            data = raw
        schema, schema_digest = inspect_source(data, selected, settings)
        child.send(
            {
                "ok": True,
                "data_file": data.name,
                "schema": schema,
                "schema_digest": schema_digest,
            }
        )
    except WorkerError as exc:
        child.send({"ok": False, "message": str(exc)})
    except BaseException as exc:
        child.send({"ok": False, "message": str(exc)})
    finally:
        child.close()
        if guard:
            guard.close()


def ingest_in_subprocess(
    raw_path: Path,
    source_format: SourceFormat,
    settings: Settings,
    monitor: Callable[[], None] | None = None,
    on_exit: Callable[[int | None], None] | None = None,
) -> tuple[Path, dict[str, Any], str]:
    context = multiprocessing.get_context("spawn")
    parent, child = context.Pipe(duplex=False)
    process = context.Process(
        target=_child_ingest,
        args=(
            child,
            str(raw_path),
            source_format.value,
            settings.execution_settings(),
            os.getpid(),
        ),
        daemon=True,
    )
    try:
        process.start()
        child.close()
        deadline = time.monotonic() + settings.ingestion_timeout_seconds
        while not parent.poll(0.1):
            if monitor:
                monitor()
            if time.monotonic() >= deadline:
                raise IngestionTimedOut(
                    f"Source ingestion exceeded the {settings.ingestion_timeout_seconds} "
                    "second timeout"
                )
        payload = parent.recv()
        if monitor:
            monitor()
    except EOFError as exc:
        raise InvalidSource("The disposable ingestion process exited unexpectedly") from exc
    finally:
        parent.close()
        child.close()
        exit_code = stop_process(process)
        if on_exit:
            on_exit(exit_code)
    if not payload.get("ok"):
        raise InvalidSource(str(payload.get("message") or "Source ingestion failed"))
    return (
        raw_path.parent / str(payload["data_file"]),
        dict(payload["schema"]),
        str(payload["schema_digest"]),
    )
