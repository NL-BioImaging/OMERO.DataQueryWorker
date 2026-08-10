from __future__ import annotations

import multiprocessing
import os
from multiprocessing.connection import Connection
from pathlib import Path
from typing import Any

from .config import Settings
from .engines import convert_csv, inspect_source
from .errors import IngestionTimedOut, InvalidSource, WorkerError
from .executor import apply_process_limits
from .models import SourceFormat


def _child_ingest(
    child: Connection,
    raw_path: str,
    source_format: str,
    settings: Settings,
) -> None:
    try:
        os.environ.pop("DQW_API_TOKEN", None)
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


def ingest_in_subprocess(
    raw_path: Path,
    source_format: SourceFormat,
    settings: Settings,
) -> tuple[Path, dict[str, Any], str]:
    context = multiprocessing.get_context("spawn")
    parent, child = context.Pipe(duplex=False)
    process = context.Process(
        target=_child_ingest,
        args=(child, str(raw_path), source_format.value, settings),
        daemon=True,
    )
    process.start()
    child.close()
    try:
        if not parent.poll(settings.ingestion_timeout_seconds):
            process.terminate()
            process.join(2)
            if process.is_alive():
                process.kill()
                process.join(2)
            raise IngestionTimedOut(
                f"Source ingestion exceeded the {settings.ingestion_timeout_seconds} second timeout"
            )
        payload = parent.recv()
    except EOFError as exc:
        raise InvalidSource("The disposable ingestion process exited unexpectedly") from exc
    finally:
        parent.close()
        process.join(2)
        if process.is_alive():
            process.terminate()
            process.join(2)
    if not payload.get("ok"):
        raise InvalidSource(str(payload.get("message") or "Source ingestion failed"))
    return (
        raw_path.parent / str(payload["data_file"]),
        dict(payload["schema"]),
        str(payload["schema_digest"]),
    )
