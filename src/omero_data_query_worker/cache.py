from __future__ import annotations

import hashlib
import json
import os
import re
import shutil
import threading
import time
import uuid
from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

from .config import Settings
from .errors import (
    CacheCapacityExceeded,
    InvalidQuery,
    QueryCapacityExceeded,
    ResultNotFound,
    SourceNotFound,
)
from .models import SourceFormat

SOURCE_ID_PATTERN = re.compile(r"^src_[a-f0-9]{64}$")
RESULT_ID_PATTERN = re.compile(r"^res_(?:[a-f0-9]{32}|[a-f0-9]{64})$")


def utc_now() -> datetime:
    return datetime.now(UTC)


def iso(value: datetime) -> str:
    return value.isoformat().replace("+00:00", "Z")


def parse_iso(value: str) -> datetime:
    return datetime.fromisoformat(value.replace("Z", "+00:00"))


def directory_bytes(path: Path) -> int:
    return sum(item.stat().st_size for item in path.rglob("*") if item.is_file())


def _write_json_atomic(path: Path, value: dict[str, Any]) -> None:
    temporary = path.with_name(f".{path.name}.{uuid.uuid4().hex}.tmp")
    with temporary.open("w", encoding="utf-8", newline="\n") as handle:
        json.dump(value, handle, sort_keys=True, separators=(",", ":"))
        handle.flush()
        os.fsync(handle.fileno())
    os.replace(temporary, path)


@dataclass(slots=True)
class SourceRecord:
    source_id: str
    scope_id: str
    source_ref: str
    format: SourceFormat
    filename: str
    size: int
    sha256: str
    schema_digest: str
    schema: dict[str, Any]
    data_file: str
    created_at: str
    accessed_at: str
    expires_at: str

    @classmethod
    def from_dict(cls, value: dict[str, Any]) -> SourceRecord:
        return cls(
            source_id=str(value["source_id"]),
            scope_id=str(value["scope_id"]),
            source_ref=str(value["source_ref"]),
            format=SourceFormat(value["format"]),
            filename=str(value["filename"]),
            size=int(value["size"]),
            sha256=str(value["sha256"]),
            schema_digest=str(value["schema_digest"]),
            schema=dict(value["schema"]),
            data_file=str(value["data_file"]),
            created_at=str(value["created_at"]),
            accessed_at=str(value["accessed_at"]),
            expires_at=str(value["expires_at"]),
        )

    def as_dict(self) -> dict[str, Any]:
        return {
            "source_id": self.source_id,
            "scope_id": self.scope_id,
            "source_ref": self.source_ref,
            "format": self.format.value,
            "filename": self.filename,
            "size": self.size,
            "sha256": self.sha256,
            "schema_digest": self.schema_digest,
            "schema": self.schema,
            "data_file": self.data_file,
            "created_at": self.created_at,
            "accessed_at": self.accessed_at,
            "expires_at": self.expires_at,
        }


@dataclass(slots=True)
class ResultRecord:
    result_id: str
    query_key: str | None
    source_id: str
    source_sha256: str
    sql_sha256: str
    columns: list[dict[str, str]]
    row_count: int
    byte_count: int
    preview: list[list[Any]]
    duration_ms: int
    result_file: str
    created_at: str
    accessed_at: str
    expires_at: str
    execution: dict[str, Any] | None = None

    @classmethod
    def from_dict(cls, value: dict[str, Any]) -> ResultRecord:
        return cls(
            result_id=str(value["result_id"]),
            query_key=value.get("query_key"),
            source_id=str(value["source_id"]),
            source_sha256=str(value["source_sha256"]),
            sql_sha256=str(value["sql_sha256"]),
            columns=[dict(item) for item in value["columns"]],
            row_count=int(value["row_count"]),
            byte_count=int(value["byte_count"]),
            preview=[list(row) for row in value["preview"]],
            duration_ms=int(value["duration_ms"]),
            result_file=str(value["result_file"]),
            created_at=str(value["created_at"]),
            accessed_at=str(value["accessed_at"]),
            expires_at=str(value["expires_at"]),
            execution=value.get("execution"),
        )

    def as_dict(self) -> dict[str, Any]:
        return {
            "result_id": self.result_id,
            "query_key": self.query_key,
            "source_id": self.source_id,
            "source_sha256": self.source_sha256,
            "sql_sha256": self.sql_sha256,
            "columns": self.columns,
            "row_count": self.row_count,
            "byte_count": self.byte_count,
            "preview": self.preview,
            "duration_ms": self.duration_ms,
            "result_file": self.result_file,
            "created_at": self.created_at,
            "accessed_at": self.accessed_at,
            "expires_at": self.expires_at,
            "execution": self.execution,
        }


class CacheManager:
    def __init__(self, settings: Settings):
        self.settings = settings
        self._lock = threading.RLock()
        self.hits = 0
        self.misses = 0
        self.bypasses = 0
        self.evictions = 0
        self.active_queries = 0
        self._leases: dict[str, int] = {}
        self._staging: dict[Path, tuple[str, int]] = {}

    @contextmanager
    def lease_source(self, source_id: str) -> Iterator[SourceRecord]:
        with self._lock:
            record = self.get_source(source_id)
            self._leases[source_id] = self._leases.get(source_id, 0) + 1
        try:
            yield record
        finally:
            self.release(source_id)

    def acquire_result(self, result_id: str) -> ResultRecord:
        with self._lock:
            record = self.get_result(result_id)
            self._leases[result_id] = self._leases.get(result_id, 0) + 1
            return record

    def release(self, identifier: str) -> None:
        with self._lock:
            count = self._leases.get(identifier, 0)
            if count <= 1:
                self._leases.pop(identifier, None)
            else:
                self._leases[identifier] = count - 1

    def reserve(self, staging: Path, size: int) -> None:
        """Account for concurrent staging bytes as well as committed cache entries."""
        with self._lock:
            kind, old_size = self._staging[staging]
            root = self.settings.sources_dir if kind == "source" else self.settings.results_dir
            maximum = (
                self.settings.source_cache_max_bytes
                if kind == "source"
                else self.settings.result_cache_max_bytes
            )
            others = sum(n for p, (k, n) in self._staging.items() if p != staging and k == kind)
            if size + others > maximum:
                raise CacheCapacityExceeded("Staged data exceeds the cache capacity")
            if max(0, size - old_size) > shutil.disk_usage(self.settings.cache_dir).free:
                raise CacheCapacityExceeded("Insufficient free space on the cache volume")
            self._cleanup(root, maximum - size - others, set())
            if directory_bytes(root) + size + others > maximum:
                raise CacheCapacityExceeded("Cache capacity is occupied by active work")
            self._staging[staging] = kind, size

    def observe_staging(self, staging: Path) -> None:
        self.reserve(staging, directory_bytes(staging))

    def discard_staging(self, staging: Path) -> None:
        with self._lock:
            self._staging.pop(staging, None)
            self._remove_directory(staging, self.settings.tmp_dir, count=False)

    @staticmethod
    def source_id(scope_id: str, source_ref: str) -> str:
        digest = hashlib.sha256(f"{scope_id}\0{source_ref}".encode()).hexdigest()
        return f"src_{digest}"

    def source_path(self, source_id: str) -> Path:
        return self.settings.sources_dir / source_id

    def result_path(self, result_id: str) -> Path:
        return self.settings.results_dir / result_id

    def staging_path(self, prefix: str) -> Path:
        with self._lock:
            path = self.settings.tmp_dir / f"{prefix}-{uuid.uuid4().hex}"
            path.mkdir(parents=True)
            self._staging[path] = prefix, 0
            return path

    def _read_manifest(self, path: Path) -> dict[str, Any]:
        try:
            value = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, ValueError, TypeError) as exc:
            raise FileNotFoundError(path) from exc
        if not isinstance(value, dict):
            raise FileNotFoundError(path)
        return value

    def get_source(self, source_id: str, *, touch: bool = True) -> SourceRecord:
        if SOURCE_ID_PATTERN.fullmatch(source_id) is None:
            raise SourceNotFound("The source identifier is invalid")
        with self._lock:
            path = self.source_path(source_id)
            try:
                record = SourceRecord.from_dict(self._read_manifest(path / "manifest.json"))
            except (FileNotFoundError, KeyError, TypeError, ValueError) as exc:
                raise SourceNotFound("The source is not cached") from exc
            if (
                (parse_iso(record.expires_at) <= utc_now() and not self._leases.get(source_id))
                or Path(record.data_file).name != record.data_file
                or not (path / record.data_file).is_file()
            ):
                self._remove_directory(path, self.settings.sources_dir)
                raise SourceNotFound("The source has expired or is incomplete")
            if touch:
                now = utc_now()
                record.accessed_at = iso(now)
                record.expires_at = iso(now + timedelta(seconds=self.settings.source_ttl_seconds))
                _write_json_atomic(path / "manifest.json", record.as_dict())
            return record

    def resolve_source(
        self,
        scope_id: str,
        source_ref: str,
        source_format: SourceFormat,
        size: int,
        expected_sha256: str | None,
    ) -> SourceRecord | None:
        try:
            record = self.get_source(self.source_id(scope_id, source_ref))
        except SourceNotFound:
            return None
        if record.format is not source_format or record.size != size:
            return None
        if expected_sha256 and record.sha256.lower() != expected_sha256.lower():
            return None
        return record

    def commit_source(self, staging: Path, record: SourceRecord) -> SourceRecord:
        target = self.source_path(record.source_id)
        with self._lock:
            _write_json_atomic(staging / "manifest.json", record.as_dict())
            if target.exists():
                self.discard_staging(staging)
                return self.get_source(record.source_id)
            self.observe_staging(staging)
            os.replace(staging, target)
            self._staging.pop(staging, None)
            self.cleanup_sources(protected={record.source_id})
            return record

    def get_result_by_query_key(
        self, query_key: str, *, require_provenance: bool = False
    ) -> ResultRecord | None:
        result_id = f"res_{query_key}"
        try:
            record = self.get_result(result_id)
        except ResultNotFound:
            return None
        if record.query_key != query_key:
            return None
        if require_provenance and not record.execution:
            with self._lock:
                if self._leases.get(result_id):
                    raise QueryCapacityExceeded("Legacy result is being downloaded; retry later")
                self._remove_directory(self.result_path(result_id), self.settings.results_dir)
            return None
        return record

    def get_result(self, result_id: str, *, touch: bool = True) -> ResultRecord:
        if RESULT_ID_PATTERN.fullmatch(result_id) is None:
            raise ResultNotFound("The result identifier is invalid")
        with self._lock:
            path = self.result_path(result_id)
            try:
                record = ResultRecord.from_dict(self._read_manifest(path / "manifest.json"))
            except (FileNotFoundError, KeyError, TypeError, ValueError) as exc:
                raise ResultNotFound("The result is not cached") from exc
            if (
                (parse_iso(record.expires_at) <= utc_now() and not self._leases.get(result_id))
                or Path(record.result_file).name != record.result_file
                or not (path / record.result_file).is_file()
            ):
                self._remove_directory(path, self.settings.results_dir)
                raise ResultNotFound("The result has expired or is incomplete")
            if touch:
                now = utc_now()
                record.accessed_at = iso(now)
                record.expires_at = iso(now + timedelta(seconds=self.settings.result_ttl_seconds))
                _write_json_atomic(path / "manifest.json", record.as_dict())
            return record

    def commit_result(self, staging: Path, record: ResultRecord) -> ResultRecord:
        target = self.result_path(record.result_id)
        with self._lock:
            _write_json_atomic(staging / "manifest.json", record.as_dict())
            if target.exists():
                self.discard_staging(staging)
                return self.get_result(record.result_id)
            self.observe_staging(staging)
            os.replace(staging, target)
            self._staging.pop(staging, None)
            self.cleanup_results(protected={record.result_id})
            return record

    def _manifest_directories(self, root: Path) -> Iterator[tuple[Path, dict[str, Any]]]:
        if not root.exists():
            return
        for path in root.iterdir():
            if not path.is_dir():
                continue
            try:
                yield path, self._read_manifest(path / "manifest.json")
            except FileNotFoundError:
                yield path, {}

    def _cleanup(
        self,
        root: Path,
        max_bytes: int,
        protected: set[str],
    ) -> None:
        with self._lock:
            protected = protected | set(self._leases)
            now = utc_now()
            entries: list[tuple[datetime, Path, int]] = []
            total = 0
            for path, manifest in list(self._manifest_directories(root)):
                size = directory_bytes(path)
                try:
                    expires = parse_iso(str(manifest["expires_at"]))
                    accessed = parse_iso(str(manifest["accessed_at"]))
                except (KeyError, TypeError, ValueError):
                    self._remove_directory(path, root)
                    continue
                if expires <= now and path.name not in protected:
                    self._remove_directory(path, root)
                    continue
                total += size
                entries.append((accessed, path, size))
            for _, path, size in sorted(entries, key=lambda item: item[0]):
                if total <= max_bytes:
                    break
                if path.name in protected:
                    continue
                self._remove_directory(path, root)
                total -= size

    def cleanup_sources(self, protected: set[str] | None = None) -> None:
        self._cleanup(
            self.settings.sources_dir,
            self.settings.source_cache_max_bytes,
            protected or set(),
        )

    def cleanup_results(self, protected: set[str] | None = None) -> None:
        self._cleanup(
            self.settings.results_dir,
            self.settings.result_cache_max_bytes,
            protected or set(),
        )

    def cleanup_temporary(self) -> None:
        cutoff = (
            time.time()
            - max(
                self.settings.query_timeout_seconds,
                self.settings.ingestion_timeout_seconds,
            )
            * 2
        )
        for path in self.settings.tmp_dir.iterdir():
            try:
                if path not in self._staging and path.is_dir() and path.stat().st_mtime < cutoff:
                    self._remove_directory(path, self.settings.tmp_dir, count=False)
            except FileNotFoundError:
                continue

    def _remove_directory(self, path: Path, root: Path, *, count: bool = True) -> None:
        if self._leases.get(path.name):
            return
        root_resolved = root.resolve()
        path_resolved = path.resolve()
        if path_resolved.parent != root_resolved:
            raise RuntimeError(f"Refusing to remove cache path outside {root_resolved}")
        if path_resolved.exists():
            shutil.rmtree(path_resolved)
            if count:
                self.evictions += 1

    def purge(
        self,
        *,
        source_id: str | None = None,
        scope_id: str | None = None,
        result_id: str | None = None,
        dry_run: bool = True,
    ) -> dict[str, Any]:
        if sum(value is not None for value in (source_id, scope_id, result_id)) != 1:
            raise InvalidQuery("Select exactly one source_id, scope_id, or result_id")
        with self._lock:
            sources = list(self._manifest_directories(self.settings.sources_dir))
            selected_sources = {
                p.name
                for p, m in sources
                if p.name == source_id or (scope_id and m.get("scope_id") == scope_id)
            }
            entries = [
                (p, self.settings.sources_dir) for p, _ in sources if p.name in selected_sources
            ]
            entries.extend(
                (p, self.settings.results_dir)
                for p, m in self._manifest_directories(self.settings.results_dir)
                if p.name == result_id or m.get("source_id") in selected_sources
            )
            busy = {p.name for p, _ in entries if self._leases.get(p.name)}
            # Do not partially purge a source while it is producing a new result.
            if busy:
                return {
                    "dry_run": dry_run,
                    "selected": [p.name for p, _ in entries],
                    "removed": [],
                    "busy": sorted(busy),
                }
            selected = [p.name for p, _ in entries]
            if not dry_run:
                for p, root in entries:
                    self._remove_directory(p, root)
            return {
                "dry_run": dry_run,
                "selected": selected,
                "removed": [] if dry_run else selected,
                "busy": [],
            }

    def status(self) -> dict[str, int]:
        with self._lock:
            source_entries = list(self._manifest_directories(self.settings.sources_dir))
            result_entries = list(self._manifest_directories(self.settings.results_dir))
            return {
                "source_count": len(source_entries),
                "source_bytes": sum(directory_bytes(path) for path, _ in source_entries),
                "result_count": len(result_entries),
                "result_bytes": sum(directory_bytes(path) for path, _ in result_entries),
                "hits": self.hits,
                "misses": self.misses,
                "bypasses": self.bypasses,
                "evictions": self.evictions,
                "active_queries": self.active_queries,
            }

    def has_capacity(self) -> bool:
        """Readiness requires headroom after eviction of inactive entries."""
        with self._lock:
            for kind, root, maximum in (
                ("source", self.settings.sources_dir, self.settings.source_cache_max_bytes),
                ("result", self.settings.results_dir, self.settings.result_cache_max_bytes),
            ):
                pinned = sum(
                    directory_bytes(root / key) for key in self._leases if (root / key).is_dir()
                )
                staged = sum(size for k, size in self._staging.values() if k == kind)
                if pinned + staged >= maximum:
                    return False
            return True
