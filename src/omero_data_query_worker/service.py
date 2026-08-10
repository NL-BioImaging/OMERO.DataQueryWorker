from __future__ import annotations

import hashlib
import json
import shutil
import threading
import uuid
from datetime import date, datetime, time
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any

from fastapi import UploadFile

from . import __version__
from .cache import CacheManager, ResultRecord, SourceRecord, iso, utc_now
from .config import Settings
from .engines import validate_filename
from .errors import (
    InvalidQuery,
    InvalidSource,
    QueryCapacityExceeded,
    SourceConflict,
    SourceNotFound,
    SourceTooLarge,
)
from .executor import execute_in_subprocess
from .ingestion import ingest_in_subprocess
from .models import (
    QueryRequest,
    QueryResponse,
    ResultColumn,
    SchemaResponse,
    SourceFormat,
    SourceResolveRequest,
    SourceResolveResponse,
    SourceSummary,
    TypedParameter,
)
from .policy import POLICY_VERSION, ValidatedQuery, validate_query


class QueryService:
    def __init__(self, settings: Settings):
        self.settings = settings
        self.cache = CacheManager(settings)
        self._query_capacity = threading.BoundedSemaphore(settings.max_concurrent_queries)
        self._source_locks: dict[str, threading.Lock] = {}
        self._query_locks: dict[str, threading.Lock] = {}
        self._locks_guard = threading.Lock()

    def prepare(self) -> None:
        self.settings.prepare()
        self.cache.cleanup_temporary()
        self.cache.cleanup_sources()
        self.cache.cleanup_results()

    def _named_lock(self, collection: dict[str, threading.Lock], key: str) -> threading.Lock:
        with self._locks_guard:
            return collection.setdefault(key, threading.Lock())

    @staticmethod
    def _summary(record: SourceRecord, cache_status: str) -> SourceSummary:
        return SourceSummary(
            source_id=record.source_id,
            format=record.format,
            filename=record.filename,
            size=record.size,
            sha256=record.sha256,
            schema_digest=record.schema_digest,
            cache_status=cache_status,  # type: ignore[arg-type]
            expires_at=record.expires_at,
        )

    def resolve_source(self, request: SourceResolveRequest) -> SourceResolveResponse:
        record = self.cache.resolve_source(
            request.scope_id,
            request.source_ref,
            request.format,
            request.size,
            request.expected_sha256,
        )
        if record is None:
            return SourceResolveResponse(cached=False)
        return SourceResolveResponse(cached=True, source=self._summary(record, "reused"))

    async def ingest_source(
        self,
        *,
        scope_id: str,
        source_ref: str,
        source_format: SourceFormat,
        declared_size: int,
        expected_sha256: str | None,
        upload: UploadFile,
    ) -> SourceSummary:
        source_request = SourceResolveRequest(
            scope_id=scope_id,
            source_ref=source_ref,
            format=source_format,
            size=declared_size,
            expected_sha256=expected_sha256,
        )
        filename = validate_filename(upload.filename or "", source_format)
        source_id = self.cache.source_id(scope_id, source_ref)
        lock = self._named_lock(self._source_locks, source_id)
        with lock:
            try:
                existing = self.cache.get_source(source_id)
            except SourceNotFound:
                existing = None
            if existing is not None:
                if (
                    existing.format is source_format
                    and existing.size == declared_size
                    and (not expected_sha256 or existing.sha256.lower() == expected_sha256.lower())
                ):
                    return self._summary(existing, "reused")
                raise SourceConflict("The source reference is already bound to different content")

            if declared_size > self.settings.max_source_bytes:
                raise SourceTooLarge(
                    f"Source exceeds the {self.settings.max_source_bytes} byte limit"
                )
            staging = self.cache.staging_path("source")
            try:
                suffix = {
                    SourceFormat.duckdb: ".duckdb",
                    SourceFormat.sqlite: ".sqlite",
                    SourceFormat.csv: ".csv",
                }[source_format]
                raw_path = staging / f"source{suffix}"
                digest = hashlib.sha256()
                actual_size = 0
                with raw_path.open("wb") as output:
                    while chunk := await upload.read(1024 * 1024):
                        actual_size += len(chunk)
                        if actual_size > self.settings.max_source_bytes:
                            raise SourceTooLarge(
                                f"Source exceeds the {self.settings.max_source_bytes} byte limit"
                            )
                        digest.update(chunk)
                        output.write(chunk)
                    output.flush()
                if actual_size != declared_size:
                    raise InvalidSource(
                        f"Declared size {declared_size} does not match uploaded size {actual_size}"
                    )
                actual_sha256 = digest.hexdigest()
                if expected_sha256 and actual_sha256.lower() != expected_sha256.lower():
                    raise InvalidSource("Uploaded content does not match expected_sha256")

                data_path, schema, schema_digest = ingest_in_subprocess(
                    raw_path, source_format, self.settings
                )
                now = utc_now()
                from datetime import timedelta

                record = SourceRecord(
                    source_id=source_id,
                    scope_id=source_request.scope_id,
                    source_ref=source_request.source_ref,
                    format=source_format,
                    filename=filename,
                    size=actual_size,
                    sha256=actual_sha256,
                    schema_digest=schema_digest,
                    schema=schema,
                    data_file=data_path.name,
                    created_at=iso(now),
                    accessed_at=iso(now),
                    expires_at=iso(now + timedelta(seconds=self.settings.source_ttl_seconds)),
                )
                committed = self.cache.commit_source(staging, record)
                return self._summary(committed, "created")
            except BaseException:
                if staging.exists():
                    shutil.rmtree(staging)
                raise
            finally:
                await upload.close()

    def schema(self, source_id: str) -> SchemaResponse:
        record = self.cache.get_source(source_id)
        return SchemaResponse(
            source_id=record.source_id,
            format=record.format,
            schema_digest=record.schema_digest,
            tables=record.schema.get("tables", []),
        )

    @staticmethod
    def _convert_parameter(parameter: TypedParameter) -> Any:
        value = parameter.value
        kind = parameter.type
        try:
            if kind == "null":
                if value is not None:
                    raise ValueError
                return None
            if kind == "boolean":
                if not isinstance(value, bool):
                    raise ValueError
                return value
            if kind == "integer":
                if not isinstance(value, int) or isinstance(value, bool):
                    raise ValueError
                return value
            if kind == "float":
                if not isinstance(value, (int, float)) or isinstance(value, bool):
                    raise ValueError
                return float(value)
            if kind == "decimal":
                if not isinstance(value, (str, int, float)) or isinstance(value, bool):
                    raise ValueError
                return Decimal(str(value))
            if kind == "string":
                if not isinstance(value, str):
                    raise ValueError
                return value
            if kind == "date":
                if not isinstance(value, str):
                    raise ValueError
                return date.fromisoformat(value)
            if kind == "time":
                if not isinstance(value, str):
                    raise ValueError
                return time.fromisoformat(value)
            if kind == "timestamp":
                if not isinstance(value, str):
                    raise ValueError
                return datetime.fromisoformat(value.replace("Z", "+00:00"))
        except (ValueError, TypeError, InvalidOperation) as exc:
            raise InvalidQuery(f"Invalid {kind} parameter value") from exc
        raise InvalidQuery(f"Unsupported parameter type {kind}")

    def _query_key(
        self,
        source: SourceRecord,
        validated: ValidatedQuery,
        request: QueryRequest,
    ) -> str:
        parameters = {
            name: parameter.model_dump(mode="json")
            for name, parameter in sorted(request.parameters.items())
        }
        value = {
            "scope_id": source.scope_id,
            "source_sha256": source.sha256,
            "schema_digest": source.schema_digest,
            "format": source.format.value,
            "sql": validated.canonical_sql,
            "parameters": parameters,
            "worker_version": __version__,
            "policy_version": POLICY_VERSION,
            "max_rows": self.settings.max_result_rows,
            "max_bytes": self.settings.max_result_bytes,
        }
        canonical = json.dumps(value, sort_keys=True, separators=(",", ":"))
        return hashlib.sha256(canonical.encode()).hexdigest()

    @staticmethod
    def _response(record: ResultRecord, cache_status: str) -> QueryResponse:
        return QueryResponse(
            result_id=record.result_id,
            columns=[ResultColumn(**item) for item in record.columns],
            row_count=record.row_count,
            byte_count=record.byte_count,
            preview=record.preview,
            source_sha256=record.source_sha256,
            sql_sha256=record.sql_sha256,
            duration_ms=record.duration_ms,
            cache_status=cache_status,  # type: ignore[arg-type]
        )

    def query(self, source_id: str, request: QueryRequest) -> QueryResponse:
        source = self.cache.get_source(source_id)
        validated = validate_query(request.sql, source.format, set(request.parameters))
        converted_parameters = {
            name: self._convert_parameter(parameter)
            for name, parameter in request.parameters.items()
        }
        if source.format is SourceFormat.sqlite:
            converted_parameters = {
                name: (
                    str(value)
                    if isinstance(value, Decimal)
                    else value.isoformat()
                    if isinstance(value, (date, time, datetime))
                    else value
                )
                for name, value in converted_parameters.items()
            }
        query_key = self._query_key(source, validated, request)
        if validated.deterministic:
            cached = self.cache.get_result_by_query_key(query_key)
            if cached is not None:
                self.cache.hits += 1
                return self._response(cached, "hit")
            query_lock = self._named_lock(self._query_locks, query_key)
        else:
            self.cache.bypasses += 1
            query_lock = threading.Lock()

        with query_lock:
            if validated.deterministic:
                cached = self.cache.get_result_by_query_key(query_key)
                if cached is not None:
                    self.cache.hits += 1
                    return self._response(cached, "hit")
                self.cache.misses += 1
                cache_status = "miss"
                result_id = f"res_{query_key}"
                stored_query_key: str | None = query_key
            else:
                cache_status = "bypass"
                result_id = f"res_{uuid.uuid4().hex}"
                stored_query_key = None

            if not self._query_capacity.acquire(blocking=False):
                raise QueryCapacityExceeded("All disposable query slots are busy")
            self.cache.active_queries += 1
            staging = self.cache.staging_path("result")
            try:
                source_path = self.cache.source_path(source.source_id) / source.data_file
                output_path = staging / "result.csv"
                executed = execute_in_subprocess(
                    source_path,
                    source.format,
                    validated.original_sql,
                    converted_parameters,
                    output_path,
                    self.settings,
                )
                now = utc_now()
                from datetime import timedelta

                record = ResultRecord(
                    result_id=result_id,
                    query_key=stored_query_key,
                    source_id=source.source_id,
                    source_sha256=source.sha256,
                    sql_sha256=validated.sql_sha256,
                    columns=executed["columns"],
                    row_count=int(executed["row_count"]),
                    byte_count=int(executed["byte_count"]),
                    preview=executed["preview"],
                    duration_ms=int(executed["duration_ms"]),
                    result_file="result.csv",
                    created_at=iso(now),
                    accessed_at=iso(now),
                    expires_at=iso(now + timedelta(seconds=self.settings.result_ttl_seconds)),
                )
                committed = self.cache.commit_result(staging, record)
                return self._response(committed, cache_status)
            except BaseException:
                if staging.exists():
                    shutil.rmtree(staging)
                raise
            finally:
                self.cache.active_queries -= 1
                self._query_capacity.release()

    def result_file(self, result_id: str) -> tuple[Path, ResultRecord]:
        record = self.cache.get_result(result_id)
        return self.cache.result_path(result_id) / record.result_file, record
