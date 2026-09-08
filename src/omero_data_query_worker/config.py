from __future__ import annotations

import os
import tempfile
from dataclasses import dataclass, replace
from pathlib import Path

GIB = 1024**3
MIB = 1024**2


def _positive_int(name: str, default: int) -> int:
    raw = os.getenv(name)
    if raw is None:
        return default
    try:
        value = int(raw)
    except ValueError as exc:
        raise RuntimeError(f"{name} must be an integer") from exc
    if value <= 0:
        raise RuntimeError(f"{name} must be positive")
    return value


@dataclass(frozen=True, slots=True)
class Settings:
    api_token: str
    cache_dir: Path = Path("/var/lib/omero-data-query-worker")
    source_cache_max_bytes: int = 100 * GIB
    result_cache_max_bytes: int = 10 * GIB
    source_ttl_seconds: int = 7 * 24 * 60 * 60
    result_ttl_seconds: int = 24 * 60 * 60
    max_source_bytes: int = 20 * GIB
    query_timeout_seconds: int = 30
    ingestion_timeout_seconds: int = 10 * 60
    max_result_rows: int = 100_000
    max_result_bytes: int = 64 * MIB
    max_concurrent_queries: int = 4
    duckdb_memory_limit: str = "1GB"
    duckdb_threads: int = 2
    token_keyring_file: str = ""
    max_concurrent_ingestions: int = 1
    cleanup_interval_seconds: int = 60

    @classmethod
    def from_env(cls) -> Settings:
        token = os.getenv("DQW_API_TOKEN", "")
        return cls(
            api_token=token,
            cache_dir=Path(os.getenv("DQW_CACHE_DIR", "/var/lib/omero-data-query-worker")),
            source_cache_max_bytes=_positive_int("DQW_SOURCE_CACHE_MAX_BYTES", 100 * GIB),
            result_cache_max_bytes=_positive_int("DQW_RESULT_CACHE_MAX_BYTES", 10 * GIB),
            source_ttl_seconds=_positive_int("DQW_SOURCE_TTL_SECONDS", 7 * 24 * 60 * 60),
            result_ttl_seconds=_positive_int("DQW_RESULT_TTL_SECONDS", 24 * 60 * 60),
            max_source_bytes=_positive_int("DQW_MAX_SOURCE_BYTES", 20 * GIB),
            query_timeout_seconds=_positive_int("DQW_QUERY_TIMEOUT_SECONDS", 30),
            ingestion_timeout_seconds=_positive_int("DQW_INGESTION_TIMEOUT_SECONDS", 10 * 60),
            max_result_rows=_positive_int("DQW_MAX_RESULT_ROWS", 100_000),
            max_result_bytes=_positive_int("DQW_MAX_RESULT_BYTES", 64 * MIB),
            max_concurrent_queries=_positive_int("DQW_MAX_CONCURRENT_QUERIES", 4),
            duckdb_memory_limit=os.getenv("DQW_DUCKDB_MEMORY_LIMIT", "1GB"),
            duckdb_threads=_positive_int("DQW_DUCKDB_THREADS", 2),
            token_keyring_file=os.getenv("DQW_TOKEN_KEYRING_FILE", ""),
            max_concurrent_ingestions=_positive_int("DQW_MAX_CONCURRENT_INGESTIONS", 1),
            cleanup_interval_seconds=_positive_int("DQW_CLEANUP_INTERVAL_SECONDS", 60),
        )

    def prepare(self) -> None:
        if not self.api_token and not self.token_keyring_file:
            raise RuntimeError("DQW_API_TOKEN is required")
        if self.api_token and len(self.api_token) < 16:
            raise RuntimeError("DQW_API_TOKEN must contain at least 16 characters")
        for path in (self.cache_dir, self.sources_dir, self.results_dir, self.tmp_dir):
            path.mkdir(parents=True, exist_ok=True)
        # Starlette's multipart parser uses tempfile.SpooledTemporaryFile before
        # the request reaches the ingestion service. Default that spool to the
        # cache volume so multi-gigabyte sources are not limited by a small,
        # hardened /tmp tmpfs. An explicit TMPDIR remains supported.
        multipart_tmp_dir = Path(os.getenv("TMPDIR", str(self.tmp_dir)))
        multipart_tmp_dir.mkdir(parents=True, exist_ok=True)
        tempfile.tempdir = str(multipart_tmp_dir)
        probe = self.cache_dir / ".write-test"
        probe.write_bytes(b"ready")
        probe.unlink()

    def execution_settings(self) -> Settings:
        """Never serialize service credentials into an engine process."""
        return replace(self, api_token="", token_keyring_file="")

    @property
    def sources_dir(self) -> Path:
        return self.cache_dir / "sources"

    @property
    def results_dir(self) -> Path:
        return self.cache_dir / "results"

    @property
    def tmp_dir(self) -> Path:
        return self.cache_dir / "tmp"
