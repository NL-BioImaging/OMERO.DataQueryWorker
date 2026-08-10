from __future__ import annotations

import json
import time
from dataclasses import replace
from datetime import timedelta

import pytest

from omero_data_query_worker.cache import CacheManager, SourceRecord, iso, utc_now
from omero_data_query_worker.config import Settings
from omero_data_query_worker.errors import SourceNotFound
from omero_data_query_worker.models import SourceFormat


def test_source_id_is_scoped_and_stable(settings: Settings) -> None:
    cache = CacheManager(settings)
    assert cache.source_id("scope", "reference") == cache.source_id("scope", "reference")
    assert cache.source_id("other", "reference") != cache.source_id("scope", "reference")


def test_expired_source_is_evicted(settings: Settings) -> None:
    settings.prepare()
    cache = CacheManager(settings)
    source_id = cache.source_id("scope", "expired")
    path = cache.source_path(source_id)
    path.mkdir()
    (path / "source.csv").write_text("x\n1\n", encoding="utf-8")
    past = utc_now() - timedelta(seconds=1)
    record = SourceRecord(
        source_id=source_id,
        scope_id="scope",
        source_ref="expired",
        format=SourceFormat.csv,
        filename="source.csv",
        size=4,
        sha256="0" * 64,
        schema_digest="1" * 64,
        schema={"tables": []},
        data_file="source.csv",
        created_at=iso(past),
        accessed_at=iso(past),
        expires_at=iso(past),
    )
    (path / "manifest.json").write_text(json.dumps(record.as_dict()), encoding="utf-8")
    with pytest.raises(SourceNotFound):
        cache.get_source(source_id)
    assert not path.exists()


def test_incomplete_atomic_entry_is_cleaned(settings: Settings) -> None:
    settings.prepare()
    cache = CacheManager(settings)
    incomplete = settings.sources_dir / "src_incomplete"
    incomplete.mkdir()
    (incomplete / "partial").write_bytes(b"partial")
    cache.cleanup_sources()
    assert not incomplete.exists()


def test_source_cache_evicts_least_recently_used(settings: Settings) -> None:
    limited = replace(settings, source_cache_max_bytes=1000)
    limited.prepare()
    cache = CacheManager(limited)

    def commit(reference: str) -> SourceRecord:
        source_id = cache.source_id("scope", reference)
        staging = cache.staging_path("source")
        (staging / "data.csv").write_bytes(b"x" * 700)
        now = utc_now()
        record = SourceRecord(
            source_id=source_id,
            scope_id="scope",
            source_ref=reference,
            format=SourceFormat.csv,
            filename="data.csv",
            size=700,
            sha256=reference.ljust(64, "0")[:64],
            schema_digest="1" * 64,
            schema={"tables": []},
            data_file="data.csv",
            created_at=iso(now),
            accessed_at=iso(now),
            expires_at=iso(now + timedelta(hours=1)),
        )
        return cache.commit_source(staging, record)

    first = commit("first")
    time.sleep(0.01)
    second = commit("second")
    assert not cache.source_path(first.source_id).exists()
    assert cache.source_path(second.source_id).exists()
    assert cache.evictions >= 1
