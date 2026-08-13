from __future__ import annotations

import tempfile
from pathlib import Path

from omero_data_query_worker.config import Settings


def test_prepare_defaults_multipart_spool_to_cache_volume(settings: Settings, monkeypatch) -> None:
    monkeypatch.delenv("TMPDIR", raising=False)
    monkeypatch.setattr(tempfile, "tempdir", None)

    settings.prepare()

    assert Path(tempfile.gettempdir()) == settings.tmp_dir
