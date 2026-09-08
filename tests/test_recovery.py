"""Recovery rejects corrupt publications without rewriting immutable inputs."""

import hashlib
import json

import pytest

from omero_data_query_worker.cache import CacheManager
from omero_data_query_worker.service import QueryService
from tests.conftest import upload_source


def test_restart_removes_fresh_staging_and_preserves_valid_cache(client, auth, settings):
    uploaded = upload_source(
        client,
        auth,
        source_ref="recovery",
        source_format="csv",
        filename="source.csv",
        content=b"id\n1\n2\n",
    ).json()
    source_id = uploaded["source_id"]
    result = client.post(
        f"/v1/sources/{source_id}/query", headers=auth, json={"sql": "SELECT * FROM data"}
    ).json()
    cache = CacheManager(settings)
    abandoned = settings.tmp_dir / "result-abandoned"
    abandoned.mkdir()
    (abandoned / "result.csv").write_bytes(b"id\n1\n")
    source_file = cache.source_path(source_id) / "data.duckdb"
    before = hashlib.sha256(source_file.read_bytes()).hexdigest()
    QueryService(settings).prepare()
    assert not abandoned.exists()
    assert cache.get_result(result["result_id"]).row_count == 2
    assert hashlib.sha256(source_file.read_bytes()).hexdigest() == before


def test_restart_discards_same_size_corruption(client, auth, settings):
    source = upload_source(
        client,
        auth,
        source_ref="corrupt-recovery",
        source_format="csv",
        filename="source.csv",
        content=b"id\n1\n",
    ).json()
    result = client.post(
        f"/v1/sources/{source['source_id']}/query", headers=auth, json={"sql": "SELECT * FROM data"}
    ).json()
    cache = CacheManager(settings)
    path = cache.result_path(result["result_id"])
    data = path / "result.csv"
    data.write_bytes(data.read_bytes().replace(b"1", b"9"))
    cache.recover()
    assert not path.exists()
    assert cache.source_path(source["source_id"]).exists()


@pytest.mark.parametrize("damage", ["execution", "byte_count", "source_bytes"])
def test_restart_recovers_from_damaged_metadata_and_sources(client, auth, settings, damage):
    source = upload_source(
        client,
        auth,
        source_ref="damaged-manifest",
        source_format="csv",
        filename="source.csv",
        content=b"id\n1\n",
    ).json()
    result = client.post(
        f"/v1/sources/{source['source_id']}/query", headers=auth, json={"sql": "SELECT * FROM data"}
    ).json()
    cache = CacheManager(settings)
    result_path = cache.result_path(result["result_id"])
    source_path = cache.source_path(source["source_id"])
    if damage == "source_bytes":
        (source_path / "data.duckdb").write_bytes(b"corrupt")
    else:
        path = result_path / "manifest.json"
        manifest = json.loads(path.read_text())
        manifest[damage] = [] if damage == "execution" else 999
        if damage == "execution":
            manifest[damage] = ["invalid"]
        path.write_text(json.dumps(manifest))
    cache.recover()
    assert not (source_path if damage == "source_bytes" else result_path).exists()
