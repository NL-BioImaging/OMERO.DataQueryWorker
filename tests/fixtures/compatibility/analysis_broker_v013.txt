"""OMERO-authorized broker for the OMERO-agnostic DataQueryWorker."""

from __future__ import annotations

import base64
import hashlib
import hmac
import json
from pathlib import Path
from typing import Any

import requests
from cryptography.fernet import Fernet, InvalidToken as InvalidFernetToken
from django.conf import settings as django_settings
from requests_toolbelt import MultipartEncoder

from .errors import InvalidToken, RemoteQueryFailed, RemoteQueryUnavailable, UnsupportedMedia
from .services import object_group_id
from .settings import (
    data_query_request_timeout_seconds,
    data_query_result_ttl_seconds,
    data_query_source_upload_timeout_seconds,
    data_query_worker_token,
    data_query_worker_url,
    remote_query_threshold_bytes,
)

SUPPORTED_FORMATS = {
    ".duckdb": "duckdb",
    ".sqlite": "sqlite",
    ".sqlite3": "sqlite",
    ".csv": "csv",
}
CAPABILITY = "omero-data-query-v1"


class ChunkReader:
    """Bounded file-like adapter over the OMERO chunk iterator."""

    def __init__(self, chunks: Any, size: int) -> None:
        self._chunks = iter(chunks)
        self._buffer = b""
        self._remaining = size

    @property
    def len(self) -> int:
        return self._remaining

    def read(self, size: int = -1) -> bytes:
        if self._remaining <= 0:
            return b""
        target = self._remaining if size is None or size < 0 else min(size, self._remaining)
        while len(self._buffer) < target:
            try:
                self._buffer += bytes(next(self._chunks))
            except StopIteration:
                break
        value, self._buffer = self._buffer[:target], self._buffer[target:]
        self._remaining -= len(value)
        return value


def source_format(name: str) -> str:
    value = SUPPORTED_FORMATS.get(Path(name).suffix.lower())
    if value is None:
        raise UnsupportedMedia(
            "Remote queries support DuckDB, SQLite, and CSV attachments"
        )
    return value


def query_policy(info: Any, worker_ready: bool) -> dict[str, Any]:
    try:
        format_name = source_format(info.name)
    except UnsupportedMedia:
        return {}
    threshold = remote_query_threshold_bytes()
    forced = threshold == 0
    default_mode = "remote" if forced or info.size >= threshold else "local"
    allowed = ["remote"] if forced else ["local", "remote"]
    if not worker_ready:
        allowed = [mode for mode in allowed if mode != "remote"]
    reason = (
        "remote-required-by-policy"
        if forced
        else "size-at-or-above-threshold"
        if info.size >= threshold
        else "below-threshold"
    )
    return {
        "query_format": format_name,
        "default_mode": default_mode,
        "allowed_modes": allowed,
        "threshold_bytes": threshold,
        "threshold_reason": reason,
        "worker_ready": worker_ready,
    }


class DataQueryBroker:
    def __init__(self, session: requests.Session | None = None) -> None:
        self.url = data_query_worker_url()
        self.token = data_query_worker_token()
        self.session = session or requests.Session()

    @property
    def configured(self) -> bool:
        return bool(self.url and self.token)

    def capabilities(self) -> dict[str, Any]:
        threshold = remote_query_threshold_bytes()
        unavailable = {
            "available": bool(self.configured),
            "ready": False,
            "capability": CAPABILITY,
            "formats": ["duckdb", "sqlite", "csv"],
            "threshold_bytes": threshold,
            "result_ttl_seconds": data_query_result_ttl_seconds(),
        }
        if not self.configured:
            return unavailable
        try:
            health = self.session.get(f"{self.url}/health/ready", timeout=3)
            health.raise_for_status()
            payload = self._request("GET", "/v1/capabilities", timeout=5)
        except (requests.RequestException, RemoteQueryFailed, RemoteQueryUnavailable):
            return unavailable
        return {
            "available": True,
            "ready": True,
            "capability": CAPABILITY,
            "formats": payload.get("formats", []),
            "limits": payload.get("limits", {}),
            "parameter_style": payload.get("parameter_style", "$name"),
            "csv_table": payload.get("csv_table", "data"),
            "threshold_bytes": threshold,
            "result_ttl_seconds": data_query_result_ttl_seconds(),
        }

    def schema(
        self, annotation: Any, info: Any, scope: str, source_ref: str
    ) -> dict[str, Any]:
        source = self._source(annotation, info, scope, source_ref)
        payload = self._request(
            "GET", f"/v1/sources/{source['source_id']}/schema"
        )
        return {key: value for key, value in payload.items() if key != "source_id"}

    def query(
        self,
        annotation: Any,
        info: Any,
        scope: str,
        source_ref: str,
        payload: dict[str, Any],
    ) -> tuple[dict[str, Any], str]:
        if set(payload) - {"sql", "parameters"}:
            raise RemoteQueryFailed("Remote query request contains unsupported fields")
        source = self._source(annotation, info, scope, source_ref)
        result = self._request(
            "POST",
            f"/v1/sources/{source['source_id']}/query",
            json=payload,
        )
        result_id = str(result.pop("result_id"))
        return result, result_id

    def download(self, result_id: str) -> requests.Response:
        return self._stream("GET", f"/v1/results/{result_id}/download")

    def _source(
        self, annotation: Any, info: Any, scope: str, source_ref: str
    ) -> dict[str, Any]:
        format_name = source_format(info.name)
        resolve = {
            "scope_id": scope,
            "source_ref": source_ref,
            "format": format_name,
            "size": info.size,
        }
        cached = self._request("POST", "/v1/sources/resolve", json=resolve)
        if cached.get("cached") and isinstance(cached.get("source"), dict):
            return cached["source"]
        reader = ChunkReader(annotation.getFileInChunks(), info.size)
        encoder = MultipartEncoder(
            fields={
                "scope_id": scope,
                "source_ref": source_ref,
                "format": format_name,
                "size": str(info.size),
                "file": (info.name, reader, info.mimetype),
            }
        )
        return self._request(
            "POST",
            "/v1/sources",
            data=encoder,
            headers={"Content-Type": encoder.content_type},
            timeout=data_query_source_upload_timeout_seconds(),
        )

    def _request(self, method: str, path: str, **kwargs: Any) -> dict[str, Any]:
        response = self._stream(method, path, **kwargs)
        try:
            value = response.json()
        except ValueError as exc:
            raise RemoteQueryFailed(
                "The data query worker returned an invalid response"
            ) from exc
        if not isinstance(value, dict):
            raise RemoteQueryFailed("The data query worker returned an invalid response")
        return value

    def _stream(self, method: str, path: str, **kwargs: Any) -> requests.Response:
        if not self.configured:
            raise RemoteQueryUnavailable(
                "The remote data query worker is not configured"
            )
        headers = {
            "Authorization": f"Bearer {self.token}",
            **kwargs.pop("headers", {}),
        }
        try:
            response = self.session.request(
                method,
                f"{self.url}{path}",
                headers=headers,
                timeout=kwargs.pop("timeout", data_query_request_timeout_seconds()),
                stream=True,
                **kwargs,
            )
        except requests.RequestException as exc:
            raise RemoteQueryUnavailable(
                "The remote data query worker is unavailable"
            ) from exc
        if not response.ok:
            try:
                error = response.json()
                message = str(error.get("message") or error.get("detail") or "")
            except (ValueError, AttributeError):
                message = ""
            raise RemoteQueryFailed(message or "Remote query failed")
        return response


def opaque_references(
    request: Any, conn: Any, claims: dict[str, Any], info: Any
) -> tuple[str, str]:
    key = hashlib.sha256(
        (str(django_settings.SECRET_KEY) + "\0data-query-refs-v1").encode()
    ).digest()
    session_key = str(
        getattr(getattr(request, "session", None), "session_key", "") or ""
    )
    scope_material = (
        f"{conn.getUserId()}:{claims['group_id']}:{session_key}:"
        f"{claims['object_type']}:{claims['object_id']}"
    )
    source_material = (
        f"{scope_material}:{info.annotation_id}:{info.file_id}:"
        f"{info.size}:{info.name}"
    )
    return _opaque(key, scope_material), _opaque(key, source_material)


def _opaque(key: bytes, value: str) -> str:
    return (
        base64.urlsafe_b64encode(hmac.new(key, value.encode(), hashlib.sha256).digest())
        .decode()
        .rstrip("=")
    )


def make_result_token(
    request: Any,
    conn: Any,
    claims: dict[str, Any],
    info: Any,
    result_id: str,
) -> str:
    payload = {
        "v": 1,
        "user": int(conn.getUserId()),
        "group": claims["group_id"],
        "session": str(getattr(request.session, "session_key", "") or ""),
        "object_type": claims["object_type"],
        "object_id": claims["object_id"],
        "annotation_id": info.annotation_id,
        "file_id": info.file_id,
        "result_id": result_id,
    }
    return _fernet().encrypt(
        json.dumps(payload, separators=(",", ":")).encode()
    ).decode()


def validate_result_token(
    request: Any, conn: Any, token: str, obj: Any, info: Any
) -> dict[str, Any]:
    try:
        payload = json.loads(
            _fernet().decrypt(
                token.encode(), ttl=data_query_result_ttl_seconds()
            )
        )
    except (InvalidFernetToken, ValueError, json.JSONDecodeError) as exc:
        raise InvalidToken(
            "The data query result token is invalid or expired"
        ) from exc
    expected = {
        "v": 1,
        "user": int(conn.getUserId()),
        "group": object_group_id(obj),
        "session": str(getattr(request.session, "session_key", "") or ""),
        "annotation_id": info.annotation_id,
        "file_id": info.file_id,
    }
    if any(payload.get(key) != value for key, value in expected.items()):
        raise InvalidToken(
            "The data query result token is not valid for this context"
        )
    return payload


def result_token_claims(token: str) -> dict[str, Any]:
    try:
        value = json.loads(
            _fernet().decrypt(
                token.encode(), ttl=data_query_result_ttl_seconds()
            )
        )
    except (InvalidFernetToken, ValueError, json.JSONDecodeError) as exc:
        raise InvalidToken(
            "The data query result token is invalid or expired"
        ) from exc
    if not isinstance(value, dict):
        raise InvalidToken("The data query result token is invalid")
    return value


def _fernet() -> Fernet:
    digest = hashlib.sha256(
        (str(django_settings.SECRET_KEY) + "\0data-query-results-v1").encode()
    ).digest()
    return Fernet(base64.urlsafe_b64encode(digest))
