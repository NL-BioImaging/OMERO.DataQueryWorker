"""Private operational controls; no SQL or data values in telemetry."""

from __future__ import annotations

import importlib
import json
import os
import re
import secrets
import threading
import uuid
from collections import Counter
from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path
from typing import Any

from .config import Settings
from .errors import AuthenticationError


def request_id(value: str | None) -> str:
    return value if value and re.fullmatch(r"[A-Za-z0-9._-]{1,80}", value) else uuid.uuid4().hex


class Credentials:
    def __init__(self, settings: Settings):
        self.settings = settings

    def tokens(self) -> list[str]:
        tokens = [self.settings.api_token] if self.settings.api_token else []
        if self.settings.token_keyring_file:
            try:
                # Atomic replacement by the operator makes each read a complete revision.
                path = Path(self.settings.token_keyring_file)
                if path.stat().st_size > 65536:
                    raise ValueError
                value = json.loads(path.read_text(encoding="utf-8"))
                if not isinstance(value, dict) or set(value) != {"tokens"}:
                    raise ValueError
                ring = value["tokens"]
                if not isinstance(ring, list) or not 1 <= len(ring) <= 16:
                    raise ValueError
                if any(not isinstance(t, str) or not 16 <= len(t) <= 512 for t in ring):
                    raise ValueError
                tokens.extend(ring)
            except (OSError, ValueError, TypeError) as exc:
                raise AuthenticationError("Worker credential configuration is unavailable") from exc
        return tokens

    def authenticate(self, authorization: str | None) -> None:
        scheme, _, value = (authorization or "").partition(" ")
        matches = [secrets.compare_digest(value.encode(), t.encode()) for t in self.tokens()]
        if scheme.lower() != "bearer" or not any(matches):
            raise AuthenticationError("A valid worker bearer token is required")


def sanitize_execution_environment() -> None:
    # Preserve only OS/runtime essentials. In particular no OMERO/session/cloud credentials.
    allowed = {
        "PATH",
        "SYSTEMROOT",
        "WINDIR",
        "TEMP",
        "TMP",
        "TMPDIR",
        "LANG",
        "LC_ALL",
        "LD_LIBRARY_PATH",
        "PYTHONPATH",
        "PYTHONHOME",
        "PYTHONDONTWRITEBYTECODE",
    }
    for name in list(os.environ):
        if name.upper() not in allowed:
            del os.environ[name]


@contextmanager
def volume_lock(directory: Path) -> Iterator[None]:
    """Prevent two service processes from independently managing the same cache."""
    directory.mkdir(parents=True, exist_ok=True)
    with (directory / ".worker.lock").open("a+b") as handle:
        handle.seek(0)
        if os.name == "nt":
            locking: Any = importlib.import_module("msvcrt")
            handle.write(b"0")
            handle.flush()
            handle.seek(0)
            try:
                locking.locking(handle.fileno(), locking.LK_NBLCK, 1)
            except OSError as exc:
                raise RuntimeError("Another worker owns this cache volume") from exc
        else:
            locking = importlib.import_module("fcntl")
            try:
                locking.flock(handle.fileno(), locking.LOCK_EX | locking.LOCK_NB)
            except OSError as exc:
                raise RuntimeError("Another worker owns this cache volume") from exc
        try:
            yield
        finally:
            if os.name == "nt":
                handle.seek(0)
                locking.locking(handle.fileno(), locking.LK_UNLCK, 1)
            else:
                locking.flock(handle.fileno(), locking.LOCK_UN)


class Metrics:
    def __init__(self) -> None:
        self._lock = threading.Lock()
        self.values: Counter[str] = Counter()

    def add(self, name: str, value: float = 1) -> None:
        with self._lock:
            self.values[name] += value  # type: ignore[assignment]

    def render(self, gauges: dict[str, int]) -> str:
        with self._lock:
            values = {**self.values, **gauges}
        return "".join(f"dqw_{name} {value}\n" for name, value in sorted(values.items()))
