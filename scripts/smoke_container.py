from __future__ import annotations

import argparse
import subprocess
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def compose(*arguments: str, capture: bool = False) -> str:
    result = subprocess.run(
        ["docker", "compose", *arguments],
        cwd=ROOT,
        check=True,
        text=True,
        capture_output=capture,
    )
    return result.stdout.strip() if capture else ""


def wait_ready(timeout: int = 90) -> None:
    container_id = compose("ps", "--quiet", "data-query-worker", capture=True)
    if not container_id:
        raise RuntimeError("worker container was not created")
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        result = subprocess.run(
            ["docker", "inspect", "--format", "{{.State.Health.Status}}", container_id],
            check=False,
            text=True,
            capture_output=True,
        )
        if result.returncode == 0 and result.stdout.strip() == "healthy":
            return
        time.sleep(1)
    compose("logs", "data-query-worker")
    raise RuntimeError("worker did not become healthy")


def run_smoke() -> None:
    compose("up", "--build", "--detach")
    wait_ready()
    compose(
        "exec",
        "--no-TTY",
        "data-query-worker",
        "python",
        "scripts/smoke_inside.py",
        "initial",
    )
    compose("restart", "data-query-worker")
    wait_ready()
    compose(
        "exec",
        "--no-TTY",
        "data-query-worker",
        "python",
        "scripts/smoke_inside.py",
        "restart",
    )


def main() -> None:
    parser = argparse.ArgumentParser(description="Run the real-container smoke test")
    parser.add_argument("--keep", action="store_true", help="leave the service and volume running")
    args = parser.parse_args()
    try:
        run_smoke()
        print("OMERO.DataQueryWorker container smoke passed")
    finally:
        if not args.keep:
            compose("down", "--volumes", "--remove-orphans")


if __name__ == "__main__":
    main()
