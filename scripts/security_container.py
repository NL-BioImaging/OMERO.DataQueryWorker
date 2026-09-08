"""Linux release gate. All destructive fault probes use new disposable containers."""

from __future__ import annotations

import argparse
import json
import subprocess
import uuid
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def run(*args: str, check: bool = True) -> subprocess.CompletedProcess[str]:
    return subprocess.run(["docker", *args], cwd=ROOT, check=check, text=True)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--image", default="omero-data-query-worker:smoke")
    args = parser.parse_args()
    image = "dqw-security-tests:" + uuid.uuid4().hex[:12]
    run(
        "build",
        "-f",
        "deploy/Dockerfile.tests",
        "--build-arg",
        f"WORKER_IMAGE={args.image}",
        "-t",
        image,
        ".",
    )
    security = [
        "--network",
        "none",
        "--read-only",
        "--cap-drop",
        "ALL",
        "--security-opt",
        "no-new-privileges",
        "--tmpfs",
        "/tmp:size=256m,mode=1777",
    ]
    try:
        run("run", "--rm", *security, "--memory", "2g", "--pids-limit", "128", image)
        for case in ("isolation", "pids"):
            run(
                "run",
                "--rm",
                *security,
                "--memory",
                "256m",
                "--pids-limit",
                "32",
                image,
                "python",
                "scripts/security_inside.py",
                case,
            )
        name = "dqw-oom-" + uuid.uuid4().hex[:12]
        try:
            result = run(
                "run",
                "--name",
                name,
                *security,
                "--memory",
                "96m",
                "--memory-swap",
                "96m",
                "--pids-limit",
                "32",
                image,
                "python",
                "scripts/security_inside.py",
                "oom",
                check=False,
            )
            state = json.loads(subprocess.check_output(["docker", "inspect", name], text=True))[0][
                "State"
            ]
            assert result.returncode == 137 and state["OOMKilled"], state
        finally:
            run("rm", "-f", "-v", name, check=False)
        print("Linux security gates passed: tests, filesystem, egress, PID, OOM")
    finally:
        run("image", "rm", image, check=False)


if __name__ == "__main__":
    main()
