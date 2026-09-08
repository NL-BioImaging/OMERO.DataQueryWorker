"""Bounded OS fault probes, executed only in disposable containers."""

from __future__ import annotations

import argparse
import errno
import json
import os
import socket
import subprocess
import sys


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("case", choices=["isolation", "pids", "oom"])
    case = parser.parse_args().case
    if case == "oom":
        # Cgroup budget is 96 MiB; this container is expected to be OOM killed.
        bytearray(512 * 1024 * 1024)
        raise AssertionError("OOM limit was not enforced")
    if case == "isolation":
        assert os.getuid() != 0
        try:
            with open("/app/forbidden", "w") as handle:
                handle.write("unexpected")
        except OSError as exc:
            assert exc.errno in (errno.EROFS, errno.EACCES)
        else:
            raise AssertionError("Root filesystem is writable")
        try:
            socket.create_connection(("1.1.1.1", 443), timeout=1)
        except OSError:
            pass
        else:
            raise AssertionError("Unexpected network egress")
    if case == "pids":
        children = []
        try:
            for _ in range(64):
                try:
                    children.append(
                        subprocess.Popen([sys.executable, "-c", "import time; time.sleep(30)"])
                    )
                except OSError as exc:
                    assert exc.errno == errno.EAGAIN
                    break
            else:
                raise AssertionError("PID limit was not enforced")
        finally:
            for child in children:
                child.terminate()
            for child in children:
                child.wait(timeout=5)
    print(json.dumps({"case": case, "status": "passed"}))


if __name__ == "__main__":
    main()
