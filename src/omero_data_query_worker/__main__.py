from __future__ import annotations

import argparse
import json
import urllib.request

import uvicorn

from .config import Settings
from .operations import Credentials


def main() -> None:
    parser = argparse.ArgumentParser()
    commands = parser.add_subparsers(dest="command")
    purge = commands.add_parser("purge", help="Purge using the running worker's lease manager")
    selectors = purge.add_mutually_exclusive_group(required=True)
    selectors.add_argument("--source-id")
    selectors.add_argument("--scope-id")
    selectors.add_argument("--result-id")
    purge.add_argument("--apply", action="store_true", help="Default is dry-run")
    args = parser.parse_args()
    if args.command == "purge":
        payload = {
            "source_id": args.source_id,
            "scope_id": args.scope_id,
            "result_id": args.result_id,
            "dry_run": not args.apply,
        }
        token = Credentials(Settings.from_env()).tokens()[0]
        request = urllib.request.Request(
            "http://127.0.0.1:8080/v1/cache/purge",
            data=json.dumps(payload).encode(),
            headers={"Content-Type": "application/json", "Authorization": f"Bearer {token}"},
        )
        with urllib.request.urlopen(request, timeout=30) as response:
            print(response.read().decode())
        return
    uvicorn.run(
        "omero_data_query_worker.app:app",
        host="0.0.0.0",  # noqa: S104 - the container API must listen on its private network
        port=8080,
        proxy_headers=False,
        server_header=False,
    )


if __name__ == "__main__":
    main()
