from __future__ import annotations

import uvicorn


def main() -> None:
    uvicorn.run(
        "omero_data_query_worker.app:app",
        host="0.0.0.0",  # noqa: S104 - the container API must listen on its private network
        port=8080,
        proxy_headers=False,
        server_header=False,
    )


if __name__ == "__main__":
    main()
