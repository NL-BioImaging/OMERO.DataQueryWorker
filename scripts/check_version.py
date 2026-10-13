"""Check release versions without installing the worker or its dependencies."""

import argparse
import runpy
import tomllib
from pathlib import Path


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--tag", help="Release tag, which must be v<package version>")
    args = parser.parse_args()
    root = Path(__file__).resolve().parents[1]
    with (root / "pyproject.toml").open("rb") as metadata:
        package_version = tomllib.load(metadata)["project"]["version"]
    runtime_version = runpy.run_path(root / "src/omero_data_query_worker/__init__.py")[
        "__version__"
    ]
    if runtime_version != package_version:
        parser.exit(1, f"Runtime version {runtime_version} != package version {package_version}\n")
    if args.tag is not None and args.tag != f"v{package_version}":
        parser.exit(1, f"Release tag {args.tag} != expected v{package_version}\n")
    print(f"Versions match: {package_version}")


if __name__ == "__main__":
    main()
