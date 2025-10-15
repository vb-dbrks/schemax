#!/usr/bin/env python3
"""
Example: Compare two schema versions

This script demonstrates how to load and compare different schema versions
using the SchemaX Python SDK with provider-based architecture.
"""

from pathlib import Path
from schemax.storage_v3 import read_snapshot, read_project


def main():
    workspace = Path(__file__).parent.parent / "basic-schema"

    print(f"Loading project from: {workspace}")

    # Load project metadata
    project = read_project(workspace)

    print(f"\nProject: {project['name']}")
    print(f"Provider: {project['provider']['type']} v{project['provider']['version']}")
    print(f"Snapshots: {len(project['snapshots'])}")

    for snapshot_meta in project["snapshots"]:
        print(f"\n  Version: {snapshot_meta['version']}")
        print(f"  Name: {snapshot_meta['name']}")
        print(f"  Operations: {snapshot_meta['opsCount']}")
        print(f"  Created: {snapshot_meta['ts']}")

        # Load full snapshot
        snapshot = read_snapshot(workspace, snapshot_meta["version"])

        # Count objects (works for Unity Catalog provider)
        if "catalogs" in snapshot["state"]:
            catalogs = len(snapshot["state"]["catalogs"])
            schemas = sum(len(c.get("schemas", [])) for c in snapshot["state"]["catalogs"])
            tables = sum(
                len(s.get("tables", []))
                for c in snapshot["state"]["catalogs"]
                for s in c.get("schemas", [])
            )

            print(f"  Objects: {catalogs} catalogs, {schemas} schemas, {tables} tables")


if __name__ == "__main__":
    main()
