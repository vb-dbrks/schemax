"""
Generate the unity_full resource project from the rich sample operations.

Run from packages/python-sdk:
  uv run python tests/scripts/generate_unity_full_project.py

Creates tests/resources/projects/unity_full/ with .schemax/project.json,
changelog.json, and snapshots/v0.1.0.json (state + operations from make_rich_sample_operations).
"""

from __future__ import annotations

import hashlib
import json
import sys
import uuid
from datetime import UTC, datetime
from pathlib import Path


# Run from packages/python-sdk so schemax and tests are importable
def _main() -> None:
    root = Path(__file__).resolve().parents[2]  # packages/python-sdk
    if str(root) not in sys.path:
        sys.path.insert(0, str(root))
    resource_root = root / "tests" / "resources" / "projects" / "unity_full"
    schemax_dir = resource_root / ".schemax"
    snapshots_dir = schemax_dir / "snapshots"
    schemax_dir.mkdir(parents=True, exist_ok=True)
    snapshots_dir.mkdir(parents=True, exist_ok=True)

    from schemax.providers.unity.models import UnityState
    from schemax.providers.unity.state_reducer import apply_operations
    from tests.utils.fixture_data import make_rich_sample_operations

    ops = make_rich_sample_operations()
    empty = UnityState(catalogs=[])
    state = apply_operations(empty, ops)
    state_dict = state.model_dump(by_alias=True)

    ops_with_ids = [op.model_dump(by_alias=True) for op in ops]
    state_hash = hashlib.sha256(
        json.dumps({"state": state_dict, "operations": ops_with_ids}, sort_keys=True).encode()
    ).hexdigest()

    snap_id = f"snap_{uuid.uuid4()}"
    ts = datetime.now(UTC).isoformat()
    snapshot_file = {
        "id": snap_id,
        "version": "v0.1.0",
        "name": "Rich fixture snapshot",
        "ts": ts,
        "createdBy": "generate_unity_full_project",
        "state": state_dict,
        "operations": ops_with_ids,
        "previousSnapshot": None,
        "hash": state_hash,
        "tags": ["fixture", "rich"],
        "comment": "Snapshot built from make_rich_sample_operations(): all data types, Delta/Iceberg, partitioning, views, functions, volumes, MVs.",
    }
    snapshot_path = snapshots_dir / "v0.1.0.json"
    with open(snapshot_path, "w", encoding="utf-8") as f:
        json.dump(snapshot_file, f, indent=2)

    project = {
        "version": 4,
        "name": "unity_full",
        "provider": {
            "type": "unity",
            "version": "1.0.0",
            "environments": {
                "dev": {
                    "topLevelName": "dev_unity_full",
                    "description": "Development environment",
                    "allowDrift": True,
                    "requireSnapshot": False,
                    "autoCreateTopLevel": True,
                    "autoCreateSchemaxSchema": True,
                    "catalogMappings": {"__implicit__": "dev_unity_full"},
                },
                "test": {
                    "topLevelName": "test_unity_full",
                    "description": "Test environment",
                    "allowDrift": False,
                    "requireSnapshot": True,
                    "autoCreateTopLevel": True,
                    "autoCreateSchemaxSchema": True,
                    "catalogMappings": {"__implicit__": "test_unity_full"},
                },
                "prod": {
                    "topLevelName": "prod_unity_full",
                    "description": "Production environment",
                    "allowDrift": False,
                    "requireSnapshot": True,
                    "requireApproval": False,
                    "autoCreateTopLevel": False,
                    "autoCreateSchemaxSchema": True,
                    "catalogMappings": {"__implicit__": "prod_unity_full"},
                },
            },
        },
        "snapshots": [
            {
                "id": snap_id,
                "version": "v0.1.0",
                "name": snapshot_file["name"],
                "ts": ts,
                "createdBy": snapshot_file["createdBy"],
                "file": ".schemax/snapshots/v0.1.0.json",
                "previousSnapshot": None,
                "opsCount": len(ops_with_ids),
                "hash": state_hash,
                "tags": snapshot_file["tags"],
                "comment": snapshot_file["comment"],
            }
        ],
        "deployments": [],
        "settings": {"autoIncrementVersion": True, "versionPrefix": "v"},
        "latestSnapshot": "v0.1.0",
    }
    with open(schemax_dir / "project.json", "w", encoding="utf-8") as f:
        json.dump(project, f, indent=2)

    changelog = {
        "version": 1,
        "sinceSnapshot": "v0.1.0",
        "ops": [],
        "lastModified": ts,
    }
    with open(schemax_dir / "changelog.json", "w", encoding="utf-8") as f:
        json.dump(changelog, f, indent=2)

    readme = resource_root / "README.md"
    readme.write_text(
        """## unity_full Fixture Project

Snapshot built from the rich sample operations (`make_rich_sample_operations()`).

Contains:
- 1 catalog, 1 schema
- Table with all Unity Catalog column types (22 types including OBJECT, VARIANT, GEOGRAPHY, etc.)
- Iceberg table, Delta table with PARTITIONED BY/CLUSTER BY
- View, SQL function, Python UDF
- Managed and external volumes
- Materialized views (with schedule and CLUSTER BY)

Use `resource_workspace("unity_full")` in tests that need a full catalog without building ops in conftest.
"""
    )
    print(f"Generated {resource_root}")
    print(f"  Snapshot: {len(ops_with_ids)} ops, state hash {state_hash[:16]}...")


if __name__ == "__main__":
    _main()
