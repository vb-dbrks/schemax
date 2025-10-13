#!/usr/bin/env python3
"""
Example: Generate SQL migration from schema changes

This script demonstrates how to use the SchemaX Python SDK to generate
SQL migration scripts programmatically.
"""

from pathlib import Path
from schemax.storage import load_current_state
from schemax.sql_generator import SQLGenerator


def main():
    # Set workspace path (directory containing .schemax/)
    workspace = Path(__file__).parent.parent / "basic-schema"

    print(f"Loading schema from: {workspace}")

    # Load current state and changelog
    state, changelog = load_current_state(workspace)

    print(f"\nProject: {len(state.catalogs)} catalogs")
    for catalog in state.catalogs:
        print(f"  - {catalog.name}: {len(catalog.schemas)} schemas")

    print(f"\nChangelog: {len(changelog.ops)} operations")

    if changelog.ops:
        # Generate SQL
        generator = SQLGenerator(state)
        sql = generator.generate_sql(changelog.ops)

        # Write to file
        output_file = workspace / "migration.sql"
        output_file.write_text(sql)

        print(f"\n✓ SQL written to: {output_file}")
    else:
        print("\n(No operations to generate SQL for)")


if __name__ == "__main__":
    main()
