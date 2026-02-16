#!/usr/bin/env python3
"""
Example: Generate SQL migration from schema changes

This script demonstrates how to use the SchemaX Python SDK with the
provider-based architecture to generate SQL migration scripts programmatically.
"""

from pathlib import Path
from schemax.core.storage import load_current_state


def main():
    # Set workspace path (directory containing .schemax/)
    workspace = Path(__file__).parent.parent / "basic-schema"

    print(f"Loading schema from: {workspace}")

    # Load current state, changelog, and provider
    state, changelog, provider = load_current_state(workspace)

    print(f"\nProvider: {provider.info.name} v{provider.info.version}")

    # Provider-specific stats (for Unity Catalog)
    if "catalogs" in state:
        print(f"Project: {len(state['catalogs'])} catalog(s)")
        for catalog in state["catalogs"]:
            print(f"  - {catalog['name']}: {len(catalog['schemas'])} schema(s)")

    print(f"\nChangelog: {len(changelog['ops'])} operation(s)")

    if changelog["ops"]:
        # Convert ops to Operation objects
        from schemax.providers.base.operations import Operation

        operations = [Operation(**op) for op in changelog["ops"]]

        # Generate SQL using provider's SQL generator
        generator = provider.get_sql_generator(state)
        sql = generator.generate_sql(operations)

        # Write to file
        output_file = workspace / "migration.sql"
        output_file.write_text(sql)

        print(f"\n✓ SQL written to: {output_file}")
        print(f"\nPreview (first 500 chars):")
        print("-" * 60)
        print(sql[:500])
        if len(sql) > 500:
            print("...")
    else:
        print("\n(No operations to generate SQL for)")


if __name__ == "__main__":
    main()
