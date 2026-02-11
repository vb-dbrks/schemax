## `schematic_demo` Fixture Project

This project fixture is used by integration tests that need realistic imported
state (catalogs/schemas/tables/views/metadata) instead of synthetic test ops.

### Contents

- `.schematic/project.json`
- `.schematic/changelog.json`
- `.schematic/snapshots/v0.1.0.json`

### Source

Derived from a local import test project and sanitized for repository use.

### Refresh workflow

1. Re-import/update the source demo project locally.
2. Copy `.schematic` files into this directory.
3. Sanitize user-specific values (for example `createdBy`).
4. Run Python SDK tests before committing.
