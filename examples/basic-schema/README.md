# Basic Schema Example

This is a minimal example project demonstrating Schematic v4 project structure.

## What's Inside

- `.schematic/project.json` - V4 project configuration with multi-environment support
- `.schematic/changelog.json` - Operation log (currently just the implicit catalog)
- `.schematic/snapshots/` - Directory for snapshot files (empty for now)
- `.schematic/migrations/` - Directory for generated SQL migrations (empty for now)

## Project Configuration

**Environments:**
- `dev` - Development (dev_example catalog)
- `test` - Test/Staging (test_example catalog)
- `prod` - Production (prod_example catalog)

**Catalog Mode:** Single catalog mode with implicit catalog

## Usage

### Validate Project
```bash
schematic validate
```

### Generate SQL
```bash
schematic sql --output migration.sql --target dev
```

### Create Snapshot
```bash
# After making changes via VS Code extension
schematic snapshot create "Initial schema" --version v0.1.0
```

### Apply to Environment
```bash
schematic apply --target dev --profile my-profile --warehouse-id <warehouse-id>
```

## Next Steps

1. Open this directory in VS Code
2. Run command: "Schematic: Open Designer"
3. Start designing your schema!

