# External Tables in Schematic

This guide covers how to create and manage external tables in Databricks Unity Catalog using Schematic.

## Table of Contents

- [Overview](#overview)
- [When to Use External Tables](#when-to-use-external-tables)
- [Prerequisites](#prerequisites)
- [Configuration](#configuration)
- [Creating External Tables](#creating-external-tables)
- [Advanced Features](#advanced-features)
- [Best Practices](#best-practices)
- [Troubleshooting](#troubleshooting)

## Overview

Unity Catalog supports two types of tables:

- **Managed Tables** (Recommended): Unity Catalog manages both metadata and data
- **External Tables**: Unity Catalog manages metadata only; data is stored in external locations

External tables are useful when you need to:
- Reference data you don't own or want to store outside Unity Catalog
- Integrate with existing data lakes
- Share data across multiple systems
- Maintain data in its original location

> **⚠️ Important**: Databricks recommends using managed tables for optimal performance and automatic maintenance. Use external tables only when necessary.
>
> [Learn more about managed tables](https://learn.microsoft.com/en-gb/azure/databricks/tables/managed)

## When to Use External Tables

### Good Use Cases ✅

- **Data Integration**: Connecting to existing data lakes or external systems
- **Data Sharing**: Multiple systems need access to the same data
- **Data Ownership**: Data is owned by another team or organization
- **Compliance**: Data must remain in specific locations for regulatory reasons
- **Migration**: Gradual migration of existing data to Unity Catalog

### When to Use Managed Tables Instead ❌

- **New Projects**: Start with managed tables for better performance
- **Full Control**: When you own and manage the data lifecycle
- **Optimal Performance**: Managed tables offer better query performance
- **Automatic Maintenance**: Let Unity Catalog handle data management

## Prerequisites

Before creating external tables in Schematic, you must:

1. **Configure External Locations** in Unity Catalog:
   ```sql
   CREATE EXTERNAL LOCATION IF NOT EXISTS raw_data_s3
   URL 's3://my-bucket/raw'
   WITH (STORAGE CREDENTIAL aws_s3_credential);
   ```

2. **Configure Storage Credentials** (if not already done):
   ```sql
   CREATE STORAGE CREDENTIAL IF NOT EXISTS aws_s3_credential
   WITH (AWS_IAM_ROLE = 'arn:aws:iam::123456789:role/my-role');
   ```

3. **Grant Permissions**:
   ```sql
   GRANT CREATE EXTERNAL TABLE ON EXTERNAL LOCATION raw_data_s3 TO `data-engineers`;
   GRANT READ FILES ON EXTERNAL LOCATION raw_data_s3 TO `data-engineers`;
   ```

> **Note**: External locations and storage credentials must be created outside of Schematic using SQL or the Databricks UI.

## Configuration

### Project Configuration

External locations are configured per environment in your `project.json` file. This allows different physical paths for dev, test, and prod environments.

#### Example Configuration

```json
{
  "version": 4,
  "name": "my_project",
  "provider": {
    "type": "unity",
    "version": "1.0.0",
    "environments": {
      "dev": {
        "topLevelName": "dev_analytics",
        "externalLocations": {
          "raw_data": {
            "path": "s3://dev-bucket/raw",
            "description": "Raw data ingestion zone"
          },
          "processed": {
            "path": "s3://dev-bucket/processed",
            "description": "Processed/curated data"
          },
          "logs": {
            "path": "s3://dev-logs-bucket/application",
            "description": "Application logs"
          }
        },
        "allowDrift": true,
        "requireSnapshot": false,
        "autoCreateTopLevel": true,
        "autoCreateSchematicSchema": true
      },
      "prod": {
        "topLevelName": "prod_analytics",
        "externalLocations": {
          "raw_data": {
            "path": "s3://prod-bucket/raw",
            "description": "Raw data ingestion zone"
          },
          "processed": {
            "path": "s3://prod-bucket/processed",
            "description": "Processed/curated data"
          },
          "logs": {
            "path": "s3://prod-logs-bucket/application",
            "description": "Application logs"
          }
        },
        "allowDrift": false,
        "requireSnapshot": true,
        "autoCreateTopLevel": true,
        "autoCreateSchematicSchema": true
      }
    }
  }
}
```

### Named External Locations

External locations use a **logical name** that maps to **environment-specific physical paths**:

| Logical Name | Dev Path | Prod Path |
|--------------|----------|-----------|
| `raw_data` | `s3://dev-bucket/raw` | `s3://prod-bucket/raw` |
| `processed` | `s3://dev-bucket/processed` | `s3://prod-bucket/processed` |
| `logs` | `s3://dev-logs-bucket/application` | `s3://prod-logs-bucket/application` |

This design enables **environment portability** – schemas can be promoted from dev to prod without changes.

## Creating External Tables

### Using the VS Code Extension

1. **Open the Designer**: `Cmd+Shift+P` → "Schematic: Open Designer"

2. **Add a Table**:
   - Select a schema in the sidebar
   - Click "Add Table"

3. **Configure the External Table**:
   - **Name**: Enter table name (e.g., `orders`)
   - **Table Type**: Select **External**
   - **Format**: Choose Delta or Iceberg
   - **External Location**: Select from dropdown (e.g., `processed`)
   - **Path (optional)**: Enter relative path (e.g., `orders/v1`)

4. **Preview**:
   - The UI shows the resolved location: `s3://dev-bucket/processed/orders/v1`

5. **Create**: Click "Add" to create the operation

### Using Python SDK

```python
from pathlib import Path
from schematic.storage_v4 import load_current_state, append_ops

workspace = Path.cwd()
state, changelog, provider = load_current_state(workspace)

# Create an external table operation
ops = [{
    "id": "op_001",
    "provider": "unity",
    "ts": "2025-01-15T10:00:00Z",
    "op": "unity.add_table",
    "target": "table_001",
    "payload": {
        "tableId": "table_001",
        "name": "orders",
        "schemaId": "schema_456",
        "format": "delta",
        "external": True,
        "externalLocationName": "processed",
        "path": "orders/v1"
    }
}]

append_ops(workspace, ops)
```

### Generated SQL

When you generate SQL for an external table:

```bash
schematic sql --target dev
```

Output:
```sql
-- External Table: orders
-- Location Name: processed
-- Relative Path: orders/v1
-- Resolved Location: s3://dev-bucket/processed/orders/v1
-- WARNING: External tables must reference pre-configured external locations
-- WARNING: Databricks recommends using managed tables for optimal performance
-- Learn more: https://learn.microsoft.com/en-gb/azure/databricks/tables/managed

CREATE EXTERNAL TABLE IF NOT EXISTS `dev_analytics`.`sales`.`orders` ()
USING DELTA
LOCATION 's3://dev-bucket/processed/orders/v1';
```

For production:
```bash
schematic sql --target prod
```

Output:
```sql
CREATE EXTERNAL TABLE IF NOT EXISTS `prod_analytics`.`sales`.`orders` ()
USING DELTA
LOCATION 's3://prod-bucket/processed/orders/v1';
```

## Advanced Features

### Partitioning

Partitioning improves query performance by organizing data into directories:

```json
{
  "op": "unity.add_table",
  "payload": {
    "tableId": "table_001",
    "name": "events",
    "schemaId": "schema_456",
    "format": "delta",
    "external": True,
    "externalLocationName": "raw_data",
    "path": "events",
    "partitionColumns": ["event_date", "region"]
  }
}
```

Generated SQL:
```sql
CREATE EXTERNAL TABLE IF NOT EXISTS `dev_analytics`.`raw`.`events` ()
USING DELTA
PARTITIONED BY (event_date, region)
LOCATION 's3://dev-bucket/raw/events';
```

### Liquid Clustering

Liquid clustering optimizes query performance without rigid partitioning:

```json
{
  "op": "unity.add_table",
  "payload": {
    "tableId": "table_002",
    "name": "user_events",
    "schemaId": "schema_456",
    "format": "delta",
    "clusterColumns": ["user_id", "event_type"]
  }
}
```

Generated SQL:
```sql
CREATE TABLE IF NOT EXISTS `dev_analytics`.`analytics`.`user_events` ()
USING DELTA
CLUSTER BY (user_id, event_type);
```

> **Note**: Clustering works with both managed and external tables.

### Combining Features

You can combine external tables with partitioning and clustering:

```json
{
  "op": "unity.add_table",
  "payload": {
    "tableId": "table_003",
    "name": "orders",
    "schemaId": "schema_456",
    "format": "delta",
    "external": True,
    "externalLocationName": "processed",
    "path": "orders/partitioned",
    "partitionColumns": ["order_date"],
    "clusterColumns": ["customer_id", "status"]
  }
}
```

Generated SQL:
```sql
CREATE EXTERNAL TABLE IF NOT EXISTS `dev_analytics`.`sales`.`orders` ()
USING DELTA
PARTITIONED BY (order_date)
CLUSTER BY (customer_id, status)
LOCATION 's3://dev-bucket/processed/orders/partitioned';
```

## Best Practices

### 1. Use Descriptive Location Names

```json
// ✅ Good
"externalLocations": {
  "raw_ingestion": { "path": "s3://bucket/raw" },
  "curated_analytics": { "path": "s3://bucket/curated" },
  "archive_cold_storage": { "path": "s3://archive/cold" }
}

// ❌ Bad
"externalLocations": {
  "loc1": { "path": "s3://bucket/raw" },
  "loc2": { "path": "s3://bucket/curated" }
}
```

### 2. Add Descriptions

```json
{
  "raw_data": {
    "path": "s3://my-bucket/raw",
    "description": "Raw data ingestion zone - unprocessed source data"
  }
}
```

### 3. Organize with Paths

Use the optional `path` field to organize tables within a location:

```
s3://bucket/processed/
├── orders/
│   ├── v1/
│   └── v2/
├── customers/
│   └── current/
└── products/
    └── catalog/
```

### 4. Prefer Managed Tables

Only use external tables when you have a specific requirement. Managed tables offer:
- Better performance
- Automatic optimization
- Simpler lifecycle management
- Automatic data cleanup on DROP

### 5. Document External Location Prerequisites

Add comments to your project configuration:

```json
{
  "externalLocations": {
    "// NOTE": "External locations must be pre-configured in Unity Catalog",
    "// See": "https://docs.databricks.com/sql/language-manual/sql-ref-external-locations.html",
    "raw_data": {
      "path": "s3://my-bucket/raw",
      "description": "Raw data zone"
    }
  }
}
```

### 6. Use Environment-Specific Paths

Always configure different paths for each environment:

```json
{
  "dev": {
    "externalLocations": {
      "data": { "path": "s3://dev-bucket/data" }
    }
  },
  "prod": {
    "externalLocations": {
      "data": { "path": "s3://prod-bucket/data" }
    }
  }
}
```

## Troubleshooting

### Error: "External location 'xyz' not found in environment"

**Cause**: The external location name doesn't exist in your project configuration.

**Solution**: Add the location to your `project.json`:

```json
{
  "environments": {
    "dev": {
      "externalLocations": {
        "xyz": {
          "path": "s3://bucket/path",
          "description": "Description here"
        }
      }
    }
  }
}
```

### Error: "External location requires environment configuration"

**Cause**: No external locations are configured for the environment.

**Solution**: Add at least one external location to the environment configuration.

### Error: "requires environment configuration with externalLocations defined"

**Cause**: Trying to create an external table without environment configuration.

**Solution**: Always specify `--target` when generating SQL for external tables:

```bash
schematic sql --target dev
```

### Warning: "Databricks recommends using managed tables"

**Not an error**: This is a reminder that managed tables are preferred.

**Action**: Review if you truly need an external table. If yes, you can safely ignore this warning.

### Data Not Deleted When Dropping External Tables

**Expected Behavior**: When you drop an external table, only the metadata is removed. The underlying data files remain in the external location.

**Why**: External tables don't own the data, so Unity Catalog doesn't delete it.

**Cleanup**: If you want to delete the data, you must do so manually:

```bash
# Example: AWS S3
aws s3 rm s3://bucket/path --recursive

# Example: Azure Blob
az storage blob delete-batch --source container --pattern "path/*"
```

### Schema Drift

**Issue**: External data can be modified outside Unity Catalog, causing schema drift.

**Solutions**:
- Use Delta Lake format (supports schema evolution)
- Implement data validation pipelines
- Document external data ownership and change processes
- Regular schema reconciliation

### Location Path Resolution

If you're unsure what path will be used:

1. **VS Code Extension**: Check the "Resolved Location" preview in the Add Table dialog
2. **Python/CLI**: Run `schematic sql --target dev` and check the SQL comments
3. **Table Designer**: View the resolved location for existing external tables

## CLI Reference

### Generate SQL

```bash
# Generate SQL with location resolution
schematic sql --target dev --output migration.sql

# View location mappings
schematic sql --target prod
```

Output includes:
```
External Tables (2):
  • orders: processed/orders → s3://prod-bucket/processed/orders
  • logs: logs/(base) → s3://prod-logs-bucket/application
```

### Apply SQL

```bash
# Apply to dev environment
schematic apply --target dev --profile dev-admin --warehouse-id abc123

# Dry run (preview only)
schematic apply --target prod --dry-run
```

## Additional Resources

- [Databricks External Locations Documentation](https://docs.databricks.com/sql/language-manual/sql-ref-external-locations.html)
- [Unity Catalog Best Practices](https://docs.databricks.com/data-governance/unity-catalog/best-practices.html)
- [Managed vs External Tables](https://learn.microsoft.com/en-gb/azure/databricks/tables/managed)
- [Delta Lake External Tables](https://docs.delta.io/latest/delta-batch.html#create-a-table)

## Examples

See working examples in the `examples/` directory:

- `examples/basic-schema/` - Simple external table setup
- `examples/python-scripts/generate_sql.py` - Python SDK usage
- `examples/github-actions/` - CI/CD with external tables

## Support

For issues or questions:
- Check existing [GitHub Issues](https://github.com/your-org/schematic/issues)
- Review [Architecture Documentation](ARCHITECTURE.md)
- Consult [Development Guide](DEVELOPMENT.md)

