## unity_full Fixture Project

Snapshot built from the rich sample operations (`make_rich_sample_operations()`).

Contains:
- 1 catalog, 1 schema
- Table with all Unity Catalog column types (22 types including OBJECT, VARIANT, GEOGRAPHY, etc.)
- Iceberg table, Delta table with PARTITIONED BY/CLUSTER BY
- View, SQL function, Python UDF
- Managed and external volumes
- Materialized views (with schedule and CLUSTER BY)

Use `resource_workspace("unity_full")` in tests that need a full catalog without building ops in conftest.
