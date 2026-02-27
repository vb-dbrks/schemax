"""
Canonical Unity Catalog fixture data: column types and rich sample operations.

Covers the art of the possible in Unity Catalog: all supported data types,
multiple table formats (Delta, Iceberg), partitioning/clustering, views,
materialized views (with schedule, partition, cluster), SQL and Python functions,
managed and external volumes.
"""

from __future__ import annotations

from tests.utils import OperationBuilder

# Canonical Unity Catalog column types for fixture coverage (name, type, nullable, comment)
UNITY_CATALOG_COLUMN_TYPES: list[tuple[str, str, bool, str]] = [
    ("user_id", "BIGINT", False, "User ID"),
    ("email", "STRING", True, "Email address"),
    ("c_tinyint", "TINYINT", True, "Tiny int"),
    ("c_smallint", "SMALLINT", True, "Small int"),
    ("c_int", "INT", True, "Int"),
    ("c_float", "FLOAT", True, "Float"),
    ("c_double", "DOUBLE", True, "Double"),
    ("c_decimal", "DECIMAL(10,2)", True, "Decimal"),
    ("c_string", "STRING", True, "String"),
    ("c_binary", "BINARY", True, "Binary"),
    ("c_boolean", "BOOLEAN", True, "Boolean"),
    ("c_date", "DATE", True, "Date"),
    ("c_timestamp", "TIMESTAMP", True, "Timestamp"),
    ("c_timestamp_ntz", "TIMESTAMP_NTZ", True, "Timestamp NTZ"),
    ("c_interval", "INTERVAL DAY TO SECOND", True, "Interval"),
    ("c_array", "ARRAY<STRING>", True, "Array"),
    ("c_map", "MAP<STRING, INT>", True, "Map"),
    ("c_struct", "STRUCT<id: BIGINT, name: STRING>", True, "Struct"),
    ("c_geography", "GEOGRAPHY(4326)", True, "Geography"),
    ("c_geometry", "GEOMETRY(0)", True, "Geometry"),
    ("c_variant", "VARIANT", True, "Variant"),
    ("c_object", "OBJECT", True, "Object (semi-structured)"),
]


def make_rich_sample_operations() -> list:
    """Build rich sample operations: full data types, tables, views, functions, volumes, MVs."""
    b = OperationBuilder()
    ops = [
        b.catalog.add_catalog("cat_123", "bronze", op_id="op_001"),
        b.schema.add_schema("schema_456", "raw", "cat_123", op_id="op_002"),
        b.table.add_table("table_789", "users", "schema_456", "delta", op_id="op_003"),
    ]
    # All data types table: one column per type (col_001 .. col_022)
    for i, (name, col_type, nullable, comment) in enumerate(UNITY_CATALOG_COLUMN_TYPES):
        col_id = f"col_{i + 1:03d}"
        ops.append(
            b.column.add_column(
                col_id,
                "table_789",
                name,
                col_type,
                nullable=nullable,
                comment=comment,
                op_id=f"op_col_{i + 1:03d}",
            )
        )
    # Second table: Iceberg
    ops.extend(
        [
            b.table.add_table(
                "table_ice",
                "events_iceberg",
                "schema_456",
                "iceberg",
                comment="Iceberg table",
                op_id="op_tice",
            ),
            b.column.add_column("col_ice_id", "table_ice", "id", "BIGINT", False, op_id="op_cice1"),
            b.column.add_column(
                "col_ice_ts", "table_ice", "event_ts", "TIMESTAMP_NTZ", True, op_id="op_cice2"
            ),
        ]
    )
    # Third table: partitioned and clustered
    ops.extend(
        [
            b.table.add_table(
                "table_pc",
                "events_partitioned",
                "schema_456",
                "delta",
                partition_columns=["dt"],
                cluster_columns=["id"],
                op_id="op_tpc",
            ),
            b.column.add_column("col_pc_id", "table_pc", "id", "BIGINT", False, op_id="op_cpc1"),
            b.column.add_column("col_pc_dt", "table_pc", "dt", "DATE", True, op_id="op_cpc2"),
        ]
    )
    # View
    ops.append(
        b.view.add_view(
            "view_001",
            "users_view",
            "schema_456",
            "SELECT user_id, email FROM users",
            comment="Users view",
            op_id="op_v1",
        )
    )
    # SQL function
    ops.append(
        b.function.add_function(
            "func_sql_001",
            "add_one",
            "schema_456",
            language="SQL",
            return_type="INT",
            body="RETURN (x + 1)",
            parameters=[{"name": "x", "dataType": "INT"}],
            comment="SQL scalar",
            op_id="op_f1",
        )
    )
    # Python function
    ops.append(
        b.function.add_function(
            "func_py_001",
            "bmi",
            "schema_456",
            language="PYTHON",
            return_type="DOUBLE",
            body="return weight_kg / (height_m ** 2)",
            parameters=[
                {"name": "weight_kg", "dataType": "DOUBLE"},
                {"name": "height_m", "dataType": "DOUBLE"},
            ],
            comment="Python UDF",
            op_id="op_f2",
        )
    )
    # Managed volume
    ops.append(
        b.volume.add_volume(
            "vol_001",
            "managed_vol",
            "schema_456",
            volume_type="managed",
            comment="Managed",
            op_id="op_vol1",
        )
    )
    # External volume (location placeholder)
    ops.append(
        b.volume.add_volume(
            "vol_002",
            "external_vol",
            "schema_456",
            volume_type="external",
            location="abfss://container@storage.dfs.core.windows.net/path",
            op_id="op_vol2",
        )
    )
    # Materialized view with schedule
    ops.append(
        b.materialized_view.add_materialized_view(
            "mv_001",
            "daily_summary",
            "schema_456",
            "SELECT id, dt, count(1) AS cnt FROM events_partitioned GROUP BY id, dt",
            comment="Daily summary MV",
            refresh_schedule="EVERY 1 DAY",
            op_id="op_mv1",
        )
    )
    # Materialized view with CLUSTER BY
    ops.append(
        b.materialized_view.add_materialized_view(
            "mv_002",
            "clustered_mv",
            "schema_456",
            "SELECT id, user_id FROM users",
            cluster_columns=["id"],
            op_id="op_mv2",
        )
    )
    return ops
