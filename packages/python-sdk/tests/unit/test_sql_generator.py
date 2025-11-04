"""
Unit tests for Unity Catalog SQL Generator

Tests SQL generation for all 31 Unity Catalog operations.
Verifies SQL idempotency, correctness, and proper escaping.
"""

import pytest

from schematic.providers.base.operations import Operation
from schematic.providers.unity.sql_generator import UnitySQLGenerator
from tests.utils import OperationBuilder


class TestCatalogSQL:
    """Test SQL generation for catalog operations"""

    def test_add_catalog(self, sample_unity_state, assert_sql):
        """Test CREATE CATALOG SQL generation"""
        builder = OperationBuilder()
        generator = UnitySQLGenerator(sample_unity_state.model_dump(by_alias=True))

        op = builder.add_catalog("cat_001", "production", op_id="op_001")

        result = generator.generate_sql_for_operation(op)
        assert "CREATE CATALOG IF NOT EXISTS" in result.sql
        assert "`production`" in result.sql
        assert result.is_idempotent

        # Validate SQL syntax with SQLGlot
        assert_sql(result.sql)

    def test_add_catalog_special_characters(self, sample_unity_state):
        """Test CREATE CATALOG with special characters in name"""
        builder = OperationBuilder()
        generator = UnitySQLGenerator(sample_unity_state.model_dump(by_alias=True))

        op = builder.add_catalog("cat_001", "my-catalog.prod", op_id="op_001")

        result = generator.generate_sql_for_operation(op)
        # Should properly escape special characters
        assert "`my-catalog.prod`" in result.sql

    def test_rename_catalog(self, sample_unity_state):
        """Test ALTER CATALOG RENAME SQL generation"""
        builder = OperationBuilder()
        generator = UnitySQLGenerator(sample_unity_state.model_dump(by_alias=True))

        op = builder.rename_catalog("cat_123", "production", "bronze", op_id="op_002")

        result = generator.generate_sql_for_operation(op)
        assert "ALTER CATALOG" in result.sql
        assert "RENAME TO" in result.sql
        assert "`bronze`" in result.sql  # old name from sample state
        assert "`production`" in result.sql

    def test_drop_catalog(self, sample_unity_state):
        """Test DROP CATALOG SQL generation"""
        OperationBuilder()
        generator = UnitySQLGenerator(sample_unity_state.model_dump(by_alias=True))

        op = Operation(
            id="op_003",
            provider="unity",
            ts="2025-01-01T00:00:00Z",
            op="unity.drop_catalog",
            target="cat_123",
            payload={},
        )

        result = generator.generate_sql_for_operation(op)
        assert "DROP CATALOG" in result.sql
        assert "`bronze`" in result.sql


class TestSchemaSQL:
    """Test SQL generation for schema operations"""

    def test_add_schema(self, sample_unity_state):
        """Test CREATE SCHEMA SQL generation"""
        builder = OperationBuilder()
        generator = UnitySQLGenerator(sample_unity_state.model_dump(by_alias=True))

        op = builder.add_schema("schema_001", "analytics", "cat_123", op_id="op_004")

        result = generator.generate_sql_for_operation(op)
        assert "CREATE SCHEMA IF NOT EXISTS" in result.sql
        assert "`bronze`.`analytics`" in result.sql
        assert result.is_idempotent

    def test_rename_schema(self, sample_unity_state, assert_sql):
        """Test ALTER SCHEMA RENAME SQL generation"""
        builder = OperationBuilder()
        generator = UnitySQLGenerator(sample_unity_state.model_dump(by_alias=True))

        op = builder.rename_schema("schema_456", "processed", "raw", op_id="op_005")

        result = generator.generate_sql_for_operation(op)
        assert "ALTER SCHEMA" in result.sql
        assert "RENAME TO" in result.sql
        assert "`bronze`.`raw`" in result.sql
        assert "`processed`" in result.sql

        # Validate SQL syntax with SQLGlot
        assert_sql(result.sql)

    def test_drop_schema(self, sample_unity_state):
        """Test DROP SCHEMA SQL generation"""
        OperationBuilder()
        generator = UnitySQLGenerator(sample_unity_state.model_dump(by_alias=True))

        op = Operation(
            id="op_006",
            provider="unity",
            ts="2025-01-01T00:00:00Z",
            op="unity.drop_schema",
            target="schema_456",
            payload={},
        )

        result = generator.generate_sql_for_operation(op)
        assert "DROP SCHEMA" in result.sql
        assert "`bronze`.`raw`" in result.sql


class TestManagedLocationSQL:
    """Test SQL generation for managed locations"""

    def test_add_catalog_with_managed_location(self, sample_unity_state):
        """Test CREATE CATALOG with MANAGED LOCATION"""
        builder = OperationBuilder()
        generator = UnitySQLGenerator(
            sample_unity_state.model_dump(by_alias=True),
            managed_locations={
                "default": {
                    "paths": {
                        "dev": "s3://data-bucket/catalogs",
                    },
                    "description": "Default catalog storage",
                }
            },
            environment_name="dev",
        )

        op = builder.add_catalog("cat_999", "warehouse", op_id="op_managed_001")
        op.payload["managedLocationName"] = "default"

        result = generator.generate_sql_for_operation(op)
        assert "CREATE CATALOG IF NOT EXISTS" in result.sql
        assert "`warehouse`" in result.sql
        assert "MANAGED LOCATION 's3://data-bucket/catalogs'" in result.sql

    def test_add_schema_with_managed_location(self, sample_unity_state):
        """Test CREATE SCHEMA with MANAGED LOCATION"""
        builder = OperationBuilder()
        generator = UnitySQLGenerator(
            sample_unity_state.model_dump(by_alias=True),
            managed_locations={
                "sales_data": {
                    "paths": {
                        "dev": "s3://data-bucket/sales",
                    },
                    "description": "Sales data storage",
                }
            },
            environment_name="dev",
        )

        op = builder.add_schema("schema_999", "sales", "cat_123", op_id="op_managed_002")
        op.payload["managedLocationName"] = "sales_data"

        result = generator.generate_sql_for_operation(op)
        assert "CREATE SCHEMA IF NOT EXISTS" in result.sql
        assert "`bronze`.`sales`" in result.sql
        assert "MANAGED LOCATION 's3://data-bucket/sales'" in result.sql

    def test_managed_location_not_found(self, sample_unity_state):
        """Test error when managed location not found"""
        builder = OperationBuilder()
        generator = UnitySQLGenerator(
            sample_unity_state.model_dump(by_alias=True),
            managed_locations={
                "default": {"paths": {"dev": "s3://data-bucket/catalogs"}, "description": ""}
            },
            environment_name="dev",
        )

        op = builder.add_catalog("cat_999", "warehouse", op_id="op_managed_003")
        op.payload["managedLocationName"] = "nonexistent"

        result = generator.generate_sql_for_operation(op)
        # Should generate error comment
        assert "-- Error generating SQL" in result.sql or "not found" in result.sql.lower()


class TestTableSQL:
    """Test SQL generation for table operations"""

    def test_add_table_delta(self, sample_unity_state):
        """Test CREATE TABLE SQL generation for Delta table"""
        builder = OperationBuilder()
        generator = UnitySQLGenerator(sample_unity_state.model_dump(by_alias=True))

        op = builder.add_table("table_001", "events", "schema_456", "delta", op_id="op_007")

        result = generator.generate_sql_for_operation(op)
        assert "CREATE TABLE IF NOT EXISTS" in result.sql
        assert "`bronze`.`raw`.`events`" in result.sql
        assert "USING DELTA" in result.sql

    def test_add_table_iceberg(self, sample_unity_state):
        """Test CREATE TABLE SQL generation for Iceberg table"""
        builder = OperationBuilder()
        generator = UnitySQLGenerator(sample_unity_state.model_dump(by_alias=True))

        op = builder.add_table("table_001", "events", "schema_456", "iceberg", op_id="op_007")

        result = generator.generate_sql_for_operation(op)
        assert "USING ICEBERG" in result.sql

    def test_rename_table(self, sample_unity_state):
        """Test ALTER TABLE RENAME SQL generation"""
        builder = OperationBuilder()
        generator = UnitySQLGenerator(sample_unity_state.model_dump(by_alias=True))

        op = builder.rename_table("table_789", "customers", "users", op_id="op_008")

        result = generator.generate_sql_for_operation(op)
        assert "ALTER TABLE" in result.sql
        assert "RENAME TO" in result.sql
        assert "`bronze`.`raw`.`users`" in result.sql
        assert "`customers`" in result.sql

    def test_drop_table(self, sample_unity_state):
        """Test DROP TABLE SQL generation"""
        OperationBuilder()
        generator = UnitySQLGenerator(sample_unity_state.model_dump(by_alias=True))

        op = Operation(
            id="op_009",
            provider="unity",
            ts="2025-01-01T00:00:00Z",
            op="unity.drop_table",
            target="table_789",
            payload={},
        )

        result = generator.generate_sql_for_operation(op)
        assert "DROP TABLE" in result.sql
        assert "`bronze`.`raw`.`users`" in result.sql

    def test_set_table_comment(self, sample_unity_state):
        """Test COMMENT ON TABLE SQL generation"""
        builder = OperationBuilder()
        generator = UnitySQLGenerator(sample_unity_state.model_dump(by_alias=True))

        op = builder.set_table_comment("table_789", "User data table", op_id="op_010")

        result = generator.generate_sql_for_operation(op)
        assert "COMMENT ON TABLE" in result.sql
        assert "`bronze`.`raw`.`users`" in result.sql
        assert "'User data table'" in result.sql

    def test_set_table_property(self, sample_unity_state):
        """Test ALTER TABLE SET TBLPROPERTIES SQL generation"""
        builder = OperationBuilder()
        generator = UnitySQLGenerator(sample_unity_state.model_dump(by_alias=True))

        op = builder.set_table_property(
            "table_789", "delta.enableChangeDataFeed", "true", op_id="op_011"
        )

        result = generator.generate_sql_for_operation(op)
        assert "ALTER TABLE" in result.sql
        assert "SET TBLPROPERTIES" in result.sql
        assert "'delta.enableChangeDataFeed' = 'true'" in result.sql

    def test_unset_table_property(self, sample_unity_state):
        """Test ALTER TABLE UNSET TBLPROPERTIES SQL generation"""
        builder = OperationBuilder()
        generator = UnitySQLGenerator(sample_unity_state.model_dump(by_alias=True))

        op = builder.unset_table_property("table_789", "delta.enableChangeDataFeed", op_id="op_012")

        result = generator.generate_sql_for_operation(op)
        assert "ALTER TABLE" in result.sql
        assert "UNSET TBLPROPERTIES" in result.sql
        assert "'delta.enableChangeDataFeed'" in result.sql

    def test_add_external_table_basic(self, sample_unity_state):
        """Test CREATE EXTERNAL TABLE SQL generation with location"""
        generator = UnitySQLGenerator(
            sample_unity_state.model_dump(by_alias=True),
            external_locations={
                "raw_data": {
                    "paths": {"dev": "s3://my-bucket/raw"},
                    "description": "Raw data zone",
                }
            },
            environment_name="dev",
        )

        op = Operation(
            id="op_ext_001",
            provider="unity",
            ts="2025-01-01T00:00:00Z",
            op="unity.add_table",
            target="table_ext_001",
            payload={
                "tableId": "table_ext_001",
                "name": "external_orders",
                "schemaId": "schema_456",
                "format": "delta",
                "external": True,
                "externalLocationName": "raw_data",
            },
        )

        result = generator.generate_sql_for_operation(op)
        assert "CREATE EXTERNAL TABLE IF NOT EXISTS" in result.sql
        assert "`bronze`.`raw`.`external_orders`" in result.sql
        assert "USING DELTA" in result.sql
        assert "LOCATION 's3://my-bucket/raw'" in result.sql
        assert (
            "-- WARNING: External tables must reference pre-configured external locations"
            in result.sql
        )
        assert "-- WARNING: Databricks recommends using managed tables" in result.sql

    def test_add_external_table_with_path(self, sample_unity_state):
        """Test CREATE EXTERNAL TABLE with relative path"""
        generator = UnitySQLGenerator(
            sample_unity_state.model_dump(by_alias=True),
            external_locations={
                "processed": {
                    "paths": {"dev": "s3://data-lake/processed"},
                    "description": "Processed data",
                }
            },
            environment_name="dev",
        )

        op = Operation(
            id="op_ext_002",
            provider="unity",
            ts="2025-01-01T00:00:00Z",
            op="unity.add_table",
            target="table_ext_002",
            payload={
                "tableId": "table_ext_002",
                "name": "customers",
                "schemaId": "schema_456",
                "format": "delta",
                "external": True,
                "externalLocationName": "processed",
                "path": "customers/v1",
            },
        )

        result = generator.generate_sql_for_operation(op)
        assert "CREATE EXTERNAL TABLE IF NOT EXISTS" in result.sql
        assert "LOCATION 's3://data-lake/processed/customers/v1'" in result.sql
        assert "-- Relative Path: customers/v1" in result.sql

    def test_add_table_with_partitioning(self, sample_unity_state):
        """Test CREATE TABLE with PARTITIONED BY clause"""
        generator = UnitySQLGenerator(sample_unity_state.model_dump(by_alias=True))

        op = Operation(
            id="op_part_001",
            provider="unity",
            ts="2025-01-01T00:00:00Z",
            op="unity.add_table",
            target="table_part_001",
            payload={
                "tableId": "table_part_001",
                "name": "events",
                "schemaId": "schema_456",
                "format": "delta",
                "partitionColumns": ["event_date", "region"],
            },
        )

        result = generator.generate_sql_for_operation(op)
        assert "CREATE TABLE IF NOT EXISTS" in result.sql
        assert "PARTITIONED BY (event_date, region)" in result.sql

    def test_add_table_with_clustering(self, sample_unity_state):
        """Test CREATE TABLE with CLUSTER BY clause (liquid clustering)"""
        generator = UnitySQLGenerator(sample_unity_state.model_dump(by_alias=True))

        op = Operation(
            id="op_cluster_001",
            provider="unity",
            ts="2025-01-01T00:00:00Z",
            op="unity.add_table",
            target="table_cluster_001",
            payload={
                "tableId": "table_cluster_001",
                "name": "analytics_events",
                "schemaId": "schema_456",
                "format": "delta",
                "clusterColumns": ["user_id", "event_type"],
            },
        )

        result = generator.generate_sql_for_operation(op)
        assert "CREATE TABLE IF NOT EXISTS" in result.sql
        assert "CLUSTER BY (user_id, event_type)" in result.sql

    def test_add_external_table_with_all_features(self, sample_unity_state):
        """Test CREATE EXTERNAL TABLE with partitioning and clustering"""
        generator = UnitySQLGenerator(
            sample_unity_state.model_dump(by_alias=True),
            external_locations={
                "analytics": {
                    "paths": {"dev": "s3://analytics-bucket/data"},
                    "description": "Analytics",
                }
            },
            environment_name="dev",
        )

        op = Operation(
            id="op_full_001",
            provider="unity",
            ts="2025-01-01T00:00:00Z",
            op="unity.add_table",
            target="table_full_001",
            payload={
                "tableId": "table_full_001",
                "name": "orders",
                "schemaId": "schema_456",
                "format": "delta",
                "external": True,
                "externalLocationName": "analytics",
                "path": "orders/partitioned",
                "partitionColumns": ["order_date"],
                "clusterColumns": ["customer_id", "status"],
            },
        )

        result = generator.generate_sql_for_operation(op)
        assert "CREATE EXTERNAL TABLE IF NOT EXISTS" in result.sql
        assert "PARTITIONED BY (order_date)" in result.sql
        assert "CLUSTER BY (customer_id, status)" in result.sql
        assert "LOCATION 's3://analytics-bucket/data/orders/partitioned'" in result.sql

    def test_add_external_table_location_not_found(self, sample_unity_state):
        """Test CREATE EXTERNAL TABLE shows error when location not found"""
        generator = UnitySQLGenerator(
            sample_unity_state.model_dump(by_alias=True),
            external_locations={
                "raw_data": {"paths": {"dev": "s3://bucket/raw"}, "description": ""}
            },
            environment_name="dev",
        )

        op = Operation(
            id="op_err_001",
            provider="unity",
            ts="2025-01-01T00:00:00Z",
            op="unity.add_table",
            target="table_err_001",
            payload={
                "tableId": "table_err_001",
                "name": "missing_location",
                "schemaId": "schema_456",
                "format": "delta",
                "external": True,
                "externalLocationName": "nonexistent",
            },
        )

        result = generator.generate_sql_for_operation(op)
        assert "-- Error generating SQL" in result.sql
        assert "External location 'nonexistent' not found" in result.sql
        assert len(result.warnings) > 0

    def test_add_external_table_no_config(self, sample_unity_state):
        """Test CREATE EXTERNAL TABLE shows error when no external locations configured"""
        generator = UnitySQLGenerator(sample_unity_state.model_dump(by_alias=True))

        op = Operation(
            id="op_err_002",
            provider="unity",
            ts="2025-01-01T00:00:00Z",
            op="unity.add_table",
            target="table_err_002",
            payload={
                "tableId": "table_err_002",
                "name": "no_config",
                "schemaId": "schema_456",
                "format": "delta",
                "external": True,
                "externalLocationName": "raw_data",
            },
        )

        result = generator.generate_sql_for_operation(op)
        assert "-- Error generating SQL" in result.sql
        assert "requires project-level externalLocations configuration" in result.sql
        assert len(result.warnings) > 0


class TestColumnSQL:
    """Test SQL generation for column operations"""

    def test_add_column(self, sample_unity_state):
        """Test ALTER TABLE ADD COLUMN SQL generation"""
        builder = OperationBuilder()
        generator = UnitySQLGenerator(sample_unity_state.model_dump(by_alias=True))

        op = builder.add_column(
            "col_003",
            "table_789",
            "created_at",
            "TIMESTAMP",
            nullable=False,
            comment="Creation timestamp",
            op_id="op_013",
        )

        result = generator.generate_sql_for_operation(op)
        assert "ALTER TABLE" in result.sql
        assert "ADD COLUMN" in result.sql
        assert "`created_at`" in result.sql
        assert "TIMESTAMP" in result.sql
        assert "NOT NULL" in result.sql
        assert "COMMENT 'Creation timestamp'" in result.sql

    def test_add_column_nullable(self, sample_unity_state):
        """Test ADD COLUMN with nullable column"""
        builder = OperationBuilder()
        generator = UnitySQLGenerator(sample_unity_state.model_dump(by_alias=True))

        op = builder.add_column(
            "col_003",
            "table_789",
            "optional_field",
            "STRING",
            nullable=True,
            comment="None",
            op_id="op_013",
        )

        result = generator.generate_sql_for_operation(op)
        # Nullable columns typically don't need explicit NULL keyword
        assert "NOT NULL" not in result.sql

    def test_rename_column(self, sample_unity_state):
        """Test ALTER TABLE RENAME COLUMN SQL generation"""
        builder = OperationBuilder()
        generator = UnitySQLGenerator(sample_unity_state.model_dump(by_alias=True))

        op = builder.rename_column("col_001", "table_789", "id", "user_id", op_id="op_014")

        result = generator.generate_sql_for_operation(op)
        assert "ALTER TABLE" in result.sql
        assert "RENAME COLUMN" in result.sql
        assert "`user_id`" in result.sql  # old name
        assert "`id`" in result.sql  # new name

    def test_drop_column(self, sample_unity_state):
        """Test ALTER TABLE DROP COLUMN SQL generation"""
        builder = OperationBuilder()
        generator = UnitySQLGenerator(sample_unity_state.model_dump(by_alias=True))

        op = builder.drop_column("col_002", "table_789", op_id="op_015")

        result = generator.generate_sql_for_operation(op)
        assert "ALTER TABLE" in result.sql
        assert "DROP COLUMN" in result.sql
        assert "`email`" in result.sql

    def test_change_column_type(self, sample_unity_state):
        """Test ALTER TABLE ALTER COLUMN TYPE SQL generation"""
        builder = OperationBuilder()
        generator = UnitySQLGenerator(sample_unity_state.model_dump(by_alias=True))

        op = builder.change_column_type("col_001", "table_789", "DECIMAL(18,0)", op_id="op_016")

        result = generator.generate_sql_for_operation(op)
        assert "ALTER TABLE" in result.sql
        assert "ALTER COLUMN" in result.sql
        assert "`user_id`" in result.sql
        assert "TYPE DECIMAL(18,0)" in result.sql or "SET DATA TYPE DECIMAL(18,0)" in result.sql

    def test_set_column_comment(self, sample_unity_state):
        """Test ALTER TABLE ALTER COLUMN COMMENT SQL generation"""
        builder = OperationBuilder()
        generator = UnitySQLGenerator(sample_unity_state.model_dump(by_alias=True))

        op = builder.set_column_comment("col_001", "table_789", "Unique identifier", op_id="op_017")

        result = generator.generate_sql_for_operation(op)
        assert "ALTER TABLE" in result.sql
        assert "ALTER COLUMN" in result.sql
        assert "COMMENT 'Unique identifier'" in result.sql


class TestColumnTagSQL:
    """Test SQL generation for column tag operations"""

    def test_set_column_tag(self, sample_unity_state):
        """Test ALTER TABLE ALTER COLUMN SET TAGS SQL generation"""
        builder = OperationBuilder()
        generator = UnitySQLGenerator(sample_unity_state.model_dump(by_alias=True))

        op = builder.set_column_tag("col_002", "table_789", "PII", "true", op_id="op_018")

        result = generator.generate_sql_for_operation(op)
        assert "ALTER TABLE" in result.sql
        assert "ALTER COLUMN" in result.sql
        assert "SET TAGS" in result.sql
        assert "'PII' = 'true'" in result.sql

    def test_unset_column_tag(self, sample_unity_state):
        """Test ALTER TABLE ALTER COLUMN UNSET TAGS SQL generation"""
        builder = OperationBuilder()
        generator = UnitySQLGenerator(sample_unity_state.model_dump(by_alias=True))

        op = builder.unset_column_tag("col_002", "table_789", "PII", op_id="op_019")

        result = generator.generate_sql_for_operation(op)
        assert "ALTER TABLE" in result.sql
        assert "ALTER COLUMN" in result.sql
        assert "UNSET TAGS" in result.sql
        assert "'PII'" in result.sql


class TestConstraintSQL:
    """Test SQL generation for constraint operations"""

    @pytest.mark.skip(reason="NOT ENFORCED clause not implemented - see issue #20")
    def test_add_primary_key_constraint(self, sample_unity_state):
        """Test ALTER TABLE ADD PRIMARY KEY SQL generation"""
        builder = OperationBuilder()
        generator = UnitySQLGenerator(sample_unity_state.model_dump(by_alias=True))

        op = builder.add_constraint(
            "constraint_001",
            "table_789",
            "primary_key",
            ["col_001"],
            name="pk_users",
            notEnforced=True,
            op_id="op_020",
        )

        result = generator.generate_sql_for_operation(op)
        assert "ALTER TABLE" in result.sql
        assert "ADD CONSTRAINT" in result.sql
        assert "`pk_users`" in result.sql
        assert "PRIMARY KEY" in result.sql
        assert "`user_id`" in result.sql
        assert "NOT ENFORCED" in result.sql

    def test_add_foreign_key_constraint(self, sample_unity_state):
        """Test ALTER TABLE ADD FOREIGN KEY SQL generation"""
        builder = OperationBuilder()
        generator = UnitySQLGenerator(sample_unity_state.model_dump(by_alias=True))

        op = builder.add_constraint(
            "constraint_002",
            "table_789",
            "foreign_key",
            ["col_001"],
            name="fk_users_parent",
            parentTable="table_parent",
            parentColumns=["col_parent"],
            notEnforced=True,
            op_id="op_021",
        )

        result = generator.generate_sql_for_operation(op)
        assert "ALTER TABLE" in result.sql
        assert "ADD CONSTRAINT" in result.sql
        assert "FOREIGN KEY" in result.sql
        assert "REFERENCES" in result.sql

    def test_add_check_constraint(self, sample_unity_state):
        """Test ALTER TABLE ADD CHECK constraint SQL generation"""
        builder = OperationBuilder()
        generator = UnitySQLGenerator(sample_unity_state.model_dump(by_alias=True))

        op = builder.add_constraint(
            "constraint_003",
            "table_789",
            "check",
            ["col_001"],
            name="chk_users_id",
            expression="user_id > 0",
            notEnforced=True,
            op_id="op_022",
        )

        result = generator.generate_sql_for_operation(op)
        assert "ALTER TABLE" in result.sql
        assert "ADD CONSTRAINT" in result.sql
        assert "CHECK" in result.sql
        assert "user_id > 0" in result.sql

    def test_drop_constraint(self, sample_unity_state):
        """Test ALTER TABLE DROP CONSTRAINT SQL generation"""
        OperationBuilder()
        generator = UnitySQLGenerator(sample_unity_state.model_dump(by_alias=True))

        op = Operation(
            id="op_023",
            provider="unity",
            ts="2025-01-01T00:00:00Z",
            op="unity.drop_constraint",
            target="constraint_001",
            payload={"tableId": "table_789", "constraintName": "pk_users"},
        )

        result = generator.generate_sql_for_operation(op)
        assert "ALTER TABLE" in result.sql
        assert "DROP CONSTRAINT" in result.sql


@pytest.mark.skip(reason="Row filter SQL generation not implemented - see issue #19")
class TestRowFilterSQL:
    """Test SQL generation for row filter operations"""

    def test_add_row_filter(self, sample_unity_state):
        """Test CREATE ROW FILTER SQL generation"""
        builder = OperationBuilder()
        generator = UnitySQLGenerator(sample_unity_state.model_dump(by_alias=True))

        op = builder.add_row_filter(
            "filter_001",
            "table_789",
            "region_filter",
            "region = current_user()",
            enabled=True,
            op_id="op_024",
        )

        result = generator.generate_sql_for_operation(op)
        assert "ROW FILTER" in result.sql or "ROW ACCESS POLICY" in result.sql

    def test_update_row_filter(self, sample_unity_state):
        """Test ALTER ROW FILTER SQL generation"""
        builder = OperationBuilder()
        generator = UnitySQLGenerator(sample_unity_state.model_dump(by_alias=True))

        op = builder.update_row_filter(
            "filter_001",
            "table_789",
            udfExpression="region = current_user() AND active = 1",
            op_id="op_025",
        )

        result = generator.generate_sql_for_operation(op)
        # Might generate ALTER or DROP+CREATE
        assert "ROW FILTER" in result.sql or "ROW ACCESS POLICY" in result.sql

    def test_remove_row_filter(self, sample_unity_state):
        """Test DROP ROW FILTER SQL generation"""
        builder = OperationBuilder()
        generator = UnitySQLGenerator(sample_unity_state.model_dump(by_alias=True))

        op = builder.remove_row_filter("filter_001", "table_789", op_id="op_026")

        result = generator.generate_sql_for_operation(op)
        assert "DROP" in result.sql or "REMOVE" in result.sql


@pytest.mark.skip(reason="Column mask SQL generation not implemented - see issue #19")
class TestColumnMaskSQL:
    """Test SQL generation for column mask operations"""

    def test_add_column_mask(self, sample_unity_state):
        """Test CREATE COLUMN MASK SQL generation"""
        builder = OperationBuilder()
        generator = UnitySQLGenerator(sample_unity_state.model_dump(by_alias=True))

        op = builder.add_column_mask(
            "mask_001",
            "table_789",
            "col_002",
            "email_mask",
            "SHA2(email, 256)",
            enabled=True,
            op_id="op_027",
        )

        result = generator.generate_sql_for_operation(op)
        assert "MASK" in result.sql or "COLUMN MASK" in result.sql

    def test_update_column_mask(self, sample_unity_state):
        """Test ALTER COLUMN MASK SQL generation"""
        builder = OperationBuilder()
        generator = UnitySQLGenerator(sample_unity_state.model_dump(by_alias=True))

        op = builder.update_column_mask(
            "mask_001", "table_789", maskFunction="MASK(email, '*')", op_id="op_028"
        )

        result = generator.generate_sql_for_operation(op)
        # Might generate ALTER or DROP+CREATE
        assert "MASK" in result.sql or "COLUMN MASK" in result.sql

    def test_remove_column_mask(self, sample_unity_state):
        """Test DROP COLUMN MASK SQL generation"""
        builder = OperationBuilder()
        generator = UnitySQLGenerator(sample_unity_state.model_dump(by_alias=True))

        op = builder.remove_column_mask("mask_001", "table_789", op_id="op_029")

        result = generator.generate_sql_for_operation(op)
        assert "DROP" in result.sql or "REMOVE" in result.sql


class TestSQLGeneration:
    """Test overall SQL generation functionality"""

    def test_can_generate_sql(self, sample_unity_state):
        """Test can_generate_sql validation"""
        builder = OperationBuilder()
        generator = UnitySQLGenerator(sample_unity_state.model_dump(by_alias=True))

        valid_op = builder.add_catalog("cat_001", "test", op_id="op_001")

        assert generator.can_generate_sql(valid_op)

        # Invalid operation (non-Unity)
        invalid_op = Operation(
            id="op_002",
            provider="unity",
            ts="2025-01-01T00:00:00Z",
            op="hive.add_database",
            target="db_001",
            payload={},
        )

        assert not generator.can_generate_sql(invalid_op)

    @pytest.mark.skip(reason="Batch optimization issues - related to issue #19")
    def test_generate_sql_batch(self, empty_unity_state, sample_operations):
        """Test generating SQL for multiple operations"""
        generator = UnitySQLGenerator(empty_unity_state.model_dump(by_alias=True))

        sql = generator.generate_sql(sample_operations)

        # Verify SQL was generated
        assert len(sql) > 0

        # Verify all operations are represented
        assert "CREATE CATALOG" in sql
        assert "CREATE SCHEMA" in sql
        assert "CREATE TABLE" in sql
        assert "ADD COLUMN" in sql

        # Verify comments are present
        assert "-- Operation:" in sql
        assert "-- Type:" in sql

    def test_sql_idempotency(self, sample_unity_state):
        """Test that generated SQL is idempotent"""
        builder = OperationBuilder()
        generator = UnitySQLGenerator(sample_unity_state.model_dump(by_alias=True))

        # Catalog
        catalog_op = builder.add_catalog("cat_001", "test", op_id="op_001")
        result = generator.generate_sql_for_operation(catalog_op)
        assert "IF NOT EXISTS" in result.sql

        # Schema
        schema_op = builder.add_schema("schema_001", "test", "cat_123", op_id="op_002")
        result = generator.generate_sql_for_operation(schema_op)
        assert "IF NOT EXISTS" in result.sql

        # Table
        table_op = builder.add_table("table_001", "test", "schema_456", "delta", op_id="op_003")
        result = generator.generate_sql_for_operation(table_op)
        assert "IF NOT EXISTS" in result.sql

    def test_identifier_escaping(self, sample_unity_state):
        """Test that identifiers are properly escaped"""
        builder = OperationBuilder()
        generator = UnitySQLGenerator(sample_unity_state.model_dump(by_alias=True))

        # Test with special characters
        op = builder.add_catalog("cat_001", "my-special.catalog", op_id="op_001")

        result = generator.generate_sql_for_operation(op)
        # Should use backticks for escaping
        assert "`my-special.catalog`" in result.sql

    def test_string_escaping(self, sample_unity_state):
        """Test that string values are properly escaped"""
        builder = OperationBuilder()
        generator = UnitySQLGenerator(sample_unity_state.model_dump(by_alias=True))

        # Test with single quote in comment
        op = builder.set_table_comment("table_789", "User's data table", op_id="op_001")

        result = generator.generate_sql_for_operation(op)
        # Single quotes should be escaped
        assert "'User''s data table'" in result.sql or "User\\'s data table" in result.sql

    def test_error_handling(self, sample_unity_state):
        """Test error handling for invalid operations"""
        OperationBuilder()
        generator = UnitySQLGenerator(sample_unity_state.model_dump(by_alias=True))

        # Operation with missing payload
        invalid_op = Operation(
            id="op_001",
            provider="unity",
            ts="2025-01-01T00:00:00Z",
            op="unity.add_catalog",
            target="cat_001",
            payload={},  # Missing required fields
        )

        result = generator.generate_sql_for_operation(invalid_op)
        # Should handle error gracefully
        assert len(result.warnings) > 0 or result.sql.startswith("--")
        assert not result.is_idempotent or result.sql.startswith("-- Error")


class TestSQLOptimization:
    """Test SQL optimization and batching"""

    def test_batch_column_operations(self, empty_unity_state):
        """Test that column operations can be batched"""
        builder = OperationBuilder()
        # Create base state with table
        from schematic.providers.unity.state_reducer import apply_operations

        setup_ops = [
            builder.add_catalog("cat_123", "test", op_id="setup_001"),
            builder.add_schema("schema_456", "test", "cat_123", op_id="setup_002"),
            builder.add_table("table_789", "test", "schema_456", "delta", op_id="setup_003"),
        ]
        state_with_table = apply_operations(empty_unity_state, setup_ops)

        # Multiple column operations on same table
        column_ops = [
            builder.add_column(
                "col_001",
                "table_789",
                "id",
                "BIGINT",
                nullable=False,
                comment="None",
                op_id="col_001",
            ),
            builder.add_column(
                "col_002",
                "table_789",
                "name",
                "STRING",
                nullable=True,
                comment="None",
                op_id="col_002",
            ),
        ]

        generator = UnitySQLGenerator(state_with_table.model_dump(by_alias=True))
        sql = generator.generate_sql(column_ops)

        # Should generate SQL for both columns
        assert "id" in sql.lower()
        assert "name" in sql.lower()

    def test_batch_multiple_add_columns_existing_table(self, empty_unity_state):
        """Test that multiple ADD COLUMN operations are batched into single ALTER TABLE"""
        builder = OperationBuilder()
        from schematic.providers.unity.state_reducer import apply_operations

        # Create base state with existing table
        setup_ops = [
            builder.add_catalog("cat_123", "test", op_id="setup_001"),
            builder.add_schema("schema_456", "test", "cat_123", op_id="setup_002"),
            builder.add_table("table_789", "test", "schema_456", "delta", op_id="setup_003"),
        ]
        state_with_table = apply_operations(empty_unity_state, setup_ops)

        # Multiple ADD COLUMN operations on existing table
        column_ops = [
            builder.add_column(
                "col_001", "table_789", "col1", "STRING", nullable=True, op_id="col_001"
            ),
            builder.add_column(
                "col_002", "table_789", "col2", "INT", nullable=False, op_id="col_002"
            ),
            builder.add_column(
                "col_003",
                "table_789",
                "col3",
                "DOUBLE",
                nullable=True,
                comment="Test comment",
                op_id="col_003",
            ),
        ]

        generator = UnitySQLGenerator(state_with_table.model_dump(by_alias=True))
        sql = generator.generate_sql(column_ops)

        # Should have only ONE ALTER TABLE statement (batched)
        assert sql.count("ALTER TABLE") == 1, (
            f"Expected 1 ALTER TABLE, found {sql.count('ALTER TABLE')}"
        )

        # Should use ADD COLUMNS (plural) syntax with parentheses
        assert "ADD COLUMNS (" in sql
        assert "`col1` STRING," in sql
        assert "`col2` INT NOT NULL," in sql
        assert "`col3` DOUBLE COMMENT 'Test comment'" in sql

        # Verify correct format with closing parenthesis and semicolon
        assert ");" in sql or sql.strip().endswith(")")

        # Should have 3 column definitions
        column_lines = [
            line
            for line in sql.split("\n")
            if "`col" in line and ("STRING" in line or "INT" in line or "DOUBLE" in line)
        ]
        assert len(column_lines) == 3, "Should have 3 column definition lines"

    def test_single_add_column_not_batched(self, empty_unity_state):
        """Test that single ADD COLUMN operation works normally (no regression)"""
        builder = OperationBuilder()
        from schematic.providers.unity.state_reducer import apply_operations

        # Create base state with existing table
        setup_ops = [
            builder.add_catalog("cat_123", "test", op_id="setup_001"),
            builder.add_schema("schema_456", "test", "cat_123", op_id="setup_002"),
            builder.add_table("table_789", "test", "schema_456", "delta", op_id="setup_003"),
        ]
        state_with_table = apply_operations(empty_unity_state, setup_ops)

        # Single ADD COLUMN operation
        column_ops = [
            builder.add_column(
                "col_001", "table_789", "col1", "STRING", nullable=True, op_id="col_001"
            )
        ]

        generator = UnitySQLGenerator(state_with_table.model_dump(by_alias=True))
        sql = generator.generate_sql(column_ops)

        # Should have standard ALTER TABLE ADD COLUMN format
        assert "ALTER TABLE" in sql
        assert "ADD COLUMN `col1` STRING" in sql
        # Should NOT have comma or batching syntax
        assert sql.count("ADD COLUMN") == 1

    def test_multiple_tables_separate_statements(self, empty_unity_state):
        """Test that ADD COLUMN operations on different tables remain separate"""
        builder = OperationBuilder()
        from schematic.providers.unity.state_reducer import apply_operations

        # Create base state with two tables
        setup_ops = [
            builder.add_catalog("cat_123", "test", op_id="setup_001"),
            builder.add_schema("schema_456", "test", "cat_123", op_id="setup_002"),
            builder.add_table("table_001", "table1", "schema_456", "delta", op_id="setup_003"),
            builder.add_table("table_002", "table2", "schema_456", "delta", op_id="setup_004"),
        ]
        state_with_tables = apply_operations(empty_unity_state, setup_ops)

        # ADD COLUMN operations on different tables
        column_ops = [
            builder.add_column(
                "col_001", "table_001", "col1", "STRING", nullable=True, op_id="col_001"
            ),
            builder.add_column(
                "col_002", "table_002", "col2", "INT", nullable=False, op_id="col_002"
            ),
        ]

        generator = UnitySQLGenerator(state_with_tables.model_dump(by_alias=True))
        sql = generator.generate_sql(column_ops)

        # Should have TWO separate ALTER TABLE statements
        assert sql.count("ALTER TABLE") == 2, (
            f"Expected 2 ALTER TABLE statements, found {sql.count('ALTER TABLE')}"
        )

        # Each should be for different table (with full qualification)
        assert "table1" in sql
        assert "table2" in sql

    def test_mixed_operations_partial_batching(self, empty_unity_state):
        """Test that mixed operations (ADD + DROP) are handled correctly"""
        builder = OperationBuilder()
        from schematic.providers.unity.state_reducer import apply_operations

        # Create base state with table that has existing columns
        setup_ops = [
            builder.add_catalog("cat_123", "test", op_id="setup_001"),
            builder.add_schema("schema_456", "test", "cat_123", op_id="setup_002"),
            builder.add_table("table_789", "test", "schema_456", "delta", op_id="setup_003"),
            builder.add_column(
                "col_old", "table_789", "old_col", "STRING", nullable=True, op_id="setup_004"
            ),
        ]
        state_with_table = apply_operations(empty_unity_state, setup_ops)

        # Mixed operations: multiple ADD and one DROP
        mixed_ops = [
            builder.add_column(
                "col_001", "table_789", "col1", "STRING", nullable=True, op_id="col_001"
            ),
            builder.add_column(
                "col_002", "table_789", "col2", "INT", nullable=False, op_id="col_002"
            ),
            Operation(
                id="drop_001",
                provider="unity",
                ts="2025-01-01T00:00:00Z",
                op="unity.drop_column",
                target="col_old",
                payload={"tableId": "table_789"},
            ),
        ]

        generator = UnitySQLGenerator(state_with_table.model_dump(by_alias=True))
        sql = generator.generate_sql(mixed_ops)

        # Should have batched ADD COLUMNS and separate DROP COLUMN
        assert sql.count("ALTER TABLE") >= 2, (
            "Should have separate statements for batched ADD and DROP"
        )

        # Batched ADD COLUMNS should use proper syntax
        assert "ADD COLUMNS (" in sql
        assert "`col1` STRING," in sql
        assert "`col2` INT NOT NULL" in sql

        # DROP should be separate
        assert "DROP COLUMN" in sql

    def test_single_column_move_to_first(self, empty_unity_state):
        """Test that moving a single column to FIRST generates optimal SQL"""
        builder = OperationBuilder()
        from schematic.providers.unity.state_reducer import apply_operations

        # Create base state with table and multiple columns (simulating post-snapshot state)
        setup_ops = [
            builder.add_catalog("cat_123", "test", op_id="setup_001"),
            builder.add_schema("schema_456", "test", "cat_123", op_id="setup_002"),
            builder.add_table("table_789", "diamond", "schema_456", "delta", op_id="setup_003"),
            builder.add_column(
                "col_id", "table_789", "id", "INT", nullable=False, op_id="setup_004"
            ),
            builder.add_column(
                "col_type", "table_789", "type", "STRING", nullable=True, op_id="setup_005"
            ),
            builder.add_column(
                "col_name", "table_789", "name", "STRING", nullable=True, op_id="setup_006"
            ),
        ]
        # Apply all setup operations to get the committed state
        state_with_table = apply_operations(empty_unity_state, setup_ops)

        # Apply reorder to get final state (simulating what state reducer does)
        reorder_op = Operation(
            id="reorder_001",
            provider="unity",
            ts="2025-01-01T00:00:00Z",
            op="unity.reorder_columns",
            target="table_789",
            payload={
                "tableId": "table_789",
                "previousOrder": ["col_id", "col_type", "col_name"],
                "order": ["col_type", "col_id", "col_name"],
            },
        )
        final_state = apply_operations(state_with_table, [reorder_op])

        # Generate SQL from the final state with just the reorder operation
        generator = UnitySQLGenerator(final_state.model_dump(by_alias=True))
        sql = generator.generate_sql([reorder_op])

        # Should generate only ONE statement (optimal)
        assert sql.count("ALTER TABLE") == 1, (
            f"Expected 1 ALTER TABLE, found {sql.count('ALTER TABLE')}"
        )
        assert sql.count("ALTER COLUMN") == 1, (
            f"Expected 1 ALTER COLUMN, found {sql.count('ALTER COLUMN')}"
        )

        # Should use FIRST keyword
        assert "ALTER COLUMN `type` FIRST" in sql
        assert "AFTER" not in sql  # Should not use AFTER

    def test_single_column_move_to_middle(self, empty_unity_state):
        """Test that moving a single column to middle position generates optimal SQL"""
        builder = OperationBuilder()
        from schematic.providers.unity.state_reducer import apply_operations

        # Create base state with table and multiple columns
        setup_ops = [
            builder.add_catalog("cat_123", "test", op_id="setup_001"),
            builder.add_schema("schema_456", "test", "cat_123", op_id="setup_002"),
            builder.add_table("table_789", "test", "schema_456", "delta", op_id="setup_003"),
            builder.add_column(
                "col_id", "table_789", "id", "INT", nullable=False, op_id="setup_004"
            ),
            builder.add_column(
                "col_type", "table_789", "type", "STRING", nullable=True, op_id="setup_005"
            ),
            builder.add_column(
                "col_name", "table_789", "name", "STRING", nullable=True, op_id="setup_006"
            ),
        ]
        state_with_table = apply_operations(empty_unity_state, setup_ops)

        # Reorder: move 'name' between 'id' and 'type' (id, type, name -> id, name, type)
        reorder_op = Operation(
            id="reorder_001",
            provider="unity",
            ts="2025-01-01T00:00:00Z",
            op="unity.reorder_columns",
            target="table_789",
            payload={
                "tableId": "table_789",
                "previousOrder": ["col_id", "col_type", "col_name"],
                "order": ["col_id", "col_name", "col_type"],
            },
        )
        final_state = apply_operations(state_with_table, [reorder_op])

        generator = UnitySQLGenerator(final_state.model_dump(by_alias=True))
        sql = generator.generate_sql([reorder_op])

        # Should generate only ONE statement (optimal)
        assert sql.count("ALTER TABLE") == 1, (
            f"Expected 1 ALTER TABLE, found {sql.count('ALTER TABLE')}"
        )
        assert sql.count("ALTER COLUMN") == 1, (
            f"Expected 1 ALTER COLUMN, found {sql.count('ALTER COLUMN')}"
        )

        # Should use AFTER keyword with previous column
        assert "ALTER COLUMN `name` AFTER `id`" in sql
        assert "FIRST" not in sql  # Should not use FIRST

    def test_multiple_columns_move_uses_general_algorithm(self, empty_unity_state):
        """Test that moving multiple columns uses the general algorithm (not optimized)"""
        builder = OperationBuilder()
        from schematic.providers.unity.state_reducer import apply_operations

        # Create base state with table and multiple columns
        setup_ops = [
            builder.add_catalog("cat_123", "test", op_id="setup_001"),
            builder.add_schema("schema_456", "test", "cat_123", op_id="setup_002"),
            builder.add_table("table_789", "test", "schema_456", "delta", op_id="setup_003"),
            builder.add_column(
                "col_id", "table_789", "id", "INT", nullable=False, op_id="setup_004"
            ),
            builder.add_column(
                "col_type", "table_789", "type", "STRING", nullable=True, op_id="setup_005"
            ),
            builder.add_column(
                "col_name", "table_789", "name", "STRING", nullable=True, op_id="setup_006"
            ),
        ]
        state_with_table = apply_operations(empty_unity_state, setup_ops)

        # Reorder: reverse all columns (id, type, name -> name, type, id)
        reorder_op = Operation(
            id="reorder_001",
            provider="unity",
            ts="2025-01-01T00:00:00Z",
            op="unity.reorder_columns",
            target="table_789",
            payload={
                "tableId": "table_789",
                "previousOrder": ["col_id", "col_type", "col_name"],
                "order": ["col_name", "col_type", "col_id"],
            },
        )
        final_state = apply_operations(state_with_table, [reorder_op])

        generator = UnitySQLGenerator(final_state.model_dump(by_alias=True))
        sql = generator.generate_sql([reorder_op])

        # Should generate MULTIPLE statements (general algorithm)
        # When reversing 3 columns, we need at least 2 moves
        assert sql.count("ALTER COLUMN") >= 2, (
            f"Expected >= 2 ALTER COLUMN for multiple moves, found {sql.count('ALTER COLUMN')}"
        )
