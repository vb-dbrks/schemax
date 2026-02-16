"""
Unit tests for Unity Catalog SQL Generator

Tests SQL generation for all 31 Unity Catalog operations.
Verifies SQL idempotency, correctness, and proper escaping.
"""

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

    def test_add_table_with_comment(self, sample_unity_state):
        """Test CREATE TABLE SQL generation with table comment"""
        builder = OperationBuilder()
        generator = UnitySQLGenerator(sample_unity_state.model_dump(by_alias=True))

        op = builder.add_table(
            "table_001",
            "events",
            "schema_456",
            "delta",
            comment="Table for storing event data",
            op_id="op_007",
        )

        result = generator.generate_sql_for_operation(op)
        assert "CREATE TABLE IF NOT EXISTS" in result.sql
        assert "`bronze`.`raw`.`events`" in result.sql
        assert "USING DELTA" in result.sql
        assert "COMMENT 'Table for storing event data'" in result.sql

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
            payload={
                "name": "users",
                "catalogId": "cat_123",
                "schemaId": "schema_456",
            },
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
        """Test ALTER TABLE ADD COLUMN SQL generation with NOT NULL constraint"""
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
        # NOT NULL constraint is handled via separate ALTER COLUMN SET NOT NULL statement
        assert "SET NOT NULL" in result.sql
        assert "ALTER COLUMN" in result.sql
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

    def test_batched_column_tags_same_column(self, sample_unity_state):
        """Multiple set_column_tag on same column produce one SET TAGS with multiple key=value."""
        builder = OperationBuilder()
        generator = UnitySQLGenerator(sample_unity_state.model_dump(by_alias=True))

        tag_ops = [
            builder.set_column_tag("col_002", "table_789", "pii", "true", op_id="tag_1"),
            builder.set_column_tag("col_002", "table_789", "category", "contact", op_id="tag_2"),
        ]
        batched = generator._generate_batched_column_tag_sql(tag_ops)
        assert "SET TAGS" in batched
        assert "pii" in batched and "true" in batched
        assert "category" in batched and "contact" in batched
        assert batched.count("ALTER TABLE") == 1
        assert batched.count("SET TAGS") == 1

    def test_batched_table_tags_same_table(self, sample_unity_state):
        """Multiple set_table_tag on same table produce one SET TAGS with multiple key=value."""
        builder = OperationBuilder()
        generator = UnitySQLGenerator(sample_unity_state.model_dump(by_alias=True))

        table_tag_ops = [
            builder.set_table_tag("table_789", "k1", "v1", op_id="tag_1"),
            builder.set_table_tag("table_789", "k2", "v2", op_id="tag_2"),
        ]
        batched = generator._generate_batched_table_tag_sql(table_tag_ops)
        assert "SET TAGS" in batched
        assert "k1" in batched and "v1" in batched
        assert "k2" in batched and "v2" in batched
        assert batched.count("ALTER TABLE") == 1
        assert batched.count("SET TAGS") == 1


class TestConstraintSQL:
    """Test SQL generation for constraint operations"""

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

    def test_add_primary_key_constraint_timeseries_multi_column(self, sample_unity_state):
        """PRIMARY KEY with timeseries and multiple columns: TIMESERIES after first column only."""
        builder = OperationBuilder()
        generator = UnitySQLGenerator(sample_unity_state.model_dump(by_alias=True))

        op = builder.add_constraint(
            "constraint_001",
            "table_789",
            "primary_key",
            ["col_001", "col_002"],
            name="pk_users",
            timeseries=True,
            op_id="op_020",
        )

        result = generator.generate_sql_for_operation(op)
        assert "PRIMARY KEY" in result.sql
        assert "TIMESERIES" in result.sql
        assert "user_id TIMESERIES" in result.sql or "`user_id` TIMESERIES" in result.sql
        assert ") TIMESERIES" not in result.sql

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
            op_id="op_022",
        )

        result = generator.generate_sql_for_operation(op)
        assert "ALTER TABLE" in result.sql
        assert "ADD CONSTRAINT" in result.sql
        assert "CHECK" in result.sql
        assert "user_id > 0" in result.sql

    def test_drop_constraint(self, sample_unity_state):
        """Test ALTER TABLE DROP CONSTRAINT SQL generation

        This test:
        1. Adds a constraint to the state
        2. Generates DROP CONSTRAINT SQL
        3. Verifies the SQL is correct
        """
        builder = OperationBuilder()

        # First, add a constraint to the state so it can be looked up
        add_op = builder.add_constraint(
            "constraint_001",
            "table_789",
            "primary_key",
            ["col_001"],
            name="pk_users",
            op_id="op_020",
        )

        # Apply the add operation to state (pass Pydantic model, not dict)
        from schematic.providers.unity.state_reducer import apply_operation

        state_with_constraint = apply_operation(sample_unity_state, add_op)

        # Now create generator with updated state (convert to dict for generator)
        generator = UnitySQLGenerator(state_with_constraint.model_dump(by_alias=True))

        # Create drop operation
        drop_op = Operation(
            id="op_023",
            provider="unity",
            ts="2025-01-01T00:00:00Z",
            op="unity.drop_constraint",
            target="constraint_001",
            payload={"tableId": "table_789"},
        )

        result = generator.generate_sql_for_operation(drop_op)
        assert "ALTER TABLE" in result.sql
        assert "DROP CONSTRAINT" in result.sql
        assert "`pk_users`" in result.sql


class TestRowFilterSQL:
    """Test SQL generation for row filter operations"""

    def test_add_row_filter(self, sample_unity_state):
        """Test SET ROW FILTER SQL generation"""
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
        assert "SET ROW FILTER" in result.sql
        assert "region_filter" in result.sql or "`region_filter`" in result.sql
        assert "ON ()" in result.sql

    def test_update_row_filter(self, sample_unity_state):
        """Test ALTER ROW FILTER (SET) SQL generation"""
        builder = OperationBuilder()
        generator = UnitySQLGenerator(sample_unity_state.model_dump(by_alias=True))

        op = builder.update_row_filter(
            "filter_001",
            "table_789",
            name="region_filter_v2",
            udfExpression="region = current_user() AND active = 1",
            op_id="op_025",
        )

        result = generator.generate_sql_for_operation(op)
        assert "SET ROW FILTER" in result.sql

    def test_remove_row_filter(self, sample_unity_state):
        """Test DROP ROW FILTER SQL generation"""
        builder = OperationBuilder()
        generator = UnitySQLGenerator(sample_unity_state.model_dump(by_alias=True))

        op = builder.remove_row_filter("filter_001", "table_789", op_id="op_026")

        result = generator.generate_sql_for_operation(op)
        assert "DROP ROW FILTER" in result.sql


class TestColumnMaskSQL:
    """Test SQL generation for column mask operations"""

    def test_add_column_mask(self, sample_unity_state):
        """Test SET COLUMN MASK SQL generation"""
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
        assert "SET MASK" in result.sql

    def test_update_column_mask(self, sample_unity_state):
        """Test ALTER COLUMN MASK (SET) SQL generation"""
        builder = OperationBuilder()
        generator = UnitySQLGenerator(sample_unity_state.model_dump(by_alias=True))

        op = builder.update_column_mask(
            "mask_001",
            "table_789",
            columnId="col_002",
            maskFunction="MASK(email, '*')",
            op_id="op_028",
        )

        result = generator.generate_sql_for_operation(op)
        assert "SET MASK" in result.sql

    def test_remove_column_mask(self, sample_unity_state):
        """Test DROP COLUMN MASK SQL generation"""
        builder = OperationBuilder()
        generator = UnitySQLGenerator(sample_unity_state.model_dump(by_alias=True))

        op = builder.remove_column_mask(
            "mask_001", "table_789", column_id="col_002", op_id="op_029"
        )

        result = generator.generate_sql_for_operation(op)
        assert "DROP MASK" in result.sql


class TestViewSQL:
    """Test SQL generation for view operations and view dependencies"""

    def test_add_view_simple(self, empty_unity_state, assert_sql):
        """Test CREATE VIEW SQL generation"""
        # Create state with a catalog, schema, and table
        state = empty_unity_state.model_dump(by_alias=True)
        state["catalogs"] = [
            {
                "id": "cat1",
                "name": "production",
                "schemas": [
                    {
                        "id": "schema1",
                        "name": "sales",
                        "tables": [
                            {"id": "table1", "name": "customers", "format": "delta", "columns": []}
                        ],
                        "views": [],
                    }
                ],
            }
        ]

        generator = UnitySQLGenerator(state)

        # Add a view that selects from a table
        op = Operation(
            id="op_v1",
            provider="unity",
            op="unity.add_view",
            target="view1",
            payload={
                "viewId": "view1",
                "name": "customer_summary",
                "schemaId": "schema1",
                "definition": "SELECT * FROM customers",
                "dependencies": ["table1"],  # Depends on a table
            },
            ts="2024-01-01T00:00:00Z",
        )

        sql = generator.generate_sql([op])
        assert "CREATE VIEW IF NOT EXISTS" in sql
        assert "`customer_summary`" in sql
        assert "SELECT" in sql
        assert "FROM" in sql
        assert "`customers`" in sql  # Table name should appear (possibly qualified)

        # Validate SQL syntax
        assert_sql(sql)

    def test_view_dependencies_ordered_correctly(self, empty_unity_state, assert_sql):
        """Test that views depending on other views are created in correct order"""
        # Create empty state with one catalog and schema
        state = empty_unity_state.model_dump(by_alias=True)
        state["catalogs"] = [
            {
                "id": "cat1",
                "name": "sales_analytics",
                "schemas": [
                    {
                        "id": "schema1",
                        "name": "product_360",
                        "tables": [
                            {
                                "id": "table1",
                                "name": "products",
                                "format": "delta",
                                "columns": [],
                            }
                        ],
                        "views": [],
                    }
                ],
            }
        ]

        generator = UnitySQLGenerator(state)

        # Create operations: view1 depends on table1, view2 depends on view1
        # Add them in WRONG order intentionally to test sorting
        op_view2 = Operation(
            id="op_v2",
            provider="unity",
            op="unity.add_view",
            target="view2",
            payload={
                "viewId": "view2",
                "name": "view2",
                "schemaId": "schema1",
                "definition": "SELECT * FROM view1",
                "dependencies": ["view1"],  # Depends on view1
            },
            ts="2024-01-01T00:00:02Z",
        )

        op_view1 = Operation(
            id="op_v1",
            provider="unity",
            op="unity.add_view",
            target="view1",
            payload={
                "viewId": "view1",
                "name": "view1",
                "schemaId": "schema1",
                "definition": "SELECT * FROM products",
                "dependencies": ["table1"],  # Depends on table1
            },
            ts="2024-01-01T00:00:01Z",
        )

        # Generate SQL with operations in wrong order
        sql = generator.generate_sql([op_view2, op_view1])

        # Debug: Print the generated SQL
        print("\n=== Generated SQL ===")
        print(sql)
        print("=== End SQL ===\n")

        # Validate SQL syntax
        assert_sql(sql)

        # Split SQL into individual statements (split by semicolon and filter for CREATE VIEW)
        all_statements = [s.strip() for s in sql.split(";") if s.strip()]
        view_statements = [s for s in all_statements if "CREATE VIEW" in s]

        print(f"Found {len(view_statements)} view statements:")
        for i, stmt in enumerate(view_statements):
            print(f"  [{i}]: {stmt[:80]}...")

        assert len(view_statements) == 2, (
            f"Expected 2 view statements, found {len(view_statements)}"
        )

        # Find which statement creates view1 and which creates view2
        view1_idx = None
        view2_idx = None
        for i, stmt in enumerate(view_statements):
            if "`view1`" in stmt:
                view1_idx = i
                print(f"  view1 found at index {i}")
            if "`view2`" in stmt:
                view2_idx = i
                print(f"  view2 found at index {i}")

        # view1 MUST be created before view2 (despite wrong input order)
        assert view1_idx is not None, "view1 CREATE statement not found"
        assert view2_idx is not None, "view2 CREATE statement not found"
        assert view1_idx < view2_idx, (
            f"view1 (index {view1_idx}) must be created before view2 (index {view2_idx})"
        )

    def test_view_dependency_chain(self, empty_unity_state, assert_sql):
        """Test complex view dependency chain: view3 → view2 → view1 → table"""
        state = empty_unity_state.model_dump(by_alias=True)
        state["catalogs"] = [
            {
                "id": "cat1",
                "name": "analytics",
                "schemas": [
                    {
                        "id": "schema1",
                        "name": "reporting",
                        "tables": [
                            {"id": "table1", "name": "base_table", "format": "delta", "columns": []}
                        ],
                        "views": [],
                    }
                ],
            }
        ]

        generator = UnitySQLGenerator(state)

        # Create chain: view3 → view2 → view1 → table1
        # Add in random order to test sorting
        ops = [
            Operation(
                id="op_v3",
                provider="unity",
                op="unity.add_view",
                target="view3",
                payload={
                    "viewId": "view3",
                    "name": "view3",
                    "schemaId": "schema1",
                    "definition": "SELECT * FROM view2",
                    "dependencies": ["view2"],
                },
                ts="2024-01-01T00:00:03Z",
            ),
            Operation(
                id="op_v1",
                provider="unity",
                op="unity.add_view",
                target="view1",
                payload={
                    "viewId": "view1",
                    "name": "view1",
                    "schemaId": "schema1",
                    "definition": "SELECT * FROM base_table",
                    "dependencies": ["table1"],
                },
                ts="2024-01-01T00:00:01Z",
            ),
            Operation(
                id="op_v2",
                provider="unity",
                op="unity.add_view",
                target="view2",
                payload={
                    "viewId": "view2",
                    "name": "view2",
                    "schemaId": "schema1",
                    "definition": "SELECT * FROM view1",
                    "dependencies": ["view1"],
                },
                ts="2024-01-01T00:00:02Z",
            ),
        ]

        sql = generator.generate_sql(ops)

        # Debug: Print the generated SQL
        print("\n=== Generated SQL (chain test) ===")
        print(sql)
        print("=== End SQL ===\n")

        # Validate SQL syntax
        assert_sql(sql)

        # Extract view creation statements
        statements = [s.strip() for s in sql.split(";") if s.strip() and "CREATE VIEW" in s]

        print(f"Found {len(statements)} view statements:")
        for i, stmt in enumerate(statements):
            print(f"  [{i}]: {stmt[:100]}...")

        assert len(statements) == 3, f"Expected 3 view statements, found {len(statements)}"

        # Find indices (need unique matching, not just contains)
        view1_idx = next(
            (
                i
                for i, s in enumerate(statements)
                if "`view1`" in s and "`view" not in s.replace("`view1`", "")
            ),
            None,
        )
        view2_idx = next((i for i, s in enumerate(statements) if "`view2`" in s), None)
        view3_idx = next((i for i, s in enumerate(statements) if "`view3`" in s), None)

        print(f"  view1 found at index {view1_idx}")
        print(f"  view2 found at index {view2_idx}")
        print(f"  view3 found at index {view3_idx}")

        # Assert correct ordering: view1 < view2 < view3
        assert view1_idx is not None
        assert view2_idx is not None
        assert view3_idx is not None
        assert view1_idx < view2_idx < view3_idx, (
            f"Views must be created in dependency order: "
            f"view1 (idx={view1_idx}), view2 (idx={view2_idx}), view3 (idx={view3_idx})"
        )


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

    def test_generate_sql_batch(self, empty_unity_state, sample_operations):
        """Test generating SQL for multiple operations"""
        generator = UnitySQLGenerator(empty_unity_state.model_dump(by_alias=True))

        sql = generator.generate_sql(sample_operations)

        # Verify SQL was generated
        assert len(sql) > 0

        # Verify all operations are represented (catalog, schema, table, column)
        assert "CREATE CATALOG" in sql
        assert "CREATE SCHEMA" in sql
        assert "CREATE TABLE" in sql
        # Column may be in CREATE TABLE (batched) or ADD COLUMN (separate)
        assert "ADD COLUMN" in sql or ("user_id" in sql and "BIGINT" in sql)

        # Verify comments are present
        assert "-- Operation:" in sql or "-- Type:" in sql or "COMMENT" in sql

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

        # Should have TWO ALTER TABLE statements:
        # 1. Batched ADD COLUMNS
        # 2. ALTER COLUMN SET NOT NULL for col2
        assert sql.count("ALTER TABLE") == 2, (
            f"Expected 2 ALTER TABLE, found {sql.count('ALTER TABLE')}"
        )

        # Should use ADD COLUMNS (plural) syntax with parentheses
        assert "ADD COLUMNS (" in sql
        assert "`col1` STRING," in sql
        # Note: NOT NULL is not supported in ALTER TABLE ADD COLUMNS for Delta tables
        # So col2 is added as nullable, then SET NOT NULL via separate statement
        assert "`col2` INT," in sql or "`col2` INT\n" in sql
        assert "`col3` DOUBLE COMMENT 'Test comment'" in sql

        # Verify SET NOT NULL statement for col2
        assert "ALTER COLUMN `col2` SET NOT NULL" in sql

        # Verify correct format with closing parenthesis and semicolon
        assert ");" in sql or sql.strip().endswith(")")

        # Should have 3 column definitions
        column_lines = [
            line
            for line in sql.split("\n")
            if "`col" in line and ("STRING" in line or "INT" in line or "DOUBLE" in line)
        ]
        assert len(column_lines) == 3, "Should have 3 column definition lines"

    def test_create_table_with_comment_and_columns_batched(self, empty_unity_state):
        """Test that CREATE TABLE with comment and columns are batched together"""
        builder = OperationBuilder()
        from schematic.providers.unity.state_reducer import apply_operations

        # Create base state with catalog and schema
        setup_ops = [
            builder.add_catalog("cat_123", "test", op_id="setup_001"),
            builder.add_schema("schema_456", "test", "cat_123", op_id="setup_002"),
        ]
        state_with_schema = apply_operations(empty_unity_state, setup_ops)

        # CREATE TABLE with comment + ADD COLUMNS
        table_ops = [
            builder.add_table(
                "table_789",
                "test",
                "schema_456",
                "delta",
                comment="Table for test data",
                op_id="table_001",
            ),
            builder.add_column(
                "col_001", "table_789", "id", "STRING", nullable=False, op_id="col_001"
            ),
            builder.add_column(
                "col_002",
                "table_789",
                "name",
                "STRING",
                nullable=True,
                comment="User name",
                op_id="col_002",
            ),
        ]

        generator = UnitySQLGenerator(state_with_schema.model_dump(by_alias=True))
        sql = generator.generate_sql(table_ops)

        # Should have single CREATE TABLE statement with columns and table comment
        assert sql.count("CREATE TABLE") == 1
        assert "COMMENT 'Table for test data'" in sql
        assert "`id` STRING NOT NULL" in sql
        assert "`name` STRING COMMENT 'User name'" in sql

        # Should NOT have separate ALTER TABLE for comment (batched into CREATE)
        assert "ALTER TABLE" not in sql or sql.count("ALTER TABLE") == 0

    def test_create_table_with_tags_generates_alter_statements(self, empty_unity_state):
        """Test that CREATE TABLE with tags generates ALTER TABLE SET TAGS statements"""
        builder = OperationBuilder()
        from schematic.providers.unity.state_reducer import apply_operations

        # Create base state with catalog and schema
        setup_ops = [
            builder.add_catalog("cat_123", "test", op_id="setup_001"),
            builder.add_schema("schema_456", "test", "cat_123", op_id="setup_002"),
        ]
        state_with_schema = apply_operations(empty_unity_state, setup_ops)

        # CREATE TABLE + table tags
        table_ops = [
            builder.add_table("table_789", "test", "schema_456", "delta", op_id="table_001"),
            builder.set_table_tag("table_789", "department", "engineering", op_id="tag_001"),
            builder.set_table_tag("table_789", "owner", "data-team", op_id="tag_002"),
        ]

        generator = UnitySQLGenerator(state_with_schema.model_dump(by_alias=True))
        sql = generator.generate_sql(table_ops)

        # Should have CREATE TABLE
        assert sql.count("CREATE TABLE") == 1

        # Should have one batched ALTER TABLE SET TAGS (table tags after creation)
        assert "ALTER TABLE" in sql
        assert "SET TAGS" in sql
        assert "'department' = 'engineering'" in sql
        assert "'owner' = 'data-team'" in sql
        # Batched: one SET TAGS statement for the table, not one per tag
        assert sql.count("SET TAGS") == 1

    def test_create_table_with_column_tags_generates_alter_statements(self, empty_unity_state):
        """Test that columns with tags generate ALTER TABLE ALTER COLUMN SET TAGS statements"""
        builder = OperationBuilder()
        from schematic.providers.unity.state_reducer import apply_operations

        # Create base state with catalog and schema
        setup_ops = [
            builder.add_catalog("cat_123", "test", op_id="setup_001"),
            builder.add_schema("schema_456", "test", "cat_123", op_id="setup_002"),
        ]
        state_with_schema = apply_operations(empty_unity_state, setup_ops)

        # CREATE TABLE + columns with tags
        table_ops = [
            builder.add_table("table_789", "test", "schema_456", "delta", op_id="table_001"),
            builder.add_column(
                "col_001", "table_789", "email", "STRING", nullable=False, op_id="col_001"
            ),
            builder.set_column_tag("col_001", "table_789", "pii", "true", op_id="tag_001"),
            builder.set_column_tag(
                "col_001", "table_789", "classification", "sensitive", op_id="tag_002"
            ),
        ]

        generator = UnitySQLGenerator(state_with_schema.model_dump(by_alias=True))
        sql = generator.generate_sql(table_ops)

        # Should have CREATE TABLE with column
        assert sql.count("CREATE TABLE") == 1
        assert "`email` STRING NOT NULL" in sql

        # Should have ALTER TABLE ALTER COLUMN SET TAGS statements
        assert "ALTER TABLE" in sql
        assert "ALTER COLUMN" in sql
        assert "SET TAGS" in sql
        assert "'pii' = 'true'" in sql
        assert "'classification' = 'sensitive'" in sql

    def test_column_tags_via_state_differ(self, empty_unity_state):
        """Test that column tags are detected by state differ and generate correct SQL"""
        from schematic.providers.unity.state_differ import UnityStateDiffer
        from schematic.providers.unity.state_reducer import apply_operations

        builder = OperationBuilder()

        # Old state: empty
        old_state = empty_unity_state

        # New state: catalog with table with columns that have tags
        setup_ops = [
            builder.add_catalog("cat_123", "test", op_id="setup_001"),
            builder.add_schema("schema_456", "test", "cat_123", op_id="setup_002"),
            builder.add_table("table_789", "users", "schema_456", "delta", op_id="table_001"),
            builder.add_column(
                "col_001", "table_789", "email", "STRING", nullable=False, op_id="col_001"
            ),
            builder.set_column_tag("col_001", "table_789", "pii", "true", op_id="tag_001"),
            builder.set_column_tag("col_001", "table_789", "category", "contact", op_id="tag_002"),
        ]
        new_state = apply_operations(empty_unity_state, setup_ops)

        # Generate diff
        differ = UnityStateDiffer(
            old_state.model_dump(by_alias=True), new_state.model_dump(by_alias=True), [], setup_ops
        )
        diff_ops = differ.generate_diff_operations()

        # Verify column tag operations were generated
        column_tag_ops = [op for op in diff_ops if op.op == "unity.set_column_tag"]
        assert len(column_tag_ops) == 2, (
            f"Expected 2 column tag operations, got {len(column_tag_ops)}"
        )

        # Verify operations have required fields
        for op in column_tag_ops:
            assert "tableId" in op.payload
            assert "name" in op.payload  # Column name for SQL generation
            assert "tagName" in op.payload
            assert "tagValue" in op.payload

        # Generate SQL and verify (tags are batched per column)
        generator = UnitySQLGenerator(new_state.model_dump(by_alias=True))
        sql = generator.generate_sql(diff_ops)

        # Should generate one ALTER COLUMN SET TAGS with both tags batched
        assert "ALTER COLUMN `email` SET TAGS" in sql
        assert "pii" in sql and "true" in sql
        assert "category" in sql and "contact" in sql

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

        # Should have THREE separate ALTER TABLE statements:
        # 1. ALTER TABLE table1 ADD COLUMN col1
        # 2. ALTER TABLE table2 ADD COLUMN col2
        # 3. ALTER TABLE table2 ALTER COLUMN col2 SET NOT NULL
        assert sql.count("ALTER TABLE") == 3, (
            f"Expected 3 ALTER TABLE statements, found {sql.count('ALTER TABLE')}"
        )

        # Each should be for different table (with full qualification)
        assert "table1" in sql
        assert "table2" in sql

        # Verify SET NOT NULL for col2
        assert "ALTER COLUMN `col2` SET NOT NULL" in sql

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

        # Should have batched ADD COLUMNS, SET NOT NULL for col2, and separate DROP COLUMN
        # Minimum: ADD COLUMNS + SET NOT NULL + DROP = 3 ALTER TABLE statements
        assert sql.count("ALTER TABLE") >= 3, (
            "Should have separate statements for batched ADD, SET NOT NULL, and DROP"
        )

        # Batched ADD COLUMNS should use proper syntax
        assert "ADD COLUMNS (" in sql
        assert "`col1` STRING," in sql
        # Note: NOT NULL is not supported in ALTER TABLE ADD COLUMNS for Delta tables
        # So col2 is added as nullable, then SET NOT NULL via separate statement
        assert "`col2` INT" in sql
        assert "ALTER COLUMN `col2` SET NOT NULL" in sql

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


class TestOperationCancellation:
    """Test create+drop operation cancellation optimization"""

    def test_table_create_drop_cancellation(self, empty_unity_state):
        """Test that create+drop table operations cancel out"""
        builder = OperationBuilder()
        from schematic.providers.unity.state_reducer import apply_operations

        # Setup: catalog and schema
        setup_ops = [
            builder.add_catalog("cat_123", "test", op_id="setup_001"),
            builder.add_schema("schema_456", "test", "cat_123", op_id="setup_002"),
        ]
        state_with_schema = apply_operations(empty_unity_state, setup_ops)

        # Create table, add columns, then drop table (all cancelled)
        ops = [
            builder.add_table("table_789", "temp", "schema_456", "delta", op_id="op_001"),
            builder.add_column(
                "col_001", "table_789", "id", "BIGINT", nullable=False, op_id="op_002"
            ),
            builder.add_column(
                "col_002", "table_789", "name", "STRING", nullable=True, op_id="op_003"
            ),
            Operation(
                id="op_004",
                provider="unity",
                ts="2099-12-31T23:59:59Z",  # Later timestamp so DROP comes after CREATE
                op="unity.drop_table",
                target="table_789",
                payload={
                    "name": "temp",
                    "catalogId": "cat_123",
                    "schemaId": "schema_456",
                },
            ),
        ]

        generator = UnitySQLGenerator(state_with_schema.model_dump(by_alias=True))
        sql = generator.generate_sql(ops)

        # Should generate NO SQL (operations cancelled)
        assert not sql.strip() or sql.strip() == "", (
            f"Expected no SQL for cancelled operations, got:\n{sql}"
        )

    def test_catalog_create_drop_cancellation(self, empty_unity_state):
        """Test that create+drop catalog operations cancel out"""
        builder = OperationBuilder()

        ops = [
            builder.add_catalog("cat_999", "temp_catalog", op_id="op_001"),
            Operation(
                id="op_002",
                provider="unity",
                ts="2099-12-31T23:59:59Z",  # Later timestamp so DROP comes after CREATE
                op="unity.drop_catalog",
                target="cat_999",
                payload={},
            ),
        ]

        generator = UnitySQLGenerator(empty_unity_state.model_dump(by_alias=True))
        sql = generator.generate_sql(ops)

        # Should generate NO SQL (operations cancelled)
        assert not sql.strip(), f"Expected no SQL for cancelled operations, got:\n{sql}"

    def test_schema_create_drop_cancellation(self, empty_unity_state):
        """Test that create+drop schema operations cancel out"""
        builder = OperationBuilder()
        from schematic.providers.unity.state_reducer import apply_operations

        # Setup: catalog
        setup_ops = [builder.add_catalog("cat_123", "test", op_id="setup_001")]
        state_with_catalog = apply_operations(empty_unity_state, setup_ops)

        ops = [
            builder.add_schema("schema_999", "temp_schema", "cat_123", op_id="op_001"),
            Operation(
                id="op_002",
                provider="unity",
                ts="2099-12-31T23:59:59Z",  # Later timestamp so DROP comes after CREATE
                op="unity.drop_schema",
                target="schema_999",
                payload={},
            ),
        ]

        generator = UnitySQLGenerator(state_with_catalog.model_dump(by_alias=True))
        sql = generator.generate_sql(ops)

        # Should generate NO SQL (operations cancelled)
        assert not sql.strip(), f"Expected no SQL for cancelled operations, got:\n{sql}"

    def test_table_create_without_drop_not_cancelled(self, empty_unity_state):
        """Test that create without drop is NOT cancelled"""
        builder = OperationBuilder()
        from schematic.providers.unity.state_reducer import apply_operations

        # Setup: catalog and schema
        setup_ops = [
            builder.add_catalog("cat_123", "test", op_id="setup_001"),
            builder.add_schema("schema_456", "test", "cat_123", op_id="setup_002"),
        ]
        state_with_schema = apply_operations(empty_unity_state, setup_ops)

        # Create table without drop (should generate SQL)
        ops = [builder.add_table("table_789", "users", "schema_456", "delta", op_id="op_001")]

        generator = UnitySQLGenerator(state_with_schema.model_dump(by_alias=True))
        sql = generator.generate_sql(ops)

        # Should generate CREATE TABLE SQL
        assert "CREATE TABLE" in sql, "Expected CREATE TABLE SQL to be generated"
        assert "`test`.`test`.`users`" in sql
