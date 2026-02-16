"""
Unit tests for Unity Catalog state reducer

Tests all 31 Unity Catalog operations:
- Catalog operations (3): add, rename, drop
- Schema operations (3): add, rename, drop
- Table operations (6): add, rename, drop, set comment, set/unset property
- Column operations (7): add, rename, drop, reorder, change type, set nullable, set comment
- Column tags (2): set, unset
- Constraints (2): add, drop
- Row filters (3): add, update, remove
- Column masks (3): add, update, remove
"""

from schemax.providers.base.operations import Operation
from schemax.providers.unity.state_reducer import apply_operation, apply_operations
from tests.utils import OperationBuilder


class TestCatalogOperations:
    """Test catalog operations"""

    def test_add_catalog(self, empty_unity_state):
        """Test adding a catalog"""
        builder = OperationBuilder()
        op = builder.add_catalog("cat_123", "bronze", op_id="op_001")

        new_state = apply_operation(empty_unity_state, op)

        assert len(new_state.catalogs) == 1
        assert new_state.catalogs[0].id == "cat_123"
        assert new_state.catalogs[0].name == "bronze"
        assert new_state.catalogs[0].schemas == []

    def test_rename_catalog(self, sample_unity_state):
        """Test renaming a catalog"""
        builder = OperationBuilder()
        op = builder.rename_catalog("cat_123", "silver", "bronze", op_id="op_002")

        new_state = apply_operation(sample_unity_state, op)

        assert new_state.catalogs[0].name == "silver"

    def test_drop_catalog(self, sample_unity_state):
        """Test dropping a catalog"""
        builder = OperationBuilder()
        op = builder.drop_catalog("cat_123", op_id="op_003")

        new_state = apply_operation(sample_unity_state, op)

        assert len(new_state.catalogs) == 0


class TestSchemaOperations:
    """Test schema operations"""

    def test_add_schema(self, empty_unity_state):
        """Test adding a schema"""
        builder = OperationBuilder()
        # First add a catalog
        catalog_op = builder.add_catalog("cat_123", "bronze", op_id="op_001")
        state = apply_operation(empty_unity_state, catalog_op)

        # Then add a schema
        schema_op = builder.add_schema("schema_456", "raw", "cat_123", op_id="op_002")
        new_state = apply_operation(state, schema_op)

        assert len(new_state.catalogs[0].schemas) == 1
        assert new_state.catalogs[0].schemas[0].id == "schema_456"
        assert new_state.catalogs[0].schemas[0].name == "raw"

    def test_rename_schema(self, sample_unity_state):
        """Test renaming a schema"""
        builder = OperationBuilder()
        op = builder.rename_schema("schema_456", "refined", "raw", op_id="op_002")

        new_state = apply_operation(sample_unity_state, op)

        assert new_state.catalogs[0].schemas[0].name == "refined"

    def test_drop_schema(self, sample_unity_state):
        """Test dropping a schema"""
        OperationBuilder()
        op = Operation(
            id="op_003",
            provider="unity",
            ts="2025-01-01T00:00:00Z",
            op="unity.drop_schema",
            target="schema_456",
            payload={},
        )

        new_state = apply_operation(sample_unity_state, op)

        assert len(new_state.catalogs[0].schemas) == 0


class TestTableOperations:
    """Test table operations"""

    def test_add_table(self, empty_unity_state):
        """Test adding a table"""
        builder = OperationBuilder()
        # Setup: add catalog and schema
        ops = [
            builder.add_catalog("cat_123", "bronze", op_id="op_001"),
            builder.add_schema("schema_456", "raw", "cat_123", op_id="op_002"),
        ]
        state = apply_operations(empty_unity_state, ops)

        # Add table
        table_op = builder.add_table("table_789", "users", "schema_456", "delta", op_id="op_003")
        new_state = apply_operation(state, table_op)

        table = new_state.catalogs[0].schemas[0].tables[0]
        assert table.id == "table_789"
        assert table.name == "users"
        assert table.format == "delta"

    def test_rename_table(self, sample_unity_state):
        """Test renaming a table"""
        builder = OperationBuilder()
        op = builder.rename_table("table_789", "customers", "users", op_id="op_004")

        new_state = apply_operation(sample_unity_state, op)

        table = new_state.catalogs[0].schemas[0].tables[0]
        assert table.name == "customers"

    def test_drop_table(self, sample_unity_state):
        """Test dropping a table"""
        OperationBuilder()
        op = Operation(
            id="op_005",
            provider="unity",
            ts="2025-01-01T00:00:00Z",
            op="unity.drop_table",
            target="table_789",
            payload={},
        )

        new_state = apply_operation(sample_unity_state, op)

        assert len(new_state.catalogs[0].schemas[0].tables) == 0

    def test_set_table_comment(self, sample_unity_state):
        """Test setting table comment"""
        builder = OperationBuilder()
        op = builder.set_table_comment("table_789", "User data table", op_id="op_006")

        new_state = apply_operation(sample_unity_state, op)

        table = new_state.catalogs[0].schemas[0].tables[0]
        assert table.comment == "User data table"

    def test_set_table_property(self, sample_unity_state):
        """Test setting table property"""
        builder = OperationBuilder()
        op = builder.set_table_property(
            "table_789", "delta.enableChangeDataFeed", "true", op_id="op_007"
        )

        new_state = apply_operation(sample_unity_state, op)

        table = new_state.catalogs[0].schemas[0].tables[0]
        assert table.properties["delta.enableChangeDataFeed"] == "true"

    def test_unset_table_property(self, sample_unity_state):
        """Test unsetting table property"""
        builder = OperationBuilder()
        # First set a property
        set_op = builder.set_table_property("table_789", "testKey", "testValue", op_id="op_007")
        state = apply_operation(sample_unity_state, set_op)

        # Then unset it
        unset_op = builder.unset_table_property("table_789", "testKey", op_id="op_008")
        new_state = apply_operation(state, unset_op)

        table = new_state.catalogs[0].schemas[0].tables[0]
        assert "testKey" not in table.properties


class TestColumnOperations:
    """Test column operations"""

    def test_add_column(self, sample_unity_state):
        """Test adding a column"""
        builder = OperationBuilder()
        op = builder.add_column(
            "col_003",
            "table_789",
            "created_at",
            "TIMESTAMP",
            nullable=False,
            comment="Creation timestamp",
            op_id="op_009",
        )

        new_state = apply_operation(sample_unity_state, op)

        table = new_state.catalogs[0].schemas[0].tables[0]
        assert len(table.columns) == 3
        new_col = table.columns[2]
        assert new_col.id == "col_003"
        assert new_col.name == "created_at"
        assert new_col.type == "TIMESTAMP"
        assert not new_col.nullable

    def test_rename_column(self, sample_unity_state):
        """Test renaming a column"""
        builder = OperationBuilder()
        op = builder.rename_column("col_001", "table_789", "id", "user_id", op_id="op_010")

        new_state = apply_operation(sample_unity_state, op)

        table = new_state.catalogs[0].schemas[0].tables[0]
        col = next((c for c in table.columns if c.id == "col_001"), None)
        assert col.name == "id"

    def test_drop_column(self, sample_unity_state):
        """Test dropping a column"""
        builder = OperationBuilder()
        op = builder.drop_column("col_002", "table_789", op_id="op_011")

        new_state = apply_operation(sample_unity_state, op)

        table = new_state.catalogs[0].schemas[0].tables[0]
        assert len(table.columns) == 1
        assert table.columns[0].id == "col_001"

    def test_reorder_columns(self, sample_unity_state):
        """Test reordering columns"""
        builder = OperationBuilder()
        op = builder.reorder_columns("table_789", ["col_002", "col_001"], op_id="op_012")

        new_state = apply_operation(sample_unity_state, op)

        table = new_state.catalogs[0].schemas[0].tables[0]
        assert table.columns[0].id == "col_002"
        assert table.columns[1].id == "col_001"

    def test_change_column_type(self, sample_unity_state):
        """Test changing column type"""
        builder = OperationBuilder()
        op = builder.change_column_type("col_001", "table_789", "DECIMAL(18,0)", op_id="op_013")

        new_state = apply_operation(sample_unity_state, op)

        table = new_state.catalogs[0].schemas[0].tables[0]
        col = next((c for c in table.columns if c.id == "col_001"), None)
        assert col.type == "DECIMAL(18,0)"

    def test_set_nullable(self, sample_unity_state):
        """Test setting column nullable"""
        builder = OperationBuilder()
        op = builder.set_nullable("col_001", "table_789", True, op_id="op_014")

        new_state = apply_operation(sample_unity_state, op)

        table = new_state.catalogs[0].schemas[0].tables[0]
        col = next((c for c in table.columns if c.id == "col_001"), None)
        assert col.nullable is True

    def test_set_column_comment(self, sample_unity_state):
        """Test setting column comment"""
        builder = OperationBuilder()
        op = builder.set_column_comment("col_001", "table_789", "Updated comment", op_id="op_015")

        new_state = apply_operation(sample_unity_state, op)

        table = new_state.catalogs[0].schemas[0].tables[0]
        col = next((c for c in table.columns if c.id == "col_001"), None)
        assert col.comment == "Updated comment"


class TestColumnTagOperations:
    """Test column tag operations"""

    def test_set_column_tag(self, sample_unity_state):
        """Test setting column tag"""
        builder = OperationBuilder()
        op = builder.set_column_tag("col_001", "table_789", "PII", "true", op_id="op_016")

        new_state = apply_operation(sample_unity_state, op)

        table = new_state.catalogs[0].schemas[0].tables[0]
        col = next((c for c in table.columns if c.id == "col_001"), None)
        assert col.tags is not None
        assert col.tags["PII"] == "true"

    def test_set_multiple_column_tags(self, sample_unity_state):
        """Test setting multiple column tags"""
        builder = OperationBuilder()
        ops = [
            builder.set_column_tag("col_001", "table_789", "PII", "true", op_id="op_016"),
            builder.set_column_tag(
                "col_001", "table_789", "classification", "sensitive", op_id="op_017"
            ),
        ]

        new_state = apply_operations(sample_unity_state, ops)

        table = new_state.catalogs[0].schemas[0].tables[0]
        col = next((c for c in table.columns if c.id == "col_001"), None)
        assert len(col.tags) == 2
        assert col.tags["PII"] == "true"
        assert col.tags["classification"] == "sensitive"

    def test_unset_column_tag(self, sample_unity_state):
        """Test unsetting column tag"""
        builder = OperationBuilder()
        # First set a tag
        set_op = builder.set_column_tag("col_001", "table_789", "PII", "true", op_id="op_016")
        state = apply_operation(sample_unity_state, set_op)

        # Then unset it
        unset_op = builder.unset_column_tag("col_001", "table_789", "PII", op_id="op_017")
        new_state = apply_operation(state, unset_op)

        table = new_state.catalogs[0].schemas[0].tables[0]
        col = next((c for c in table.columns if c.id == "col_001"), None)
        assert "PII" not in col.tags


class TestConstraintOperations:
    """Test constraint operations"""

    def test_add_primary_key_constraint(self, sample_unity_state):
        """Test adding primary key constraint"""
        builder = OperationBuilder()
        op = builder.add_constraint(
            "constraint_001",
            "table_789",
            "primary_key",
            ["col_001"],
            name="pk_users",
            timeseries=False,
            op_id="op_018",
        )

        new_state = apply_operation(sample_unity_state, op)

        table = new_state.catalogs[0].schemas[0].tables[0]
        assert len(table.constraints) == 1
        constraint = table.constraints[0]
        assert constraint.type == "primary_key"
        assert constraint.name == "pk_users"
        assert constraint.columns == ["col_001"]

    def test_add_foreign_key_constraint(self, sample_unity_state):
        """Test adding foreign key constraint"""
        builder = OperationBuilder()
        op = builder.add_constraint(
            "constraint_002",
            "table_789",
            "foreign_key",
            ["col_001"],
            name="fk_users_parent",
            parentTable="parent_table_id",
            parentColumns=["parent_col_id"],
            op_id="op_019",
        )

        new_state = apply_operation(sample_unity_state, op)

        table = new_state.catalogs[0].schemas[0].tables[0]
        constraint = table.constraints[0]
        assert constraint.type == "foreign_key"
        assert constraint.parent_table == "parent_table_id"
        assert constraint.parent_columns == ["parent_col_id"]

    def test_add_check_constraint(self, sample_unity_state):
        """Test adding CHECK constraint"""
        builder = OperationBuilder()
        op = builder.add_constraint(
            "constraint_003",
            "table_789",
            "check",
            ["col_001"],
            name="chk_users_id",
            expression="user_id > 0",
            op_id="op_020",
        )

        new_state = apply_operation(sample_unity_state, op)

        table = new_state.catalogs[0].schemas[0].tables[0]
        constraint = table.constraints[0]
        assert constraint.type == "check"
        assert constraint.expression == "user_id > 0"

    def test_drop_constraint(self, sample_unity_state):
        """Test dropping constraint"""
        builder = OperationBuilder()
        # First add a constraint
        add_op = builder.add_constraint(
            "constraint_001", "table_789", "primary_key", ["col_001"], op_id="op_018"
        )
        state = apply_operation(sample_unity_state, add_op)

        # Then drop it
        drop_op = Operation(
            id="op_019",
            provider="unity",
            ts="2025-01-01T00:00:00Z",
            op="unity.drop_constraint",
            target="constraint_001",
            payload={"tableId": "table_789"},
        )
        new_state = apply_operation(state, drop_op)

        table = new_state.catalogs[0].schemas[0].tables[0]
        assert len(table.constraints) == 0


class TestRowFilterOperations:
    """Test row filter operations"""

    def test_add_row_filter(self, sample_unity_state):
        """Test adding row filter"""
        builder = OperationBuilder()
        op = builder.add_row_filter(
            "filter_001",
            "table_789",
            "region_filter",
            "region = current_user()",
            enabled=True,
            description="Filter by user region",
            op_id="op_021",
        )

        new_state = apply_operation(sample_unity_state, op)

        table = new_state.catalogs[0].schemas[0].tables[0]
        assert len(table.row_filters) == 1
        filter_obj = table.row_filters[0]
        assert filter_obj.name == "region_filter"
        assert filter_obj.enabled is True
        assert filter_obj.udf_expression == "region = current_user()"

    def test_update_row_filter(self, sample_unity_state):
        """Test updating row filter"""
        builder = OperationBuilder()
        # First add a filter
        add_op = builder.add_row_filter(
            "filter_001",
            "table_789",
            "region_filter",
            "region = current_user()",
            enabled=True,
            op_id="op_021",
        )
        state = apply_operation(sample_unity_state, add_op)

        # Then update it
        update_op = builder.update_row_filter(
            "filter_001",
            "table_789",
            udfExpression="region = current_user() AND active = true",
            enabled=False,
            op_id="op_022",
        )
        new_state = apply_operation(state, update_op)

        table = new_state.catalogs[0].schemas[0].tables[0]
        filter_obj = table.row_filters[0]
        assert filter_obj.enabled is False
        assert filter_obj.udf_expression == "region = current_user() AND active = true"

    def test_remove_row_filter(self, sample_unity_state):
        """Test removing row filter"""
        builder = OperationBuilder()
        # First add a filter
        add_op = builder.add_row_filter(
            "filter_001",
            "table_789",
            "region_filter",
            "region = current_user()",
            enabled=True,
            op_id="op_021",
        )
        state = apply_operation(sample_unity_state, add_op)

        # Then remove it
        remove_op = builder.remove_row_filter("filter_001", "table_789", op_id="op_022")
        new_state = apply_operation(state, remove_op)

        table = new_state.catalogs[0].schemas[0].tables[0]
        assert table.row_filters == [] or table.row_filters is None


class TestColumnMaskOperations:
    """Test column mask operations"""

    def test_add_column_mask(self, sample_unity_state):
        """Test adding column mask"""
        builder = OperationBuilder()
        op = builder.add_column_mask(
            "mask_001",
            "table_789",
            "col_002",
            "email_mask",
            "REDACT_EMAIL(email)",
            enabled=True,
            description="Mask email addresses",
            op_id="op_023",
        )

        new_state = apply_operation(sample_unity_state, op)

        table = new_state.catalogs[0].schemas[0].tables[0]
        assert len(table.column_masks) == 1
        mask = table.column_masks[0]
        assert mask.name == "email_mask"
        assert mask.column_id == "col_002"
        assert mask.mask_function == "REDACT_EMAIL(email)"

        # Verify column has mask_id set
        col = next((c for c in table.columns if c.id == "col_002"), None)
        assert col.mask_id == "mask_001"

    def test_update_column_mask(self, sample_unity_state):
        """Test updating column mask"""
        builder = OperationBuilder()
        # First add a mask
        add_op = builder.add_column_mask(
            "mask_001",
            "table_789",
            "col_002",
            "email_mask",
            "REDACT_EMAIL(email)",
            enabled=True,
            op_id="op_023",
        )
        state = apply_operation(sample_unity_state, add_op)

        # Then update it
        update_op = builder.update_column_mask(
            "mask_001", "table_789", enabled=False, maskFunction="MASK(email, '*')", op_id="op_024"
        )
        new_state = apply_operation(state, update_op)

        table = new_state.catalogs[0].schemas[0].tables[0]
        mask = table.column_masks[0]
        assert mask.enabled is False
        assert mask.mask_function == "MASK(email, '*')"

    def test_remove_column_mask(self, sample_unity_state):
        """Test removing column mask"""
        builder = OperationBuilder()
        # First add a mask
        add_op = builder.add_column_mask(
            "mask_001",
            "table_789",
            "col_002",
            "email_mask",
            "REDACT_EMAIL(email)",
            enabled=True,
            op_id="op_023",
        )
        state = apply_operation(sample_unity_state, add_op)

        # Then remove it
        remove_op = builder.remove_column_mask("mask_001", "table_789", op_id="op_024")
        new_state = apply_operation(state, remove_op)

        table = new_state.catalogs[0].schemas[0].tables[0]
        assert table.column_masks == [] or table.column_masks is None

        # Verify column mask_id was unlinked
        col = next((c for c in table.columns if c.id == "col_002"), None)
        assert col.mask_id is None


class TestStateImmutability:
    """Test that state operations are immutable"""

    def test_state_immutability(self, sample_unity_state):
        """Test that applying operations doesn't mutate original state"""
        builder = OperationBuilder()
        original_catalog_count = len(sample_unity_state.catalogs)
        original_catalog_name = sample_unity_state.catalogs[0].name

        op = builder.rename_catalog("cat_123", "modified", "bronze", op_id="op_001")

        new_state = apply_operation(sample_unity_state, op)

        # Original state should be unchanged
        assert len(sample_unity_state.catalogs) == original_catalog_count
        assert sample_unity_state.catalogs[0].name == original_catalog_name

        # New state should have changes
        assert new_state.catalogs[0].name == "modified"


class TestOperationSequences:
    """Test sequences of operations"""

    def test_apply_operations_batch(self, empty_unity_state, sample_operations):
        """Test applying multiple operations in batch"""
        new_state = apply_operations(empty_unity_state, sample_operations)

        # Verify all operations were applied
        assert len(new_state.catalogs) == 1
        assert new_state.catalogs[0].name == "bronze"
        assert len(new_state.catalogs[0].schemas) == 1
        assert len(new_state.catalogs[0].schemas[0].tables) == 1
        assert len(new_state.catalogs[0].schemas[0].tables[0].columns) == 1

    def test_complex_workflow(self, empty_unity_state):
        """Test a complex workflow with multiple operations"""
        builder = OperationBuilder()
        ops = [
            # Create catalog
            builder.add_catalog("cat_123", "production", op_id="op_001"),
            # Create schema
            builder.add_schema("schema_456", "analytics", "cat_123", op_id="op_002"),
            # Create table
            builder.add_table("table_789", "events", "schema_456", "delta", op_id="op_003"),
            # Add columns
            builder.add_column(
                "col_001",
                "table_789",
                "event_id",
                "BIGINT",
                nullable=False,
                comment="None",
                op_id="op_004",
            ),
            builder.add_column(
                "col_002",
                "table_789",
                "user_email",
                "STRING",
                nullable=True,
                comment="None",
                op_id="op_005",
            ),
            # Add PK constraint
            builder.add_constraint(
                "constraint_001", "table_789", "primary_key", ["col_001"], op_id="op_006"
            ),
            # Add column tag
            builder.set_column_tag("col_002", "table_789", "PII", "true", op_id="op_007"),
            # Add column mask
            builder.add_column_mask(
                "mask_001",
                "table_789",
                "col_002",
                "email_mask",
                "SHA2(user_email, 256)",
                enabled=True,
                op_id="op_008",
            ),
        ]

        final_state = apply_operations(empty_unity_state, ops)

        # Verify final state
        assert len(final_state.catalogs) == 1
        catalog = final_state.catalogs[0]
        assert catalog.name == "production"

        schema = catalog.schemas[0]
        assert schema.name == "analytics"

        table = schema.tables[0]
        assert table.name == "events"
        assert len(table.columns) == 2
        assert len(table.constraints) == 1
        assert len(table.column_masks) == 1

        # Verify PII tag
        email_col = next((c for c in table.columns if c.name == "user_email"), None)
        assert email_col.tags["PII"] == "true"
        assert email_col.mask_id == "mask_001"
