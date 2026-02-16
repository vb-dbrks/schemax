"""
Operation builders for tests

Provides a clean, fluent API for creating operations in tests without verbose boilerplate.
All methods use the create_operation() helper with provider="unity" pre-configured.
"""

from typing import Any, Literal

from schematic.providers.base.operations import Operation, create_operation


class OperationBuilder:
    """
    Builder for creating Unity Catalog operations in tests.

    Usage:
        builder = OperationBuilder()
        op = builder.add_catalog("cat_123", "bronze")

        # Or with custom operation ID:
        op = builder.add_catalog("cat_123", "bronze", op_id="op_001")
    """

    def __init__(self, provider: str = "unity"):
        self.provider = provider

    # Catalog Operations
    def add_catalog(self, catalog_id: str, name: str, op_id: str | None = None) -> Operation:
        """Create an add_catalog operation"""
        return create_operation(
            provider=self.provider,
            op_type="add_catalog",
            target=catalog_id,
            payload={"catalogId": catalog_id, "name": name},
            op_id=op_id,
        )

    def rename_catalog(
        self, catalog_id: str, new_name: str, old_name: str, op_id: str | None = None
    ) -> Operation:
        """Create a rename_catalog operation"""
        return create_operation(
            provider=self.provider,
            op_type="rename_catalog",
            target=catalog_id,
            payload={"oldName": old_name, "newName": new_name},
            op_id=op_id,
        )

    def drop_catalog(self, catalog_id: str, op_id: str | None = None) -> Operation:
        """Create a drop_catalog operation"""
        return create_operation(
            provider=self.provider,
            op_type="drop_catalog",
            target=catalog_id,
            payload={},
            op_id=op_id,
        )

    # Schema Operations
    def add_schema(
        self, schema_id: str, name: str, catalog_id: str, op_id: str | None = None
    ) -> Operation:
        """Create an add_schema operation"""
        return create_operation(
            provider=self.provider,
            op_type="add_schema",
            target=schema_id,
            payload={"schemaId": schema_id, "name": name, "catalogId": catalog_id},
            op_id=op_id,
        )

    def rename_schema(
        self, schema_id: str, new_name: str, old_name: str, op_id: str | None = None
    ) -> Operation:
        """Create a rename_schema operation"""
        return create_operation(
            provider=self.provider,
            op_type="rename_schema",
            target=schema_id,
            payload={"oldName": old_name, "newName": new_name},
            op_id=op_id,
        )

    def drop_schema(self, schema_id: str, op_id: str | None = None) -> Operation:
        """Create a drop_schema operation"""
        return create_operation(
            provider=self.provider,
            op_type="drop_schema",
            target=schema_id,
            payload={},
            op_id=op_id,
        )

    # Table Operations
    def add_table(
        self,
        table_id: str,
        name: str,
        schema_id: str,
        format: Literal["delta", "iceberg"] = "delta",
        comment: str | None = None,
        op_id: str | None = None,
    ) -> Operation:
        """Create an add_table operation"""
        payload: dict[str, Any] = {
            "tableId": table_id,
            "name": name,
            "schemaId": schema_id,
            "format": format,
        }
        if comment is not None:
            payload["comment"] = comment
        return create_operation(
            provider=self.provider,
            op_type="add_table",
            target=table_id,
            payload=payload,
            op_id=op_id,
        )

    def rename_table(
        self, table_id: str, new_name: str, old_name: str, op_id: str | None = None
    ) -> Operation:
        """Create a rename_table operation"""
        return create_operation(
            provider=self.provider,
            op_type="rename_table",
            target=table_id,
            payload={"oldName": old_name, "newName": new_name},
            op_id=op_id,
        )

    def drop_table(self, table_id: str, op_id: str | None = None) -> Operation:
        """Create a drop_table operation"""
        return create_operation(
            provider=self.provider,
            op_type="drop_table",
            target=table_id,
            payload={},
            op_id=op_id,
        )

    def set_table_comment(self, table_id: str, comment: str, op_id: str | None = None) -> Operation:
        """Create a set_table_comment operation"""
        return create_operation(
            provider=self.provider,
            op_type="set_table_comment",
            target=table_id,
            payload={"tableId": table_id, "comment": comment},
            op_id=op_id,
        )

    def set_table_property(
        self, table_id: str, key: str, value: str, op_id: str | None = None
    ) -> Operation:
        """Create a set_table_property operation"""
        return create_operation(
            provider=self.provider,
            op_type="set_table_property",
            target=table_id,
            payload={"tableId": table_id, "key": key, "value": value},
            op_id=op_id,
        )

    def unset_table_property(self, table_id: str, key: str, op_id: str | None = None) -> Operation:
        """Create an unset_table_property operation"""
        return create_operation(
            provider=self.provider,
            op_type="unset_table_property",
            target=table_id,
            payload={"tableId": table_id, "key": key},
            op_id=op_id,
        )

    def set_table_tag(
        self, table_id: str, tag_name: str, tag_value: str, op_id: str | None = None
    ) -> Operation:
        """Create a set_table_tag operation"""
        return create_operation(
            provider=self.provider,
            op_type="set_table_tag",
            target=table_id,
            payload={"tableId": table_id, "tagName": tag_name, "tagValue": tag_value},
            op_id=op_id,
        )

    def unset_table_tag(self, table_id: str, tag_name: str, op_id: str | None = None) -> Operation:
        """Create an unset_table_tag operation"""
        return create_operation(
            provider=self.provider,
            op_type="unset_table_tag",
            target=table_id,
            payload={"tableId": table_id, "tagName": tag_name},
            op_id=op_id,
        )

    # Column Operations
    def add_column(
        self,
        col_id: str,
        table_id: str,
        name: str,
        type: str,
        nullable: bool = True,
        comment: str | None = None,
        op_id: str | None = None,
    ) -> Operation:
        """Create an add_column operation"""
        payload = {
            "tableId": table_id,
            "colId": col_id,
            "name": name,
            "type": type,
            "nullable": nullable,
        }
        if comment is not None:
            payload["comment"] = comment
        return create_operation(
            provider=self.provider,
            op_type="add_column",
            target=col_id,
            payload=payload,
            op_id=op_id,
        )

    def rename_column(
        self,
        col_id: str,
        table_id: str,
        new_name: str,
        old_name: str,
        op_id: str | None = None,
    ) -> Operation:
        """Create a rename_column operation"""
        return create_operation(
            provider=self.provider,
            op_type="rename_column",
            target=col_id,
            payload={"tableId": table_id, "oldName": old_name, "newName": new_name},
            op_id=op_id,
        )

    def drop_column(self, col_id: str, table_id: str, op_id: str | None = None) -> Operation:
        """Create a drop_column operation"""
        return create_operation(
            provider=self.provider,
            op_type="drop_column",
            target=col_id,
            payload={"tableId": table_id},
            op_id=op_id,
        )

    def reorder_columns(
        self, table_id: str, order: list[str], op_id: str | None = None
    ) -> Operation:
        """Create a reorder_columns operation"""
        return create_operation(
            provider=self.provider,
            op_type="reorder_columns",
            target=table_id,
            payload={"tableId": table_id, "order": order},
            op_id=op_id,
        )

    def change_column_type(
        self, col_id: str, table_id: str, new_type: str, op_id: str | None = None
    ) -> Operation:
        """Create a change_column_type operation"""
        return create_operation(
            provider=self.provider,
            op_type="change_column_type",
            target=col_id,
            payload={"tableId": table_id, "newType": new_type},
            op_id=op_id,
        )

    def set_nullable(
        self, col_id: str, table_id: str, nullable: bool, op_id: str | None = None
    ) -> Operation:
        """Create a set_nullable operation"""
        return create_operation(
            provider=self.provider,
            op_type="set_nullable",
            target=col_id,
            payload={"tableId": table_id, "nullable": nullable},
            op_id=op_id,
        )

    def set_column_comment(
        self, col_id: str, table_id: str, comment: str, op_id: str | None = None
    ) -> Operation:
        """Create a set_column_comment operation"""
        return create_operation(
            provider=self.provider,
            op_type="set_column_comment",
            target=col_id,
            payload={"tableId": table_id, "comment": comment},
            op_id=op_id,
        )

    # Column Tag Operations
    def set_column_tag(
        self, col_id: str, table_id: str, tag_name: str, tag_value: str, op_id: str | None = None
    ) -> Operation:
        """Create a set_column_tag operation"""
        return create_operation(
            provider=self.provider,
            op_type="set_column_tag",
            target=col_id,
            payload={"tableId": table_id, "tagName": tag_name, "tagValue": tag_value},
            op_id=op_id,
        )

    def unset_column_tag(
        self, col_id: str, table_id: str, tag_name: str, op_id: str | None = None
    ) -> Operation:
        """Create an unset_column_tag operation"""
        return create_operation(
            provider=self.provider,
            op_type="unset_column_tag",
            target=col_id,
            payload={"tableId": table_id, "tagName": tag_name},
            op_id=op_id,
        )

    # Constraint Operations
    def add_constraint(
        self,
        constraint_id: str,
        table_id: str,
        type: Literal["primary_key", "foreign_key", "check"],
        columns: list[str],
        name: str | None = None,
        op_id: str | None = None,
        **kwargs,
    ) -> Operation:
        """
        Create an add_constraint operation

        For PRIMARY KEY: can include timeseries=True
        For FOREIGN KEY: requires parentTable, parentColumns
        For CHECK: requires expression

        Note: Unity Catalog constraints are informational only (not enforced).
        """
        payload = {
            "tableId": table_id,
            "constraintId": constraint_id,
            "type": type,
            "columns": columns,
        }
        if name:
            payload["name"] = name
        payload.update(kwargs)
        return create_operation(
            provider=self.provider,
            op_type="add_constraint",
            target=constraint_id,
            payload=payload,
            op_id=op_id,
        )

    def drop_constraint(
        self, constraint_id: str, table_id: str, op_id: str | None = None, **kwargs
    ) -> Operation:
        """Create a drop_constraint operation"""
        payload = {"tableId": table_id}
        payload.update(kwargs)
        return create_operation(
            provider=self.provider,
            op_type="drop_constraint",
            target=constraint_id,
            payload=payload,
            op_id=op_id,
        )

    # Row Filter Operations
    def add_row_filter(
        self,
        filter_id: str,
        table_id: str,
        name: str,
        udf_expression: str,
        enabled: bool = True,
        description: str | None = None,
        op_id: str | None = None,
    ) -> Operation:
        """Create an add_row_filter operation"""
        payload = {
            "tableId": table_id,
            "filterId": filter_id,
            "name": name,
            "udfExpression": udf_expression,
            "enabled": enabled,
        }
        if description:
            payload["description"] = description
        return create_operation(
            provider=self.provider,
            op_type="add_row_filter",
            target=filter_id,
            payload=payload,
            op_id=op_id,
        )

    def update_row_filter(
        self, filter_id: str, table_id: str, op_id: str | None = None, **kwargs
    ) -> Operation:
        """Create an update_row_filter operation"""
        payload = {"tableId": table_id}
        payload.update(kwargs)
        return create_operation(
            provider=self.provider,
            op_type="update_row_filter",
            target=filter_id,
            payload=payload,
            op_id=op_id,
        )

    def remove_row_filter(
        self, filter_id: str, table_id: str, op_id: str | None = None
    ) -> Operation:
        """Create a remove_row_filter operation"""
        return create_operation(
            provider=self.provider,
            op_type="remove_row_filter",
            target=filter_id,
            payload={"tableId": table_id},
            op_id=op_id,
        )

    # Column Mask Operations
    def add_column_mask(
        self,
        mask_id: str,
        table_id: str,
        column_id: str,
        name: str,
        mask_function: str,
        enabled: bool = True,
        description: str | None = None,
        op_id: str | None = None,
    ) -> Operation:
        """Create an add_column_mask operation"""
        payload = {
            "tableId": table_id,
            "maskId": mask_id,
            "columnId": column_id,
            "name": name,
            "maskFunction": mask_function,
            "enabled": enabled,
        }
        if description:
            payload["description"] = description
        return create_operation(
            provider=self.provider,
            op_type="add_column_mask",
            target=mask_id,
            payload=payload,
            op_id=op_id,
        )

    def update_column_mask(
        self, mask_id: str, table_id: str, op_id: str | None = None, **kwargs
    ) -> Operation:
        """Create an update_column_mask operation"""
        payload = {"tableId": table_id}
        payload.update(kwargs)
        return create_operation(
            provider=self.provider,
            op_type="update_column_mask",
            target=mask_id,
            payload=payload,
            op_id=op_id,
        )

    def remove_column_mask(
        self,
        mask_id: str,
        table_id: str,
        column_id: str | None = None,
        op_id: str | None = None,
    ) -> Operation:
        """Create a remove_column_mask operation"""
        payload: dict[str, str] = {"tableId": table_id}
        if column_id is not None:
            payload["columnId"] = column_id
        return create_operation(
            provider=self.provider,
            op_type="remove_column_mask",
            target=mask_id,
            payload=payload,
            op_id=op_id,
        )

    # View Operations
    def add_view(
        self,
        view_id: str,
        name: str,
        schema_id: str,
        definition: str,
        comment: str | None = None,
        dependencies: list[str] | None = None,
        op_id: str | None = None,
    ) -> Operation:
        """Create an add_view operation"""
        payload = {
            "viewId": view_id,
            "name": name,
            "schemaId": schema_id,
            "definition": definition,
        }
        if comment:
            payload["comment"] = comment
        if dependencies:
            payload["dependencies"] = dependencies

        return create_operation(
            provider=self.provider,
            op_type="add_view",
            target=view_id,
            payload=payload,
            op_id=op_id,
        )

    def rename_view(
        self, view_id: str, new_name: str, old_name: str, op_id: str | None = None
    ) -> Operation:
        """Create a rename_view operation"""
        return create_operation(
            provider=self.provider,
            op_type="rename_view",
            target=view_id,
            payload={"newName": new_name},
            op_id=op_id,
        )

    def drop_view(self, view_id: str, op_id: str | None = None) -> Operation:
        """Create a drop_view operation"""
        return create_operation(
            provider=self.provider,
            op_type="drop_view",
            target=view_id,
            payload={},
            op_id=op_id,
        )

    def update_view(
        self,
        view_id: str,
        definition: str | None = None,
        dependencies: list[str] | None = None,
        extracted_dependencies: dict[str, list[str]] | None = None,
        op_id: str | None = None,
    ) -> Operation:
        """Create an update_view operation"""
        payload = {}
        if definition:
            payload["definition"] = definition
        if dependencies:
            payload["dependencies"] = dependencies
        if extracted_dependencies:
            payload["extractedDependencies"] = extracted_dependencies

        return create_operation(
            provider=self.provider,
            op_type="update_view",
            target=view_id,
            payload=payload,
            op_id=op_id,
        )


def make_operation_sequence(
    specs: list[dict[str, Any]], provider: str = "unity"
) -> list[Operation]:
    """
    Create a sequence of operations from specifications.

    Args:
        specs: List of operation specifications, each containing:
            - op_type: Operation type (e.g., "add_catalog")
            - target: Target object ID
            - payload: Operation payload
            - op_id: Optional operation ID
        provider: Provider ID (default: "unity")

    Returns:
        List of Operation instances

    Example:
        ops = make_operation_sequence([
            {
                "op_type": "add_catalog",
                "target": "cat_1",
                "payload": {"catalogId": "cat_1", "name": "bronze"},
                "op_id": "op_001"
            },
            {
                "op_type": "add_schema",
                "target": "sch_1",
                "payload": {"schemaId": "sch_1", "name": "raw", "catalogId": "cat_1"},
                "op_id": "op_002"
            }
        ])
    """
    return [
        create_operation(
            provider=provider,
            op_type=spec["op_type"],
            target=spec["target"],
            payload=spec["payload"],
            op_id=spec.get("op_id"),
        )
        for spec in specs
    ]
