"""
Operation builders for tests

Domain builders (CatalogOpBuilder, SchemaOpBuilder, etc.) each handle one object kind.
OperationBuilder is a container: use builder.catalog.add_catalog(...), builder.schema.add_schema(...), etc.
"""

from typing import Any, Literal, TypedDict

from schemax.providers.base.operations import Operation, create_operation


class TableAddOptions(TypedDict, total=False):
    """Optional fields for add_table payload."""

    comment: str
    external: bool
    external_location_name: str
    path: str
    partition_columns: list[str]
    cluster_columns: list[str]


class CatalogOpBuilder:
    """Builds catalog operations (add, rename, drop)."""

    def __init__(self, provider: str = "unity") -> None:
        self.provider = provider

    def add_catalog(
        self,
        catalog_id: str,
        name: str,
        op_id: str | None = None,
        managed_location_name: str | None = None,
    ) -> Operation:
        payload: dict[str, Any] = {"catalogId": catalog_id, "name": name}
        if managed_location_name is not None:
            payload["managedLocationName"] = managed_location_name
        return create_operation(
            provider=self.provider,
            op_type="add_catalog",
            target=catalog_id,
            payload=payload,
            op_id=op_id,
        )

    def rename_catalog(
        self, catalog_id: str, new_name: str, old_name: str, op_id: str | None = None
    ) -> Operation:
        return create_operation(
            provider=self.provider,
            op_type="rename_catalog",
            target=catalog_id,
            payload={"oldName": old_name, "newName": new_name},
            op_id=op_id,
        )

    def drop_catalog(self, catalog_id: str, op_id: str | None = None) -> Operation:
        return create_operation(
            provider=self.provider,
            op_type="drop_catalog",
            target=catalog_id,
            payload={},
            op_id=op_id,
        )


class SchemaOpBuilder:
    """Builds schema operations (add, rename, drop)."""

    def __init__(self, provider: str = "unity") -> None:
        self.provider = provider

    def add_schema(
        self, schema_id: str, name: str, catalog_id: str, op_id: str | None = None
    ) -> Operation:
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
        return create_operation(
            provider=self.provider,
            op_type="rename_schema",
            target=schema_id,
            payload={"oldName": old_name, "newName": new_name},
            op_id=op_id,
        )

    def drop_schema(self, schema_id: str, op_id: str | None = None) -> Operation:
        return create_operation(
            provider=self.provider,
            op_type="drop_schema",
            target=schema_id,
            payload={},
            op_id=op_id,
        )


class TableOpBuilder:
    """Builds table operations (add, rename, drop, comment, properties, tags)."""

    def __init__(self, provider: str = "unity") -> None:
        self.provider = provider

    def add_table(
        self,
        table_id: str,
        name: str,
        schema_id: str,
        table_format: Literal["delta", "iceberg"] = "delta",
        options: TableAddOptions | None = None,
        op_id: str | None = None,
    ) -> Operation:
        payload: dict[str, Any] = {
            "tableId": table_id,
            "name": name,
            "schemaId": schema_id,
            "format": table_format,
        }
        if options:
            if "comment" in options and options["comment"] is not None:
                payload["comment"] = options["comment"]
            if options.get("external"):
                payload["external"] = True
            if options.get("external_location_name") is not None:
                payload["externalLocationName"] = options["external_location_name"]
            if options.get("path") is not None:
                payload["path"] = options["path"]
            if options.get("partition_columns") is not None:
                payload["partitionColumns"] = options["partition_columns"]
            if options.get("cluster_columns") is not None:
                payload["clusterColumns"] = options["cluster_columns"]
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
        return create_operation(
            provider=self.provider,
            op_type="rename_table",
            target=table_id,
            payload={"oldName": old_name, "newName": new_name},
            op_id=op_id,
        )

    def drop_table(self, table_id: str, op_id: str | None = None) -> Operation:
        return create_operation(
            provider=self.provider,
            op_type="drop_table",
            target=table_id,
            payload={},
            op_id=op_id,
        )

    def set_table_comment(self, table_id: str, comment: str, op_id: str | None = None) -> Operation:
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
        return create_operation(
            provider=self.provider,
            op_type="set_table_property",
            target=table_id,
            payload={"tableId": table_id, "key": key, "value": value},
            op_id=op_id,
        )

    def unset_table_property(self, table_id: str, key: str, op_id: str | None = None) -> Operation:
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
        return create_operation(
            provider=self.provider,
            op_type="set_table_tag",
            target=table_id,
            payload={"tableId": table_id, "tagName": tag_name, "tagValue": tag_value},
            op_id=op_id,
        )

    def unset_table_tag(self, table_id: str, tag_name: str, op_id: str | None = None) -> Operation:
        return create_operation(
            provider=self.provider,
            op_type="unset_table_tag",
            target=table_id,
            payload={"tableId": table_id, "tagName": tag_name},
            op_id=op_id,
        )


class ColumnOpBuilder:
    """Builds column operations (add, rename, drop, reorder, type, nullable, comment, tags)."""

    def __init__(self, provider: str = "unity") -> None:
        self.provider = provider

    def add_column(
        self,
        col_id: str,
        table_id: str,
        name: str,
        col_type: str,
        nullable: bool = True,
        comment: str | None = None,
        op_id: str | None = None,
    ) -> Operation:
        payload: dict[str, Any] = {
            "tableId": table_id,
            "colId": col_id,
            "name": name,
            "type": col_type,
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
        return create_operation(
            provider=self.provider,
            op_type="rename_column",
            target=col_id,
            payload={"tableId": table_id, "oldName": old_name, "newName": new_name},
            op_id=op_id,
        )

    def drop_column(self, col_id: str, table_id: str, op_id: str | None = None) -> Operation:
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
        return create_operation(
            provider=self.provider,
            op_type="set_column_comment",
            target=col_id,
            payload={"tableId": table_id, "comment": comment},
            op_id=op_id,
        )

    def set_column_tag(
        self,
        col_id: str,
        table_id: str,
        tag_name: str,
        tag_value: str,
        op_id: str | None = None,
    ) -> Operation:
        return create_operation(
            provider=self.provider,
            op_type="set_column_tag",
            target=col_id,
            payload={
                "tableId": table_id,
                "tagName": tag_name,
                "tagValue": tag_value,
            },
            op_id=op_id,
        )

    def unset_column_tag(
        self, col_id: str, table_id: str, tag_name: str, op_id: str | None = None
    ) -> Operation:
        return create_operation(
            provider=self.provider,
            op_type="unset_column_tag",
            target=col_id,
            payload={"tableId": table_id, "tagName": tag_name},
            op_id=op_id,
        )


class ConstraintOpBuilder:
    """Builds constraint operations (add, drop)."""

    def __init__(self, provider: str = "unity") -> None:
        self.provider = provider

    def add_constraint(
        self,
        constraint_id: str,
        table_id: str,
        constraint_type: Literal["primary_key", "foreign_key", "check"],
        columns: list[str],
        name: str | None = None,
        op_id: str | None = None,
        **kwargs: Any,
    ) -> Operation:
        payload: dict[str, Any] = {
            "tableId": table_id,
            "constraintId": constraint_id,
            "type": constraint_type,
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
        self,
        constraint_id: str,
        table_id: str,
        op_id: str | None = None,
        **kwargs: Any,
    ) -> Operation:
        payload: dict[str, Any] = {"tableId": table_id}
        payload.update(kwargs)
        return create_operation(
            provider=self.provider,
            op_type="drop_constraint",
            target=constraint_id,
            payload=payload,
            op_id=op_id,
        )


class RowFilterOpBuilder:
    """Builds row filter operations (add, update, remove)."""

    def __init__(self, provider: str = "unity") -> None:
        self.provider = provider

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
        payload: dict[str, Any] = {
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
        self, filter_id: str, table_id: str, op_id: str | None = None, **kwargs: Any
    ) -> Operation:
        payload: dict[str, Any] = {"tableId": table_id}
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
        return create_operation(
            provider=self.provider,
            op_type="remove_row_filter",
            target=filter_id,
            payload={"tableId": table_id},
            op_id=op_id,
        )


class ColumnMaskOpBuilder:
    """Builds column mask operations (add, update, remove)."""

    def __init__(self, provider: str = "unity") -> None:
        self.provider = provider

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
        payload: dict[str, Any] = {
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
        self, mask_id: str, table_id: str, op_id: str | None = None, **kwargs: Any
    ) -> Operation:
        payload: dict[str, Any] = {"tableId": table_id}
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


class GrantOpBuilder:
    """Builds grant operations (add, revoke)."""

    def __init__(self, provider: str = "unity") -> None:
        self.provider = provider

    def add_grant(
        self,
        target_type: Literal["catalog", "schema", "table", "view"],
        target_id: str,
        principal: str,
        privileges: list[str],
        op_id: str | None = None,
    ) -> Operation:
        return create_operation(
            provider=self.provider,
            op_type="add_grant",
            target=target_id,
            payload={
                "targetType": target_type,
                "targetId": target_id,
                "principal": principal,
                "privileges": privileges,
            },
            op_id=op_id,
        )

    def revoke_grant(
        self,
        target_type: Literal["catalog", "schema", "table", "view"],
        target_id: str,
        principal: str,
        privileges: list[str] | None = None,
        op_id: str | None = None,
    ) -> Operation:
        payload: dict[str, Any] = {
            "targetType": target_type,
            "targetId": target_id,
            "principal": principal,
        }
        if privileges is not None:
            payload["privileges"] = privileges
        return create_operation(
            provider=self.provider,
            op_type="revoke_grant",
            target=target_id,
            payload=payload,
            op_id=op_id,
        )


class ViewOpBuilder:
    """Builds view operations (add, rename, drop, update)."""

    def __init__(self, provider: str = "unity") -> None:
        self.provider = provider

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
        payload: dict[str, Any] = {
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
        self, view_id: str, new_name: str, _old_name: str, op_id: str | None = None
    ) -> Operation:
        return create_operation(
            provider=self.provider,
            op_type="rename_view",
            target=view_id,
            payload={"newName": new_name},
            op_id=op_id,
        )

    def drop_view(self, view_id: str, op_id: str | None = None) -> Operation:
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
        payload: dict[str, Any] = {}
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


class VolumeOpBuilder:
    """Builds volume operations (add, rename, update, drop)."""

    def __init__(self, provider: str = "unity") -> None:
        self.provider = provider

    def add_volume(
        self,
        volume_id: str,
        name: str,
        schema_id: str,
        volume_type: Literal["managed", "external"] = "managed",
        comment: str | None = None,
        location: str | None = None,
        op_id: str | None = None,
    ) -> Operation:
        payload: dict[str, Any] = {
            "volumeId": volume_id,
            "name": name,
            "schemaId": schema_id,
            "volumeType": volume_type,
        }
        if comment is not None:
            payload["comment"] = comment
        if location is not None:
            payload["location"] = location
        return create_operation(
            provider=self.provider,
            op_type="add_volume",
            target=volume_id,
            payload=payload,
            op_id=op_id,
        )

    def rename_volume(
        self, volume_id: str, new_name: str, _old_name: str, op_id: str | None = None
    ) -> Operation:
        return create_operation(
            provider=self.provider,
            op_type="rename_volume",
            target=volume_id,
            payload={"newName": new_name},
            op_id=op_id,
        )

    def update_volume(
        self,
        volume_id: str,
        comment: str | None = None,
        location: str | None = None,
        op_id: str | None = None,
    ) -> Operation:
        payload: dict[str, Any] = {}
        if comment is not None:
            payload["comment"] = comment
        if location is not None:
            payload["location"] = location
        return create_operation(
            provider=self.provider,
            op_type="update_volume",
            target=volume_id,
            payload=payload,
            op_id=op_id,
        )

    def drop_volume(self, volume_id: str, op_id: str | None = None) -> Operation:
        return create_operation(
            provider=self.provider,
            op_type="drop_volume",
            target=volume_id,
            payload={},
            op_id=op_id,
        )


class FunctionOpBuilder:
    """Builds function operations (add, rename, update, drop, comment)."""

    def __init__(self, provider: str = "unity") -> None:
        self.provider = provider

    def add_function(
        self,
        function_id: str,
        name: str,
        schema_id: str,
        language: Literal["SQL", "PYTHON"],
        return_type: str,
        body: str,
        comment: str | None = None,
        parameters: list[dict[str, Any]] | None = None,
        op_id: str | None = None,
    ) -> Operation:
        payload: dict[str, Any] = {
            "functionId": function_id,
            "name": name,
            "schemaId": schema_id,
            "language": language,
            "returnType": return_type,
            "body": body,
        }
        if comment is not None:
            payload["comment"] = comment
        if parameters is not None:
            payload["parameters"] = parameters
        return create_operation(
            provider=self.provider,
            op_type="add_function",
            target=function_id,
            payload=payload,
            op_id=op_id,
        )

    def rename_function(
        self, function_id: str, new_name: str, _old_name: str, op_id: str | None = None
    ) -> Operation:
        return create_operation(
            provider=self.provider,
            op_type="rename_function",
            target=function_id,
            payload={"newName": new_name},
            op_id=op_id,
        )

    def update_function(
        self,
        function_id: str,
        body: str | None = None,
        comment: str | None = None,
        op_id: str | None = None,
    ) -> Operation:
        payload: dict[str, Any] = {}
        if body is not None:
            payload["body"] = body
        if comment is not None:
            payload["comment"] = comment
        return create_operation(
            provider=self.provider,
            op_type="update_function",
            target=function_id,
            payload=payload,
            op_id=op_id,
        )

    def drop_function(self, function_id: str, op_id: str | None = None) -> Operation:
        return create_operation(
            provider=self.provider,
            op_type="drop_function",
            target=function_id,
            payload={},
            op_id=op_id,
        )

    def set_function_comment(
        self, function_id: str, comment: str, op_id: str | None = None
    ) -> Operation:
        return create_operation(
            provider=self.provider,
            op_type="set_function_comment",
            target=function_id,
            payload={"functionId": function_id, "comment": comment},
            op_id=op_id,
        )


class MaterializedViewOpBuilder:
    """Builds materialized view operations (add, rename, update, drop, comment)."""

    def __init__(self, provider: str = "unity") -> None:
        self.provider = provider

    def add_materialized_view(
        self,
        mv_id: str,
        name: str,
        schema_id: str,
        definition: str,
        comment: str | None = None,
        refresh_schedule: str | None = None,
        partition_columns: list[str] | None = None,
        cluster_columns: list[str] | None = None,
        extracted_dependencies: dict[str, list[str]] | None = None,
        op_id: str | None = None,
    ) -> Operation:
        payload: dict[str, Any] = {
            "materializedViewId": mv_id,
            "name": name,
            "schemaId": schema_id,
            "definition": definition,
        }
        if comment is not None:
            payload["comment"] = comment
        if refresh_schedule is not None:
            payload["refreshSchedule"] = refresh_schedule
        if partition_columns is not None:
            payload["partitionColumns"] = partition_columns
        if cluster_columns is not None:
            payload["clusterColumns"] = cluster_columns
        if extracted_dependencies is not None:
            payload["extractedDependencies"] = extracted_dependencies
        return create_operation(
            provider=self.provider,
            op_type="add_materialized_view",
            target=mv_id,
            payload=payload,
            op_id=op_id,
        )

    def rename_materialized_view(
        self, mv_id: str, new_name: str, _old_name: str, op_id: str | None = None
    ) -> Operation:
        return create_operation(
            provider=self.provider,
            op_type="rename_materialized_view",
            target=mv_id,
            payload={"newName": new_name},
            op_id=op_id,
        )

    def update_materialized_view(
        self,
        mv_id: str,
        definition: str | None = None,
        comment: str | None = None,
        refresh_schedule: str | None = None,
        op_id: str | None = None,
    ) -> Operation:
        payload: dict[str, Any] = {}
        if definition is not None:
            payload["definition"] = definition
        if comment is not None:
            payload["comment"] = comment
        if refresh_schedule is not None:
            payload["refreshSchedule"] = refresh_schedule
        return create_operation(
            provider=self.provider,
            op_type="update_materialized_view",
            target=mv_id,
            payload=payload,
            op_id=op_id,
        )

    def drop_materialized_view(self, mv_id: str, op_id: str | None = None) -> Operation:
        return create_operation(
            provider=self.provider,
            op_type="drop_materialized_view",
            target=mv_id,
            payload={},
            op_id=op_id,
        )

    def set_materialized_view_comment(
        self, mv_id: str, comment: str, op_id: str | None = None
    ) -> Operation:
        return create_operation(
            provider=self.provider,
            op_type="set_materialized_view_comment",
            target=mv_id,
            payload={"materializedViewId": mv_id, "comment": comment},
            op_id=op_id,
        )


class OperationBuilder:
    """
    Container of domain builders for Unity Catalog operations in tests.

    Use builder.catalog.add_catalog(...), builder.schema.add_schema(...), etc.
    """

    def __init__(self, provider: str = "unity") -> None:
        self.provider = provider
        self.catalog = CatalogOpBuilder(provider)
        self.schema = SchemaOpBuilder(provider)
        self.table = TableOpBuilder(provider)
        self.column = ColumnOpBuilder(provider)
        self.constraint = ConstraintOpBuilder(provider)
        self.row_filter = RowFilterOpBuilder(provider)
        self.column_mask = ColumnMaskOpBuilder(provider)
        self.grant = GrantOpBuilder(provider)
        self.view = ViewOpBuilder(provider)
        self.volume = VolumeOpBuilder(provider)
        self.function = FunctionOpBuilder(provider)
        self.materialized_view = MaterializedViewOpBuilder(provider)


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
