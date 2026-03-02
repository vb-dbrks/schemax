"""Operation metadata for Hive provider MVP."""

from schemax.providers.base.operations import ManagedCategory, OperationCategory, OperationMetadata

HIVE_OPERATIONS = {
    "ADD_DATABASE": "hive.add_database",
    "DROP_DATABASE": "hive.drop_database",
    "ADD_TABLE": "hive.add_table",
    "DROP_TABLE": "hive.drop_table",
}

hive_operation_metadata = [
    OperationMetadata(
        type=HIVE_OPERATIONS["ADD_DATABASE"],
        display_name="Add Database",
        description="Create a Hive database",
        category=OperationCategory.CATALOG,
        required_fields=["name"],
        optional_fields=["comment"],
        is_destructive=False,
        managed_category=ManagedCategory.CATALOG_STRUCTURE,
    ),
    OperationMetadata(
        type=HIVE_OPERATIONS["DROP_DATABASE"],
        display_name="Drop Database",
        description="Drop a Hive database",
        category=OperationCategory.CATALOG,
        required_fields=[],
        optional_fields=[],
        is_destructive=True,
        managed_category=ManagedCategory.CATALOG_STRUCTURE,
    ),
    OperationMetadata(
        type=HIVE_OPERATIONS["ADD_TABLE"],
        display_name="Add Table",
        description="Create a Hive table",
        category=OperationCategory.TABLE,
        required_fields=["database", "name", "columns"],
        optional_fields=["comment"],
        is_destructive=False,
        managed_category=ManagedCategory.TABLE_STRUCTURE,
    ),
    OperationMetadata(
        type=HIVE_OPERATIONS["DROP_TABLE"],
        display_name="Drop Table",
        description="Drop a Hive table",
        category=OperationCategory.TABLE,
        required_fields=[],
        optional_fields=[],
        is_destructive=True,
        managed_category=ManagedCategory.TABLE_STRUCTURE,
    ),
]
