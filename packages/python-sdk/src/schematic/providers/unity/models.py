"""
Unity Catalog Models

Migrated from TypeScript Unity provider with Unity-specific types
"""

from typing import Literal

from pydantic import BaseModel, ConfigDict, Field


class UnityColumn(BaseModel):
    """Column definition"""

    id: str
    name: str
    type: str
    nullable: bool
    comment: str | None = None
    tags: dict[str, str] | None = None  # tag_name: tag_value
    mask_id: str | None = Field(None, alias="maskId")  # Reference to active column mask

    model_config = ConfigDict(populate_by_name=True)


class UnityRowFilter(BaseModel):
    """Row-level security filter"""

    id: str
    name: str
    enabled: bool
    udf_expression: str = Field(..., alias="udfExpression")  # SQL UDF expression
    description: str | None = None

    model_config = ConfigDict(populate_by_name=True)


class UnityColumnMask(BaseModel):
    """Column-level data masking"""

    id: str
    column_id: str = Field(..., alias="columnId")  # Reference to column
    name: str
    enabled: bool
    mask_function: str = Field(..., alias="maskFunction")  # SQL UDF that returns same type
    description: str | None = None

    model_config = ConfigDict(populate_by_name=True)


class UnityConstraint(BaseModel):
    """Table constraint (PRIMARY KEY, FOREIGN KEY, or CHECK)"""

    id: str
    type: Literal["primary_key", "foreign_key", "check"]
    name: str | None = None  # CONSTRAINT name
    columns: list[str]  # column IDs

    # For PRIMARY KEY
    timeseries: bool | None = None

    # For FOREIGN KEY
    parent_table: str | None = Field(None, alias="parentTable")  # Parent table ID
    parent_columns: list[str] | None = Field(None, alias="parentColumns")  # Parent column IDs
    match_full: bool | None = Field(None, alias="matchFull")
    on_update: Literal["NO_ACTION"] | None = Field(None, alias="onUpdate")
    on_delete: Literal["NO_ACTION"] | None = Field(None, alias="onDelete")

    # For CHECK
    expression: str | None = None  # CHECK expression

    # Constraint options (all types)
    not_enforced: bool | None = Field(None, alias="notEnforced")
    deferrable: bool | None = None
    initially_deferred: bool | None = Field(None, alias="initiallyDeferred")
    rely: bool | None = None  # For query optimization (Photon)

    model_config = ConfigDict(populate_by_name=True)


class UnityGrant(BaseModel):
    """Access grant definition"""

    principal: str
    privileges: list[str]


class UnityTable(BaseModel):
    """Table definition"""

    id: str
    name: str
    format: Literal["delta", "iceberg"]
    column_mapping: Literal["name", "id"] | None = Field(None, alias="columnMapping")
    columns: list[UnityColumn] = []
    properties: dict[str, str] = {}
    constraints: list[UnityConstraint] = []
    grants: list[UnityGrant] = []
    comment: str | None = None
    row_filters: list[UnityRowFilter] | None = Field(None, alias="rowFilters")
    column_masks: list[UnityColumnMask] | None = Field(None, alias="columnMasks")

    model_config = ConfigDict(populate_by_name=True)


class UnitySchema(BaseModel):
    """Schema definition"""

    id: str
    name: str
    tables: list[UnityTable] = []


class UnityCatalog(BaseModel):
    """Catalog definition"""

    id: str
    name: str
    schemas: list[UnitySchema] = []


class UnityState(BaseModel):
    """Unity Catalog state"""

    catalogs: list[UnityCatalog] = []
