"""
Unity Catalog Models

Migrated from TypeScript Unity provider with Unity-specific types
"""

from typing import Dict, List, Literal, Optional

from pydantic import BaseModel, Field


class UnityColumn(BaseModel):
    """Column definition"""

    id: str
    name: str
    type: str
    nullable: bool
    comment: Optional[str] = None
    tags: Optional[Dict[str, str]] = None  # tag_name: tag_value
    mask_id: Optional[str] = Field(None, alias="maskId")  # Reference to active column mask

    class Config:
        populate_by_name = True


class UnityRowFilter(BaseModel):
    """Row-level security filter"""

    id: str
    name: str
    enabled: bool
    udf_expression: str = Field(..., alias="udfExpression")  # SQL UDF expression
    description: Optional[str] = None

    class Config:
        populate_by_name = True


class UnityColumnMask(BaseModel):
    """Column-level data masking"""

    id: str
    column_id: str = Field(..., alias="columnId")  # Reference to column
    name: str
    enabled: bool
    mask_function: str = Field(..., alias="maskFunction")  # SQL UDF that returns same type
    description: Optional[str] = None

    class Config:
        populate_by_name = True


class UnityConstraint(BaseModel):
    """Table constraint (PRIMARY KEY, FOREIGN KEY, or CHECK)"""

    id: str
    type: Literal["primary_key", "foreign_key", "check"]
    name: Optional[str] = None  # CONSTRAINT name
    columns: List[str]  # column IDs

    # For PRIMARY KEY
    timeseries: Optional[bool] = None

    # For FOREIGN KEY
    parent_table: Optional[str] = Field(None, alias="parentTable")  # Parent table ID
    parent_columns: Optional[List[str]] = Field(None, alias="parentColumns")  # Parent column IDs
    match_full: Optional[bool] = Field(None, alias="matchFull")
    on_update: Optional[Literal["NO_ACTION"]] = Field(None, alias="onUpdate")
    on_delete: Optional[Literal["NO_ACTION"]] = Field(None, alias="onDelete")

    # For CHECK
    expression: Optional[str] = None  # CHECK expression

    # Constraint options (all types)
    not_enforced: Optional[bool] = Field(None, alias="notEnforced")
    deferrable: Optional[bool] = None
    initially_deferred: Optional[bool] = Field(None, alias="initiallyDeferred")
    rely: Optional[bool] = None  # For query optimization (Photon)

    class Config:
        populate_by_name = True


class UnityGrant(BaseModel):
    """Access grant definition"""

    principal: str
    privileges: List[str]


class UnityTable(BaseModel):
    """Table definition"""

    id: str
    name: str
    format: Literal["delta", "iceberg"]
    column_mapping: Optional[Literal["name", "id"]] = Field(None, alias="columnMapping")
    columns: List[UnityColumn] = []
    properties: Dict[str, str] = {}
    constraints: List[UnityConstraint] = []
    grants: List[UnityGrant] = []
    comment: Optional[str] = None
    row_filters: Optional[List[UnityRowFilter]] = Field(None, alias="rowFilters")
    column_masks: Optional[List[UnityColumnMask]] = Field(None, alias="columnMasks")

    class Config:
        populate_by_name = True


class UnitySchema(BaseModel):
    """Schema definition"""

    id: str
    name: str
    tables: List[UnityTable] = []


class UnityCatalog(BaseModel):
    """Catalog definition"""

    id: str
    name: str
    schemas: List[UnitySchema] = []


class UnityState(BaseModel):
    """Unity Catalog state"""

    catalogs: List[UnityCatalog] = []
