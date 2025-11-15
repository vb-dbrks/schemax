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
    """Table constraint (PRIMARY KEY, FOREIGN KEY, or CHECK)

    Note: Unity Catalog constraints are informational only (not enforced).
    They are used for query optimization and documentation purposes.
    """

    id: str
    type: Literal["primary_key", "foreign_key", "check"]
    name: str | None = None  # CONSTRAINT name
    columns: list[str]  # column IDs

    # For PRIMARY KEY
    timeseries: bool | None = None

    # For FOREIGN KEY
    parent_table: str | None = Field(None, alias="parentTable")  # Parent table ID
    parent_columns: list[str] | None = Field(None, alias="parentColumns")  # Parent column IDs

    # For CHECK
    expression: str | None = None  # CHECK expression

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
    external: bool | None = None  # Whether this is an external table
    external_location_name: str | None = Field(
        None, alias="externalLocationName"
    )  # Reference to environment external location
    path: str | None = None  # Relative path under the external location
    partition_columns: list[str] | None = Field(
        None, alias="partitionColumns"
    )  # For PARTITIONED BY
    cluster_columns: list[str] | None = Field(None, alias="clusterColumns")  # For CLUSTER BY
    column_mapping: Literal["name", "id"] | None = Field(None, alias="columnMapping")
    columns: list[UnityColumn] = []
    properties: dict[str, str] = {}  # TBLPROPERTIES (Delta Lake config)
    tags: dict[str, str] = {}  # TABLE TAGS (Unity Catalog governance)
    constraints: list[UnityConstraint] = []
    grants: list[UnityGrant] = []
    comment: str | None = None
    row_filters: list[UnityRowFilter] | None = Field(None, alias="rowFilters")
    column_masks: list[UnityColumnMask] | None = Field(None, alias="columnMasks")

    model_config = ConfigDict(populate_by_name=True)


class UnityView(BaseModel):
    """View definition"""

    id: str
    name: str
    definition: str  # SQL query (SELECT statement)
    comment: str | None = None
    # Explicit dependencies (user-specified)
    dependencies: list[str] | None = None  # IDs of tables/views this view depends on
    # Extracted dependencies (from SQL parsing)
    extracted_dependencies: dict[str, list[str]] | None = Field(
        None, alias="extractedDependencies"
    )  # tables, views, catalogs, schemas
    # Metadata
    tags: dict[str, str] = {}  # VIEW TAGS (Unity Catalog governance)
    properties: dict[str, str] = {}  # View properties

    model_config = ConfigDict(populate_by_name=True)


class UnitySchema(BaseModel):
    """Schema definition"""

    id: str
    name: str
    managed_location_name: str | None = Field(
        None, alias="managedLocationName"
    )  # Reference to env managedLocations
    comment: str | None = None  # Schema comment
    tags: dict[str, str] = {}  # Schema tags (Unity Catalog governance)
    tables: list[UnityTable] = []
    views: list[UnityView] = []  # Views stored alongside tables in schema

    model_config = ConfigDict(populate_by_name=True)


class UnityCatalog(BaseModel):
    """Catalog definition"""

    id: str
    name: str
    managed_location_name: str | None = Field(
        None, alias="managedLocationName"
    )  # Reference to env managedLocations
    comment: str | None = None  # Catalog comment
    tags: dict[str, str] = {}  # Catalog tags (Unity Catalog governance)
    schemas: list[UnitySchema] = []

    model_config = ConfigDict(populate_by_name=True)


class UnityState(BaseModel):
    """Unity Catalog state"""

    catalogs: list[UnityCatalog] = []
