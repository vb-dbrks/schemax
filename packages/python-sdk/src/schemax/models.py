"""
Pydantic models for SchemaX schema definitions.

These models are ports of the TypeScript Zod schemas from the VS Code extension,
ensuring compatibility between the two implementations.
"""

from typing import Any, Dict, List, Literal, Optional
from pydantic import BaseModel, Field


class Column(BaseModel):
    """Column definition with optional tags and masking"""

    id: str
    name: str
    type: str
    nullable: bool
    comment: Optional[str] = None
    tags: Optional[Dict[str, str]] = None  # tag_name: tag_value
    mask_id: Optional[str] = Field(None, alias="maskId")  # Reference to active column mask

    class Config:
        populate_by_name = True


class RowFilter(BaseModel):
    """Row-level security filter with UDF expression"""

    id: str
    name: str
    enabled: bool
    udf_expression: str = Field(..., alias="udfExpression")  # SQL UDF expression
    description: Optional[str] = None

    class Config:
        populate_by_name = True


class ColumnMask(BaseModel):
    """Column-level data masking function"""

    id: str
    column_id: str = Field(..., alias="columnId")  # Reference to column
    name: str
    enabled: bool
    mask_function: str = Field(..., alias="maskFunction")  # SQL UDF that returns same type
    description: Optional[str] = None

    class Config:
        populate_by_name = True


class Constraint(BaseModel):
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


class Grant(BaseModel):
    """Access grant definition"""

    principal: str
    privileges: List[str]


class Table(BaseModel):
    """Table definition with columns, properties, and governance features"""

    id: str
    name: str
    format: Literal["delta", "iceberg"]
    column_mapping: Optional[Literal["name", "id"]] = Field(None, alias="columnMapping")
    columns: List[Column]
    properties: Dict[str, str]
    constraints: List[Constraint]
    grants: List[Grant]
    comment: Optional[str] = None
    row_filters: Optional[List[RowFilter]] = Field(None, alias="rowFilters")
    column_masks: Optional[List[ColumnMask]] = Field(None, alias="columnMasks")

    class Config:
        populate_by_name = True


class Schema(BaseModel):
    """Schema definition with tables"""

    id: str
    name: str
    tables: List[Table]


class Catalog(BaseModel):
    """Catalog definition with schemas"""

    id: str
    name: str
    schemas: List[Schema]


class State(BaseModel):
    """Complete schema state"""

    catalogs: List[Catalog]


class SnapshotMetadata(BaseModel):
    """Snapshot metadata (stored in project.json)"""

    id: str
    version: str  # semantic version
    name: str
    ts: str  # ISO timestamp
    created_by: Optional[str] = Field(None, alias="createdBy")
    file: str  # relative path to snapshot file
    previous_snapshot: Optional[str] = Field(None, alias="previousSnapshot")  # version
    ops_count: int = Field(..., alias="opsCount")  # number of ops included
    hash: str
    tags: List[str] = Field(default_factory=list)
    comment: Optional[str] = None

    class Config:
        populate_by_name = True


class SnapshotFile(BaseModel):
    """Snapshot file content (stored in .schemax/snapshots/vX.Y.Z.json)"""

    id: str
    version: str
    name: str
    ts: str  # ISO timestamp
    created_by: Optional[str] = Field(None, alias="createdBy")
    state: State
    ops_included: List[str] = Field(..., alias="opsIncluded")  # op IDs
    previous_snapshot: Optional[str] = Field(None, alias="previousSnapshot")  # version
    hash: str
    tags: List[str] = Field(default_factory=list)
    comment: Optional[str] = None

    class Config:
        populate_by_name = True


class Op(BaseModel):
    """Operation definition"""

    id: Optional[str] = None  # unique identifier (backwards compatible)
    ts: str  # ISO timestamp
    op: str  # operation type
    target: str  # id of the object the op applies to
    payload: Dict[str, Any]


class ChangelogFile(BaseModel):
    """Changelog file (changelog.json)"""

    version: Literal[1]
    since_snapshot: Optional[str] = Field(None, alias="sinceSnapshot")  # version
    ops: List[Op]
    last_modified: str = Field(..., alias="lastModified")  # ISO timestamp

    class Config:
        populate_by_name = True


class DriftInfo(BaseModel):
    """Drift detection information"""

    object_type: Literal["catalog", "schema", "table", "column"] = Field(..., alias="objectType")
    object_name: str = Field(..., alias="objectName")
    expected_version: str = Field(..., alias="expectedVersion")
    actual_version: Optional[str] = Field(None, alias="actualVersion")
    issue: Literal["missing", "modified", "extra"]

    class Config:
        populate_by_name = True


class Deployment(BaseModel):
    """Deployment record"""

    id: str
    environment: str
    ts: str  # ISO timestamp
    deployed_by: Optional[str] = Field(None, alias="deployedBy")
    snapshot_id: Optional[str] = Field(None, alias="snapshotId")
    ops_applied: List[str] = Field(..., alias="opsApplied")
    schema_version: str = Field(..., alias="schemaVersion")
    sql_generated: Optional[str] = Field(None, alias="sqlGenerated")
    status: Literal["success", "failed", "rolled_back"]
    error: Optional[str] = None
    drift_detected: bool = Field(False, alias="driftDetected")
    drift_details: Optional[List[DriftInfo]] = Field(None, alias="driftDetails")

    class Config:
        populate_by_name = True


class ProjectSettings(BaseModel):
    """Project settings"""

    auto_increment_version: bool = Field(True, alias="autoIncrementVersion")
    version_prefix: str = Field("v", alias="versionPrefix")
    require_snapshot_for_prod: bool = Field(True, alias="requireSnapshotForProd")
    allow_drift: bool = Field(False, alias="allowDrift")
    require_comments: bool = Field(False, alias="requireComments")
    warn_on_breaking_changes: bool = Field(True, alias="warnOnBreakingChanges")

    class Config:
        populate_by_name = True


class ProjectFile(BaseModel):
    """Project file definition (v2)"""

    version: Literal[2]
    name: str
    environments: List[Literal["dev", "test", "prod"]]
    snapshots: List[SnapshotMetadata] = Field(default_factory=list)
    deployments: List[Deployment] = Field(default_factory=list)
    settings: ProjectSettings = Field(default_factory=ProjectSettings)
    latest_snapshot: Optional[str] = Field(None, alias="latestSnapshot")  # version string

    class Config:
        populate_by_name = True
