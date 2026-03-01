import type { Operation } from '../../contracts/workspace';

interface BaseObject {
  id: string;
  name: string;
}

// Column definition
export interface UnityColumn extends BaseObject {
  type: string;
  nullable: boolean;
  comment?: string;
  tags?: Record<string, string>;
  maskId?: string;
}

// Row Filter definition (row-level security)
export interface UnityRowFilter extends BaseObject {
  enabled: boolean;
  udfExpression: string;
  description?: string;
}

// Column Mask definition (column-level data masking)
export interface UnityColumnMask extends BaseObject {
  columnId: string;
  enabled: boolean;
  maskFunction: string;
  description?: string;
}

// Constraint definition
export interface UnityConstraint extends BaseObject {
  type: 'primary_key' | 'foreign_key' | 'check';
  name: string;
  columns: string[];
  timeseries?: boolean;
  parentTable?: string;
  parentColumns?: string[];
  expression?: string;
}

// Grant definition
export interface UnityGrant {
  principal: string;
  privileges: string[];
}

// Table definition
export interface UnityTable extends BaseObject {
  format: 'delta' | 'iceberg';
  external?: boolean;
  externalLocationName?: string;
  path?: string;
  partitionColumns?: string[];
  clusterColumns?: string[];
  columnMapping?: 'name' | 'id';
  columns: UnityColumn[];
  properties: Record<string, string>;
  tags: Record<string, string>;
  constraints: UnityConstraint[];
  grants: UnityGrant[];
  comment?: string;
  rowFilters?: UnityRowFilter[];
  columnMasks?: UnityColumnMask[];
}

// View definition
export interface UnityView extends BaseObject {
  definition: string;
  comment?: string;
  dependencies?: string[];
  extractedDependencies?: {
    tables: string[];
    views: string[];
    catalogs: string[];
    schemas: string[];
  };
  tags: Record<string, string>;
  properties: Record<string, string>;
  grants: UnityGrant[];
}

// Volume definition
export interface UnityVolume extends BaseObject {
  volumeType: 'managed' | 'external';
  comment?: string;
  location?: string;
  grants?: UnityGrant[];
}

// Function parameter
export interface UnityFunctionParameter {
  name: string;
  dataType: string;
  defaultExpression?: string;
  comment?: string;
}

// Function definition
export interface UnityFunction extends BaseObject {
  language: 'SQL' | 'PYTHON';
  returnType?: string;
  returnsTable?: Array<{ columnName: string; dataType: string }>;
  body: string;
  comment?: string;
  parameters?: UnityFunctionParameter[];
  grants?: UnityGrant[];
}

// Materialized view definition
export interface UnityMaterializedView extends BaseObject {
  definition: string;
  comment?: string;
  refreshSchedule?: string;
  partitionColumns?: string[];
  clusterColumns?: string[];
  properties?: Record<string, string>;
  dependencies?: string[];
  extractedDependencies?: {
    tables: string[];
    views: string[];
    catalogs: string[];
    schemas: string[];
  };
  grants?: UnityGrant[];
}

// Schema definition
export interface UnitySchema extends BaseObject {
  managedLocationName?: string;
  comment?: string;
  tags?: Record<string, string>;
  tables: UnityTable[];
  views: UnityView[];
  volumes?: UnityVolume[];
  functions?: UnityFunction[];
  materializedViews?: UnityMaterializedView[];
  grants: UnityGrant[];
}

// Catalog definition
export interface UnityCatalog extends BaseObject {
  managedLocationName?: string;
  comment?: string;
  tags?: Record<string, string>;
  schemas: UnitySchema[];
  grants: UnityGrant[];
}

// Unity Catalog State
export interface UnityState {
  catalogs: UnityCatalog[];
}

export interface UnityProjectEnvironment {
  topLevelName: string;
  description?: string;
  catalogMappings?: Record<string, string>;
  allowDrift?: boolean;
  requireSnapshot?: boolean;
  requireApproval?: boolean;
  autoCreateTopLevel?: boolean;
  autoCreateSchemaxSchema?: boolean;
  managedCategories?: string[];
  existingObjects?: {
    catalog?: string[];
    schema?: string[];
    table?: string[];
  };
  [key: string]: unknown;
}

export interface ProjectSnapshot {
  id: string;
  version: string;
  name: string;
  ts: string;
  comment?: string;
  createdBy?: string;
  opsCount?: number;
  tags?: string[];
}

export interface ProjectDeployment {
  snapshotId: string | null;
  environment: string;
}

export interface ProjectFile {
  version: number;
  name: string;
  activeEnvironment?: string;
  provider: {
    type: string;
    version: string;
    environments?: Record<string, UnityProjectEnvironment>;
  };
  managedLocations?: Record<
    string,
    {
      description?: string;
      paths: Record<string, string>;
    }
  >;
  externalLocations?: Record<
    string,
    {
      description?: string;
      paths: Record<string, string>;
    }
  >;
  state: UnityState;
  ops: Operation[];
  snapshots: ProjectSnapshot[];
  deployments: ProjectDeployment[];
  settings: {
    autoIncrementVersion: boolean;
    versionPrefix: string;
  };
  latestSnapshot: string | null;
}

// Legacy compatibility aliases used across webview components.
export type Column = UnityColumn;
export type RowFilter = UnityRowFilter;
export type ColumnMask = UnityColumnMask;
export type Constraint = UnityConstraint;
export type Table = UnityTable;
export type View = UnityView;
export type Schema = UnitySchema;
export type Catalog = UnityCatalog;
