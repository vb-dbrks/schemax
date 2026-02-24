/**
 * Unity Catalog Models
 * 
 * Migrated from shared/model.ts with Unity-specific types
 */

import { BaseObject, ProviderState } from '../base/models';

// Column definition
export interface UnityColumn extends BaseObject {
  type: string;
  nullable: boolean;
  comment?: string;
  tags?: Record<string, string>; // tag_name: tag_value
  maskId?: string; // Reference to active column mask
}

// Row Filter definition (row-level security)
export interface UnityRowFilter extends BaseObject {
  enabled: boolean;
  udfExpression: string; // SQL UDF expression
  description?: string;
}

// Column Mask definition (column-level data masking)
export interface UnityColumnMask extends BaseObject {
  columnId: string; // Reference to column
  enabled: boolean;
  maskFunction: string; // SQL UDF that returns same type
  description?: string;
}

// Constraint definition
export interface UnityConstraint extends BaseObject {
  type: 'primary_key' | 'foreign_key' | 'check';
  name?: string; // CONSTRAINT name
  columns: string[]; // column IDs
  
  // For PRIMARY KEY
  timeseries?: boolean;
  
  // For FOREIGN KEY
  parentTable?: string; // Reference to parent table ID
  parentColumns?: string[]; // Parent column IDs
  
  // For CHECK
  expression?: string; // CHECK expression
  
  // Note: Unity Catalog constraints are informational only (not enforced).
  // They are used for query optimization and documentation purposes.
}

// Grant definition
export interface UnityGrant {
  principal: string;
  privileges: string[];
}

// Table definition
export interface UnityTable extends BaseObject {
  format: 'delta' | 'iceberg';
  external?: boolean; // Whether this is an external table
  externalLocationName?: string; // Reference to environment external location
  path?: string; // Relative path under the external location
  partitionColumns?: string[]; // For PARTITIONED BY clause
  clusterColumns?: string[]; // For CLUSTER BY clause (liquid clustering)
  columnMapping?: 'name' | 'id';
  columns: UnityColumn[];
  properties: Record<string, string>; // TBLPROPERTIES (Delta Lake config)
  tags: Record<string, string>; // TABLE TAGS (Unity Catalog governance)
  constraints: UnityConstraint[];
  grants: UnityGrant[];
  comment?: string;
  rowFilters?: UnityRowFilter[]; // Row-level security
  columnMasks?: UnityColumnMask[]; // Column-level masking
}

// View definition
export interface UnityView extends BaseObject {
  definition: string; // SQL query (SELECT statement)
  comment?: string;
  // Explicit dependencies (user-specified)
  dependencies?: string[]; // IDs of tables/views this view depends on
  // Extracted dependencies (from SQL parsing)
  extractedDependencies?: {
    tables: string[];
    views: string[];
    catalogs: string[];
    schemas: string[];
  };
  // Metadata
  tags: Record<string, string>; // VIEW TAGS (Unity Catalog governance)
  properties: Record<string, string>; // View properties
  grants: UnityGrant[];
}

// Volume definition (managed or external)
export interface UnityVolume extends BaseObject {
  volumeType: 'managed' | 'external';
  comment?: string;
  location?: string; // For external volumes
  grants?: UnityGrant[];
}

// Function parameter
export interface UnityFunctionParameter {
  name: string;
  dataType: string;
  defaultExpression?: string;
  comment?: string;
}

// Function definition (SQL or Python UDF)
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
  managedLocationName?: string; // Reference to env managedLocations
  comment?: string; // Schema comment
  tags?: Record<string, string>; // Schema tags (Unity Catalog governance)
  tables: UnityTable[];
  views: UnityView[]; // Views stored alongside tables in schema
  volumes?: UnityVolume[];
  functions?: UnityFunction[];
  materializedViews?: UnityMaterializedView[];
  grants: UnityGrant[];
}

// Object types for naming rules (catalog-level)
export type NamingRuleObjectType =
  | 'schema'
  | 'table'
  | 'view'
  | 'materialized_view'
  | 'column'
  | 'volume'
  | 'function';

// Table type for table naming rules (dimension, fact, staging, etc.)
export type NamingRuleTableType = 'dimension' | 'fact' | 'staging' | 'any';

// Single naming rule (object type + optional table type for tables)
export interface NamingStandardsRule {
  id: string;
  objectType: NamingRuleObjectType;
  tableType?: NamingRuleTableType; // only when objectType === 'table'
  pattern?: string;
  enabled?: boolean;
}

// Naming standards (per-catalog): rules for schema/table/view/column names in this catalog
export interface NamingStandardsConfig {
  applyToRenames?: boolean;
  rules?: NamingStandardsRule[];
  // Legacy / optional keyed config (kept for backward compatibility)
  schema?: Record<string, unknown>;
  table?: Record<string, unknown>;
  view?: Record<string, unknown>;
  column?: Record<string, unknown>;
}

// Catalog definition
export interface UnityCatalog extends BaseObject {
  managedLocationName?: string; // Reference to env managedLocations
  comment?: string; // Catalog comment
  tags?: Record<string, string>; // Catalog tags (Unity Catalog governance)
  namingStandards?: NamingStandardsConfig; // Naming rules for objects in this catalog
  schemas: UnitySchema[];
  grants: UnityGrant[];
}

// Unity Catalog State
export interface UnityState extends ProviderState {
  catalogs: UnityCatalog[];
}

// Project file structure (what webview receives)
export interface ProjectFile {
  version: number;
  name: string;
  provider: {
    type: string;
    version: string;
    environments?: Record<string, {
      topLevelName: string;
      description?: string;
      [key: string]: any;
    }>;
  };
  state: UnityState;
  ops: any[];
  snapshots: any[];
  deployments: any[];
  settings: {
    autoIncrementVersion: boolean;
    versionPrefix: string;
    /** Project-level naming standards: apply to catalog names (add/rename) across the project */
    namingStandards?: {
      applyToRenames?: boolean;
      /** @deprecated Prefer rules array. Single catalog rule for backward compatibility. */
      catalog?: Record<string, unknown>;
      /** Project-level catalog naming rules (objectType is always 'catalog'). */
      rules?: Array<{
        id: string;
        objectType: 'catalog';
        pattern?: string;
        enabled?: boolean;
      }>;
    };
  };
  latestSnapshot: string | null;
}

