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
  matchFull?: boolean;
  onUpdate?: 'NO_ACTION';
  onDelete?: 'NO_ACTION';
  
  // For CHECK
  expression?: string; // CHECK expression
  
  // Constraint options (all types)
  notEnforced?: boolean;
  deferrable?: boolean;
  initiallyDeferred?: boolean;
  rely?: boolean; // For query optimization (Photon)
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
}

// Schema definition
export interface UnitySchema extends BaseObject {
  managedLocationName?: string; // Reference to env managedLocations
  tables: UnityTable[];
  views: UnityView[]; // Views stored alongside tables in schema
}

// Catalog definition
export interface UnityCatalog extends BaseObject {
  managedLocationName?: string; // Reference to env managedLocations
  schemas: UnitySchema[];
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
  };
  latestSnapshot: string | null;
}

