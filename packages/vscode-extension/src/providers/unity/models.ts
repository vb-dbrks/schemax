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
  columnMapping?: 'name' | 'id';
  columns: UnityColumn[];
  properties: Record<string, string>;
  constraints: UnityConstraint[];
  grants: UnityGrant[];
  comment?: string;
  rowFilters?: UnityRowFilter[]; // Row-level security
  columnMasks?: UnityColumnMask[]; // Column-level masking
}

// Schema definition
export interface UnitySchema extends BaseObject {
  tables: UnityTable[];
}

// Catalog definition
export interface UnityCatalog extends BaseObject {
  schemas: UnitySchema[];
}

// Unity Catalog State
export interface UnityState extends ProviderState {
  catalogs: UnityCatalog[];
}

