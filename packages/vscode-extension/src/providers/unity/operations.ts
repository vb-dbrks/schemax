/**
 * Unity Catalog Operations
 * 
 * All operation types supported by Unity Catalog provider.
 * Operations are prefixed with 'unity.' to indicate the provider.
 */

import { OperationMetadata, OperationCategory } from '../base/operations';

// Operation type constants
export const UNITY_OPERATIONS = {
  // Catalog operations
  ADD_CATALOG: 'unity.add_catalog',
  RENAME_CATALOG: 'unity.rename_catalog',
  UPDATE_CATALOG: 'unity.update_catalog',
  DROP_CATALOG: 'unity.drop_catalog',
  
  // Schema operations
  ADD_SCHEMA: 'unity.add_schema',
  RENAME_SCHEMA: 'unity.rename_schema',
  UPDATE_SCHEMA: 'unity.update_schema',
  DROP_SCHEMA: 'unity.drop_schema',
  
  // Table operations
  ADD_TABLE: 'unity.add_table',
  RENAME_TABLE: 'unity.rename_table',
  DROP_TABLE: 'unity.drop_table',
  SET_TABLE_COMMENT: 'unity.set_table_comment',
  SET_TABLE_PROPERTY: 'unity.set_table_property',
  UNSET_TABLE_PROPERTY: 'unity.unset_table_property',
  
  // Table tag operations
  SET_TABLE_TAG: 'unity.set_table_tag',
  UNSET_TABLE_TAG: 'unity.unset_table_tag',
  
  // View operations
  ADD_VIEW: 'unity.add_view',
  RENAME_VIEW: 'unity.rename_view',
  DROP_VIEW: 'unity.drop_view',
  UPDATE_VIEW: 'unity.update_view',
  SET_VIEW_COMMENT: 'unity.set_view_comment',
  SET_VIEW_PROPERTY: 'unity.set_view_property',
  UNSET_VIEW_PROPERTY: 'unity.unset_view_property',
  
  // Column operations
  ADD_COLUMN: 'unity.add_column',
  RENAME_COLUMN: 'unity.rename_column',
  DROP_COLUMN: 'unity.drop_column',
  REORDER_COLUMNS: 'unity.reorder_columns',
  CHANGE_COLUMN_TYPE: 'unity.change_column_type',
  SET_NULLABLE: 'unity.set_nullable',
  SET_COLUMN_COMMENT: 'unity.set_column_comment',
  
  // Column tag operations
  SET_COLUMN_TAG: 'unity.set_column_tag',
  UNSET_COLUMN_TAG: 'unity.unset_column_tag',
  
  // Constraint operations
  ADD_CONSTRAINT: 'unity.add_constraint',
  DROP_CONSTRAINT: 'unity.drop_constraint',
  
  // Row filter operations
  ADD_ROW_FILTER: 'unity.add_row_filter',
  UPDATE_ROW_FILTER: 'unity.update_row_filter',
  REMOVE_ROW_FILTER: 'unity.remove_row_filter',
  
  // Column mask operations
  ADD_COLUMN_MASK: 'unity.add_column_mask',
  UPDATE_COLUMN_MASK: 'unity.update_column_mask',
  REMOVE_COLUMN_MASK: 'unity.remove_column_mask',

  // Grant operations
  ADD_GRANT: 'unity.add_grant',
  REVOKE_GRANT: 'unity.revoke_grant',
} as const;

// Export operation metadata for UI and validation
export const unityOperationMetadata: OperationMetadata[] = [
  // Catalog operations
  {
    type: UNITY_OPERATIONS.ADD_CATALOG,
    displayName: 'Add Catalog',
    description: 'Create a new catalog',
    category: OperationCategory.Catalog,
    requiredFields: ['catalogId', 'name'],
    optionalFields: ['managedLocationName', 'comment'],
    isDestructive: false,
  },
  {
    type: UNITY_OPERATIONS.RENAME_CATALOG,
    displayName: 'Rename Catalog',
    description: 'Rename an existing catalog',
    category: OperationCategory.Catalog,
    requiredFields: ['newName'],
    optionalFields: [],
    isDestructive: false,
  },
  {
    type: UNITY_OPERATIONS.UPDATE_CATALOG,
    displayName: 'Update Catalog',
    description: 'Update catalog properties (e.g., managed location)',
    category: OperationCategory.Catalog,
    requiredFields: [],
    optionalFields: ['managedLocationName'],
    isDestructive: false,
  },
  {
    type: UNITY_OPERATIONS.DROP_CATALOG,
    displayName: 'Drop Catalog',
    description: 'Delete a catalog and all its contents',
    category: OperationCategory.Catalog,
    requiredFields: [],
    optionalFields: [],
    isDestructive: true,
  },
  
  // Schema operations
  {
    type: UNITY_OPERATIONS.ADD_SCHEMA,
    displayName: 'Add Schema',
    description: 'Create a new schema in a catalog',
    category: OperationCategory.Schema,
    requiredFields: ['schemaId', 'name', 'catalogId'],
    optionalFields: ['managedLocationName', 'comment'],
    isDestructive: false,
  },
  {
    type: UNITY_OPERATIONS.RENAME_SCHEMA,
    displayName: 'Rename Schema',
    description: 'Rename an existing schema',
    category: OperationCategory.Schema,
    requiredFields: ['newName'],
    optionalFields: [],
    isDestructive: false,
  },
  {
    type: UNITY_OPERATIONS.UPDATE_SCHEMA,
    displayName: 'Update Schema',
    description: 'Update schema properties (e.g., managed location)',
    category: OperationCategory.Schema,
    requiredFields: [],
    optionalFields: ['managedLocationName'],
    isDestructive: false,
  },
  {
    type: UNITY_OPERATIONS.DROP_SCHEMA,
    displayName: 'Drop Schema',
    description: 'Delete a schema and all its contents',
    category: OperationCategory.Schema,
    requiredFields: [],
    optionalFields: [],
    isDestructive: true,
  },
  
  // Table operations
  {
    type: UNITY_OPERATIONS.ADD_TABLE,
    displayName: 'Add Table',
    description: 'Create a new managed or external table in a schema',
    category: OperationCategory.Table,
    requiredFields: ['tableId', 'name', 'schemaId', 'format'],
    optionalFields: ['external', 'externalLocationName', 'path', 'partitionColumns', 'clusterColumns', 'comment'],
    isDestructive: false,
  },
  {
    type: UNITY_OPERATIONS.RENAME_TABLE,
    displayName: 'Rename Table',
    description: 'Rename an existing table',
    category: OperationCategory.Table,
    requiredFields: ['newName'],
    optionalFields: [],
    isDestructive: false,
  },
  {
    type: UNITY_OPERATIONS.DROP_TABLE,
    displayName: 'Drop Table',
    description: 'Delete a table',
    category: OperationCategory.Table,
    requiredFields: [],
    optionalFields: ['name', 'catalogId', 'schemaId'],  // Added for SQL generation when table no longer in state
    isDestructive: true,
  },
  {
    type: UNITY_OPERATIONS.SET_TABLE_COMMENT,
    displayName: 'Set Table Comment',
    description: 'Add or update table comment',
    category: OperationCategory.Metadata,
    requiredFields: ['tableId', 'comment'],
    optionalFields: [],
    isDestructive: false,
  },
  {
    type: UNITY_OPERATIONS.SET_TABLE_PROPERTY,
    displayName: 'Set Table Property',
    description: 'Set a table property (TBLPROPERTIES)',
    category: OperationCategory.Metadata,
    requiredFields: ['tableId', 'key', 'value'],
    optionalFields: [],
    isDestructive: false,
  },
  {
    type: UNITY_OPERATIONS.UNSET_TABLE_PROPERTY,
    displayName: 'Unset Table Property',
    description: 'Remove a table property',
    category: OperationCategory.Metadata,
    requiredFields: ['tableId', 'key'],
    optionalFields: [],
    isDestructive: false,
  },
  
  // Table tag operations
  {
    type: UNITY_OPERATIONS.SET_TABLE_TAG,
    displayName: 'Set Table Tag',
    description: 'Set a Unity Catalog table tag (for governance)',
    category: OperationCategory.Metadata,
    requiredFields: ['tableId', 'tagName', 'tagValue'],
    optionalFields: [],
    isDestructive: false,
  },
  {
    type: UNITY_OPERATIONS.UNSET_TABLE_TAG,
    displayName: 'Unset Table Tag',
    description: 'Remove a Unity Catalog table tag',
    category: OperationCategory.Metadata,
    requiredFields: ['tableId', 'tagName'],
    optionalFields: [],
    isDestructive: false,
  },
  
  // View operations
  {
    type: UNITY_OPERATIONS.ADD_VIEW,
    displayName: 'Add View',
    description: 'Create a new view in a schema',
    category: OperationCategory.Table, // Views are table-like objects
    requiredFields: ['viewId', 'name', 'schemaId', 'definition'],
    optionalFields: ['comment', 'dependencies', 'extractedDependencies'],
    isDestructive: false,
  },
  {
    type: UNITY_OPERATIONS.RENAME_VIEW,
    displayName: 'Rename View',
    description: 'Rename an existing view',
    category: OperationCategory.Table,
    requiredFields: ['newName'],
    optionalFields: [],
    isDestructive: false,
  },
  {
    type: UNITY_OPERATIONS.DROP_VIEW,
    displayName: 'Drop View',
    description: 'Delete a view',
    category: OperationCategory.Table,
    requiredFields: [],
    optionalFields: ['name', 'catalogId', 'schemaId'], // For SQL generation
    isDestructive: true,
  },
  {
    type: UNITY_OPERATIONS.UPDATE_VIEW,
    displayName: 'Update View',
    description: 'Update view definition or dependencies',
    category: OperationCategory.Table,
    requiredFields: [],
    optionalFields: ['definition', 'dependencies', 'extractedDependencies'],
    isDestructive: false,
  },
  {
    type: UNITY_OPERATIONS.SET_VIEW_COMMENT,
    displayName: 'Set View Comment',
    description: 'Add or update view comment',
    category: OperationCategory.Metadata,
    requiredFields: ['viewId', 'comment'],
    optionalFields: [],
    isDestructive: false,
  },
  {
    type: UNITY_OPERATIONS.SET_VIEW_PROPERTY,
    displayName: 'Set View Property',
    description: 'Set a view property',
    category: OperationCategory.Metadata,
    requiredFields: ['viewId', 'key', 'value'],
    optionalFields: [],
    isDestructive: false,
  },
  {
    type: UNITY_OPERATIONS.UNSET_VIEW_PROPERTY,
    displayName: 'Unset View Property',
    description: 'Remove a view property',
    category: OperationCategory.Metadata,
    requiredFields: ['viewId', 'key'],
    optionalFields: [],
    isDestructive: false,
  },
  
  // Column operations
  {
    type: UNITY_OPERATIONS.ADD_COLUMN,
    displayName: 'Add Column',
    description: 'Add a new column to a table',
    category: OperationCategory.Column,
    requiredFields: ['tableId', 'colId', 'name', 'type', 'nullable'],
    optionalFields: ['comment'],
    isDestructive: false,
  },
  {
    type: UNITY_OPERATIONS.RENAME_COLUMN,
    displayName: 'Rename Column',
    description: 'Rename an existing column',
    category: OperationCategory.Column,
    requiredFields: ['tableId', 'newName'],
    optionalFields: [],
    isDestructive: false,
  },
  {
    type: UNITY_OPERATIONS.DROP_COLUMN,
    displayName: 'Drop Column',
    description: 'Delete a column from a table',
    category: OperationCategory.Column,
    requiredFields: ['tableId'],
    optionalFields: [],
    isDestructive: true,
  },
  {
    type: UNITY_OPERATIONS.REORDER_COLUMNS,
    displayName: 'Reorder Columns',
    description: 'Change the order of columns in a table',
    category: OperationCategory.Column,
    requiredFields: ['tableId', 'order'],
    optionalFields: [],
    isDestructive: false,
  },
  {
    type: UNITY_OPERATIONS.CHANGE_COLUMN_TYPE,
    displayName: 'Change Column Type',
    description: 'Change the data type of a column',
    category: OperationCategory.Column,
    requiredFields: ['tableId', 'newType'],
    optionalFields: [],
    isDestructive: false,
  },
  {
    type: UNITY_OPERATIONS.SET_NULLABLE,
    displayName: 'Set Column Nullable',
    description: 'Change whether a column allows NULL values',
    category: OperationCategory.Column,
    requiredFields: ['tableId', 'nullable'],
    optionalFields: [],
    isDestructive: false,
  },
  {
    type: UNITY_OPERATIONS.SET_COLUMN_COMMENT,
    displayName: 'Set Column Comment',
    description: 'Add or update column comment',
    category: OperationCategory.Metadata,
    requiredFields: ['tableId', 'comment'],
    optionalFields: [],
    isDestructive: false,
  },
  
  // Column tag operations
  {
    type: UNITY_OPERATIONS.SET_COLUMN_TAG,
    displayName: 'Set Column Tag',
    description: 'Set a tag on a column',
    category: OperationCategory.Metadata,
    requiredFields: ['tableId', 'tagName', 'tagValue'],
    optionalFields: [],
    isDestructive: false,
  },
  {
    type: UNITY_OPERATIONS.UNSET_COLUMN_TAG,
    displayName: 'Unset Column Tag',
    description: 'Remove a tag from a column',
    category: OperationCategory.Metadata,
    requiredFields: ['tableId', 'tagName'],
    optionalFields: [],
    isDestructive: false,
  },
  
  // Constraint operations
  {
    type: UNITY_OPERATIONS.ADD_CONSTRAINT,
    displayName: 'Add Constraint',
    description: 'Add a constraint (PRIMARY KEY, FOREIGN KEY, CHECK)',
    category: OperationCategory.Constraint,
    requiredFields: ['tableId', 'constraintId', 'type', 'columns'],
    optionalFields: ['name', 'timeseries', 'parentTable', 'parentColumns', 'expression', 'notEnforced', 'rely'],
    isDestructive: false,
  },
  {
    type: UNITY_OPERATIONS.DROP_CONSTRAINT,
    displayName: 'Drop Constraint',
    description: 'Remove a constraint from a table',
    category: OperationCategory.Constraint,
    requiredFields: ['tableId'],
    optionalFields: [],
    isDestructive: true,
  },
  
  // Row filter operations
  {
    type: UNITY_OPERATIONS.ADD_ROW_FILTER,
    displayName: 'Add Row Filter',
    description: 'Add row-level security filter',
    category: OperationCategory.Security,
    requiredFields: ['tableId', 'filterId', 'name', 'udfExpression'],
    optionalFields: ['enabled', 'description'],
    isDestructive: false,
  },
  {
    type: UNITY_OPERATIONS.UPDATE_ROW_FILTER,
    displayName: 'Update Row Filter',
    description: 'Update an existing row filter',
    category: OperationCategory.Security,
    requiredFields: ['tableId'],
    optionalFields: ['name', 'udfExpression', 'enabled', 'description'],
    isDestructive: false,
  },
  {
    type: UNITY_OPERATIONS.REMOVE_ROW_FILTER,
    displayName: 'Remove Row Filter',
    description: 'Remove a row filter',
    category: OperationCategory.Security,
    requiredFields: ['tableId'],
    optionalFields: [],
    isDestructive: true,
  },
  
  // Column mask operations
  {
    type: UNITY_OPERATIONS.ADD_COLUMN_MASK,
    displayName: 'Add Column Mask',
    description: 'Add column-level data masking',
    category: OperationCategory.Security,
    requiredFields: ['tableId', 'maskId', 'columnId', 'name', 'maskFunction'],
    optionalFields: ['enabled', 'description'],
    isDestructive: false,
  },
  {
    type: UNITY_OPERATIONS.UPDATE_COLUMN_MASK,
    displayName: 'Update Column Mask',
    description: 'Update an existing column mask',
    category: OperationCategory.Security,
    requiredFields: ['tableId'],
    optionalFields: ['name', 'maskFunction', 'enabled', 'description'],
    isDestructive: false,
  },
  {
    type: UNITY_OPERATIONS.REMOVE_COLUMN_MASK,
    displayName: 'Remove Column Mask',
    description: 'Remove a column mask',
    category: OperationCategory.Security,
    requiredFields: ['tableId'],
    optionalFields: [],
    isDestructive: true,
  },

  // Grant operations
  {
    type: UNITY_OPERATIONS.ADD_GRANT,
    displayName: 'Add Grant',
    description: 'Grant privileges on a catalog, schema, table, or view to a principal',
    category: OperationCategory.Security,
    requiredFields: ['targetType', 'targetId', 'principal', 'privileges'],
    optionalFields: [],
    isDestructive: false,
  },
  {
    type: UNITY_OPERATIONS.REVOKE_GRANT,
    displayName: 'Revoke Grant',
    description: 'Revoke privileges from a principal on a catalog, schema, table, or view',
    category: OperationCategory.Security,
    requiredFields: ['targetType', 'targetId', 'principal'],
    optionalFields: ['privileges'],
    isDestructive: false,
  },
];

