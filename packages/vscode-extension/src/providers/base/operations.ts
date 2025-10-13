/**
 * Base Operation Types
 * 
 * Operations represent user actions that modify the schema state.
 * All operations follow an event-sourcing pattern and are immutable.
 */

/**
 * Base operation structure
 */
export interface Operation {
  /** Unique identifier for this operation */
  id: string;
  
  /** ISO timestamp when operation was created */
  ts: string;
  
  /** Provider ID (e.g., 'unity', 'hive', 'postgres') */
  provider: string;
  
  /** Operation type with provider prefix (e.g., 'unity.add_catalog') */
  op: string;
  
  /** ID of the target object this operation applies to */
  target: string;
  
  /** Operation-specific payload data */
  payload: Record<string, any>;
}

/**
 * Operation category for grouping related operations
 */
export enum OperationCategory {
  Catalog = 'catalog',
  Schema = 'schema',
  Table = 'table',
  Column = 'column',
  Constraint = 'constraint',
  Security = 'security',
  Metadata = 'metadata',
}

/**
 * Operation metadata for UI and documentation
 */
export interface OperationMetadata {
  /** Operation type (e.g., 'unity.add_catalog') */
  type: string;
  
  /** Human-readable display name */
  displayName: string;
  
  /** Description of what this operation does */
  description: string;
  
  /** Category this operation belongs to */
  category: OperationCategory;
  
  /** Required payload fields */
  requiredFields: string[];
  
  /** Optional payload fields */
  optionalFields: string[];
  
  /** Whether this operation is destructive (drops/deletes) */
  isDestructive: boolean;
}

/**
 * Helper to create a new operation with defaults
 */
export function createOperation(
  provider: string,
  opType: string,
  target: string,
  payload: Record<string, any>,
  id?: string
): Operation {
  return {
    id: id || `op_${Date.now()}_${Math.random().toString(36).substr(2, 9)}`,
    ts: new Date().toISOString(),
    provider,
    op: `${provider}.${opType}`,
    target,
    payload,
  };
}

