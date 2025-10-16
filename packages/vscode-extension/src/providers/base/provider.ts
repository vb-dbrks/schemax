/**
 * Base Provider Interface
 * 
 * Defines the contract that all catalog providers must implement.
 * This enables Schematic to support multiple catalogs (Unity Catalog, Hive, PostgreSQL, etc.)
 * through a unified interface while allowing provider-specific features.
 */

import { ProviderHierarchy } from './hierarchy';
import { Operation, OperationMetadata } from './operations';
import { ValidationResult, ProviderState } from './models';
import { SQLGenerator } from './sql-generator';

/**
 * Provider capabilities - defines what features this provider supports
 */
export interface ProviderCapabilities {
  /** List of supported operations (e.g., ['unity.add_catalog', 'unity.add_schema']) */
  supportedOperations: string[];
  
  /** List of supported object types (e.g., ['catalog', 'schema', 'table']) */
  supportedObjectTypes: string[];
  
  /** Hierarchy definition for this provider */
  hierarchy: ProviderHierarchy;
  
  /** Feature flags */
  features: {
    /** Supports table constraints (PRIMARY KEY, FOREIGN KEY, CHECK) */
    constraints: boolean;
    
    /** Supports row-level security filters */
    rowFilters: boolean;
    
    /** Supports column-level data masking */
    columnMasks: boolean;
    
    /** Supports column tags/metadata */
    columnTags: boolean;
    
    /** Supports table properties/options */
    tableProperties: boolean;
    
    /** Supports table comments */
    comments: boolean;
    
    /** Supports partitioning */
    partitioning: boolean;
    
    /** Supports views */
    views: boolean;
    
    /** Supports materialized views */
    materializedViews: boolean;
    
    /** Supports stored procedures/functions */
    functions: boolean;
    
    /** Supports indexes */
    indexes: boolean;
  };
}

/**
 * Provider metadata
 */
export interface ProviderInfo {
  /** Unique provider ID (e.g., 'unity', 'hive', 'postgres') */
  id: string;
  
  /** Human-readable provider name */
  name: string;
  
  /** Provider version (semantic versioning) */
  version: string;
  
  /** Provider description */
  description: string;
  
  /** Provider author/maintainer */
  author?: string;
  
  /** Provider documentation URL */
  docsUrl?: string;
}

/**
 * Main Provider interface
 */
export interface Provider {
  /** Provider metadata */
  readonly info: ProviderInfo;
  
  /** Provider capabilities */
  readonly capabilities: ProviderCapabilities;
  
  /**
   * Get operation metadata for a specific operation type
   * @param operationType - Operation type (e.g., 'unity.add_catalog')
   */
  getOperationMetadata(operationType: string): OperationMetadata | undefined;
  
  /**
   * Get all operation metadata
   */
  getAllOperations(): OperationMetadata[];
  
  /**
   * Validate an operation
   * @param op - Operation to validate
   * @returns Validation result
   */
  validateOperation(op: Operation): ValidationResult;
  
  /**
   * Apply an operation to state (state reducer)
   * @param state - Current state
   * @param op - Operation to apply
   * @returns New state (must be immutable)
   */
  applyOperation(state: ProviderState, op: Operation): ProviderState;
  
  /**
   * Apply multiple operations to state
   * @param state - Current state
   * @param ops - Operations to apply
   * @returns New state (must be immutable)
   */
  applyOperations(state: ProviderState, ops: Operation[]): ProviderState;
  
  /**
   * Get SQL generator for this provider
   * @param state - Current state (for ID-to-name mapping)
   * @returns SQL generator instance
   */
  getSQLGenerator(state: ProviderState): SQLGenerator;
  
  /**
   * Create an empty/initial state for this provider
   * @returns Empty provider state
   */
  createInitialState(): ProviderState;
  
  /**
   * Validate the entire state structure
   * @param state - State to validate
   * @returns Validation result
   */
  validateState(state: ProviderState): ValidationResult;
}

/**
 * Base implementation of Provider with common functionality
 */
export abstract class BaseProvider implements Provider {
  abstract readonly info: ProviderInfo;
  abstract readonly capabilities: ProviderCapabilities;
  
  protected operationMetadata: Map<string, OperationMetadata> = new Map();
  
  getOperationMetadata(operationType: string): OperationMetadata | undefined {
    return this.operationMetadata.get(operationType);
  }
  
  getAllOperations(): OperationMetadata[] {
    return Array.from(this.operationMetadata.values());
  }
  
  abstract validateOperation(op: Operation): ValidationResult;
  
  abstract applyOperation(state: ProviderState, op: Operation): ProviderState;
  
  applyOperations(state: ProviderState, ops: Operation[]): ProviderState {
    let currentState = state;
    for (const op of ops) {
      currentState = this.applyOperation(currentState, op);
    }
    return currentState;
  }
  
  abstract getSQLGenerator(state: ProviderState): SQLGenerator;
  
  abstract createInitialState(): ProviderState;
  
  abstract validateState(state: ProviderState): ValidationResult;
  
  /**
   * Helper to register operation metadata
   */
  protected registerOperation(metadata: OperationMetadata): void {
    this.operationMetadata.set(metadata.type, metadata);
  }
  
  /**
   * Helper to check if operation is supported
   */
  protected isOperationSupported(operationType: string): boolean {
    return this.capabilities.supportedOperations.includes(operationType);
  }
}

