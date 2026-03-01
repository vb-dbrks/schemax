/**
 * Unity Catalog Provider
 * 
 * Main provider implementation for Databricks Unity Catalog.
 * Implements the Provider interface to enable Unity Catalog support in SchemaX.
 */

import type {
  Provider,
  ProviderInfo,
  ProviderCapabilities} from '../base/provider';
import {
  BaseProvider
} from '../base/provider';
import type { Operation, OperationMetadata } from '../base/operations';
import type { ValidationResult, ProviderState } from '../base/models';
import type { SQLGenerator } from '../base/sql-generator';
import { unityHierarchy } from './hierarchy';
import type { UnityState } from './models';
import { UNITY_OPERATIONS, unityOperationMetadata } from './operations';
import { applyOperation, applyOperations } from './state-reducer';
import { UnitySQLGenerator } from './sql-generator';
import type { LocationDefinition } from '../../storage-v4';

/**
 * Unity Catalog Provider Implementation
 */
export class UnityProvider extends BaseProvider implements Provider {
  readonly info: ProviderInfo = {
    id: 'unity',
    name: 'Unity Catalog',
    version: '1.0.0',
    description: 'Databricks Unity Catalog provider with full governance features',
    author: 'SchemaX Team',
    docsUrl: 'https://docs.databricks.com/data-governance/unity-catalog/index.html',
  };
  
  readonly capabilities: ProviderCapabilities = {
    supportedOperations: Object.values(UNITY_OPERATIONS),
    supportedObjectTypes: ['catalog', 'schema', 'table', 'column'],
    hierarchy: unityHierarchy,
    features: {
      constraints: true,
      rowFilters: true,
      columnMasks: true,
      columnTags: true,
      tableProperties: true,
      comments: true,
      partitioning: false, // Future
      views: false, // Future
      materializedViews: false, // Future
      functions: false, // Future
      indexes: false, // Not applicable to Unity Catalog
    },
  };
  
  constructor() {
    super();
    // Register all operation metadata
    unityOperationMetadata.forEach(metadata => {
      this.registerOperation(metadata);
    });
  }
  
  validateOperation(op: Operation): ValidationResult {
    const errors: Array<{field: string; message: string; code?: string}> = [];
    
    // Check if operation is supported
    if (!this.isOperationSupported(op.op)) {
      errors.push({
        field: 'op',
        message: `Unsupported operation type: ${op.op}`,
        code: 'UNSUPPORTED_OPERATION',
      });
      return { valid: false, errors };
    }
    
    // Get operation metadata
    const metadata = this.getOperationMetadata(op.op);
    if (!metadata) {
      errors.push({
        field: 'op',
        message: `No metadata found for operation: ${op.op}`,
        code: 'MISSING_METADATA',
      });
      return { valid: false, errors };
    }
    
    // Validate required fields
    for (const field of metadata.requiredFields) {
      if (!(field in op.payload) || op.payload[field] === null || op.payload[field] === undefined) {
        errors.push({
          field: `payload.${field}`,
          message: `Required field missing: ${field}`,
          code: 'MISSING_REQUIRED_FIELD',
        });
      }
    }
    
    // Validate operation-specific rules
    errors.push(...this.validateOperationSpecificRules(op, metadata));
    
    return {
      valid: errors.length === 0,
      errors,
    };
  }
  
  private validateOperationSpecificRules(
    _op: Operation,
    _metadata: OperationMetadata
  ): Array<{field: string; message: string; code?: string}> {
    const errors: Array<{field: string; message: string; code?: string}> = [];
    
    // Add operation-specific validation logic here
    // For example, validate constraint types, column types, etc.
    
    return errors;
  }
  
  applyOperation(state: ProviderState, op: Operation): ProviderState {
    return applyOperation(state as UnityState, op);
  }
  
  applyOperations(state: ProviderState, ops: Operation[]): ProviderState {
    return applyOperations(state as UnityState, ops);
  }
  
  getSQLGenerator(
    state: ProviderState,
    catalogNameMapping?: Record<string, string>,
    options?: {
      managedLocations?: Record<string, LocationDefinition>;
      externalLocations?: Record<string, LocationDefinition>;
      environmentName?: string;
    }
  ): SQLGenerator {
    return new UnitySQLGenerator(state as UnityState, catalogNameMapping, options);
  }
  
  createInitialState(): ProviderState {
    return {
      catalogs: [],
    } as UnityState;
  }
  
  validateState(state: ProviderState): ValidationResult {
    const errors: Array<{field: string; message: string; code?: string}> = [];
    const unityState = state as UnityState;
    
    // Validate state structure
    if (!Array.isArray(unityState.catalogs)) {
      errors.push({
        field: 'catalogs',
        message: 'Catalogs must be an array',
        code: 'INVALID_STATE_STRUCTURE',
      });
      return { valid: false, errors };
    }
    
    // Validate each catalog
    for (let i = 0; i < unityState.catalogs.length; i++) {
      const catalog = unityState.catalogs[i];
      
      if (!catalog.id || !catalog.name) {
        errors.push({
          field: `catalogs[${i}]`,
          message: 'Catalog must have id and name',
          code: 'INVALID_CATALOG',
        });
      }
      
      if (!Array.isArray(catalog.schemas)) {
        errors.push({
          field: `catalogs[${i}].schemas`,
          message: 'Schemas must be an array',
          code: 'INVALID_SCHEMA_STRUCTURE',
        });
      }
      
      // Validate schemas
      for (let j = 0; j < catalog.schemas.length; j++) {
        const schema = catalog.schemas[j];
        
        if (!schema.id || !schema.name) {
          errors.push({
            field: `catalogs[${i}].schemas[${j}]`,
            message: 'Schema must have id and name',
            code: 'INVALID_SCHEMA',
          });
        }
        
        if (!Array.isArray(schema.tables)) {
          errors.push({
            field: `catalogs[${i}].schemas[${j}].tables`,
            message: 'Tables must be an array',
            code: 'INVALID_TABLE_STRUCTURE',
          });
        }
      }
    }
    
    return {
      valid: errors.length === 0,
      errors,
    };
  }
}

// Export singleton instance
export const unityProvider = new UnityProvider();
