/**
 * Base Model Types for Provider System
 * 
 * These are common types used across all providers.
 * Provider-specific models should extend or compose these base types.
 */

/**
 * Base interface for all objects in the hierarchy
 */
export interface BaseObject {
  id: string;
  name: string;
}

/**
 * Validation result for operations
 */
export interface ValidationResult {
  valid: boolean;
  errors: ValidationError[];
}

export interface ValidationError {
  field: string;
  message: string;
  code?: string;
}

/**
 * Provider state - generic container for provider-specific state
 */
export interface ProviderState {
  [key: string]: unknown;
}
