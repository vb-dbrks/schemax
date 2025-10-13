/**
 * Base SQL Generator Interface
 * 
 * Defines the contract for generating SQL DDL statements from operations.
 * Each provider implements this interface to generate provider-specific SQL.
 */

import { Operation } from './operations';
import { ProviderState } from './models';

/**
 * SQL generation result
 */
export interface SQLGenerationResult {
  /** Generated SQL statements */
  sql: string;
  
  /** Any warnings or notes about the generation */
  warnings: string[];
  
  /** Whether the SQL is idempotent (safe to run multiple times) */
  isIdempotent: boolean;
}

/**
 * Base SQL Generator interface
 */
export interface SQLGenerator {
  /**
   * Generate SQL for a list of operations
   * @param ops - Operations to convert to SQL
   * @returns SQL script as string
   */
  generateSQL(ops: Operation[]): string;
  
  /**
   * Generate SQL for a single operation
   * @param op - Operation to convert to SQL
   * @returns SQL generation result
   */
  generateSQLForOperation(op: Operation): SQLGenerationResult;
  
  /**
   * Validate that an operation can be converted to SQL
   * @param op - Operation to validate
   * @returns True if operation can be converted to SQL
   */
  canGenerateSQL(op: Operation): boolean;
}

/**
 * Base implementation of SQLGenerator with common functionality
 */
export abstract class BaseSQLGenerator implements SQLGenerator {
  constructor(protected state: ProviderState) {}
  
  generateSQL(ops: Operation[]): string {
    const statements: string[] = [];
    
    for (const op of ops) {
      if (!this.canGenerateSQL(op)) {
        console.warn(`Cannot generate SQL for operation: ${op.op}`);
        continue;
      }
      
      const result = this.generateSQLForOperation(op);
      
      // Add header comment with operation metadata
      const header = `-- Operation: ${op.id} (${op.ts})\n-- Type: ${op.op}`;
      
      // Add warnings if any
      const warningsComment = result.warnings.length > 0
        ? `\n-- Warnings: ${result.warnings.join(', ')}`
        : '';
      
      statements.push(`${header}${warningsComment}\n${result.sql};`);
    }
    
    return statements.join('\n\n');
  }
  
  abstract generateSQLForOperation(op: Operation): SQLGenerationResult;
  
  abstract canGenerateSQL(op: Operation): boolean;
  
  /**
   * Helper to escape SQL identifiers
   */
  protected escapeIdentifier(identifier: string): string {
    return `\`${identifier.replace(/`/g, '``')}\``;
  }
  
  /**
   * Helper to escape SQL string literals
   */
  protected escapeString(str: string): string {
    return str.replace(/'/g, "''");
  }
}

