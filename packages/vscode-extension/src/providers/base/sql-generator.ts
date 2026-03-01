/**
 * Base SQL Generator Interface
 * 
 * Defines the contract for generating SQL DDL statements from operations.
 * Each provider implements this interface to generate provider-specific SQL.
 */

import type { Operation } from './operations';
import type { ProviderState } from './models';
import type {
  DependencyNode,
  DependencyType,
  DependencyEnforcement} from './dependency-graph';
import {
  DependencyGraph
} from './dependency-graph';

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
  
  /**
   * Build dependency graph for validation
   * @param ops - Operations to analyze
   * @returns Dependency graph
   */
  buildDependencyGraph(ops: Operation[]): DependencyGraph;
  
  /**
   * Get the fully-qualified name for an object ID
   * @param objectId - Object ID
   * @returns Object name or null if not found
   */
  getObjectName(objectId: string): string | null;
}

/**
 * Base implementation of SQLGenerator with common functionality
 */
export abstract class BaseSQLGenerator implements SQLGenerator {
  protected dependencyGraph: DependencyGraph | null = null;

  constructor(protected state: ProviderState) {}
  
  generateSQL(ops: Operation[]): string {
    // Use dependency-aware generation if supported
    try {
      return this.generateSQLWithDependencies(ops);
    } catch (error) {
      console.warn('Dependency-aware generation failed, falling back to simple generation:', error);
      return this.generateSQLSimple(ops);
    }
  }

  /**
   * Generate SQL with dependency-aware ordering
   * This is the main entry point that orchestrates:
   * 1. Dependency graph building
   * 2. Cycle detection
   * 3. Breaking change detection
   * 4. Topological sorting
   * 5. SQL generation
   */
  protected generateSQLWithDependencies(ops: Operation[]): string {
    if (ops.length === 0) {
      return '';
    }

    const warnings: string[] = [];

    try {
      // Build dependency graph
      const graph = this._buildDependencyGraph(ops);

      // Detect breaking changes
      const breakingWarnings = this._detectBreakingChanges(ops, graph);
      warnings.push(...breakingWarnings);

      // Check for cycles
      const cycles = graph.detectCycles();
      if (cycles.length > 0) {
        const cycleStr = cycles.map(c => c.join(' → ')).join('\n');
        warnings.push(`Circular dependencies detected:\n${cycleStr}`);
        // Fall back to level-based sorting
        const sorted = this._sortOperationsByLevel(ops);
        return this._generateSQLFromOperations(sorted, warnings);
      }

      // Topological sort
      const sortedOps = graph.topologicalSort();

      // Validate dependencies
      const depWarnings = graph.validateDependencies();
      warnings.push(...depWarnings);

      return this._generateSQLFromOperations(sortedOps, warnings);
    } catch (error) {
      warnings.push(`Dependency analysis failed: ${error}. Using level-based sorting.`);
      const sorted = this._sortOperationsByLevel(ops);
      return this._generateSQLFromOperations(sorted, warnings);
    }
  }

  /**
   * Simple SQL generation (original implementation)
   */
  protected generateSQLSimple(ops: Operation[]): string {
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

  /**
   * Generate SQL from operations with warnings
   */
  protected _generateSQLFromOperations(ops: Operation[], globalWarnings: string[]): string {
    const statements: string[] = [];

    // Add global warnings at the top
    if (globalWarnings.length > 0) {
      statements.push('-- WARNINGS:');
      for (const warning of globalWarnings) {
        statements.push(`-- ${warning}`);
      }
      statements.push('');
    }

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

  /**
   * Build dependency graph from operations
   */
  protected _buildDependencyGraph(ops: Operation[]): DependencyGraph {
    const graph = new DependencyGraph();

    // First pass: Add all nodes
    for (const op of ops) {
      const hierarchyLevel = this._getDependencyLevel(op);
      const targetId = this._getTargetObjectId(op);

      if (!targetId) continue;

      const node: DependencyNode = {
        id: targetId,
        type: this._getObjectTypeFromOperation(op),
        hierarchyLevel,
        operation: op,
        metadata: { opType: op.op },
      };

      graph.addNode(node);
    }

    // Second pass: Extract and add dependencies
    for (const op of ops) {
      const targetId = this._getTargetObjectId(op);
      if (!targetId) continue;

      const dependencies = this._extractOperationDependencies(op);
      for (const [depId, depType, enforcement] of dependencies) {
        try {
          graph.addEdge(targetId, depId, depType, enforcement);
        } catch (error) {
          // Dependency target might not exist in current operations
          console.warn(`Failed to add dependency edge: ${error}`);
        }
      }
    }

    this.dependencyGraph = graph;
    return graph;
  }

  /**
   * Detect breaking changes (e.g., dropping objects with dependents)
   */
  protected _detectBreakingChanges(ops: Operation[], graph: DependencyGraph): string[] {
    const warnings: string[] = [];

    for (const op of ops) {
      const opType = op.op.split('.').pop() || '';
      
      // Check if this is a DROP operation
      if (opType.startsWith('drop_')) {
        const targetId = this._getTargetObjectId(op);
        if (targetId) {
          const dependents = graph.getBreakingChanges(targetId);
          if (dependents.length > 0) {
            const displayName = this._getObjectDisplayNameFromOp(op);
            const depNames = dependents.map(id => {
              const node = graph.getNode(id);
              return node ? `${node.type}:${id}` : id;
            }).join(', ');
            warnings.push(
              `BREAKING CHANGE: Dropping ${displayName} will affect: ${depNames}`
            );
          }
        }
      }
    }

    return warnings;
  }

  /**
   * Fallback: Sort operations by dependency level
   */
  protected _sortOperationsByLevel(ops: Operation[]): Operation[] {
    return [...ops].sort((a, b) => {
      const levelA = this._getDependencyLevel(a);
      const levelB = this._getDependencyLevel(b);

      if (levelA !== levelB) {
        return levelA - levelB;
      }

      // Within same level, sort by timestamp
      return new Date(a.ts).getTime() - new Date(b.ts).getTime();
    });
  }

  /**
   * Get dependency level for an operation (for hierarchy-based ordering)
   */
  protected abstract _getDependencyLevel(op: Operation): number;

  /**
   * Get target object ID from operation
   */
  protected _getTargetObjectId(op: Operation): string | null {
    if (op.target) {
      return op.target;
    }

    // For create operations, extract ID from payload
    if (op.payload) {
      if ('catalogId' in op.payload) return op.payload.catalogId as string;
      if ('schemaId' in op.payload) return op.payload.schemaId as string;
      if ('tableId' in op.payload) return op.payload.tableId as string;
      if ('viewId' in op.payload) return op.payload.viewId as string;
      if ('colId' in op.payload) return op.payload.colId as string;
    }

    return null;
  }

  /**
   * Get object type from operation
   */
  protected _getObjectTypeFromOperation(op: Operation): string {
    const opType = op.op.split('.').pop() || '';

    if (opType.includes('catalog')) return 'catalog';
    if (opType.includes('schema')) return 'schema';
    if (opType.includes('table')) return 'table';
    if (opType.includes('view')) return 'view';
    if (opType.includes('column') || opType.includes('col')) return 'column';

    return 'unknown';
  }

  /**
   * Get human-readable display name from operation
   */
  protected _getObjectDisplayNameFromOp(op: Operation): string {
    const type = this._getObjectTypeFromOperation(op);
    const id = this._getTargetObjectId(op);
    return `${type}:${id}`;
  }

  /**
   * Extract dependencies from an operation
   * Providers override this to implement specific dependency extraction logic
   * (e.g., parsing view SQL to find table references)
   *
   * @returns Array of tuples: [dependencyId, dependencyType, enforcement]
   */
  protected _extractOperationDependencies(
    _op: Operation
  ): Array<[string, DependencyType, DependencyEnforcement]> {
    // Base implementation returns empty array
    // Providers override this to extract dependencies (e.g., from view SQL)
    return [];
  }
  
  abstract generateSQLForOperation(op: Operation): SQLGenerationResult;
  
  abstract canGenerateSQL(op: Operation): boolean;
  
  /**
   * Public method to build dependency graph for validation
   */
  buildDependencyGraph(ops: Operation[]): DependencyGraph {
    return this._buildDependencyGraph(ops);
  }
  
  /**
   * Public method to get object name for validation
   */
  getObjectName(objectId: string): string | null {
    return objectId || null;
  }
  
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
