/**
 * Dependency Graph for SQL Generation
 *
 * This module provides a graph-based dependency management system for generating
 * properly ordered SQL DDL statements. It tracks dependencies between database
 * objects (catalogs, schemas, tables, views) and provides:
 * - Cycle detection
 * - Topological sorting
 * - Breaking change detection
 * - Dependency validation
 */

import { Operation } from './operations';

/**
 * Types of dependencies between objects
 */
export enum DependencyType {
  /** Schema-level hierarchy (catalog → schema → table/view) */
  HIERARCHY = 'hierarchy',
  /** Foreign key constraint dependency */
  FOREIGN_KEY = 'foreign_key',
  /** View depends on table or another view */
  VIEW_DEPENDENCY = 'view_dependency',
  /** Column mask dependency */
  COLUMN_MASK = 'column_mask',
  /** Row filter dependency */
  ROW_FILTER = 'row_filter',
}

/**
 * How dependencies are enforced
 */
export enum DependencyEnforcement {
  /** Must be satisfied (Unity Catalog hierarchical dependencies) */
  ENFORCED = 'enforced',
  /** Not enforced by database, but logical dependency (Unity Catalog FKs) */
  INFORMATIONAL = 'informational',
  /** Only for warnings */
  WARNING = 'warning',
}

/**
 * A node in the dependency graph
 */
export interface DependencyNode {
  /** Unique identifier (table/view/schema ID) */
  id: string;
  /** Type of object (catalog, schema, table, view, column) */
  type: string;
  /** Hierarchy level for ordering (catalog=0, schema=1, table/view=2, column=3) */
  hierarchyLevel: number;
  /** Associated operation */
  operation: Operation;
  /** Additional metadata */
  metadata?: Record<string, unknown>;
}

/**
 * An edge in the dependency graph
 */
export interface DependencyEdge {
  /** Source node (depends on toId) */
  fromId: string;
  /** Target node (dependency) */
  toId: string;
  /** Type of dependency */
  depType: DependencyType;
  /** How it's enforced */
  enforcement: DependencyEnforcement;
}

/**
 * Dependency graph for managing object dependencies
 */
export class DependencyGraph {
  private nodes: Map<string, DependencyNode>;
  private edges: Map<string, DependencyEdge[]>; // adjacency list: node -> list of edges

  constructor() {
    this.nodes = new Map();
    this.edges = new Map();
  }

  /**
   * Add a node to the graph
   */
  addNode(node: DependencyNode): void {
    if (this.nodes.has(node.id)) {
      // Node already exists, update it
      this.nodes.set(node.id, node);
    } else {
      this.nodes.set(node.id, node);
      this.edges.set(node.id, []);
    }
  }

  /**
   * Add an edge (dependency) to the graph
   * Edge direction: fromId → toId means "fromId depends on toId"
   */
  addEdge(
    fromId: string,
    toId: string,
    depType: DependencyType,
    enforcement: DependencyEnforcement = DependencyEnforcement.ENFORCED
  ): void {
    if (!this.nodes.has(fromId)) {
      throw new Error(`Node ${fromId} not found in graph`);
    }
    if (!this.nodes.has(toId)) {
      throw new Error(`Node ${toId} not found in graph`);
    }

    const edge: DependencyEdge = { fromId, toId, depType, enforcement };
    const edges = this.edges.get(fromId) || [];
    edges.push(edge);
    this.edges.set(fromId, edges);
  }

  /**
   * Get a node by ID
   */
  getNode(id: string): DependencyNode | undefined {
    return this.nodes.get(id);
  }

  /**
   * Get all dependencies for a node (outgoing edges)
   */
  getDependencies(nodeId: string): DependencyEdge[] {
    return this.edges.get(nodeId) || [];
  }

  /**
   * Get all dependents of a node (incoming edges)
   */
  getDependents(nodeId: string): DependencyEdge[] {
    const dependents: DependencyEdge[] = [];
    for (const [fromId, edges] of this.edges.entries()) {
      for (const edge of edges) {
        if (edge.toId === nodeId) {
          dependents.push(edge);
        }
      }
    }
    return dependents;
  }

  /**
   * Detect cycles in the graph using DFS
   * Returns array of cycles, where each cycle is an array of node IDs
   */
  detectCycles(): string[][] {
    const visited = new Set<string>();
    const recStack = new Set<string>();
    const cycles: string[][] = [];

    const dfs = (nodeId: string, path: string[]): void => {
      if (recStack.has(nodeId)) {
        // Found a cycle
        const cycleStart = path.indexOf(nodeId);
        if (cycleStart !== -1) {
          cycles.push([...path.slice(cycleStart), nodeId]);
        }
        return;
      }

      if (visited.has(nodeId)) {
        return;
      }

      visited.add(nodeId);
      recStack.add(nodeId);
      path.push(nodeId);

      const edges = this.getDependencies(nodeId);
      for (const edge of edges) {
        dfs(edge.toId, [...path]);
      }

      recStack.delete(nodeId);
    };

    for (const nodeId of this.nodes.keys()) {
      if (!visited.has(nodeId)) {
        dfs(nodeId, []);
      }
    }

    return cycles;
  }

  /**
   * Topological sort using Kahn's algorithm
   * Returns operations in dependency order (dependencies first)
   * Throws error if cycles are detected
   */
  topologicalSort(): Operation[] {
    const cycles = this.detectCycles();
    if (cycles.length > 0) {
      throw new Error(`Circular dependencies detected: ${JSON.stringify(cycles)}`);
    }

    // Calculate in-degrees
    const inDegree = new Map<string, number>();
    for (const nodeId of this.nodes.keys()) {
      inDegree.set(nodeId, 0);
    }
    for (const edges of this.edges.values()) {
      for (const edge of edges) {
        inDegree.set(edge.toId, (inDegree.get(edge.toId) || 0) + 1);
      }
    }

    // Queue for nodes with no dependencies
    const queue: string[] = [];
    for (const [nodeId, degree] of inDegree.entries()) {
      if (degree === 0) {
        queue.push(nodeId);
      }
    }

    const sorted: string[] = [];
    while (queue.length > 0) {
      const nodeId = queue.shift()!;
      sorted.push(nodeId);

      const edges = this.getDependencies(nodeId);
      for (const edge of edges) {
        const newDegree = (inDegree.get(edge.toId) || 0) - 1;
        inDegree.set(edge.toId, newDegree);
        if (newDegree === 0) {
          queue.push(edge.toId);
        }
      }
    }

    if (sorted.length !== this.nodes.size) {
      throw new Error('Topological sort failed - likely due to cycles');
    }

    // Reverse the sorted order for execution (dependencies first)
    sorted.reverse();

    // Extract operations in sorted order
    return sorted
      .map(nodeId => this.nodes.get(nodeId)?.operation)
      .filter((op): op is Operation => op !== undefined);
  }

  /**
   * Topological sort with hierarchy level grouping
   * Groups operations by hierarchy level, then sorts within each level
   */
  topologicalSortByLevel(): Operation[] {
    const sorted = this.topologicalSort();

    // Group by hierarchy level
    const levels = new Map<number, Operation[]>();
    for (const op of sorted) {
      const nodeId = this.getTargetObjectId(op);
      if (!nodeId) continue;

      const node = this.nodes.get(nodeId);
      if (!node) continue;

      const level = node.hierarchyLevel;
      if (!levels.has(level)) {
        levels.set(level, []);
      }
      levels.get(level)!.push(op);
    }

    // Flatten levels in order
    const result: Operation[] = [];
    const sortedLevels = Array.from(levels.keys()).sort((a, b) => a - b);
    for (const level of sortedLevels) {
      result.push(...levels.get(level)!);
    }

    return result;
  }

  /**
   * Get breaking changes for a DROP operation
   * Returns list of dependent object IDs that would be affected
   */
  getBreakingChanges(droppedObjectId: string): string[] {
    const dependents = this.getDependents(droppedObjectId);
    return dependents.map(edge => edge.fromId);
  }

  /**
   * Validate dependencies
   * Returns array of warning messages
   */
  validateDependencies(): string[] {
    const warnings: string[] = [];

    for (const [nodeId, node] of this.nodes.entries()) {
      const edges = this.getDependencies(nodeId);

      for (const edge of edges) {
        // Check if dependency exists
        if (!this.nodes.has(edge.toId)) {
          const displayName = this.getNodeDisplayName(nodeId);
          warnings.push(
            `Missing dependency: ${displayName} depends on ${edge.toId} (type: ${edge.depType})`
          );
          continue;
        }

        // Check hierarchy levels (child should have higher level than parent)
        const depNode = this.nodes.get(edge.toId);
        if (depNode && node.hierarchyLevel < depNode.hierarchyLevel) {
          const displayName = this.getNodeDisplayName(nodeId);
          const depDisplayName = this.getNodeDisplayName(edge.toId);
          warnings.push(
            `Invalid hierarchy: ${displayName} (level ${node.hierarchyLevel}) depends on ` +
            `${depDisplayName} (level ${depNode.hierarchyLevel})`
          );
        }
      }
    }

    return warnings;
  }

  /**
   * Get human-readable display name for a node
   */
  private getNodeDisplayName(nodeId: string): string {
    const node = this.nodes.get(nodeId);
    if (!node) return nodeId;
    return `${node.type}:${nodeId}`;
  }

  /**
   * Extract target object ID from operation
   */
  private getTargetObjectId(op: Operation): string | null {
    // Most operations have a target field
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
}

