/**
 * Provider Hierarchy Definitions
 * 
 * Defines the hierarchical structure of objects within a provider.
 * Different providers have different hierarchies:
 * - Unity Catalog: Catalog → Schema → Table/Volume/Function
 * - Hive Metastore: Database → Table
 * - PostgreSQL: Database → Schema → Table/View/Function
 */

export interface HierarchyLevel {
  /** Internal name (e.g., "catalog", "schema", "table") */
  name: string;
  
  /** Display name for UI (e.g., "Catalog", "Schema", "Table") */
  displayName: string;
  
  /** Plural form for UI (e.g., "catalogs", "schemas", "tables") */
  pluralName: string;
  
  /** Optional icon identifier for UI */
  icon?: string;
  
  /** Whether this level supports being a container for other levels */
  isContainer: boolean;
}

export interface ProviderHierarchy {
  /** Ordered list of hierarchy levels from top to bottom */
  levels: HierarchyLevel[];
  
  /**
   * Get hierarchy level at a specific depth (0-indexed)
   * @param depth - Depth in hierarchy (0 = root level)
   */
  getLevel(depth: number): HierarchyLevel | undefined;
  
  /**
   * Get the total depth of the hierarchy
   */
  getDepth(): number;
  
  /**
   * Get level by name
   * @param name - Internal name of the level
   */
  getLevelByName(name: string): HierarchyLevel | undefined;
  
  /**
   * Get the depth of a specific level by name
   * @param name - Internal name of the level
   */
  getLevelDepth(name: string): number;
}

/**
 * Implementation of ProviderHierarchy
 */
export class Hierarchy implements ProviderHierarchy {
  constructor(public readonly levels: HierarchyLevel[]) {
    if (levels.length === 0) {
      throw new Error('Hierarchy must have at least one level');
    }
  }
  
  getLevel(depth: number): HierarchyLevel | undefined {
    return this.levels[depth];
  }
  
  getDepth(): number {
    return this.levels.length;
  }
  
  getLevelByName(name: string): HierarchyLevel | undefined {
    return this.levels.find(level => level.name === name);
  }
  
  getLevelDepth(name: string): number {
    return this.levels.findIndex(level => level.name === name);
  }
}

