/**
 * Unity Catalog SQL Generator
 * 
 * Generates Databricks SQL DDL statements from operations.
 * Migrated from sql-generator.ts
 */

import { Operation } from '../base/operations';
import { BaseSQLGenerator, SQLGenerationResult } from '../base/sql-generator';
import { UnityState } from './models';
import { UNITY_OPERATIONS } from './operations';
import { LocationDefinition } from '../../storage-v4';

interface LocationResolution {
  resolved: string;
  locationName: string;
  relativePath?: string;
}

export class UnitySQLGenerator extends BaseSQLGenerator {
  private idNameMap: Record<string, string> = {};
  private catalogNameMapping: Record<string, string> = {}; // logical → physical
  private managedLocations?: Record<string, LocationDefinition>; // project-level
  private externalLocations?: Record<string, LocationDefinition>; // project-level
  private environmentName?: string; // current environment for path resolution
  
  constructor(
    protected state: UnityState, 
    catalogNameMapping?: Record<string, string>,
    options?: {
      managedLocations?: Record<string, LocationDefinition>;
      externalLocations?: Record<string, LocationDefinition>;
      environmentName?: string;
    }
  ) {
    super(state);
    this.catalogNameMapping = catalogNameMapping || {};
    this.managedLocations = options?.managedLocations;
    this.externalLocations = options?.externalLocations;
    this.environmentName = options?.environmentName;
    this.idNameMap = this.buildIdNameMap();
  }
  
  /**
   * Build a mapping from IDs to fully-qualified names
   * 
   * Uses catalogNameMapping to replace logical catalog names with physical names
   * when generating environment-specific SQL.
   */
  private buildIdNameMap(): Record<string, string> {
    const map: Record<string, string> = {};
    
    for (const catalog of this.state.catalogs) {
      // Apply catalog name mapping if present (for environment-specific SQL)
      const catalogName = this.catalogNameMapping[catalog.name] || catalog.name;
      map[catalog.id] = catalogName;
      
      for (const schema of catalog.schemas) {
        map[schema.id] = `${catalogName}.${schema.name}`;
        
        for (const table of schema.tables) {
          map[table.id] = `${catalogName}.${schema.name}.${table.name}`;
          
          for (const column of table.columns) {
            map[column.id] = column.name;
          }
        }
      }
    }
    
    return map;
  }
  
  /**
   * Build a fully-qualified name with each part escaped separately.
   * 
   * This ensures consistent identifier formatting: `catalog`.`schema`.`table`
   * 
   * @param parts - Catalog, schema, table, etc. (in order)
   * @returns Escaped FQN like `catalog`.`schema`.`table`
   * 
   * @example
   * buildFqn("bronze", "raw", "users") // Returns: `bronze`.`raw`.`users`
   */
  private buildFqn(...parts: string[]): string {
    return parts
      .filter(part => part) // Remove empty parts
      .map(part => this.escapeIdentifier(part))
      .join('.');
  }
  
  /**
   * Resolve external location name and path to absolute path.
   * 
   * @param externalLocationName - Logical location name from project config
   * @param path - Relative path under the external location (optional)
   * @returns Location resolution details or null if not an external table
   */
  private resolveTableLocation(
    externalLocationName: string | undefined,
    path: string | undefined
  ): LocationResolution | null {
    if (!externalLocationName) {
      return null; // Not an external table
    }
    
    if (!this.externalLocations) {
      throw new Error(
        `External location '${externalLocationName}' requires project-level externalLocations configuration.`
      );
    }
    
    const locDef = this.externalLocations[externalLocationName];
    if (!locDef) {
      const available = Object.keys(this.externalLocations).join(', ');
      throw new Error(
        `External location '${externalLocationName}' not found in project. ` +
        `Available: ${available || '(none)'}`
      );
    }
    
    if (!this.environmentName) {
      throw new Error(
        `Cannot resolve external location '${externalLocationName}': environment name not provided.`
      );
    }
    
    const envPath = locDef.paths[this.environmentName];
    if (!envPath) {
      const availableEnvs = Object.keys(locDef.paths).join(', ');
      throw new Error(
        `External location '${externalLocationName}' does not have a path configured for environment '${this.environmentName}'. ` +
        `Configured environments: ${availableEnvs || '(none)'}`
      );
    }
    
    const basePath = envPath.replace(/\/$/, ''); // Remove trailing slash
    
    if (path) {
      const relPath = path.replace(/^\//, ''); // Remove leading slash
      return {
        resolved: `${basePath}/${relPath}`,
        locationName: externalLocationName,
        relativePath: path
      };
    }
    
    return {
      resolved: basePath,
      locationName: externalLocationName
    };
  }
  
  /**
   * Resolve managed location name to absolute path.
   * 
   * @param locationName - Logical managed location name from project config
   * @returns Location resolution details or null if no managed location
   */
  private resolveManagedLocation(
    locationName: string | undefined
  ): LocationResolution | null {
    if (!locationName) {
      return null;
    }
    
    if (!this.managedLocations) {
      throw new Error(
        `Managed location '${locationName}' requires project-level managedLocations configuration.`
      );
    }
    
    const locDef = this.managedLocations[locationName];
    if (!locDef) {
      const available = Object.keys(this.managedLocations).join(', ');
      throw new Error(
        `Managed location '${locationName}' not found in project. ` +
        `Available: ${available || '(none)'}`
      );
    }
    
    if (!this.environmentName) {
      throw new Error(
        `Cannot resolve managed location '${locationName}': environment name not provided.`
      );
    }
    
    const envPath = locDef.paths[this.environmentName];
    if (!envPath) {
      const availableEnvs = Object.keys(locDef.paths).join(', ');
      throw new Error(
        `Managed location '${locationName}' does not have a path configured for environment '${this.environmentName}'. ` +
        `Configured environments: ${availableEnvs || '(none)'}`
      );
    }
    
    return {
      resolved: envPath.replace(/\/$/, ''),
      locationName: locationName
    };
  }
  
  canGenerateSQL(op: Operation): boolean {
    const supportedOps = Object.values(UNITY_OPERATIONS);
    return supportedOps.includes(op.op as any);
  }

  /**
   * Get dependency level for operation type (for ordering)
   * 0 = catalog, 1 = schema, 2 = table, 3 = table modifications
   */
  private getOperationLevel(opType: string): number {
    if (opType.includes('catalog')) return 0;
    if (opType.includes('schema')) return 1;
    if (opType.includes('add_table')) return 2;
    return 3; // All other table operations (columns, properties, etc.)
  }

  /**
   * Filter out create+drop pairs that cancel each other.
   * 
   * If an object is created and then dropped in the same changeset,
   * skip ALL operations for that object (they cancel out).
   * 
   * Examples:
   * - add_table + drop_table → skip both (and any operations in between)
   * - add_catalog + drop_catalog → skip both
   * - add_schema + drop_schema → skip both
   * - add_column + drop_column → skip both
   * 
   * This prevents errors when trying to drop objects that were never
   * created in the database (e.g., table in non-existent schema).
   */
  private filterCancelledOperations(ops: Operation[]): Operation[] {
    // Group operations by target object
    const byTarget: Record<string, Operation[]> = {};
    for (const op of ops) {
      const targetId = this.getTargetObjectId(op);
      if (targetId) {
        if (!byTarget[targetId]) {
          byTarget[targetId] = [];
        }
        byTarget[targetId].push(op);
      } else {
        // Keep operations without specific target (global operations)
        if (!byTarget['__global__']) {
          byTarget['__global__'] = [];
        }
        byTarget['__global__'].push(op);
      }
    }

    // Filter each group
    const filtered: Operation[] = [];
    for (const [targetId, targetOps] of Object.entries(byTarget)) {
      // Skip empty groups
      if (!targetOps || targetOps.length === 0) {
        continue;
      }

      // Check if first op is CREATE and last op is DROP
      if (targetOps.length >= 2) {
        const firstOpType = targetOps[0].op;
        const lastOpType = targetOps[targetOps.length - 1].op;

        // Define create/drop pairs
        const cancelPairs: Array<[string, string]> = [
          ['unity.add_catalog', 'unity.drop_catalog'],
          ['unity.add_schema', 'unity.drop_schema'],
          ['unity.add_table', 'unity.drop_table'],
        ];

        // Check if this is a cancellation pair
        let cancelled = false;
        for (const [createOp, dropOp] of cancelPairs) {
          if (firstOpType === createOp && lastOpType === dropOp) {
            // Cancel out: skip ALL operations for this object
            // (including any modifications in between)
            cancelled = true;
            break;
          }
        }

        // Special handling for columns: only cancel if adding and dropping THE SAME column
        if (
          !cancelled &&
          firstOpType === 'unity.add_column' &&
          lastOpType === 'unity.drop_column'
        ) {
          // For columns, we need to check if we're adding and dropping the same column
          // (not just any columns on the same table)
          if (targetOps.length === 2 && targetOps[0].target === targetOps[1].target) {
            // Same column ID - cancel it
            cancelled = true;
          }
        }

        if (!cancelled) {
          // Not cancelled - keep all operations
          filtered.push(...targetOps);
        }
      } else {
        // Single operation or global - keep it
        filtered.push(...targetOps);
      }
    }

    return filtered;
  }

  /**
   * Generate SQL statements with comprehensive batch optimizations.
   * 
   * Optimizations include:
   * - Dependency-ordered operations (catalog → schema → table)
   * - CREATE + DROP cancellation (skip objects created then dropped)
   * - Complete CREATE TABLE statements (no empty tables + ALTERs)
   * - Batched column reordering (minimal ALTER statements)
   * - Table property consolidation
   */
  generateSQL(ops: Operation[]): string {
    // Sort operations by dependency level first
    const sortedOps = [...ops].sort((a, b) => {
      const levelA = this.getOperationLevel(a.op);
      const levelB = this.getOperationLevel(b.op);
      return levelA - levelB;
    });

    // Filter out create+drop pairs that cancel each other
    const filteredOps = this.filterCancelledOperations(sortedOps);

    // Pre-process: batch operations by object (catalog, schema, table)
    const batches = this.batchOperations(filteredOps);
    const processedOpIds = new Set<string>();
    const statements: string[] = [];

    // Separate batched operations by type
    const catalogBatches: string[] = [];
    const schemaBatches: string[] = [];
    const tableBatches: string[] = [];
    const otherOps: Operation[] = [];

    // Process batched operations
    for (const [objectId, batchInfo] of Object.entries(batches)) {
      const { objectType, opIds, operationTypes } = batchInfo;
      
      // Mark these operations as processed
      opIds.forEach(id => processedOpIds.add(id));
      
      // Generate optimized SQL based on object type
      let sql = '';
      const uniqueTypes = [...new Set(operationTypes.map(op => op.replace('unity.', '')))];
      const header = `-- Batch ${objectType.charAt(0).toUpperCase() + objectType.slice(1)} Operations: ${opIds.length} operations\n-- Object: ${objectId}\n-- Types: ${uniqueTypes.sort().join(', ')}\n-- Operations: ${opIds.join(', ')}`;
      
      if (objectType === 'catalog') {
        sql = this.generateOptimizedCatalogSQL(objectId, batchInfo);
        if (sql && !sql.startsWith('--')) {
          catalogBatches.push(`${header}\n${sql};`);
        }
      } else if (objectType === 'schema') {
        sql = this.generateOptimizedSchemaSQL(objectId, batchInfo);
        if (sql && !sql.startsWith('--')) {
          schemaBatches.push(`${header}\n${sql};`);
        }
      } else if (objectType === 'table') {
        sql = this.generateOptimizedTableSQL(objectId, batchInfo);
        if (sql && !sql.startsWith('--')) {
          tableBatches.push(`${header}\n${sql};`);
        }
      }
    }

    // Collect remaining (non-batched) operations
    for (const op of sortedOps) {
      if (processedOpIds.has(op.id)) {
        continue; // Skip already processed operations
      }
      
      if (!this.canGenerateSQL(op)) {
        console.warn(`Cannot generate SQL for operation: ${op.op}`);
        continue;
      }

      otherOps.push(op);
    }

    // Generate SQL in dependency order: catalogs → schemas → tables → others
    statements.push(...catalogBatches);
    statements.push(...schemaBatches);
    statements.push(...tableBatches);

    // Add other non-batched operations
    for (const op of otherOps) {
      const result = this.generateSQLForOperation(op);
      const header = `-- Operation: ${op.id} (${op.ts})\n-- Type: ${op.op}`;
      const warningsComment = result.warnings.length > 0
        ? `\n-- Warnings: ${result.warnings.join(', ')}`
        : '';
      statements.push(`${header}${warningsComment}\n${result.sql};`);
    }
    
    return statements.join('\n\n');
  }
  
  generateSQLForOperation(op: Operation): SQLGenerationResult {
    // Strip provider prefix for switch statement
    const opType = op.op.replace('unity.', '');
    
    try {
      const sql = this.generateSQLForOpType(opType, op);
      return {
        sql,
        warnings: [],
        isIdempotent: true,
      };
    } catch (error) {
      return {
        sql: `-- Error generating SQL: ${error}`,
        warnings: [(error as Error).message],
        isIdempotent: false,
      };
    }
  }
  
  // Catalog operations
  private addCatalog(op: Operation): string {
    // Use mapped name from idNameMap (handles __implicit__ → physical catalog)
    const name = this.idNameMap[op.target] || op.payload.name;
    const managedLocationName = op.payload.managedLocationName;
    
    let sql = `CREATE CATALOG IF NOT EXISTS ${this.escapeIdentifier(name)}`;
    
    if (managedLocationName) {
      const location = this.resolveManagedLocation(managedLocationName);
      if (location) {
        sql += ` MANAGED LOCATION '${this.escapeString(location.resolved)}'`;
      }
    }
    
    return sql;
  }
  
  private renameCatalog(op: Operation): string {
    const oldName = op.payload.oldName;
    const newName = op.payload.newName;
    return `ALTER CATALOG ${this.escapeIdentifier(oldName)} RENAME TO ${this.escapeIdentifier(newName)}`;
  }
  
  private updateCatalog(op: Operation): string {
    const name = this.idNameMap[op.target] || op.target;
    const managedLocationName = op.payload.managedLocationName;
    
    if (managedLocationName) {
      const location = this.resolveManagedLocation(managedLocationName);
      if (location) {
        return `ALTER CATALOG ${this.escapeIdentifier(name)} SET MANAGED LOCATION '${this.escapeString(location.resolved)}'`;
      } else {
        return `-- Warning: Managed location '${managedLocationName}' not configured`;
      }
    } else {
      return `-- Warning: No managed location specified for catalog update`;
    }
  }
  
  private dropCatalog(op: Operation): string {
    const name = this.idNameMap[op.target] || op.target;
    return `DROP CATALOG IF EXISTS ${this.escapeIdentifier(name)}`;
  }
  
  // Schema operations
  private addSchema(op: Operation): string {
    const catalogName = this.idNameMap[op.payload.catalogId] || 'unknown';
    const schemaName = op.payload.name;
    const managedLocationName = op.payload.managedLocationName;
    
    let sql = `CREATE SCHEMA IF NOT EXISTS ${this.buildFqn(catalogName, schemaName)}`;
    
    if (managedLocationName) {
      const location = this.resolveManagedLocation(managedLocationName);
      if (location) {
        sql += ` MANAGED LOCATION '${this.escapeString(location.resolved)}'`;
      }
    }
    
    return sql;
  }
  
  private renameSchema(op: Operation): string {
    const oldName = op.payload.oldName;
    const newName = op.payload.newName;
    // Get catalog name from idNameMap (catalog doesn't change during schema rename)
    const fqn = this.idNameMap[op.target] || 'unknown.unknown';
    const catalogName = fqn.split('.')[0];
    return `ALTER SCHEMA ${this.buildFqn(catalogName, oldName)} RENAME TO ${this.buildFqn(catalogName, newName)}`;
  }
  
  private updateSchema(op: Operation): string {
    const fqn = this.idNameMap[op.target] || 'unknown.unknown';
    const parts = fqn.split('.');
    const catalogName = parts[0];
    const schemaName = parts[1] || 'unknown';
    const managedLocationName = op.payload.managedLocationName;
    
    if (managedLocationName) {
      const location = this.resolveManagedLocation(managedLocationName);
      if (location) {
        return `ALTER SCHEMA ${this.buildFqn(catalogName, schemaName)} SET MANAGED LOCATION '${this.escapeString(location.resolved)}'`;
      } else {
        return `-- Warning: Managed location '${managedLocationName}' not configured`;
      }
    } else {
      return `-- Warning: No managed location specified for schema update`;
    }
  }
  
  private dropSchema(op: Operation): string {
    const fqn = this.idNameMap[op.target] || 'unknown.unknown';
    const parts = fqn.split('.');
    return `DROP SCHEMA IF EXISTS ${this.buildFqn(...parts)}`;
  }
  
  // Table operations
  private addTable(op: Operation): string {
    const schemaFqn = this.idNameMap[op.payload.schemaId] || 'unknown.unknown';
    const parts = schemaFqn.split('.');
    const catalogName = parts[0];
    const schemaName = parts[1] || 'unknown';
    const tableName = op.payload.name;
    const tableFormat = op.payload.format.toUpperCase();
    const isExternal = op.payload.external === true;
    
    // Resolve location
    const locationInfo = isExternal 
      ? this.resolveTableLocation(
          op.payload.externalLocationName,
          op.payload.path
        )
      : null;
    
    const partitionCols = op.payload.partitionColumns;
    const clusterCols = op.payload.clusterColumns;
    
    // Build SQL clauses
    const externalKeyword = isExternal ? 'EXTERNAL ' : '';
    const locationClause = locationInfo 
      ? ` LOCATION '${this.escapeString(locationInfo.resolved)}'` 
      : '';
    const partitionClause = (partitionCols && partitionCols.length > 0)
      ? ` PARTITIONED BY (${partitionCols.join(', ')})`
      : '';
    const clusterClause = (clusterCols && clusterCols.length > 0)
      ? ` CLUSTER BY (${clusterCols.join(', ')})`
      : '';
    
    // Add warnings and metadata as SQL comments
    let warnings = '';
    if (isExternal && locationInfo) {
      warnings = `-- External Table: ${tableName}\n` +
                 `-- Location Name: ${locationInfo.locationName}\n`;
      
      if (locationInfo.relativePath) {
        warnings += `-- Relative Path: ${locationInfo.relativePath}\n`;
      }
      
      warnings += `-- Resolved Location: ${locationInfo.resolved}\n` +
                 `-- WARNING: External tables must reference pre-configured external locations\n` +
                 `-- WARNING: Databricks recommends using managed tables for optimal performance\n` +
                 `-- Learn more: https://learn.microsoft.com/en-gb/azure/databricks/tables/managed\n`;
    }
    
    // Create empty table (columns added via add_column ops)
    return warnings + 
      `CREATE ${externalKeyword}TABLE IF NOT EXISTS ${this.buildFqn(catalogName, schemaName, tableName)} () ` +
      `USING ${tableFormat}${partitionClause}${clusterClause}${locationClause}`;
  }
  
  private renameTable(op: Operation): string {
    const oldName = op.payload.oldName;
    const newName = op.payload.newName;
    // Get catalog and schema names from idNameMap (they don't change during table rename)
    const fqn = this.idNameMap[op.target] || 'unknown.unknown.unknown';
    const parts = fqn.split('.');
    const catalogName = parts[0];
    const schemaName = parts[1] || 'unknown';
    return `ALTER TABLE ${this.buildFqn(catalogName, schemaName, oldName)} RENAME TO ${this.buildFqn(catalogName, schemaName, newName)}`;
  }
  
  private dropTable(op: Operation): string {
    // Payload contains required fields (created by state differ with table context)
    const tableName = op.payload.name;
    const catalogId = op.payload.catalogId;
    const schemaId = op.payload.schemaId;
    
    // Resolve catalog and schema names from IDs
    const catalogFqn = this.idNameMap[catalogId] || 'unknown';
    const catalogName = catalogFqn.includes('.') ? catalogFqn.split('.')[0] : catalogFqn;
    
    const schemaFqn = this.idNameMap[schemaId] || `${catalogName}.unknown`;
    const parts = schemaFqn.split('.');
    const schemaName = parts[1] || 'unknown';
    
    return `DROP TABLE IF EXISTS ${this.buildFqn(catalogName, schemaName, tableName)}`;
  }
  
  private setTableComment(op: Operation): string {
    const fqn = this.idNameMap[op.payload.tableId] || 'unknown';
    const tableParts = fqn.split('.');
    const tableEscaped = this.buildFqn(...tableParts);
    const comment = this.escapeString(op.payload.comment);
    return `COMMENT ON TABLE ${tableEscaped} IS '${comment}'`;
  }
  
  private setTableProperty(op: Operation): string {
    const fqn = this.idNameMap[op.payload.tableId] || 'unknown';
    const tableParts = fqn.split('.');
    const tableEscaped = this.buildFqn(...tableParts);
    const key = op.payload.key;
    const value = op.payload.value;
    return `ALTER TABLE ${tableEscaped} SET TBLPROPERTIES ('${key}' = '${value}')`;
  }
  
  private unsetTableProperty(op: Operation): string {
    const fqn = this.idNameMap[op.payload.tableId] || 'unknown';
    const tableParts = fqn.split('.');
    const tableEscaped = this.buildFqn(...tableParts);
    const key = op.payload.key;
    return `ALTER TABLE ${tableEscaped} UNSET TBLPROPERTIES ('${key}')`;
  }
  
  private setTableTag(op: Operation): string {
    const fqn = this.idNameMap[op.payload.tableId] || 'unknown';
    const tableParts = fqn.split('.');
    const tableEscaped = this.buildFqn(...tableParts);
    const tagName = op.payload.tagName;
    const tagValue = this.escapeString(op.payload.tagValue);
    return `ALTER TABLE ${tableEscaped} SET TAGS ('${tagName}' = '${tagValue}')`;
  }
  
  private unsetTableTag(op: Operation): string {
    const fqn = this.idNameMap[op.payload.tableId] || 'unknown';
    const tableParts = fqn.split('.');
    const tableEscaped = this.buildFqn(...tableParts);
    const tagName = op.payload.tagName;
    return `ALTER TABLE ${tableEscaped} UNSET TAGS ('${tagName}')`;
  }
  
  // Column operations
  private addColumn(op: Operation): string {
    const tableFqn = this.idNameMap[op.payload.tableId] || 'unknown';
    const tableParts = tableFqn.split('.');
    const tableEscaped = this.buildFqn(...tableParts);
    const colName = op.payload.name;
    const colType = op.payload.type;
    const comment = op.payload.comment || '';
    
    // Note: NOT NULL is not supported in ALTER TABLE ADD COLUMN for Delta tables
    // New columns added to existing tables must be nullable
    const commentClause = comment ? ` COMMENT '${this.escapeString(comment)}'` : '';
    
    return `ALTER TABLE ${tableEscaped} ADD COLUMN ${this.escapeIdentifier(colName)} ${colType}${commentClause}`;
  }
  
  private renameColumn(op: Operation): string {
    const tableFqn = this.idNameMap[op.payload.tableId] || 'unknown';
    const tableParts = tableFqn.split('.');
    const tableEscaped = this.buildFqn(...tableParts);
    const oldName = op.payload.oldName;
    const newName = op.payload.newName;
    return `ALTER TABLE ${tableEscaped} RENAME COLUMN ${this.escapeIdentifier(oldName)} TO ${this.escapeIdentifier(newName)}`;
  }
  
  private dropColumn(op: Operation): string {
    const tableFqn = this.idNameMap[op.payload.tableId] || 'unknown';
    const tableParts = tableFqn.split('.');
    const tableEscaped = this.buildFqn(...tableParts);
    // Get column name from payload (for dropped columns not in current state)
    const colName = op.payload.name || this.idNameMap[op.target] || 'unknown';
    return `ALTER TABLE ${tableEscaped} DROP COLUMN ${this.escapeIdentifier(colName)}`;
  }
  
  private reorderColumns(op: Operation): string {
    // Databricks doesn't support direct column reordering
    return '-- Column reordering not directly supported in Databricks SQL';
  }

  /**
   * Batch reorder_columns operations by table to generate minimal SQL.
   */
  private batchReorderOperations(ops: Operation[]): Record<string, {
    originalOrder: string[];
    finalOrder: string[];
    opIds: string[];
  }> {
    const reorderBatches: Record<string, {
      originalOrder: string[];
      finalOrder: string[];
      opIds: string[];
    }> = {};
    
    for (const op of ops) {
      const opType = op.op.replace('unity.', '');
      
      if (opType === 'reorder_columns') {
        const tableId = op.payload.tableId;
        const desiredOrder = op.payload.order;
        
        if (!reorderBatches[tableId]) {
          // Find original column order from current state
          const originalOrder = this.getTableColumnOrder(tableId);
          reorderBatches[tableId] = {
            originalOrder: originalOrder,
            finalOrder: [...originalOrder], // Will be updated
            opIds: []
          };
        }
        
        // Update final order and track operation
        reorderBatches[tableId].finalOrder = desiredOrder;
        reorderBatches[tableId].opIds.push(op.id);
      }
    }
    
    return reorderBatches;
  }

  /**
   * Get current column order for a table from state
   */
  private getTableColumnOrder(tableId: string): string[] {
    for (const catalog of this.state.catalogs) {
      for (const schema of catalog.schemas) {
        for (const table of schema.tables) {
          if (table.id === tableId) {
            return table.columns.map(col => col.id);
          }
        }
      }
    }
    return [];
  }

  /**
   * Generate minimal SQL to reorder columns from original to final order
   */
  private generateOptimizedReorderSQL(
    tableId: string,
    originalOrder: string[],
    finalOrder: string[],
    opIds: string[]
  ): string {
    if (!originalOrder.length || !finalOrder.length) {
      return '-- No columns to reorder';
    }
    
    if (JSON.stringify(originalOrder) === JSON.stringify(finalOrder)) {
      return '-- Column order unchanged';
    }
    
    // Get table name for ALTER statements
    const tableFqn = this.idNameMap[tableId] || 'unknown';
    const tableParts = tableFqn.split('.');
    const tableEsc = this.buildFqn(...tableParts);
    
    // OPTIMIZATION: Detect single-column drag for cleaner SQL
    // A single drag means one column moved, and all others maintained relative order
    // Find the column that was dragged by checking which column broke the sequence
    let singleMove: { colId: string; origPos: number; newPos: number } | null = null;
    for (let i = 0; i < finalOrder.length; i++) {
      const colId = finalOrder[i];
      // Check if removing this column from both orders leaves them identical
      const origWithout = originalOrder.filter(c => c !== colId);
      const finalWithout = finalOrder.filter(c => c !== colId);
      if (JSON.stringify(origWithout) === JSON.stringify(finalWithout)) {
        // This column was the one that moved
        const originalPos = originalOrder.indexOf(colId);
        const newPos = i;
        if (originalPos !== newPos) {
          singleMove = { colId, origPos: originalPos, newPos };
          break;
        }
      }
    }
    
    // If we detected a single column drag, generate optimal SQL
    if (singleMove) {
      const { colId, newPos } = singleMove;
      const colName = this.idNameMap[colId] || colId;
      const colEsc = this.escapeIdentifier(colName);
      
      if (newPos === 0) {
        // Column moved to first position
        return `ALTER TABLE ${tableEsc} ALTER COLUMN ${colEsc} FIRST`;
      } else {
        // Column moved after another column
        const prevColId = finalOrder[newPos - 1];
        const prevColName = this.idNameMap[prevColId] || prevColId;
        const prevColEsc = this.escapeIdentifier(prevColName);
        return `ALTER TABLE ${tableEsc} ALTER COLUMN ${colEsc} AFTER ${prevColEsc}`;
      }
    }
    
    // Multiple columns moved - use general algorithm
    const statements: string[] = [];
    const currentOrder = [...originalOrder];
    
    // Process columns in reverse order to handle dependencies correctly
    for (let i = finalOrder.length - 1; i >= 0; i--) {
      const colId = finalOrder[i];
      const currentPos = currentOrder.indexOf(colId);
      
      if (currentPos === -1) {
        continue; // Column not found
      }
      
      // If column is already in correct position, skip
      if (currentPos === i) {
        continue;
      }
      
      const colName = this.idNameMap[colId] || colId;
      const colEsc = this.escapeIdentifier(colName);
      
      if (i === 0) {
        // Move to first position
        statements.push(`ALTER TABLE ${tableEsc} ALTER COLUMN ${colEsc} FIRST`);
      } else {
        // Move after the previous column
        const prevColId = finalOrder[i - 1];
        const prevColName = this.idNameMap[prevColId] || prevColId;
        const prevColEsc = this.escapeIdentifier(prevColName);
        statements.push(`ALTER TABLE ${tableEsc} ALTER COLUMN ${colEsc} AFTER ${prevColEsc}`);
      }
      
      // Update currentOrder to reflect the change for next iteration
      currentOrder.splice(currentPos, 1);
      currentOrder.splice(i, 0, colId);
    }
    
    if (statements.length === 0) {
      return '-- No column reordering needed';
    }
    
    return statements.join(';\n');
  }

  /**
   * Batch operations by table to generate optimal DDL.
   */
  /**
   * Batch operations by object (catalog, schema, or table).
   * Returns a map of object_id -> batch info with all related operations.
   */
  private batchOperations(ops: Operation[]): Record<string, {
    objectType: 'catalog' | 'schema' | 'table';
    isNew: boolean;
    createOp: Operation | null;
    modifyOps: Operation[];
    opIds: string[];
    operationTypes: string[];
  }> {
    const batches: Record<string, {
      objectType: 'catalog' | 'schema' | 'table';
      isNew: boolean;
      createOp: Operation | null;
      modifyOps: Operation[];
      opIds: string[];
      operationTypes: string[];
    }> = {};
    
    for (const op of ops) {
      const opType = op.op.replace('unity.', '');
      
      // Identify object type and ID
      let objectId: string | null = null;
      let objectType: 'catalog' | 'schema' | 'table' | null = null;
      
      // Catalog operations
      if (['add_catalog', 'rename_catalog', 'update_catalog', 'drop_catalog'].includes(opType)) {
        objectId = `catalog:${op.target}`;
        objectType = 'catalog';
      }
      // Schema operations
      else if (['add_schema', 'rename_schema', 'update_schema', 'drop_schema'].includes(opType)) {
        objectId = `schema:${op.target}`;
        objectType = 'schema';
      }
      // Table-level operations
      else if (['add_table', 'rename_table', 'drop_table', 'set_table_comment'].includes(opType)) {
        objectId = `table:${op.target}`;
        objectType = 'table';
      } 
      // Column and table property operations
      else if ([
        'add_column', 'rename_column', 'drop_column', 'reorder_columns',
        'change_column_type', 'set_nullable', 'set_column_comment',
        'set_column_tag', 'unset_column_tag', 'set_table_property', 'unset_table_property',
        'set_table_tag', 'unset_table_tag',
        'add_constraint', 'drop_constraint', 'add_row_filter', 'update_row_filter',
        'remove_row_filter', 'add_column_mask', 'update_column_mask', 'remove_column_mask'
      ].includes(opType)) {
        const tableId = op.payload.tableId;
        if (tableId) {
          objectId = `table:${tableId}`;
          objectType = 'table';
        }
      }
      
      if (!objectId || !objectType) {
        continue; // Not a batchable operation
      }
      
      // Initialize batch if doesn't exist
      if (!batches[objectId]) {
        batches[objectId] = {
          objectType,
          isNew: false,
          createOp: null,
          modifyOps: [],
          opIds: [],
          operationTypes: []
        };
      }
      
      const batch = batches[objectId];
      batch.opIds.push(op.id);
      batch.operationTypes.push(op.op);
      
      // Categorize operation
      const isCreate = ['add_catalog', 'add_schema', 'add_table'].includes(opType);
      if (isCreate) {
        batch.isNew = true;
        batch.createOp = op;
      } else {
        batch.modifyOps.push(op);
      }
    }
    
    return batches;
  }

  /**
   * Generate optimal SQL for catalog operations (squashes CREATE + UPDATE)
   */
  private generateOptimizedCatalogSQL(objectId: string, batchInfo: {
    objectType: 'catalog' | 'schema' | 'table';
    isNew: boolean;
    createOp: Operation | null;
    modifyOps: Operation[];
    opIds: string[];
    operationTypes: string[];
  }): string {
    if (!batchInfo.createOp) {
      // No CREATE operation, just generate individual ALTER statements
      return batchInfo.modifyOps.map(op => this.generateSQLForOperation(op).sql).join(';\n');
    }

    const createOp = batchInfo.createOp;
    const catalogId = createOp.target;
    const name = this.idNameMap[catalogId] || createOp.payload.name;

    // Check for update_catalog operations to squash into CREATE
    let managedLocationName = createOp.payload.managedLocationName;
    for (const op of batchInfo.modifyOps) {
      const opType = op.op.replace('unity.', '');
      if (opType === 'update_catalog' && 'managedLocationName' in op.payload) {
        // Squash: Use the updated value
        managedLocationName = op.payload.managedLocationName;
      }
    }

    // Generate optimized CREATE CATALOG statement
    let sql = `CREATE CATALOG IF NOT EXISTS ${this.escapeIdentifier(name)}`;

    if (managedLocationName) {
      const location = this.resolveManagedLocation(managedLocationName);
      if (location) {
        sql += ` MANAGED LOCATION '${this.escapeString(location.resolved)}'`;
      }
    }

    return sql;
  }

  /**
   * Generate optimal SQL for schema operations (squashes CREATE + UPDATE)
   */
  private generateOptimizedSchemaSQL(objectId: string, batchInfo: {
    objectType: 'catalog' | 'schema' | 'table';
    isNew: boolean;
    createOp: Operation | null;
    modifyOps: Operation[];
    opIds: string[];
    operationTypes: string[];
  }): string {
    if (!batchInfo.createOp) {
      // No CREATE operation, just generate individual ALTER statements
      return batchInfo.modifyOps.map(op => this.generateSQLForOperation(op).sql).join(';\n');
    }

    const createOp = batchInfo.createOp;
    const catalogName = this.idNameMap[createOp.payload.catalogId] || 'unknown';
    const schemaName = createOp.payload.name;

    // Check for update_schema operations to squash into CREATE
    let managedLocationName = createOp.payload.managedLocationName;
    for (const op of batchInfo.modifyOps) {
      const opType = op.op.replace('unity.', '');
      if (opType === 'update_schema' && 'managedLocationName' in op.payload) {
        // Squash: Use the updated value
        managedLocationName = op.payload.managedLocationName;
      }
    }

    // Generate optimized CREATE SCHEMA statement
    let sql = `CREATE SCHEMA IF NOT EXISTS ${this.buildFqn(catalogName, schemaName)}`;

    if (managedLocationName) {
      const location = this.resolveManagedLocation(managedLocationName);
      if (location) {
        sql += ` MANAGED LOCATION '${this.escapeString(location.resolved)}'`;
      }
    }

    return sql;
  }

  /**
   * Generate optimal SQL for table operations
   */
  private generateOptimizedTableSQL(objectId: string, batchInfo: {
    objectType: 'catalog' | 'schema' | 'table';
    isNew: boolean;
    createOp: Operation | null;
    modifyOps: Operation[];
    opIds: string[];
    operationTypes: string[];
  }): string {
    // Categorize modify operations by type
    const columnOps: Operation[] = [];
    const propertyOps: Operation[] = [];
    const reorderOps: Operation[] = [];
    const constraintOps: Operation[] = [];
    const governanceOps: Operation[] = [];
    const otherOps: Operation[] = [];

    for (const op of batchInfo.modifyOps) {
      const opType = op.op.replace('unity.', '');
      if (opType === 'add_column') {
        columnOps.push(op);
      } else if (['set_table_property', 'unset_table_property'].includes(opType)) {
        propertyOps.push(op);
      } else if (opType === 'reorder_columns') {
        reorderOps.push(op);
      } else if (['add_constraint', 'drop_constraint'].includes(opType)) {
        constraintOps.push(op);
      } else if ([
        'add_row_filter', 'update_row_filter', 'remove_row_filter',
        'add_column_mask', 'update_column_mask', 'remove_column_mask'
      ].includes(opType)) {
        governanceOps.push(op);
      } else {
        otherOps.push(op);
      }
    }

    const legacyBatchInfo = {
      tableOp: batchInfo.createOp,
      columnOps,
      propertyOps,
      reorderOps,
      constraintOps,
      governanceOps,
      otherOps,
      opIds: batchInfo.opIds,
      operationTypes: batchInfo.operationTypes
    };

    if (batchInfo.isNew) {
      // Generate complete CREATE TABLE statement
      return this.generateCreateTableWithColumns(objectId, legacyBatchInfo);
    } else {
      // Generate optimized ALTER statements for existing table
      return this.generateAlterStatementsForTable(objectId, legacyBatchInfo);
    }
  }

  /**
   * Generate complete CREATE TABLE statement with all columns included
   */
  private generateCreateTableWithColumns(tableId: string, batchInfo: {
    tableOp: Operation | null;
    columnOps: Operation[];
    propertyOps: Operation[];
    reorderOps: Operation[];
    otherOps: Operation[];
  }): string {
    const tableOp = batchInfo.tableOp;
    const columnOps = batchInfo.columnOps;
    const propertyOps = batchInfo.propertyOps;
    const reorderOps = batchInfo.reorderOps;
    const otherOps = batchInfo.otherOps;
    
    if (!tableOp) {
      return '-- Error: No table creation operation found';
    }
    
    // Get table name and schema info
    const tableName = tableOp.payload.name || 'unknown';
    const schemaId = tableOp.payload.schemaId;
    const schemaFqn = schemaId ? (this.idNameMap[schemaId] || 'unknown.unknown') : 'unknown.unknown';
    const tableFqn = `${schemaFqn}.${tableName}`;
    const tableParts = tableFqn.split('.');
    const tableEsc = this.buildFqn(...tableParts);
    
    // Build column definitions as a map (by column ID)
    const columnsMap: Record<string, string> = {};
    for (const colOp of columnOps) {
      const colId = colOp.payload.colId;
      const colName = this.escapeIdentifier(colOp.payload.name);
      const colType = colOp.payload.type;
      const nullable = colOp.payload.nullable !== false ? '' : ' NOT NULL';
      const comment = colOp.payload.comment ? ` COMMENT '${this.escapeString(colOp.payload.comment)}'` : '';
      columnsMap[colId] = `  ${colName} ${colType}${nullable}${comment}`;
    }
    
    // Apply column reordering if present
    // Use the final order from the last reorder operation
    let columns: string[];
    if (reorderOps.length > 0) {
      const finalOrder = reorderOps[reorderOps.length - 1].payload.order;
      // Include columns from the reorder in their specified order
      columns = finalOrder
        .filter((colId: string) => colId in columnsMap)
        .map((colId: string) => columnsMap[colId]);
      // Append any columns added AFTER the reorder (not in the reorder list)
      for (const colId of Object.keys(columnsMap)) {
        if (!finalOrder.includes(colId)) {
          columns.push(columnsMap[colId]);
        }
      }
    } else {
      // No reorder: use the order columns were added
      columns = columnOps
        .filter(colOp => colOp.payload.colId in columnsMap)
        .map(colOp => columnsMap[colOp.payload.colId]);
    }
    
    // Build table format
    const tableFormat = (tableOp.payload.format || 'DELTA').toUpperCase();
    const isExternal = tableOp.payload.external === true;
    
    // Resolve location for external tables
    const locationInfo = isExternal 
      ? this.resolveTableLocation(
          tableOp.payload.externalLocationName,
          tableOp.payload.path
        )
      : null;
    
    const partitionCols = tableOp.payload.partitionColumns;
    const clusterCols = tableOp.payload.clusterColumns;
    
    // Build SQL clauses
    const externalKeyword = isExternal ? 'EXTERNAL ' : '';
    const locationClause = locationInfo 
      ? ` LOCATION '${this.escapeString(locationInfo.resolved)}'` 
      : '';
    const partitionClause = (partitionCols && partitionCols.length > 0)
      ? `\nPARTITIONED BY (${partitionCols.join(', ')})`
      : '';
    const clusterClause = (clusterCols && clusterCols.length > 0)
      ? `\nCLUSTER BY (${clusterCols.join(', ')})`
      : '';
    
    // Build table properties
    const properties: string[] = [];
    for (const propOp of propertyOps) {
      if (propOp.op.endsWith('set_table_property')) {
        const key = propOp.payload.key;
        const value = propOp.payload.value;
        properties.push(`'${key}' = '${this.escapeString(value)}'`);
      }
    }
    
    // Build table comment
    // Check both tableOp payload and set_table_comment operations in otherOps
    let commentValue = tableOp.payload.comment;
    for (const op of otherOps) {
      if (op.op.endsWith('set_table_comment')) {
        commentValue = op.payload.comment;
        break;
      }
    }
    const tableComment = commentValue ? `\nCOMMENT '${this.escapeString(commentValue)}'` : '';
    
    // Add warnings for external tables
    let warnings = '';
    if (isExternal && locationInfo) {
      warnings = `-- External Table: ${tableName}\n` +
                 `-- Location Name: ${locationInfo.locationName}\n`;
      
      if (locationInfo.relativePath) {
        warnings += `-- Relative Path: ${locationInfo.relativePath}\n`;
      }
      
      warnings += `-- Resolved Location: ${locationInfo.resolved}\n` +
                 `-- WARNING: External tables must reference pre-configured external locations\n` +
                 `-- WARNING: Databricks recommends using managed tables for optimal performance\n` +
                 `-- Learn more: https://learn.microsoft.com/en-gb/azure/databricks/tables/managed\n`;
    }
    
    // Assemble CREATE TABLE statement
    const columnsSql = columns.length > 0 ? columns.join(',\n') : '';
    const propertiesSql = properties.length > 0 ? `\nTBLPROPERTIES (${properties.join(', ')})` : '';
    
    let createSql: string;
    if (columnsSql) {
      createSql = warnings + `CREATE ${externalKeyword}TABLE IF NOT EXISTS ${tableEsc} (
${columnsSql}
) USING ${tableFormat}${tableComment}${partitionClause}${clusterClause}${propertiesSql}${locationClause}`;
    } else {
      // No columns yet - create empty table (fallback to original behavior)
      createSql = warnings + `CREATE ${externalKeyword}TABLE IF NOT EXISTS ${tableEsc} () USING ${tableFormat}${tableComment}${partitionClause}${clusterClause}${propertiesSql}${locationClause}`;
    }
    
    // Generate ALTER TABLE statements for operations that must happen after table creation
    // (e.g., table tags, column tags)
    // Skip set_table_comment since it's already included in CREATE TABLE
    const statements: string[] = [createSql];
    
    for (const op of otherOps) {
      const opType = op.op.replace('unity.', '');
      // Skip set_table_comment as it's already in CREATE TABLE
      if (opType === 'set_table_comment') {
        continue;
      }
      try {
        const sql = this.generateSQLForOpType(opType, op);
        if (sql && !sql.startsWith('--')) {
          statements.push(sql);
        }
      } catch (e) {
        statements.push(`-- Error generating SQL for ${op.id}: ${e}`);
      }
    }
    
    return statements.join(';\n');
  }

  /**
   * Generate optimized ALTER statements for existing table modifications
   */
  private generateAlterStatementsForTable(tableId: string, batchInfo: {
    columnOps: Operation[];
    propertyOps: Operation[];
    reorderOps: Operation[];
    constraintOps: Operation[];
    governanceOps: Operation[];
    otherOps: Operation[];
  }): string {
    const statements: string[] = [];
    
    // Handle column reordering first (using existing optimization)
    // For existing tables, reorder_columns generates ALTER statements
    if (batchInfo.reorderOps.length > 0) {
      const lastReorderOp = batchInfo.reorderOps[batchInfo.reorderOps.length - 1];
      // Use previousOrder from the operation payload if available (prevents comparing state with itself)
      let originalOrder = lastReorderOp.payload.previousOrder;
      if (!originalOrder) {
        // Fallback: derive from current state (for backward compatibility with old operations)
        originalOrder = this.getTableColumnOrder(tableId);
      }
      const finalOrder = lastReorderOp.payload.order;
      const reorderSql = this.generateOptimizedReorderSQL(
        tableId, originalOrder, finalOrder, 
        batchInfo.reorderOps.map(op => op.id)
      );
      if (reorderSql && !reorderSql.startsWith('--')) {
        statements.push(reorderSql);
      }
    }
    
    // Batch ADD COLUMN operations if multiple exist
    const addColumnOps = batchInfo.columnOps.filter(op => op.op.endsWith('add_column'));
    
    if (addColumnOps.length > 1) {
      // Multiple ADD COLUMN operations - batch them into single ALTER TABLE ADD COLUMNS
      const tableFqn = this.idNameMap[addColumnOps[0].payload.tableId] || 'unknown';
      const tableParts = tableFqn.split('.');
      const tableEscaped = this.buildFqn(...tableParts);
      const columnDefs: string[] = [];
      
      for (const op of addColumnOps) {
        const colName = op.payload.name;
        const colType = op.payload.type;
        const comment = op.payload.comment || '';
        
        // Note: NOT NULL is not supported in ALTER TABLE ADD COLUMNS for Delta tables
        // New columns added to existing tables must be nullable
        const commentClause = comment ? ` COMMENT '${this.escapeString(comment)}'` : '';
        
        columnDefs.push(`    ${this.escapeIdentifier(colName)} ${colType}${commentClause}`);
      }
      
      const batchedSql = `ALTER TABLE ${tableEscaped}\nADD COLUMNS (\n${columnDefs.join(',\n')}\n)`;
      statements.push(batchedSql);
    } else if (addColumnOps.length === 1) {
      // Single ADD COLUMN - use existing method
      try {
        const sql = this.addColumn(addColumnOps[0]);
        if (sql && !sql.startsWith('--')) {
          statements.push(sql);
        }
      } catch (e) {
        statements.push(`-- Error generating SQL for ${addColumnOps[0].id}: ${e}`);
      }
    }
    
    // Batch DROP COLUMN operations if multiple exist
    const dropColumnOps = batchInfo.columnOps.filter(op => op.op.endsWith('drop_column'));
    
    if (dropColumnOps.length > 1) {
      // Multiple DROP COLUMN operations - batch them into single ALTER TABLE DROP COLUMNS
      const tableFqn = this.idNameMap[dropColumnOps[0].payload.tableId] || 'unknown';
      const tableParts = tableFqn.split('.');
      const tableEscaped = this.buildFqn(...tableParts);
      
      const columnNames: string[] = [];
      for (const op of dropColumnOps) {
        // Get column name from payload (for dropped columns not in current state)
        const colName = op.payload.name || this.idNameMap[op.target] || 'unknown';
        columnNames.push(this.escapeIdentifier(colName));
      }
      
      const batchedSql = `ALTER TABLE ${tableEscaped}\nDROP COLUMNS (${columnNames.join(', ')})`;
      statements.push(batchedSql);
    } else if (dropColumnOps.length === 1) {
      // Single DROP COLUMN - use existing method
      try {
        const sql = this.dropColumn(dropColumnOps[0]);
        if (sql && !sql.startsWith('--')) {
          statements.push(sql);
        }
      } catch (e) {
        statements.push(`-- Error generating SQL for ${dropColumnOps[0].id}: ${e}`);
      }
    }
    
    // Handle other column operations (non-ADD/DROP COLUMN)
    const otherColumnOps = batchInfo.columnOps.filter(
      op => !op.op.endsWith('add_column') && !op.op.endsWith('drop_column')
    );
    
    // Handle all other operations normally
    const allOtherOps = [
      ...otherColumnOps,
      ...batchInfo.propertyOps,
      ...batchInfo.constraintOps,
      ...batchInfo.governanceOps,
      ...batchInfo.otherOps
    ];
    
    for (const op of allOtherOps) {
      const opType = op.op.replace('unity.', '');
      
      // Skip reorder operations (already handled)
      if (opType === 'reorder_columns') {
        continue;
      }
      
      // Generate SQL for individual operation
      try {
        const sql = this.generateSQLForOpType(opType, op);
        if (sql && !sql.startsWith('--')) {
          statements.push(sql);
        }
      } catch (e) {
        statements.push(`-- Error generating SQL for ${op.id}: ${e}`);
      }
    }
    
    return statements.length > 0 ? statements.join(';\n') : '-- No ALTER statements needed';
  }

  /**
   * Generate SQL based on operation type (helper method)
   */
  private generateSQLForOpType(opType: string, op: Operation): string {
    switch (opType) {
      // Catalog operations
      case 'add_catalog':
        return this.addCatalog(op);
      case 'rename_catalog':
        return this.renameCatalog(op);
      case 'update_catalog':
        return this.updateCatalog(op);
      case 'drop_catalog':
        return this.dropCatalog(op);
      
      // Schema operations
      case 'add_schema':
        return this.addSchema(op);
      case 'rename_schema':
        return this.renameSchema(op);
      case 'update_schema':
        return this.updateSchema(op);
      case 'drop_schema':
        return this.dropSchema(op);
      
      // Table operations
      case 'add_table':
        return this.addTable(op);
      case 'rename_table':
        return this.renameTable(op);
      case 'drop_table':
        return this.dropTable(op);
      case 'set_table_comment':
        return this.setTableComment(op);
      case 'reorder_columns':
        return this.reorderColumns(op);
      
      // Column operations
      case 'add_column':
        return this.addColumn(op);
      case 'rename_column':
        return this.renameColumn(op);
      case 'drop_column':
        return this.dropColumn(op);
      case 'change_column_type':
        return this.changeColumnType(op);
      case 'set_nullable':
        return this.setNullable(op);
      case 'set_column_comment':
        return this.setColumnComment(op);
      
      // Column tag operations
      case 'set_column_tag':
        return this.setColumnTag(op);
      case 'unset_column_tag':
        return this.unsetColumnTag(op);
      
      // Table property operations
      case 'set_table_property':
        return this.setTableProperty(op);
      case 'unset_table_property':
        return this.unsetTableProperty(op);
      case 'set_table_tag':
        return this.setTableTag(op);
      case 'unset_table_tag':
        return this.unsetTableTag(op);
      
      // Constraint operations
      case 'add_constraint':
        return this.addConstraint(op);
      case 'drop_constraint':
        return this.dropConstraint(op);
      
      // Row filter operations
      case 'add_row_filter':
        return this.addRowFilter(op);
      case 'update_row_filter':
        return this.updateRowFilter(op);
      case 'remove_row_filter':
        return this.removeRowFilter(op);
      
      // Column mask operations
      case 'add_column_mask':
        return this.addColumnMask(op);
      case 'update_column_mask':
        return this.updateColumnMask(op);
      case 'remove_column_mask':
        return this.removeColumnMask(op);
      
      default:
        return `-- Unsupported operation: ${opType}`;
    }
  }
  
  private changeColumnType(op: Operation): string {
    const tableFqn = this.idNameMap[op.payload.tableId] || 'unknown';
    const tableParts = tableFqn.split('.');
    const tableEscaped = this.buildFqn(...tableParts);
    const colName = this.idNameMap[op.target] || 'unknown';
    const newType = op.payload.newType;
    return `ALTER TABLE ${tableEscaped} ALTER COLUMN ${this.escapeIdentifier(colName)} TYPE ${newType}`;
  }
  
  private setNullable(op: Operation): string {
    const tableFqn = this.idNameMap[op.payload.tableId] || 'unknown';
    const tableParts = tableFqn.split('.');
    const tableEscaped = this.buildFqn(...tableParts);
    const colName = this.idNameMap[op.target] || 'unknown';
    const nullable = op.payload.nullable;
    
    if (nullable) {
      return `ALTER TABLE ${tableEscaped} ALTER COLUMN ${this.escapeIdentifier(colName)} DROP NOT NULL`;
    } else {
      return `ALTER TABLE ${tableEscaped} ALTER COLUMN ${this.escapeIdentifier(colName)} SET NOT NULL`;
    }
  }
  
  private setColumnComment(op: Operation): string {
    const tableFqn = this.idNameMap[op.payload.tableId] || 'unknown';
    const tableParts = tableFqn.split('.');
    const tableEscaped = this.buildFqn(...tableParts);
    const colName = this.idNameMap[op.target] || 'unknown';
    const comment = this.escapeString(op.payload.comment);
    return `ALTER TABLE ${tableEscaped} ALTER COLUMN ${this.escapeIdentifier(colName)} COMMENT '${comment}'`;
  }
  
  // Column tag operations
  private setColumnTag(op: Operation): string {
    const tableFqn = this.idNameMap[op.payload.tableId] || 'unknown';
    const tableParts = tableFqn.split('.');
    const tableEscaped = this.buildFqn(...tableParts);
    const colName = this.idNameMap[op.target] || 'unknown';
    const tagName = op.payload.tagName;
    const tagValue = this.escapeString(op.payload.tagValue);
    return `ALTER TABLE ${tableEscaped} ALTER COLUMN ${this.escapeIdentifier(colName)} SET TAGS ('${tagName}' = '${tagValue}')`;
  }
  
  private unsetColumnTag(op: Operation): string {
    const tableFqn = this.idNameMap[op.payload.tableId] || 'unknown';
    const tableParts = tableFqn.split('.');
    const tableEscaped = this.buildFqn(...tableParts);
    const colName = this.idNameMap[op.target] || 'unknown';
    const tagName = op.payload.tagName;
    return `ALTER TABLE ${tableEscaped} ALTER COLUMN ${this.escapeIdentifier(colName)} UNSET TAGS ('${tagName}')`;
  }
  
  // Constraint operations
  private addConstraint(op: Operation): string {
    const tableFqn = this.idNameMap[op.payload.tableId] || 'unknown';
    const tableParts = tableFqn.split('.');
    const tableEscaped = this.buildFqn(...tableParts);
    const constraintType = op.payload.type;
    const constraintName = op.payload.name || '';
    const columns = op.payload.columns.map((cid: string) => this.idNameMap[cid] || cid);
    
    const nameClause = constraintName ? `CONSTRAINT ${this.escapeIdentifier(constraintName)} ` : '';
    
    if (constraintType === 'primary_key') {
      const timeseriesClause = op.payload.timeseries ? ' TIMESERIES' : '';
      const colList = columns.map((c: string) => this.escapeIdentifier(c)).join(', ');
      return `ALTER TABLE ${tableEscaped} ADD ${nameClause}PRIMARY KEY(${colList})${timeseriesClause}`;
    } else if (constraintType === 'foreign_key') {
      const parentTable = this.idNameMap[op.payload.parentTable] || 'unknown';
      const parentParts = parentTable.split('.');
      const parentEscaped = this.buildFqn(...parentParts);
      const parentColumns = op.payload.parentColumns.map((cid: string) => this.idNameMap[cid] || cid);
      const colList = columns.map((c: string) => this.escapeIdentifier(c)).join(', ');
      const parentColList = parentColumns.map((c: string) => this.escapeIdentifier(c)).join(', ');
      return `ALTER TABLE ${tableEscaped} ADD ${nameClause}FOREIGN KEY(${colList}) REFERENCES ${parentEscaped}(${parentColList})`;
    } else if (constraintType === 'check') {
      const expression = op.payload.expression || 'TRUE';
      return `ALTER TABLE ${tableEscaped} ADD ${nameClause}CHECK (${expression})`;
    }
    
    return '';
  }
  
  private dropConstraint(op: Operation): string {
    const tableFqn = this.idNameMap[op.payload.tableId] || 'unknown';
    const tableParts = tableFqn.split('.');
    const tableEscaped = this.buildFqn(...tableParts);
    // Would need constraint name from state
    return `-- ALTER TABLE ${tableEscaped} DROP CONSTRAINT (constraint name lookup needed)`;
  }
  
  // Row filter operations
  private addRowFilter(op: Operation): string {
    // Row filters require UDF creation first
    return `-- Row filter: ${op.payload.name} - UDF: ${op.payload.udfExpression}`;
  }
  
  private updateRowFilter(op: Operation): string {
    return '-- Row filter update';
  }
  
  private removeRowFilter(op: Operation): string {
    return '-- Row filter removal';
  }
  
  // Column mask operations
  private addColumnMask(op: Operation): string {
    // Column masks require UDF creation first
    return `-- Column mask: ${op.payload.name} - Function: ${op.payload.maskFunction}`;
  }
  
  private updateColumnMask(op: Operation): string {
    return '-- Column mask update';
  }
  
  private removeColumnMask(op: Operation): string {
    return '-- Column mask removal';
  }
}

