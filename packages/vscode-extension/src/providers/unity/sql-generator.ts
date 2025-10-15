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

export class UnitySQLGenerator extends BaseSQLGenerator {
  private idNameMap: Record<string, string> = {};
  
  constructor(protected state: UnityState) {
    super(state);
    this.idNameMap = this.buildIdNameMap();
  }
  
  /**
   * Build a mapping from IDs to fully-qualified names
   */
  private buildIdNameMap(): Record<string, string> {
    const map: Record<string, string> = {};
    
    for (const catalog of this.state.catalogs) {
      map[catalog.id] = catalog.name;
      
      for (const schema of catalog.schemas) {
        map[schema.id] = `${catalog.name}.${schema.name}`;
        
        for (const table of schema.tables) {
          map[table.id] = `${catalog.name}.${schema.name}.${table.name}`;
          
          for (const column of table.columns) {
            map[column.id] = column.name;
          }
        }
      }
    }
    
    return map;
  }
  
  canGenerateSQL(op: Operation): boolean {
    const supportedOps = Object.values(UNITY_OPERATIONS);
    return supportedOps.includes(op.op as any);
  }

  /**
   * Generate SQL statements with comprehensive batch optimizations.
   * 
   * Optimizations include:
   * - Complete CREATE TABLE statements (no empty tables + ALTERs)
   * - Batched column reordering (minimal ALTER statements)
   * - Table property consolidation
   */
  generateSQL(ops: Operation[]): string {
    // Pre-process: batch operations by table and type
    const tableBatches = this.batchTableOperations(ops);
    const processedOpIds = new Set<string>();
    const statements: string[] = [];

    // Generate SQL for batched table operations
    for (const [tableId, batchInfo] of Object.entries(tableBatches)) {
      const { opIds, operationTypes } = batchInfo;
      
      // Mark these operations as processed
      opIds.forEach(id => processedOpIds.add(id));
      
      // Generate optimized SQL for this table
      const tableSql = this.generateOptimizedTableSQL(tableId, batchInfo);
      
      if (tableSql && !tableSql.startsWith('--')) {
        // Add batch header comment
        const uniqueTypes = [...new Set(operationTypes.map(op => op.replace('unity.', '')))];
        const header = `-- Batch Table Operations: ${opIds.length} operations\n-- Table: ${tableId}\n-- Types: ${uniqueTypes.sort().join(', ')}\n-- Operations: ${opIds.join(', ')}`;
        statements.push(`${header}\n${tableSql};`);
      }
    }

    // Process remaining operations normally (catalogs, schemas, etc.)
    for (const op of ops) {
      if (processedOpIds.has(op.id)) {
        continue; // Skip already processed table operations
      }
      
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
    const name = op.payload.name;
    return `CREATE CATALOG IF NOT EXISTS ${this.escapeIdentifier(name)}`;
  }
  
  private renameCatalog(op: Operation): string {
    const oldName = this.idNameMap[op.target] || op.target;
    const newName = op.payload.newName;
    return `ALTER CATALOG ${this.escapeIdentifier(oldName)} RENAME TO ${this.escapeIdentifier(newName)}`;
  }
  
  private dropCatalog(op: Operation): string {
    const name = this.idNameMap[op.target] || op.target;
    return `DROP CATALOG IF EXISTS ${this.escapeIdentifier(name)}`;
  }
  
  // Schema operations
  private addSchema(op: Operation): string {
    const catalogName = this.idNameMap[op.payload.catalogId] || 'unknown';
    const schemaName = op.payload.name;
    return `CREATE SCHEMA IF NOT EXISTS ${this.escapeIdentifier(catalogName)}.${this.escapeIdentifier(schemaName)}`;
  }
  
  private renameSchema(op: Operation): string {
    const oldFqn = this.idNameMap[op.target] || 'unknown.unknown';
    const parts = oldFqn.split('.');
    const catalogName = parts[0];
    const newName = op.payload.newName;
    return `ALTER SCHEMA ${this.escapeIdentifier(oldFqn)} RENAME TO ${this.escapeIdentifier(catalogName)}.${this.escapeIdentifier(newName)}`;
  }
  
  private dropSchema(op: Operation): string {
    const fqn = this.idNameMap[op.target] || 'unknown.unknown';
    return `DROP SCHEMA IF EXISTS ${this.escapeIdentifier(fqn)}`;
  }
  
  // Table operations
  private addTable(op: Operation): string {
    const schemaFqn = this.idNameMap[op.payload.schemaId] || 'unknown.unknown';
    const tableName = op.payload.name;
    const tableFormat = op.payload.format.toUpperCase();
    const fqn = `${schemaFqn}.${tableName}`;
    
    // Create empty table (columns added via add_column ops)
    return `CREATE TABLE IF NOT EXISTS ${this.escapeIdentifier(fqn)} () USING ${tableFormat}`;
  }
  
  private renameTable(op: Operation): string {
    const oldFqn = this.idNameMap[op.target] || 'unknown.unknown.unknown';
    const parts = oldFqn.split('.');
    const schemaFqn = parts.slice(0, 2).join('.');
    const newName = op.payload.newName;
    return `ALTER TABLE ${this.escapeIdentifier(oldFqn)} RENAME TO ${this.escapeIdentifier(schemaFqn)}.${this.escapeIdentifier(newName)}`;
  }
  
  private dropTable(op: Operation): string {
    const fqn = this.idNameMap[op.target] || 'unknown.unknown.unknown';
    return `DROP TABLE IF EXISTS ${this.escapeIdentifier(fqn)}`;
  }
  
  private setTableComment(op: Operation): string {
    const fqn = this.idNameMap[op.payload.tableId] || 'unknown';
    const comment = this.escapeString(op.payload.comment);
    return `ALTER TABLE ${this.escapeIdentifier(fqn)} SET COMMENT '${comment}'`;
  }
  
  private setTableProperty(op: Operation): string {
    const fqn = this.idNameMap[op.payload.tableId] || 'unknown';
    const key = op.payload.key;
    const value = op.payload.value;
    return `ALTER TABLE ${this.escapeIdentifier(fqn)} SET TBLPROPERTIES ('${key}' = '${value}')`;
  }
  
  private unsetTableProperty(op: Operation): string {
    const fqn = this.idNameMap[op.payload.tableId] || 'unknown';
    const key = op.payload.key;
    return `ALTER TABLE ${this.escapeIdentifier(fqn)} UNSET TBLPROPERTIES ('${key}')`;
  }
  
  // Column operations
  private addColumn(op: Operation): string {
    const tableFqn = this.idNameMap[op.payload.tableId] || 'unknown';
    const colName = op.payload.name;
    const colType = op.payload.type;
    const nullable = op.payload.nullable;
    const comment = op.payload.comment || '';
    
    const nullClause = nullable ? '' : ' NOT NULL';
    const commentClause = comment ? ` COMMENT '${this.escapeString(comment)}'` : '';
    
    return `ALTER TABLE ${this.escapeIdentifier(tableFqn)} ADD COLUMN ${this.escapeIdentifier(colName)} ${colType}${nullClause}${commentClause}`;
  }
  
  private renameColumn(op: Operation): string {
    const tableFqn = this.idNameMap[op.payload.tableId] || 'unknown';
    const oldName = this.idNameMap[op.target] || 'unknown';
    const newName = op.payload.newName;
    return `ALTER TABLE ${this.escapeIdentifier(tableFqn)} RENAME COLUMN ${this.escapeIdentifier(oldName)} TO ${this.escapeIdentifier(newName)}`;
  }
  
  private dropColumn(op: Operation): string {
    const tableFqn = this.idNameMap[op.payload.tableId] || 'unknown';
    const colName = this.idNameMap[op.target] || 'unknown';
    return `ALTER TABLE ${this.escapeIdentifier(tableFqn)} DROP COLUMN ${this.escapeIdentifier(colName)}`;
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
    const tableEsc = this.escapeIdentifier(tableFqn);
    
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
  private batchTableOperations(ops: Operation[]): Record<string, {
    isNewTable: boolean;
    tableOp: Operation | null;
    columnOps: Operation[];
    propertyOps: Operation[];
    reorderOps: Operation[];
    constraintOps: Operation[];
    governanceOps: Operation[];
    otherOps: Operation[];
    opIds: string[];
    operationTypes: string[];
  }> {
    const tableBatches: Record<string, {
      isNewTable: boolean;
      tableOp: Operation | null;
      columnOps: Operation[];
      propertyOps: Operation[];
      reorderOps: Operation[];
      constraintOps: Operation[];
      governanceOps: Operation[];
      otherOps: Operation[];
      opIds: string[];
      operationTypes: string[];
    }> = {};
    
    for (const op of ops) {
      const opType = op.op.replace('unity.', '');
      
      // Identify table-related operations
      let tableId: string | null = null;
      if (['add_table', 'rename_table', 'drop_table', 'set_table_comment'].includes(opType)) {
        tableId = op.target;
      } else if ([
        'add_column', 'rename_column', 'drop_column', 'reorder_columns',
        'change_column_type', 'set_nullable', 'set_column_comment',
        'set_column_tag', 'unset_column_tag', 'set_table_property', 'unset_table_property'
      ].includes(opType)) {
        tableId = op.payload.tableId;
      } else if ([
        'add_constraint', 'drop_constraint', 'add_row_filter', 'update_row_filter',
        'remove_row_filter', 'add_column_mask', 'update_column_mask', 'remove_column_mask'
      ].includes(opType)) {
        tableId = op.payload.tableId;
      }
      
      if (!tableId) {
        continue; // Not a table operation
      }
      
      if (!tableBatches[tableId]) {
        tableBatches[tableId] = {
          isNewTable: false,
          tableOp: null,
          columnOps: [],
          propertyOps: [],
          reorderOps: [],
          constraintOps: [],
          governanceOps: [],
          otherOps: [],
          opIds: [],
          operationTypes: []
        };
      }
      
      const batch = tableBatches[tableId];
      batch.opIds.push(op.id);
      batch.operationTypes.push(op.op);
      
      // Categorize operation
      if (opType === 'add_table') {
        batch.isNewTable = true;
        batch.tableOp = op;
      } else if (opType === 'add_column') {
        batch.columnOps.push(op);
      } else if (['set_table_property', 'unset_table_property'].includes(opType)) {
        batch.propertyOps.push(op);
      } else if (opType === 'reorder_columns') {
        batch.reorderOps.push(op);
      } else if (['add_constraint', 'drop_constraint'].includes(opType)) {
        batch.constraintOps.push(op);
      } else if ([
        'add_row_filter', 'update_row_filter', 'remove_row_filter',
        'add_column_mask', 'update_column_mask', 'remove_column_mask'
      ].includes(opType)) {
        batch.governanceOps.push(op);
      } else {
        batch.otherOps.push(op);
      }
    }
    
    return tableBatches;
  }

  /**
   * Generate optimal SQL for table operations
   */
  private generateOptimizedTableSQL(tableId: string, batchInfo: {
    isNewTable: boolean;
    tableOp: Operation | null;
    columnOps: Operation[];
    propertyOps: Operation[];
    reorderOps: Operation[];
    constraintOps: Operation[];
    governanceOps: Operation[];
    otherOps: Operation[];
    opIds: string[];
    operationTypes: string[];
  }): string {
    if (batchInfo.isNewTable) {
      // Generate complete CREATE TABLE statement
      return this.generateCreateTableWithColumns(tableId, batchInfo);
    } else {
      // Generate optimized ALTER statements for existing table
      return this.generateAlterStatementsForTable(tableId, batchInfo);
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
  }): string {
    const tableOp = batchInfo.tableOp;
    const columnOps = batchInfo.columnOps;
    const propertyOps = batchInfo.propertyOps;
    
    if (!tableOp) {
      return '-- Error: No table creation operation found';
    }
    
    // Get table name and schema info
    const tableName = tableOp.payload.name || 'unknown';
    const schemaId = tableOp.payload.schemaId;
    const schemaFqn = schemaId ? (this.idNameMap[schemaId] || 'unknown.unknown') : 'unknown.unknown';
    const tableFqn = `${schemaFqn}.${tableName}`;
    const tableEsc = this.escapeIdentifier(tableFqn);
    
    // Build column definitions
    let columns: string[] = [];
    for (const colOp of columnOps) {
      const colName = this.escapeIdentifier(colOp.payload.name);
      const colType = colOp.payload.type;
      const nullable = colOp.payload.nullable !== false ? '' : ' NOT NULL';
      const comment = colOp.payload.comment ? ` COMMENT '${this.escapeString(colOp.payload.comment)}'` : '';
      columns.push(`  ${colName} ${colType}${nullable}${comment}`);
    }
    
    // Handle column reordering for new table
    // TEMPORARILY DISABLED: Testing core table batching without reordering
    // TODO: Fix column ID mapping issue in reordering logic
    if (false && batchInfo.reorderOps.length > 0 && columns.length > 0) {
      // Get final column order from last reorder operation
      const finalOrder = batchInfo.reorderOps[batchInfo.reorderOps.length - 1].payload.order;
      const columnMap = new Map<string, string>();
      for (let i = 0; i < columnOps.length; i++) {
        columnMap.set(columnOps[i].target, columns[i]);
      }
      columns = finalOrder.map((colId: string) => columnMap.get(colId)).filter((col: string | undefined) => col !== undefined) as string[];
    }
    
    // Build table format
    const tableFormat = (tableOp.payload.format || 'DELTA').toUpperCase();
    
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
    const tableComment = tableOp.payload.comment ? ` COMMENT '${this.escapeString(tableOp.payload.comment)}'` : '';
    
    // Assemble CREATE TABLE statement
    const columnsSql = columns.length > 0 ? columns.join(',\n') : '';
    const propertiesSql = properties.length > 0 ? `\nTBLPROPERTIES (${properties.join(', ')})` : '';
    
    if (columnsSql) {
      return `CREATE TABLE IF NOT EXISTS ${tableEsc} (
${columnsSql}
) USING ${tableFormat}${tableComment}${propertiesSql}`;
    } else {
      // No columns yet - create empty table (fallback to original behavior)
      return `CREATE TABLE IF NOT EXISTS ${tableEsc} () USING ${tableFormat}${tableComment}${propertiesSql}`;
    }
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
    // TEMPORARILY DISABLED: Testing core table batching without reordering
    // TODO: Fix column ID mapping issue in reordering logic
    if (false && batchInfo.reorderOps.length > 0) {
      const originalOrder = this.getTableColumnOrder(tableId);
      const finalOrder = batchInfo.reorderOps[batchInfo.reorderOps.length - 1].payload.order;
      const reorderSql = this.generateOptimizedReorderSQL(
        tableId, originalOrder, finalOrder, 
        batchInfo.reorderOps.map(op => op.id)
      );
      if (reorderSql && !reorderSql.startsWith('--')) {
        statements.push(reorderSql);
      }
    }
    
    // Handle other operations normally
    const allOtherOps = [
      ...batchInfo.columnOps,
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
      
      // Other operations would be handled here...
      default:
        return `-- Unsupported operation: ${opType}`;
    }
  }
  
  private changeColumnType(op: Operation): string {
    const tableFqn = this.idNameMap[op.payload.tableId] || 'unknown';
    const colName = this.idNameMap[op.target] || 'unknown';
    const newType = op.payload.newType;
    return `ALTER TABLE ${this.escapeIdentifier(tableFqn)} ALTER COLUMN ${this.escapeIdentifier(colName)} TYPE ${newType}`;
  }
  
  private setNullable(op: Operation): string {
    const tableFqn = this.idNameMap[op.payload.tableId] || 'unknown';
    const colName = this.idNameMap[op.target] || 'unknown';
    const nullable = op.payload.nullable;
    
    if (nullable) {
      return `ALTER TABLE ${this.escapeIdentifier(tableFqn)} ALTER COLUMN ${this.escapeIdentifier(colName)} DROP NOT NULL`;
    } else {
      return `ALTER TABLE ${this.escapeIdentifier(tableFqn)} ALTER COLUMN ${this.escapeIdentifier(colName)} SET NOT NULL`;
    }
  }
  
  private setColumnComment(op: Operation): string {
    const tableFqn = this.idNameMap[op.payload.tableId] || 'unknown';
    const colName = this.idNameMap[op.target] || 'unknown';
    const comment = this.escapeString(op.payload.comment);
    return `ALTER TABLE ${this.escapeIdentifier(tableFqn)} ALTER COLUMN ${this.escapeIdentifier(colName)} COMMENT '${comment}'`;
  }
  
  // Column tag operations
  private setColumnTag(op: Operation): string {
    const tableFqn = this.idNameMap[op.payload.tableId] || 'unknown';
    const colName = this.idNameMap[op.target] || 'unknown';
    const tagName = op.payload.tagName;
    const tagValue = this.escapeString(op.payload.tagValue);
    return `ALTER TABLE ${this.escapeIdentifier(tableFqn)} ALTER COLUMN ${this.escapeIdentifier(colName)} SET TAGS ('${tagName}' = '${tagValue}')`;
  }
  
  private unsetColumnTag(op: Operation): string {
    const tableFqn = this.idNameMap[op.payload.tableId] || 'unknown';
    const colName = this.idNameMap[op.target] || 'unknown';
    const tagName = op.payload.tagName;
    return `ALTER TABLE ${this.escapeIdentifier(tableFqn)} ALTER COLUMN ${this.escapeIdentifier(colName)} UNSET TAGS ('${tagName}')`;
  }
  
  // Constraint operations
  private addConstraint(op: Operation): string {
    const tableFqn = this.idNameMap[op.payload.tableId] || 'unknown';
    const constraintType = op.payload.type;
    const constraintName = op.payload.name || '';
    const columns = op.payload.columns.map((cid: string) => this.idNameMap[cid] || cid);
    
    const nameClause = constraintName ? `CONSTRAINT ${this.escapeIdentifier(constraintName)} ` : '';
    
    if (constraintType === 'primary_key') {
      const timeseriesClause = op.payload.timeseries ? ' TIMESERIES' : '';
      const colList = columns.map((c: string) => this.escapeIdentifier(c)).join(', ');
      return `ALTER TABLE ${this.escapeIdentifier(tableFqn)} ADD ${nameClause}PRIMARY KEY(${colList})${timeseriesClause}`;
    } else if (constraintType === 'foreign_key') {
      const parentTable = this.idNameMap[op.payload.parentTable] || 'unknown';
      const parentColumns = op.payload.parentColumns.map((cid: string) => this.idNameMap[cid] || cid);
      const colList = columns.map((c: string) => this.escapeIdentifier(c)).join(', ');
      const parentColList = parentColumns.map((c: string) => this.escapeIdentifier(c)).join(', ');
      return `ALTER TABLE ${this.escapeIdentifier(tableFqn)} ADD ${nameClause}FOREIGN KEY(${colList}) REFERENCES ${this.escapeIdentifier(parentTable)}(${parentColList})`;
    } else if (constraintType === 'check') {
      const expression = op.payload.expression || 'TRUE';
      return `ALTER TABLE ${this.escapeIdentifier(tableFqn)} ADD ${nameClause}CHECK (${expression})`;
    }
    
    return '';
  }
  
  private dropConstraint(op: Operation): string {
    const tableFqn = this.idNameMap[op.payload.tableId] || 'unknown';
    // Would need constraint name from state
    return `-- ALTER TABLE ${this.escapeIdentifier(tableFqn)} DROP CONSTRAINT (constraint name lookup needed)`;
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

