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
  
  private generateSQLForOpType(opType: string, op: Operation): string {
    switch (opType) {
      // Catalog operations
      case 'add_catalog':
        return this.addCatalog(op);
      case 'rename_catalog':
        return this.renameCatalog(op);
      case 'drop_catalog':
        return this.dropCatalog(op);
      
      // Schema operations
      case 'add_schema':
        return this.addSchema(op);
      case 'rename_schema':
        return this.renameSchema(op);
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
      case 'set_table_property':
        return this.setTableProperty(op);
      case 'unset_table_property':
        return this.unsetTableProperty(op);
      
      // Column operations
      case 'add_column':
        return this.addColumn(op);
      case 'rename_column':
        return this.renameColumn(op);
      case 'drop_column':
        return this.dropColumn(op);
      case 'reorder_columns':
        return this.reorderColumns(op);
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
        throw new Error(`Unsupported operation type: ${opType}`);
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
      const colList = columns.map(c => this.escapeIdentifier(c)).join(', ');
      return `ALTER TABLE ${this.escapeIdentifier(tableFqn)} ADD ${nameClause}PRIMARY KEY(${colList})${timeseriesClause}`;
    } else if (constraintType === 'foreign_key') {
      const parentTable = this.idNameMap[op.payload.parentTable] || 'unknown';
      const parentColumns = op.payload.parentColumns.map((cid: string) => this.idNameMap[cid] || cid);
      const colList = columns.map(c => this.escapeIdentifier(c)).join(', ');
      const parentColList = parentColumns.map(c => this.escapeIdentifier(c)).join(', ');
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

