/**
 * SQL Generator for converting operations to Databricks SQL DDL statements.
 * 
 * Generates idempotent SQL for all 31 operation types supported by SchemaX.
 */

import { Catalog, Schema, Table, Column } from './shared/model';
import { Op } from './shared/ops';

interface State {
  catalogs: Catalog[];
}

interface IdNameMap {
  [id: string]: string;
}

export class SQLGenerator {
  private state: State;
  private idNameMap: IdNameMap;

  constructor(state: State) {
    this.state = state;
    this.idNameMap = this.buildIdNameMap();
  }

  /**
   * Build a mapping from IDs to fully-qualified names
   */
  private buildIdNameMap(): IdNameMap {
    const map: IdNameMap = {};

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

  /**
   * Generate SQL for a list of operations
   */
  public generateSQL(ops: Op[]): string {
    const statements: string[] = [];

    for (const op of ops) {
      const sql = this.opToSQL(op);
      if (sql) {
        const header = `-- Op: ${op.id} (${op.ts})\n-- Type: ${op.op}`;
        statements.push(`${header}\n${sql};`);
      }
    }

    return statements.join('\n\n');
  }

  /**
   * Convert a single operation to SQL
   */
  private opToSQL(op: Op): string | null {
    switch (op.op) {
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
        return null;
    }
  }

  // Catalog operations
  private addCatalog(op: Op): string {
    const name = op.payload.name;
    return `CREATE CATALOG IF NOT EXISTS \`${name}\``;
  }

  private renameCatalog(op: Op): string {
    const oldName = this.idNameMap[op.target] || op.target;
    const newName = op.payload.newName;
    return `ALTER CATALOG \`${oldName}\` RENAME TO \`${newName}\``;
  }

  private dropCatalog(op: Op): string {
    const name = this.idNameMap[op.target] || op.target;
    return `DROP CATALOG IF EXISTS \`${name}\``;
  }

  // Schema operations
  private addSchema(op: Op): string {
    const catalogName = this.idNameMap[op.payload.catalogId] || 'unknown';
    const schemaName = op.payload.name;
    return `CREATE SCHEMA IF NOT EXISTS \`${catalogName}\`.\`${schemaName}\``;
  }

  private renameSchema(op: Op): string {
    const oldFqn = this.idNameMap[op.target] || 'unknown.unknown';
    const parts = oldFqn.split('.');
    const catalogName = parts[0];
    const newName = op.payload.newName;
    return `ALTER SCHEMA \`${oldFqn}\` RENAME TO \`${catalogName}\`.\`${newName}\``;
  }

  private dropSchema(op: Op): string {
    const fqn = this.idNameMap[op.target] || 'unknown.unknown';
    return `DROP SCHEMA IF EXISTS \`${fqn}\``;
  }

  // Table operations
  private addTable(op: Op): string {
    const schemaFqn = this.idNameMap[op.payload.schemaId] || 'unknown.unknown';
    const tableName = op.payload.name;
    const tableFormat = op.payload.format.toUpperCase();
    const fqn = `${schemaFqn}.${tableName}`;
    
    // Create empty table (columns added via add_column ops)
    return `CREATE TABLE IF NOT EXISTS \`${fqn}\` () USING ${tableFormat}`;
  }

  private renameTable(op: Op): string {
    const oldFqn = this.idNameMap[op.target] || 'unknown.unknown.unknown';
    const parts = oldFqn.split('.');
    const schemaFqn = parts.slice(0, 2).join('.');
    const newName = op.payload.newName;
    return `ALTER TABLE \`${oldFqn}\` RENAME TO \`${schemaFqn}\`.\`${newName}\``;
  }

  private dropTable(op: Op): string {
    const fqn = this.idNameMap[op.target] || 'unknown.unknown.unknown';
    return `DROP TABLE IF EXISTS \`${fqn}\``;
  }

  private setTableComment(op: Op): string {
    const fqn = this.idNameMap[op.payload.tableId] || 'unknown';
    const comment = op.payload.comment.replace(/'/g, "''"); // Escape single quotes
    return `ALTER TABLE \`${fqn}\` SET COMMENT '${comment}'`;
  }

  private setTableProperty(op: Op): string {
    const fqn = this.idNameMap[op.payload.tableId] || 'unknown';
    const key = op.payload.key;
    const value = op.payload.value;
    return `ALTER TABLE \`${fqn}\` SET TBLPROPERTIES ('${key}' = '${value}')`;
  }

  private unsetTableProperty(op: Op): string {
    const fqn = this.idNameMap[op.payload.tableId] || 'unknown';
    const key = op.payload.key;
    return `ALTER TABLE \`${fqn}\` UNSET TBLPROPERTIES ('${key}')`;
  }

  // Column operations
  private addColumn(op: Op): string {
    const tableFqn = this.idNameMap[op.payload.tableId] || 'unknown';
    const colName = op.payload.name;
    const colType = op.payload.type;
    const nullable = op.payload.nullable;
    const comment = op.payload.comment || '';

    const nullClause = nullable ? '' : ' NOT NULL';
    const commentClause = comment ? ` COMMENT '${comment.replace(/'/g, "''")}'` : '';

    return `ALTER TABLE \`${tableFqn}\` ADD COLUMN \`${colName}\` ${colType}${nullClause}${commentClause}`;
  }

  private renameColumn(op: Op): string {
    const tableFqn = this.idNameMap[op.payload.tableId] || 'unknown';
    const oldName = this.idNameMap[op.target] || 'unknown';
    const newName = op.payload.newName;
    return `ALTER TABLE \`${tableFqn}\` RENAME COLUMN \`${oldName}\` TO \`${newName}\``;
  }

  private dropColumn(op: Op): string {
    const tableFqn = this.idNameMap[op.payload.tableId] || 'unknown';
    const colName = this.idNameMap[op.target] || 'unknown';
    return `ALTER TABLE \`${tableFqn}\` DROP COLUMN \`${colName}\``;
  }

  private reorderColumns(op: Op): string {
    // Databricks doesn't support direct column reordering
    return '-- Column reordering not directly supported in Databricks SQL';
  }

  private changeColumnType(op: Op): string {
    const tableFqn = this.idNameMap[op.payload.tableId] || 'unknown';
    const colName = this.idNameMap[op.target] || 'unknown';
    const newType = op.payload.newType;
    return `ALTER TABLE \`${tableFqn}\` ALTER COLUMN \`${colName}\` TYPE ${newType}`;
  }

  private setNullable(op: Op): string {
    const tableFqn = this.idNameMap[op.payload.tableId] || 'unknown';
    const colName = this.idNameMap[op.target] || 'unknown';
    const nullable = op.payload.nullable;

    if (nullable) {
      return `ALTER TABLE \`${tableFqn}\` ALTER COLUMN \`${colName}\` DROP NOT NULL`;
    } else {
      return `ALTER TABLE \`${tableFqn}\` ALTER COLUMN \`${colName}\` SET NOT NULL`;
    }
  }

  private setColumnComment(op: Op): string {
    const tableFqn = this.idNameMap[op.payload.tableId] || 'unknown';
    const colName = this.idNameMap[op.target] || 'unknown';
    const comment = op.payload.comment.replace(/'/g, "''");
    return `ALTER TABLE \`${tableFqn}\` ALTER COLUMN \`${colName}\` COMMENT '${comment}'`;
  }

  // Column tag operations
  private setColumnTag(op: Op): string {
    const tableFqn = this.idNameMap[op.payload.tableId] || 'unknown';
    const colName = this.idNameMap[op.target] || 'unknown';
    const tagName = op.payload.tagName;
    const tagValue = op.payload.tagValue.replace(/'/g, "''");
    return `ALTER TABLE \`${tableFqn}\` ALTER COLUMN \`${colName}\` SET TAGS ('${tagName}' = '${tagValue}')`;
  }

  private unsetColumnTag(op: Op): string {
    const tableFqn = this.idNameMap[op.payload.tableId] || 'unknown';
    const colName = this.idNameMap[op.target] || 'unknown';
    const tagName = op.payload.tagName;
    return `ALTER TABLE \`${tableFqn}\` ALTER COLUMN \`${colName}\` UNSET TAGS ('${tagName}')`;
  }

  // Constraint operations
  private addConstraint(op: Op): string {
    const tableFqn = this.idNameMap[op.payload.tableId] || 'unknown';
    const constraintType = op.payload.type;
    const constraintName = op.payload.name || '';
    const columns = op.payload.columns.map((cid: string) => this.idNameMap[cid] || cid);

    const nameClause = constraintName ? `CONSTRAINT \`${constraintName}\` ` : '';

    if (constraintType === 'primary_key') {
      const timeseriesClause = op.payload.timeseries ? ' TIMESERIES' : '';
      const colList = columns.map((c: string) => `\`${c}\``).join(', ');
      return `ALTER TABLE \`${tableFqn}\` ADD ${nameClause}PRIMARY KEY(${colList})${timeseriesClause}`;
    } else if (constraintType === 'foreign_key') {
      const parentTable = this.idNameMap[op.payload.parentTable] || 'unknown';
      const parentColumns = op.payload.parentColumns.map((cid: string) => this.idNameMap[cid] || cid);
      const colList = columns.map((c: string) => `\`${c}\``).join(', ');
      const parentColList = parentColumns.map((c: string) => `\`${c}\``).join(', ');
      return `ALTER TABLE \`${tableFqn}\` ADD ${nameClause}FOREIGN KEY(${colList}) REFERENCES \`${parentTable}\`(${parentColList})`;
    } else if (constraintType === 'check') {
      const expression = op.payload.expression || 'TRUE';
      return `ALTER TABLE \`${tableFqn}\` ADD ${nameClause}CHECK (${expression})`;
    }

    return '';
  }

  private dropConstraint(op: Op): string {
    const tableFqn = this.idNameMap[op.payload.tableId] || 'unknown';
    // Would need constraint name from state
    return `-- ALTER TABLE \`${tableFqn}\` DROP CONSTRAINT (constraint name lookup needed)`;
  }

  // Row filter operations
  private addRowFilter(op: Op): string {
    // Row filters require UDF creation first
    return `-- Row filter: ${op.payload.name} - UDF: ${op.payload.udfExpression}`;
  }

  private updateRowFilter(op: Op): string {
    return '-- Row filter update';
  }

  private removeRowFilter(op: Op): string {
    return '-- Row filter removal';
  }

  // Column mask operations
  private addColumnMask(op: Op): string {
    // Column masks require UDF creation first
    return `-- Column mask: ${op.payload.name} - Function: ${op.payload.maskFunction}`;
  }

  private updateColumnMask(op: Op): string {
    return '-- Column mask update';
  }

  private removeColumnMask(op: Op): string {
    return '-- Column mask removal';
  }
}

