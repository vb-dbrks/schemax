/**
 * SQL Parser utilities used by the designer UI.
 *
 * Kept in webview layer so UI validation helpers are decoupled from provider runtime modules.
 */

export interface ExtractedDependencies {
  tables: string[];
  views: string[];
  catalogs: string[];
  schemas: string[];
}

export function extractDependenciesFromView(viewSql: string): ExtractedDependencies {
  const dependencies: ExtractedDependencies = {
    tables: [],
    views: [],
    catalogs: [],
    schemas: [],
  };

  try {
    const normalizedSql = viewSql.replace(/\n/g, ' ').replace(/\s+/g, ' ').trim();
    const tablePattern = /(?:FROM|JOIN)\s+(?:(\w+)\.)?(?:(\w+)\.)?(\w+)/gi;
    let match: RegExpExecArray | null;

    while ((match = tablePattern.exec(normalizedSql)) !== null) {
      const catalog = match[1];
      const schema = match[2];
      const table = match[3];
      const parts: string[] = [];
      if (catalog) {
        parts.push(catalog);
        if (!dependencies.catalogs.includes(catalog)) {
          dependencies.catalogs.push(catalog);
        }
      }
      if (schema) {
        parts.push(schema);
        if (!dependencies.schemas.includes(schema)) {
          dependencies.schemas.push(schema);
        }
      }
      if (table) {
        parts.push(table);
      }
      const fqn = parts.join('.');
      if (fqn && !dependencies.tables.includes(fqn)) {
        dependencies.tables.push(fqn);
      }
    }

    const ctePattern = /WITH\s+(\w+)\s+AS/gi;
    const cteNames: string[] = [];
    while ((match = ctePattern.exec(normalizedSql)) !== null) {
      const cteName = match[1];
      if (cteName) {
        cteNames.push(cteName.toLowerCase());
      }
    }

    dependencies.tables = dependencies.tables.filter(
      (table) => !cteNames.includes(table.toLowerCase().split('.').pop() || '')
    );
  } catch (error) {
    console.warn('Failed to parse view SQL for dependencies:', error);
  }

  return dependencies;
}

export function isValidSelectStatement(sql: string): boolean {
  const normalized = sql.trim().toLowerCase();
  return normalized.startsWith('select') || normalized.startsWith('with');
}

export function extractTableNames(sql: string): string[] {
  const deps = extractDependenciesFromView(sql);
  return deps.tables;
}
