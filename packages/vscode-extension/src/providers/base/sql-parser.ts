/**
 * SQL Parser Utilities
 *
 * Simple regex-based SQL parser for extracting table/view references from SQL queries.
 * This is a lightweight alternative to full SQL parsing (like SQLGlot in Python).
 *
 * Supports:
 * - Basic SELECT statements
 * - JOINs (INNER, LEFT, RIGHT, FULL, CROSS)
 * - CTEs (WITH clauses)
 * - Subqueries
 * - Fully qualified names (catalog.schema.table)
 */

export interface ExtractedDependencies {
  tables: string[];
  views: string[];
  catalogs: string[];
  schemas: string[];
}

/**
 * Extract dependencies from a SQL view definition
 *
 * @param viewSql - The SQL SELECT statement defining the view
 * @returns Object containing lists of referenced objects
 */
export function extractDependenciesFromView(viewSql: string): ExtractedDependencies {
  const dependencies: ExtractedDependencies = {
    tables: [],
    views: [],
    catalogs: [],
    schemas: [],
  };

  try {
    // Normalize SQL: remove extra whitespace, convert to lowercase for pattern matching
    const normalizedSql = viewSql
      .replace(/\n/g, ' ')
      .replace(/\s+/g, ' ')
      .trim();

    // Extract table references from FROM and JOIN clauses
    // Pattern matches: FROM/JOIN [catalog.][ schema.]table
    const tablePattern = /(?:FROM|JOIN)\s+(?:(\w+)\.)?(?:(\w+)\.)?(\w+)/gi;
    let match;

    while ((match = tablePattern.exec(normalizedSql)) !== null) {
      const [_, catalog, schema, table] = match;

      // Build fully qualified name
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
        // For now, treat all table expressions as 'tables'
        // Further logic could distinguish tables from views based on state
        dependencies.tables.push(fqn);
      }
    }

    // Extract CTE references (WITH clause)
    // Pattern: WITH cte_name AS (...) SELECT ... FROM cte_name
    const ctePattern = /WITH\s+(\w+)\s+AS/gi;
    const cteNames: string[] = [];
    while ((match = ctePattern.exec(normalizedSql)) !== null) {
      const cteName = match[1];
      if (cteName) {
        cteNames.push(cteName.toLowerCase());
      }
    }

    // Filter out CTE references from table dependencies
    // (CTEs are temporary and not actual table dependencies)
    dependencies.tables = dependencies.tables.filter(
      table => !cteNames.includes(table.toLowerCase().split('.').pop() || '')
    );

  } catch (error) {
    console.warn('Failed to parse view SQL for dependencies:', error);
  }

  return dependencies;
}

/**
 * Validate if a SQL string is a valid SELECT statement
 */
export function isValidSelectStatement(sql: string): boolean {
  const normalized = sql.trim().toLowerCase();
  return normalized.startsWith('select') || normalized.startsWith('with');
}

/**
 * Extract all table names from SQL (simplified)
 * @param sql - SQL query
 * @returns Array of table names (may include catalog.schema prefixes)
 */
export function extractTableNames(sql: string): string[] {
  const deps = extractDependenciesFromView(sql);
  return deps.tables;
}

