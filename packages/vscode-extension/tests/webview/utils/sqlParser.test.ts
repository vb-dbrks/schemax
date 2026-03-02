import { describe, expect, test } from '@jest/globals';

import {
  extractDependenciesFromView,
  extractTableNames,
  isValidSelectStatement,
} from '../../../src/webview/utils/sqlParser';

describe('webview sqlParser', () => {
  test('extracts table dependencies from joins', () => {
    const sql = `
      SELECT o.id, c.name
      FROM bronze.core.orders o
      JOIN bronze.core.customers c ON c.id = o.customer_id
    `;

    const deps = extractDependenciesFromView(sql);

    expect(deps.tables).toEqual(['bronze.core.orders', 'bronze.core.customers']);
    expect(deps.catalogs).toEqual(['bronze']);
    expect(deps.schemas).toEqual(['core']);
  });

  test('filters out CTE references from dependencies', () => {
    const sql = `
      WITH ranked AS (
        SELECT * FROM bronze.core.orders
      )
      SELECT * FROM ranked
    `;

    const deps = extractDependenciesFromView(sql);

    expect(deps.tables).toEqual(['bronze.core.orders']);
  });

  test('validates select-like statements', () => {
    expect(isValidSelectStatement('SELECT * FROM t')).toBe(true);
    expect(isValidSelectStatement('WITH cte AS (SELECT 1) SELECT * FROM cte')).toBe(true);
    expect(isValidSelectStatement('CREATE TABLE t (id INT)')).toBe(false);
  });

  test('extractTableNames delegates to dependency extraction', () => {
    const tables = extractTableNames('SELECT * FROM a.b.table_x');
    expect(tables).toEqual(['a.b.table_x']);
  });
});
