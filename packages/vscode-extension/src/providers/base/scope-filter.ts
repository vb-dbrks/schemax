/**
 * Provider-agnostic filter for deployment scope (managed categories and existing objects).
 * Used by Generate SQL (and apply via Python) to restrict which operations are emitted per environment.
 */

import type { Operation, OperationMetadata } from './operations';

export interface ScopeFilterEnvConfig {
  managedCategories?: string[];
  existingObjects?: { catalog?: string[] };
}

/**
 * Filter operations by environment's managed categories and existing objects.
 * - If managedCategories is set, only ops whose managedCategory is in that list are kept.
 * - If managedCategories is missing or empty, all ops pass (full scope).
 * - If existingObjects.catalog is set, drop add_catalog ops whose payload name is in that list.
 */
export function filterOpsByManagedScope(
  ops: Operation[],
  envConfig: ScopeFilterEnvConfig,
  getOperationMetadata: (opType: string) => OperationMetadata | undefined
): Operation[] {
  const managedCategories = envConfig.managedCategories;
  const existingObjects = envConfig.existingObjects ?? {};
  const rawCatalogs = existingObjects.catalog ?? [];
  const existingCatalogs = new Set(
    rawCatalogs
      .filter((s) => s != null && String(s).trim() !== '')
      .map((s) => String(s).trim())
  );

  const result: Operation[] = [];
  for (const op of ops) {
    if (existingCatalogs.size > 0 && op.op?.endsWith('add_catalog')) {
      const name = op.payload?.name;
      if (name != null && String(name).trim() !== '' && existingCatalogs.has(String(name).trim())) {
        continue;
      }
    }

    if (!managedCategories || managedCategories.length === 0) {
      result.push(op);
      continue;
    }

    const meta = getOperationMetadata(op.op);
    if (meta == null) {
      result.push(op);
      continue;
    }
    const cat = meta.managedCategory;
    if (cat == null) {
      result.push(op);
      continue;
    }
    const catValue = typeof cat === 'string' ? cat : String(cat);
    if (managedCategories.includes(catValue)) {
      result.push(op);
    }
  }
  return result;
}
