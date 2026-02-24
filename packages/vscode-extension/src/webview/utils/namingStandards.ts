/**
 * Naming standards validation: test names against regex patterns from catalog/project rules.
 * Run after validateUnityCatalogObjectName (Unity basic rules) so both apply.
 */

import type {
  NamingStandardsRule,
  NamingRuleObjectType,
  NamingRuleTableType,
  UnityCatalog,
  NamingStandardsConfig,
} from '../../providers/unity/models';
import type { ProjectFile } from '../../providers/unity/models';

export interface ValidationResult {
  valid: boolean;
  error?: string;
  suggestion?: string;
}

/** Sanitize name for suggestion: lowercase, replace non-alphanumeric with underscore, collapse underscores */
function suggestSanitized(name: string): string {
  const trimmed = name.trim();
  if (!trimmed) return 'name';
  const replaced = trimmed
    .replace(/[\s.\-]+/g, '_')
    .replace(/[^a-zA-Z0-9_]/g, '_')
    .replace(/_+/g, '_')
    .replace(/^_|_$/g, '');
  const lower = replaced.toLowerCase();
  return lower || 'name';
}

/**
 * Validate a name against a single rule's regex pattern.
 * If rule has no pattern or empty pattern, returns valid (no regex constraint).
 */
export function validateNameAgainstRule(
  name: string,
  rule: { pattern?: string; enabled?: boolean }
): ValidationResult {
  if (rule.enabled === false) return { valid: true };
  const pattern = rule.pattern?.trim();
  if (!pattern) return { valid: true };

  try {
    const re = new RegExp(pattern);
    if (re.test(name)) return { valid: true };
    const suggestion = suggestSanitized(name);
    return {
      valid: false,
      error: 'Name does not match the naming pattern.',
      suggestion: suggestion !== name ? suggestion : undefined,
    };
  } catch {
    return { valid: false, error: 'Invalid regex in naming rule.' };
  }
}

/**
 * Find the first applicable rule for the given object type (and optional table type).
 * Prefers exact tableType match for tables, then falls back to tableType 'any'.
 */
export function getApplicableRule(
  objectType: NamingRuleObjectType,
  rules: NamingStandardsRule[],
  tableType?: NamingRuleTableType
): NamingStandardsRule | null {
  const enabled = rules.filter((r) => r.enabled !== false);
  if (objectType === 'table' && tableType && tableType !== 'any') {
    const exact = enabled.find((r) => r.objectType === 'table' && r.tableType === tableType);
    if (exact) return exact;
  }
  const fallback = enabled.find(
    (r) =>
      r.objectType === objectType &&
      (objectType !== 'table' || !r.tableType || r.tableType === 'any')
  );
  return fallback ?? null;
}

/**
 * Validate catalog name against project-level catalog naming rule.
 * Prefers settings.namingStandards.rules (first catalog rule with pattern); falls back to settings.namingStandards.catalog.
 */
export function validateCatalogName(name: string, project: ProjectFile | null): ValidationResult {
  const ns = project?.settings?.namingStandards;
  const rules = (ns as { rules?: Array<{ objectType: string; pattern?: string; enabled?: boolean }> } | undefined)
    ?.rules;
  const fromRules =
    Array.isArray(rules) &&
    rules.find(
      (r) => r.objectType === 'catalog' && r.enabled !== false && (r.pattern ?? '').trim().length > 0
    );
  if (fromRules) return validateNameAgainstRule(name, fromRules);
  const catalogRule = (ns as { catalog?: { pattern?: string; enabled?: boolean } } | undefined)?.catalog;
  if (!catalogRule || catalogRule.enabled === false) return { valid: true };
  return validateNameAgainstRule(name, catalogRule);
}

/**
 * Validate a name for an object inside a catalog (schema, table, view, column, etc.).
 * Resolves the applicable rule from catalog.namingStandards.rules and runs regex check.
 */
export function validateNameInCatalog(
  name: string,
  objectType: NamingRuleObjectType,
  catalog: UnityCatalog | null | undefined,
  tableType?: NamingRuleTableType
): ValidationResult {
  const rules = catalog?.namingStandards?.rules ?? [];
  const rule = getApplicableRule(objectType, rules, tableType);
  if (!rule) return { valid: true };
  return validateNameAgainstRule(name, rule);
}

/**
 * Resolve the catalog that contains the given schema (by schema id).
 */
export function findCatalogBySchemaId(
  catalogs: UnityCatalog[],
  schemaId: string
): UnityCatalog | null {
  for (const catalog of catalogs) {
    if (catalog.schemas?.some((s: { id: string }) => s.id === schemaId)) return catalog;
  }
  return null;
}

type RenameDialogType =
  | 'catalog'
  | 'schema'
  | 'table'
  | 'view'
  | 'volume'
  | 'function'
  | 'materialized_view';

/**
 * Resolve the catalog that contains the entity being renamed (for applyToRenames validation).
 */
export function findCatalogForRename(
  catalogs: UnityCatalog[],
  type: RenameDialogType,
  id: string
): UnityCatalog | null {
  if (type === 'catalog') return catalogs.find((c) => c.id === id) ?? null;
  if (type === 'schema') return findCatalogBySchemaId(catalogs, id);
  for (const catalog of catalogs) {
    for (const schema of catalog.schemas ?? []) {
      const has =
        (type === 'table' && schema.tables?.some((t: { id: string }) => t.id === id)) ||
        (type === 'view' && schema.views?.some((v: { id: string }) => v.id === id)) ||
        (type === 'volume' && schema.volumes?.some((v: { id: string }) => v.id === id)) ||
        (type === 'function' && schema.functions?.some((f: { id: string }) => f.id === id)) ||
        (type === 'materialized_view' &&
          schema.materializedViews?.some((m: { id: string }) => m.id === id));
      if (has) return catalog;
    }
  }
  return null;
}
