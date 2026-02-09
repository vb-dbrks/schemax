/**
 * Unity Catalog object name validation.
 * Valid names: only alphanumeric characters and underscores.
 * Invalid: spaces, periods, forward slashes, control characters.
 * See Databricks: CreateSchema / catalog and table naming rules.
 */
const UNITY_NAME_REGEX = /^[a-zA-Z0-9_]+$/;

export function validateUnityCatalogObjectName(name: string): string | null {
  if (!name || !name.trim()) return 'Name is required.';
  const trimmed = name.trim();
  if (!UNITY_NAME_REGEX.test(trimmed)) {
    return 'Name must contain only letters, numbers, and underscores (no spaces, periods, or slashes).';
  }
  return null;
}
