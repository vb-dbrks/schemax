/**
 * Parse privileges string from grant form input.
 * Privileges are comma-separated (e.g. "READ VOLUME, WRITE VOLUME").
 * Do NOT split on spaces, so "READ VOLUME" stays one privilege.
 */
export function parsePrivileges(privilegesStr: string): string[] {
  if (!privilegesStr || !privilegesStr.trim()) return [];
  return privilegesStr.split(',').map((p) => p.trim()).filter(Boolean);
}
