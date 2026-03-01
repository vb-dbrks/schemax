/**
 * Unity Catalog Hierarchy Configuration
 */

import type { HierarchyLevel } from '../base/hierarchy';
import { Hierarchy } from '../base/hierarchy';

/**
 * Unity Catalog has a 3-level hierarchy:
 * Catalog → Schema → Table (and other objects)
 */
export const unityHierarchyLevels: HierarchyLevel[] = [
  {
    name: 'catalog',
    displayName: 'Catalog',
    pluralName: 'catalogs',
    icon: 'database',
    isContainer: true,
  },
  {
    name: 'schema',
    displayName: 'Schema',
    pluralName: 'schemas',
    icon: 'folder',
    isContainer: true,
  },
  {
    name: 'table',
    displayName: 'Table',
    pluralName: 'tables',
    icon: 'table',
    isContainer: false,
  },
];

export const unityHierarchy = new Hierarchy(unityHierarchyLevels);

