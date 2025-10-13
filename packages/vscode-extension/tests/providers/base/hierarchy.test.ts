import { describe, test, expect } from '@jest/globals';
import { Hierarchy, HierarchyLevel } from '../../../src/providers/base/hierarchy';

describe('Hierarchy', () => {
  const levels: HierarchyLevel[] = [
    { name: 'catalog', displayName: 'Catalog', pluralName: 'catalogs', icon: 'database', isContainer: true },
    { name: 'schema', displayName: 'Schema', pluralName: 'schemas', icon: 'folder', isContainer: true },
    { name: 'table', displayName: 'Table', pluralName: 'tables', icon: 'table', isContainer: false },
  ];

  test('should create hierarchy with correct depth', () => {
    const hierarchy = new Hierarchy(levels);
    expect(hierarchy.getDepth()).toBe(3);
  });

  test('should get correct level by depth', () => {
    const hierarchy = new Hierarchy(levels);
    expect(hierarchy.getLevel(0)).toEqual(levels[0]);
    expect(hierarchy.getLevel(1)).toEqual(levels[1]);
    expect(hierarchy.getLevel(2)).toEqual(levels[2]);
  });

  test('should handle invalid depth gracefully', () => {
    const hierarchy = new Hierarchy(levels);
    // Current implementation returns undefined for invalid depth
    expect(hierarchy.getLevel(3)).toBeUndefined();
    expect(hierarchy.getLevel(-1)).toBeUndefined();
  });

  test('should return all levels', () => {
    const hierarchy = new Hierarchy(levels);
    expect(hierarchy.levels).toEqual(levels);
  });
});

