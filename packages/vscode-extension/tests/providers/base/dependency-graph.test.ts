import { describe, expect, test } from '@jest/globals';
import { DependencyGraph } from '../../../src/providers/base/dependency-graph';
import { Operation } from '../../../src/providers/base/operations';

describe('DependencyGraph', () => {
  test('topologicalSortByLevel returns operations without runtime binding errors', () => {
    const graph = new DependencyGraph();

    const op: Operation = {
      id: 'op_1',
      ts: '2026-02-11T00:00:00.000Z',
      provider: 'unity',
      op: 'unity.add_schema',
      target: 'schema_1',
      payload: {
        schemaId: 'schema_1',
        catalogId: 'catalog_1',
        name: 'analytics',
      },
    };

    graph.addNode({
      id: 'schema_1',
      type: 'schema',
      hierarchyLevel: 1,
      operation: op,
    });

    const sorted = graph.topologicalSortByLevel();
    expect(sorted).toHaveLength(1);
    expect(sorted[0].id).toBe('op_1');
  });
});
