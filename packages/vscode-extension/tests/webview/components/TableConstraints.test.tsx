/**
 * Tests for TableConstraints component - Managing table constraints
 */

import React from 'react';
import { render, screen, fireEvent } from '@testing-library/react';
import { describe, test, expect, jest, beforeEach } from '@jest/globals';
import { TableConstraints } from '../../../src/webview/components/TableConstraints';
import { Table } from '../../../src/webview/models/unity';

// Mock the Zustand store
const mockAddConstraint = jest.fn();
const mockDropConstraint = jest.fn();

jest.mock('../../../src/webview/state/useDesignerStore', () => ({
  useDesignerStore: () => ({
    addConstraint: mockAddConstraint,
    dropConstraint: mockDropConstraint,
  }),
}));

describe('TableConstraints Component', () => {
  const mockTable: Table = {
    id: 'table_001',
    name: 'users',
    schemaId: 'schema_001',
    format: 'delta',
    columns: [
      {
        id: 'col_001',
        name: 'id',
        type: 'BIGINT',
        nullable: false,
      },
      {
        id: 'col_002',
        name: 'email',
        type: 'STRING',
        nullable: false,
      },
    ],
    properties: {},
    constraints: [],
    grants: [],
  };

  beforeEach(() => {
    jest.clearAllMocks();
  });

  test('should render constraints component', () => {
    const { container } = render(<TableConstraints table={mockTable} />);
    expect(container).toBeTruthy();
  });

  test('should render with empty constraints', () => {
    const { container } = render(<TableConstraints table={mockTable} />);
    expect(container).toBeTruthy();
  });

  test('should render with existing constraints', () => {
    const tableWithConstraints: Table = {
      ...mockTable,
      constraints: [
        {
          id: 'constraint_001',
          type: 'primary_key',
          name: 'users_pk',
          columns: ['col_001'],
        },
      ],
    };

    const { container } = render(<TableConstraints table={tableWithConstraints} />);
    expect(container).toBeTruthy();
  });
});

describe('TableConstraints Constraint Types', () => {
  const mockTable: Table = {
    id: 'table_001',
    name: 'users',
    schemaId: 'schema_001',
    format: 'delta',
    columns: [
      { id: 'col_001', name: 'id', type: 'BIGINT', nullable: false },
    ],
    properties: {},
    constraints: [],
    grants: [],
  };

  beforeEach(() => {
    jest.clearAllMocks();
  });

  test('should render PRIMARY KEY constraint', () => {
    const tableWithPK: Table = {
      ...mockTable,
      constraints: [
        {
          id: 'constraint_001',
          type: 'primary_key',
          name: 'users_pk',
          columns: ['col_001'],
        },
      ],
    };

    const { container } = render(<TableConstraints table={tableWithPK} />);
    expect(container).toBeTruthy();
  });

  test('should render FOREIGN KEY constraint', () => {
    const tableWithFK: Table = {
      ...mockTable,
      constraints: [
        {
          id: 'constraint_002',
          type: 'foreign_key',
          name: 'orders_user_fk',
          columns: ['col_001'],
          parentTable: 'users',
          parentColumns: ['id'],
        },
      ],
    };

    const { container } = render(<TableConstraints table={tableWithFK} />);
    expect(container).toBeTruthy();
  });

  test('should render CHECK constraint', () => {
    const tableWithCheck: Table = {
      ...mockTable,
      constraints: [
        {
          id: 'constraint_003',
          type: 'check',
          name: 'age_check',
          columns: [],
          expression: 'age >= 18',
        },
      ],
    };

    const { container } = render(<TableConstraints table={tableWithCheck} />);
    expect(container).toBeTruthy();
  });
});

