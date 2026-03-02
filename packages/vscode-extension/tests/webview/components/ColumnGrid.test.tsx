/**
 * Tests for ColumnGrid component - Inline column editing
 */

import React from 'react';
import { render, screen, fireEvent } from '@testing-library/react';
import { describe, test, expect, jest, beforeEach } from '@jest/globals';
import { ColumnGrid } from '../../../src/webview/components/ColumnGrid';
import { Column } from '../../../src/webview/models/unity';

// Mock the Zustand store
const mockAddColumn = jest.fn();
const mockRenameColumn = jest.fn();
const mockDropColumn = jest.fn();
const mockChangeColumnType = jest.fn();
const mockSetColumnNullable = jest.fn();
const mockSetColumnComment = jest.fn();
const mockReorderColumns = jest.fn();
const mockSetColumnTag = jest.fn();
const mockUnsetColumnTag = jest.fn();

jest.mock('../../../src/webview/state/useDesignerStore', () => ({
  useDesignerStore: () => ({
    addColumn: mockAddColumn,
    renameColumn: mockRenameColumn,
    dropColumn: mockDropColumn,
    changeColumnType: mockChangeColumnType,
    setColumnNullable: mockSetColumnNullable,
    setColumnComment: mockSetColumnComment,
    reorderColumns: mockReorderColumns,
    setColumnTag: mockSetColumnTag,
    unsetColumnTag: mockUnsetColumnTag,
  }),
}));

describe('ColumnGrid Component', () => {
  const mockColumns: Column[] = [
    {
      id: 'col_001',
      name: 'id',
      type: 'BIGINT',
      nullable: false,
      comment: 'Primary key',
    },
    {
      id: 'col_002',
      name: 'name',
      type: 'STRING',
      nullable: true,
      comment: 'User name',
    },
  ];

  beforeEach(() => {
    jest.clearAllMocks();
  });

  test('should render column grid with columns', () => {
    render(<ColumnGrid tableId="table_001" columns={mockColumns} />);

    expect(screen.getByText('id')).toBeInTheDocument();
    expect(screen.getByText('name')).toBeInTheDocument();
    expect(screen.getByText('BIGINT')).toBeInTheDocument();
    expect(screen.getByText('STRING')).toBeInTheDocument();
  });

  test('should display column types correctly', () => {
    render(<ColumnGrid tableId="table_001" columns={mockColumns} />);

    expect(screen.getByText('BIGINT')).toBeInTheDocument();
    expect(screen.getByText('STRING')).toBeInTheDocument();
  });

  test('should display nullable status', () => {
    render(<ColumnGrid tableId="table_001" columns={mockColumns} />);

    // Check for NOT NULL badge
    const notNullBadges = screen.getAllByText(/NOT NULL|NULL/i);
    expect(notNullBadges.length).toBeGreaterThan(0);
  });

  test('should display column comments', () => {
    render(<ColumnGrid tableId="table_001" columns={mockColumns} />);

    expect(screen.getByText('Primary key')).toBeInTheDocument();
    expect(screen.getByText('User name')).toBeInTheDocument();
  });

  test('should render with empty columns array', () => {
    const { container } = render(<ColumnGrid tableId="table_001" columns={[]} />);
    // Component should render without crashing
    expect(container).toBeTruthy();
  });

  test('should handle column with tags', () => {
    const columnsWithTags: Column[] = [
      {
        id: 'col_001',
        name: 'email',
        type: 'STRING',
        nullable: false,
        tags: {
          pii: 'true',
          classification: 'sensitive',
        },
      },
    ];

    render(<ColumnGrid tableId="table_001" columns={columnsWithTags} />);

    expect(screen.getByText('email')).toBeInTheDocument();
    // Tags might be rendered as badges
    expect(screen.getByText(/pii/i)).toBeInTheDocument();
  });
});

describe('ColumnGrid Rendering', () => {
  const mockColumns: Column[] = [
    {
      id: 'col_001',
      name: 'id',
      type: 'BIGINT',
      nullable: false,
    },
  ];

  beforeEach(() => {
    jest.clearAllMocks();
  });

  test('should render without crashing', () => {
    const { container } = render(<ColumnGrid tableId="table_001" columns={mockColumns} />);
    expect(container).toBeTruthy();
  });

  test('should handle multiple columns', () => {
    const multipleColumns: Column[] = [
      { id: 'col_001', name: 'id', type: 'BIGINT', nullable: false },
      { id: 'col_002', name: 'name', type: 'STRING', nullable: true },
      { id: 'col_003', name: 'email', type: 'STRING', nullable: false },
    ];
    
    const { container } = render(<ColumnGrid tableId="table_001" columns={multipleColumns} />);
    expect(container).toBeTruthy();
  });
});

