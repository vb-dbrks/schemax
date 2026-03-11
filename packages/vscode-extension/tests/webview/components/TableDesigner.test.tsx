/**
 * Unit tests for TableDesigner - Table header, metadata badges, comment, and child sections
 */

import React from 'react';
import { render, screen } from '@testing-library/react';
import { describe, test, expect, jest, beforeEach } from '@jest/globals';
import { TableDesigner } from '../../../src/webview/components/TableDesigner';

// Mock child components to avoid deep rendering
jest.mock('../../../src/webview/components/ColumnGrid', () => ({
  ColumnGrid: () => React.createElement('div', { 'data-testid': 'column-grid' }, 'ColumnGrid'),
}));
jest.mock('../../../src/webview/components/TableProperties', () => ({
  TableProperties: () => React.createElement('div', { 'data-testid': 'table-properties' }, 'TableProperties'),
}));
jest.mock('../../../src/webview/components/TableTags', () => ({
  TableTags: () => React.createElement('div', { 'data-testid': 'table-tags' }, 'TableTags'),
}));
jest.mock('../../../src/webview/components/TableConstraints', () => ({
  TableConstraints: () => React.createElement('div', { 'data-testid': 'table-constraints' }, 'TableConstraints'),
}));
jest.mock('../../../src/webview/components/SecurityGovernance', () => ({
  SecurityGovernance: () => React.createElement('div', { 'data-testid': 'security-governance' }, 'SecurityGovernance'),
}));
jest.mock('../../../src/webview/components/RichComment', () => ({
  RichComment: ({ text }: { text: string }) => React.createElement('span', null, text),
}));

jest.mock('@vscode/webview-ui-toolkit/react', () => ({
  VSCodeButton: ({ children, ...props }: React.PropsWithChildren<Record<string, unknown>>) =>
    React.createElement('button', props, children),
}));

const mockFindTable = jest.fn();
const mockSetTableComment = jest.fn();
const mockRenameTable = jest.fn();
const mockUseDesignerStore = jest.fn();

jest.mock('../../../src/webview/state/useDesignerStore', () => ({
  useDesignerStore: () => mockUseDesignerStore(),
}));

const defaultTable = {
  catalog: { id: 'cat_1', name: 'my_catalog' },
  schema: { id: 'sch_1', name: 'my_schema' },
  table: {
    id: 'tbl_1',
    name: 'my_table',
    format: 'delta',
    external: false,
    columns: [
      { id: 'col_1', name: 'id', type: 'INT', nullable: false },
      { id: 'col_2', name: 'name', type: 'STRING', nullable: true },
    ],
    properties: {},
    tags: {},
    constraints: [],
    grants: [],
    comment: 'A test table',
  },
};

describe('TableDesigner', () => {
  beforeEach(() => {
    jest.clearAllMocks();
    mockFindTable.mockReturnValue(defaultTable);
    mockUseDesignerStore.mockReturnValue({
      project: { state: { catalogs: [] } },
      selectedTableId: 'tbl_1',
      findTable: mockFindTable,
      setTableComment: mockSetTableComment,
      renameTable: mockRenameTable,
    });
  });

  test('renders table header with full name', () => {
    render(React.createElement(TableDesigner));
    expect(screen.getByRole('heading', { level: 2 })).toHaveTextContent(
      'my_catalog.my_schema.my_table',
    );
  });

  test('renders format badge', () => {
    render(React.createElement(TableDesigner));
    expect(screen.getByText('DELTA')).toBeInTheDocument();
  });

  test('renders Managed badge', () => {
    render(React.createElement(TableDesigner));
    expect(screen.getByText('Managed')).toBeInTheDocument();
  });

  test('shows no table selected when selectedTableId is null', () => {
    mockUseDesignerStore.mockReturnValue({
      project: { state: { catalogs: [] } },
      selectedTableId: null,
      findTable: mockFindTable,
      setTableComment: mockSetTableComment,
      renameTable: mockRenameTable,
    });
    render(React.createElement(TableDesigner));
    expect(screen.getByText('No table selected')).toBeInTheDocument();
  });

  test('shows table not found when findTable returns null', () => {
    mockFindTable.mockReturnValue(null);
    render(React.createElement(TableDesigner));
    expect(screen.getByText('Table not found')).toBeInTheDocument();
  });

  test('renders comment text', () => {
    render(React.createElement(TableDesigner));
    expect(screen.getByText('A test table')).toBeInTheDocument();
  });

  test('shows Comment recommended when no comment', () => {
    mockFindTable.mockReturnValue({
      ...defaultTable,
      table: { ...defaultTable.table, comment: '' },
    });
    render(React.createElement(TableDesigner));
    expect(screen.getByText('Comment recommended')).toBeInTheDocument();
  });

  test('renders Columns section with count', () => {
    render(React.createElement(TableDesigner));
    expect(screen.getByText('Columns (2)')).toBeInTheDocument();
  });

  test('renders child components', () => {
    render(React.createElement(TableDesigner));
    expect(screen.getByTestId('column-grid')).toBeInTheDocument();
    expect(screen.getByTestId('table-properties')).toBeInTheDocument();
    expect(screen.getByTestId('table-tags')).toBeInTheDocument();
    expect(screen.getByTestId('table-constraints')).toBeInTheDocument();
    expect(screen.getByTestId('security-governance')).toBeInTheDocument();
  });

  test('renders External badge for external tables', () => {
    mockFindTable.mockReturnValue({
      ...defaultTable,
      table: { ...defaultTable.table, external: true },
    });
    render(React.createElement(TableDesigner));
    expect(screen.getByText('External')).toBeInTheDocument();
  });

  test('renders partition info when partitioned', () => {
    mockFindTable.mockReturnValue({
      ...defaultTable,
      table: {
        ...defaultTable.table,
        partitionColumns: ['date', 'region'],
      },
    });
    render(React.createElement(TableDesigner));
    expect(screen.getByText(/Partitioned: date, region/)).toBeInTheDocument();
  });
});
