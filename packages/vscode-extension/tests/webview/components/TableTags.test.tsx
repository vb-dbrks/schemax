/**
 * Unit tests for TableTags - Table tag management for Unity Catalog
 */

import React from 'react';
import { render, screen } from '@testing-library/react';
import { describe, test, expect, jest, beforeEach } from '@jest/globals';
import { TableTags } from '../../../src/webview/components/TableTags';

const mockSetTableTag = jest.fn();
const mockUnsetTableTag = jest.fn();

const defaultProject = {
  state: {
    catalogs: [
      {
        id: 'cat_1',
        name: 'my_catalog',
        schemas: [
          {
            id: 'sch_1',
            name: 'my_schema',
            tables: [
              {
                id: 'tbl_1',
                name: 'my_table',
                format: 'delta',
                columns: [],
                properties: {},
                tags: {
                  data_classification: 'confidential',
                  team: 'analytics',
                },
                constraints: [],
                grants: [],
              },
              {
                id: 'tbl_no_tags',
                name: 'empty_table',
                format: 'delta',
                columns: [],
                properties: {},
                tags: {},
                constraints: [],
                grants: [],
              },
            ],
            views: [],
            grants: [],
          },
        ],
        grants: [],
      },
    ],
  },
};

jest.mock('../../../src/webview/state/useDesignerStore', () => ({
  useDesignerStore: () => ({
    project: defaultProject,
    setTableTag: mockSetTableTag,
    unsetTableTag: mockUnsetTableTag,
  }),
}));

jest.mock('../../../src/webview/vscode-api', () => ({
  getVsCodeApi: () => ({ postMessage: jest.fn(), getState: jest.fn(), setState: jest.fn() }),
}));

jest.mock('@vscode/webview-ui-toolkit/react', () => ({
  VSCodeButton: ({ children, ...props }: any) => <button {...props}>{children}</button>,
}));

describe('TableTags', () => {
  beforeEach(() => {
    jest.clearAllMocks();
  });

  test('renders Table Tags heading', () => {
    render(<TableTags tableId="tbl_1" />);
    expect(screen.getByText('Table Tags (Unity Catalog)')).toBeTruthy();
  });

  test('renders hint text about governance', () => {
    render(<TableTags tableId="tbl_1" />);
    expect(
      screen.getByText(/Tags are used for governance, discovery, and attribute-based access control/)
    ).toBeTruthy();
  });

  test('renders tag names and values', () => {
    render(<TableTags tableId="tbl_1" />);
    expect(screen.getByText('data_classification')).toBeTruthy();
    expect(screen.getByText('confidential')).toBeTruthy();
    expect(screen.getByText('team')).toBeTruthy();
    expect(screen.getByText('analytics')).toBeTruthy();
  });

  test('renders table column headers', () => {
    render(<TableTags tableId="tbl_1" />);
    expect(screen.getByText('Tag Name')).toBeTruthy();
    expect(screen.getByText('Value')).toBeTruthy();
    expect(screen.getByText('Actions')).toBeTruthy();
  });

  test('has Add Tag button', () => {
    render(<TableTags tableId="tbl_1" />);
    expect(screen.getByText('+ Add Tag')).toBeTruthy();
  });

  test('renders nothing when table not found', () => {
    const { container } = render(<TableTags tableId="nonexistent" />);
    expect(container.innerHTML).toBe('');
  });

  test('renders empty state when table has no tags', () => {
    render(<TableTags tableId="tbl_no_tags" />);
    expect(screen.getByText('No table tags defined')).toBeTruthy();
  });
});
