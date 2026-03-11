/**
 * Unit tests for TableProperties - Table property key-value management
 */

import React from 'react';
import { render, screen } from '@testing-library/react';
import { describe, test, expect, jest, beforeEach } from '@jest/globals';
import { TableProperties } from '../../../src/webview/components/TableProperties';

const mockSetTableProperty = jest.fn();
const mockUnsetTableProperty = jest.fn();

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
                properties: {
                  'delta.appendOnly': 'true',
                  'delta.enableChangeDataFeed': 'false',
                },
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
    setTableProperty: mockSetTableProperty,
    unsetTableProperty: mockUnsetTableProperty,
  }),
}));

describe('TableProperties', () => {
  beforeEach(() => {
    jest.clearAllMocks();
  });

  test('renders Table Properties heading', () => {
    render(<TableProperties tableId="tbl_1" />);
    expect(screen.getByText('Table Properties (TBLPROPERTIES)')).toBeTruthy();
  });

  test('renders property keys and values', () => {
    render(<TableProperties tableId="tbl_1" />);
    expect(screen.getAllByText('delta.appendOnly').length).toBeGreaterThanOrEqual(1);
    expect(screen.getAllByText('delta.enableChangeDataFeed').length).toBeGreaterThanOrEqual(1);
    // Values in the properties table
    const rows = screen.getAllByRole('row');
    expect(rows.length).toBeGreaterThanOrEqual(3); // header + 2 properties
  });

  test('has Add Property button', () => {
    render(<TableProperties tableId="tbl_1" />);
    expect(screen.getByText('+ Add Property')).toBeTruthy();
  });

  test('renders Common Delta Lake Properties help section', () => {
    render(<TableProperties tableId="tbl_1" />);
    expect(screen.getByText('Common Delta Lake Properties')).toBeTruthy();
  });

  test('renders nothing when table not found', () => {
    const { container } = render(<TableProperties tableId="nonexistent" />);
    expect(container.innerHTML).toBe('');
  });

  test('has edit buttons for each property', () => {
    render(<TableProperties tableId="tbl_1" />);
    const editButtons = screen.getAllByText(/Edit/);
    expect(editButtons.length).toBe(2);
  });

  test('has delete buttons for each property', () => {
    render(<TableProperties tableId="tbl_1" />);
    const deleteButtons = screen.getAllByText(/Delete/);
    expect(deleteButtons.length).toBe(2);
  });
});
