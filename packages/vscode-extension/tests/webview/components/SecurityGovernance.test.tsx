/**
 * Unit tests for SecurityGovernance - Row filters, column masks, and grants
 */

import React from 'react';
import { render, screen } from '@testing-library/react';
import { describe, test, expect, jest, beforeEach } from '@jest/globals';
import { SecurityGovernance } from '../../../src/webview/components/SecurityGovernance';

const mockAddRowFilter = jest.fn();
const mockUpdateRowFilter = jest.fn();
const mockRemoveRowFilter = jest.fn();
const mockAddColumnMask = jest.fn();
const mockUpdateColumnMask = jest.fn();
const mockRemoveColumnMask = jest.fn();
const mockAddGrant = jest.fn();
const mockRevokeGrant = jest.fn();

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
                columns: [
                  { id: 'col_1', name: 'email', type: 'STRING', nullable: true },
                  { id: 'col_2', name: 'region', type: 'STRING', nullable: true },
                ],
                properties: {},
                tags: {},
                constraints: [],
                grants: [{ principal: 'data_engineers', privileges: ['SELECT', 'MODIFY'] }],
                rowFilters: [
                  {
                    id: 'rf_1',
                    name: 'region_filter',
                    enabled: true,
                    udfExpression: 'region = current_user()',
                    description: 'Filter by region',
                  },
                ],
                columnMasks: [
                  {
                    id: 'cm_1',
                    name: 'email_mask',
                    columnId: 'col_1',
                    enabled: true,
                    maskFunction: 'REDACT_EMAIL(email)',
                    description: 'Redact email',
                  },
                ],
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
    addRowFilter: mockAddRowFilter,
    updateRowFilter: mockUpdateRowFilter,
    removeRowFilter: mockRemoveRowFilter,
    addColumnMask: mockAddColumnMask,
    updateColumnMask: mockUpdateColumnMask,
    removeColumnMask: mockRemoveColumnMask,
    addGrant: mockAddGrant,
    revokeGrant: mockRevokeGrant,
  }),
}));

beforeEach(() => {
  jest.clearAllMocks();
});

describe('SecurityGovernance', () => {
  test('renders Security & Governance heading', () => {
    render(<SecurityGovernance tableId="tbl_1" />);
    expect(screen.getByText('Security & Governance')).toBeDefined();
  });

  test('renders Row Filters section with count', () => {
    render(<SecurityGovernance tableId="tbl_1" />);
    expect(screen.getByText('Row Filters (1)')).toBeDefined();
  });

  test('renders Column Masks section with count', () => {
    render(<SecurityGovernance tableId="tbl_1" />);
    expect(screen.getByText('Column Masks (1)')).toBeDefined();
  });

  test('renders Grants section with count', () => {
    render(<SecurityGovernance tableId="tbl_1" />);
    expect(screen.getByText('Grants (1)')).toBeDefined();
  });

  test('renders row filter name and expression', () => {
    render(<SecurityGovernance tableId="tbl_1" />);
    expect(screen.getByText('region_filter')).toBeDefined();
    expect(screen.getByText('region = current_user()')).toBeDefined();
  });

  test('renders row filter status as Enabled', () => {
    render(<SecurityGovernance tableId="tbl_1" />);
    const enabledBadges = screen.getAllByText('Enabled');
    expect(enabledBadges.length).toBeGreaterThan(0);
  });

  test('renders column mask name and function', () => {
    render(<SecurityGovernance tableId="tbl_1" />);
    expect(screen.getByText('email_mask')).toBeDefined();
    expect(screen.getByText('REDACT_EMAIL(email)')).toBeDefined();
  });

  test('renders column mask column name', () => {
    render(<SecurityGovernance tableId="tbl_1" />);
    expect(screen.getByText('email')).toBeDefined();
  });

  test('renders grant principal and privileges', () => {
    render(<SecurityGovernance tableId="tbl_1" />);
    expect(screen.getByText('data_engineers')).toBeDefined();
    expect(screen.getByText('SELECT, MODIFY')).toBeDefined();
  });

  test('has Add Row Filter button', () => {
    render(<SecurityGovernance tableId="tbl_1" />);
    expect(screen.getByText('+ Add Row Filter')).toBeDefined();
  });

  test('has Add Column Mask button', () => {
    render(<SecurityGovernance tableId="tbl_1" />);
    expect(screen.getByText('+ Add Column Mask')).toBeDefined();
  });

  test('has Add Grant button', () => {
    render(<SecurityGovernance tableId="tbl_1" />);
    expect(screen.getByText('+ Add Grant')).toBeDefined();
  });

  test('renders nothing when table not found', () => {
    const { container } = render(<SecurityGovernance tableId="nonexistent" />);
    expect(container.innerHTML).toBe('');
  });
});
