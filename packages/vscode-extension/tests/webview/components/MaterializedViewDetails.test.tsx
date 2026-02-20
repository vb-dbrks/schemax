/**
 * Unit tests for MaterializedViewDetails - MV properties, SQL definition, grants
 */

import React from 'react';
import { render, screen } from '@testing-library/react';
import { describe, test, expect, jest, beforeEach } from '@jest/globals';
import { MaterializedViewDetails } from '../../../src/webview/components/MaterializedViewDetails';

const mockUpdateMaterializedView = jest.fn();
const mockAddGrant = jest.fn();
const mockRevokeGrant = jest.fn();

const defaultMvInfo = {
  catalog: { id: 'cat_1', name: 'my_catalog' },
  schema: { id: 'sch_1', name: 'my_schema' },
  mv: {
    id: 'mv_1',
    name: 'my_mv',
    definition: 'SELECT id, name FROM t',
    refreshSchedule: 'EVERY 1 DAY',
    comment: 'A materialized view',
    grants: [{ principal: 'analysts', privileges: ['SELECT'] }],
  },
};
const mockFindMaterializedView = jest.fn(() => defaultMvInfo);

jest.mock('../../../src/webview/state/useDesignerStore', () => ({
  useDesignerStore: () => ({
    findMaterializedView: mockFindMaterializedView,
    updateMaterializedView: mockUpdateMaterializedView,
    addGrant: mockAddGrant,
    revokeGrant: mockRevokeGrant,
  }),
}));

describe('MaterializedViewDetails Component', () => {
  beforeEach(() => {
    jest.clearAllMocks();
    mockFindMaterializedView.mockReturnValue(defaultMvInfo);
  });

  test('renders MV header with full name', () => {
    render(<MaterializedViewDetails materializedViewId="mv_1" />);
    expect(screen.getByText('my_catalog.my_schema.my_mv')).toBeInTheDocument();
    expect(screen.getByText('MATERIALIZED VIEW')).toBeInTheDocument();
  });

  test('renders Properties with Refresh schedule and Comment', () => {
    render(<MaterializedViewDetails materializedViewId="mv_1" />);
    expect(screen.getByText('Properties')).toBeInTheDocument();
    expect(screen.getByText('EVERY 1 DAY')).toBeInTheDocument();
    expect(screen.getByText('A materialized view')).toBeInTheDocument();
  });

  test('renders SQL Definition section', () => {
    render(<MaterializedViewDetails materializedViewId="mv_1" />);
    expect(screen.getByText('SQL Definition')).toBeInTheDocument();
    expect(screen.getByText('SELECT id, name FROM t')).toBeInTheDocument();
  });

  test('renders Grants section', () => {
    render(<MaterializedViewDetails materializedViewId="mv_1" />);
    expect(screen.getByText(/Grants \(1\)/)).toBeInTheDocument();
    expect(screen.getByText('analysts')).toBeInTheDocument();
    expect(screen.getByText('SELECT')).toBeInTheDocument();
  });

  test('shows empty state when MV not found', () => {
    mockFindMaterializedView.mockReturnValueOnce(null);
    render(<MaterializedViewDetails materializedViewId="mv_999" />);
    expect(screen.getByText('Materialized view not found')).toBeInTheDocument();
  });

  test('has Add Grant button', () => {
    render(<MaterializedViewDetails materializedViewId="mv_1" />);
    expect(screen.getByRole('button', { name: /Add Grant/i })).toBeInTheDocument();
  });
});
