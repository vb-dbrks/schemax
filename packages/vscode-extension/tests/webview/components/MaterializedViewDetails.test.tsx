/**
 * Unit tests for MaterializedViewDetails - MV properties, SQL definition, dependencies, grants
 */

import React from 'react';
import { render, screen } from '@testing-library/react';
import { describe, test, expect, jest, beforeEach } from '@jest/globals';

jest.mock('../../../src/webview/components/RichComment', () => ({
  RichComment: ({ text }: { text: string }) => React.createElement('span', null, text),
}));

const mockFindMaterializedView = jest.fn();
const mockUpdateMaterializedView = jest.fn();
const mockAddGrant = jest.fn();
const mockRevokeGrant = jest.fn();

jest.mock('../../../src/webview/state/useDesignerStore', () => ({
  useDesignerStore: () => ({
    findMaterializedView: mockFindMaterializedView,
    updateMaterializedView: mockUpdateMaterializedView,
    addGrant: mockAddGrant,
    revokeGrant: mockRevokeGrant,
  }),
}));

import { MaterializedViewDetails } from '../../../src/webview/components/MaterializedViewDetails';

const defaultMVInfo = {
  catalog: { id: 'cat_1', name: 'my_catalog' },
  schema: { id: 'sch_1', name: 'my_schema' },
  mv: {
    id: 'mv_1',
    name: 'my_mv',
    definition: 'SELECT * FROM my_table',
    comment: 'Materialized summary',
    refreshSchedule: 'EVERY 1 HOUR',
    extractedDependencies: { tables: ['my_table'], views: [] },
    grants: [{ principal: 'analysts', privileges: ['SELECT'] }],
    properties: { 'delta.appendOnly': 'true' },
  },
};

describe('MaterializedViewDetails Component', () => {
  beforeEach(() => {
    jest.clearAllMocks();
    mockFindMaterializedView.mockReturnValue(defaultMVInfo);
  });

  test('renders full name header (catalog.schema.mv_name)', () => {
    render(<MaterializedViewDetails materializedViewId="mv_1" />);
    expect(screen.getByText('my_catalog.my_schema.my_mv')).toBeInTheDocument();
  });

  test('renders MATERIALIZED VIEW badge', () => {
    render(<MaterializedViewDetails materializedViewId="mv_1" />);
    expect(screen.getByText('MATERIALIZED VIEW')).toBeInTheDocument();
  });

  test('shows empty state when MV not found', () => {
    mockFindMaterializedView.mockReturnValueOnce(null);
    render(<MaterializedViewDetails materializedViewId="mv_999" />);
    expect(screen.getByText('Materialized view not found')).toBeInTheDocument();
  });

  test('renders comment text', () => {
    render(<MaterializedViewDetails materializedViewId="mv_1" />);
    expect(screen.getByText('Comment')).toBeInTheDocument();
    expect(screen.getByText('Materialized summary')).toBeInTheDocument();
  });

  test('renders SQL definition', () => {
    render(<MaterializedViewDetails materializedViewId="mv_1" />);
    expect(screen.getByText('SQL Definition')).toBeInTheDocument();
    expect(screen.getByText('SELECT * FROM my_table')).toBeInTheDocument();
  });

  test('renders refresh schedule', () => {
    render(<MaterializedViewDetails materializedViewId="mv_1" />);
    expect(screen.getByText('Refresh schedule')).toBeInTheDocument();
    expect(screen.getByText('EVERY 1 HOUR')).toBeInTheDocument();
  });

  test('renders dependencies', () => {
    render(<MaterializedViewDetails materializedViewId="mv_1" />);
    expect(screen.getByText('Dependencies')).toBeInTheDocument();
    expect(screen.getByText('Dependent Tables')).toBeInTheDocument();
    expect(screen.getByText('my_table')).toBeInTheDocument();
  });

  test('renders grants with principal and privileges', () => {
    render(<MaterializedViewDetails materializedViewId="mv_1" />);
    expect(screen.getByText(/Grants \(1\)/)).toBeInTheDocument();
    expect(screen.getByText('analysts')).toBeInTheDocument();
    expect(screen.getByText('SELECT')).toBeInTheDocument();
  });

  test('has Add Grant button', () => {
    render(<MaterializedViewDetails materializedViewId="mv_1" />);
    expect(screen.getByRole('button', { name: /Add Grant/i })).toBeInTheDocument();
  });

  test('has edit buttons for properties', () => {
    render(<MaterializedViewDetails materializedViewId="mv_1" />);
    const editButtons = screen.getAllByRole('button', { name: /Edit/i });
    expect(editButtons.length).toBeGreaterThanOrEqual(1);
    // Properties section should have an Edit button
    expect(screen.getByText('Properties')).toBeInTheDocument();
    expect(editButtons[0].closest('.section-header')).toBeTruthy();
  });

  test('comment recommended when no comment', () => {
    const noCommentInfo = {
      ...defaultMVInfo,
      mv: {
        ...defaultMVInfo.mv,
        comment: undefined,
      },
    };
    mockFindMaterializedView.mockReturnValueOnce(noCommentInfo);
    render(<MaterializedViewDetails materializedViewId="mv_1" />);
    expect(screen.getByText('Comment')).toBeInTheDocument();
    // When no comment, the component renders "—" as fallback
    expect(screen.getByText('—')).toBeInTheDocument();
  });
});
