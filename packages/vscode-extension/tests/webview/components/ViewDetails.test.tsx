/**
 * Unit tests for ViewDetails - View properties, SQL definition, dependencies, grants
 */

import React from 'react';
import { render, screen } from '@testing-library/react';
import { describe, test, expect, jest, beforeEach } from '@jest/globals';
import { ViewDetails } from '../../../src/webview/components/ViewDetails';

const mockUpdateView = jest.fn();
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
            views: [
              {
                id: 'view_1',
                name: 'my_view',
                definition: 'SELECT * FROM my_table',
                comment: 'A test view',
                tags: {},
                properties: {},
                grants: [{ principal: 'analysts', privileges: ['SELECT'] }],
                extractedDependencies: { tables: ['my_table'], views: [], catalogs: [], schemas: [] },
              },
            ],
            tables: [],
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
    updateView: mockUpdateView,
    addGrant: mockAddGrant,
    revokeGrant: mockRevokeGrant,
  }),
}));

jest.mock('../../../src/webview/components/RichComment', () => ({
  RichComment: ({ text }: { text: string }) => React.createElement('span', null, text),
}));

describe('ViewDetails Component', () => {
  beforeEach(() => {
    jest.clearAllMocks();
  });

  test('renders view header with full name', () => {
    render(<ViewDetails viewId="view_1" />);
    expect(screen.getByText('my_catalog.my_schema.my_view')).toBeInTheDocument();
  });

  test('renders VIEW badge', () => {
    render(<ViewDetails viewId="view_1" />);
    expect(screen.getByText('VIEW')).toBeInTheDocument();
  });

  test('shows empty state when view not found', () => {
    render(<ViewDetails viewId="nonexistent" />);
    expect(screen.getByText('View not found')).toBeInTheDocument();
  });

  test('renders SQL definition', () => {
    render(<ViewDetails viewId="view_1" />);
    expect(screen.getByText('SQL Definition')).toBeInTheDocument();
    expect(screen.getByText('SELECT * FROM my_table')).toBeInTheDocument();
  });

  test('renders Dependencies section with table deps', () => {
    render(<ViewDetails viewId="view_1" />);
    expect(screen.getByText('Dependencies')).toBeInTheDocument();
    expect(screen.getByText('my_table')).toBeInTheDocument();
  });

  test('renders Grants section with count', () => {
    render(<ViewDetails viewId="view_1" />);
    expect(screen.getByText(/Grants \(1\)/)).toBeInTheDocument();
    expect(screen.getByText('analysts')).toBeInTheDocument();
    expect(screen.getByText('SELECT')).toBeInTheDocument();
  });

  test('has Add Grant button', () => {
    render(<ViewDetails viewId="view_1" />);
    expect(screen.getByRole('button', { name: /Add Grant/i })).toBeInTheDocument();
  });

  test('shows comment when present', () => {
    render(<ViewDetails viewId="view_1" />);
    expect(screen.getByText('Comment')).toBeInTheDocument();
    expect(screen.getByText('A test view')).toBeInTheDocument();
  });

  test('renders Edit button for SQL', () => {
    render(<ViewDetails viewId="view_1" />);
    const editButtons = screen.getAllByRole('button', { name: /Edit/i });
    expect(editButtons.length).toBeGreaterThanOrEqual(1);
    // The first Edit button is for SQL Definition
    expect(editButtons[0].closest('.section-header')).toBeTruthy();
  });
});
