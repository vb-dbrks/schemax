/**
 * Unit tests for CatalogDetails - Catalog properties, tags, and grants
 */

import React from 'react';
import { render, screen, fireEvent } from '@testing-library/react';
import { describe, test, expect, jest, beforeEach } from '@jest/globals';
import { CatalogDetails } from '../../../src/webview/components/CatalogDetails';

const mockUpdateCatalog = jest.fn();
const mockRenameCatalog = jest.fn();
const mockAddGrant = jest.fn();
const mockRevokeGrant = jest.fn();

const defaultCatalog = {
  id: 'cat_1',
  name: 'my_catalog',
  comment: 'A test catalog',
  tags: { env: 'dev' },
  schemas: [],
  grants: [{ principal: 'data_engineers', privileges: ['USE CATALOG', 'CREATE SCHEMA'] }],
};

const defaultProject = { managedLocations: {} };

const mockFindCatalog = jest.fn(() => defaultCatalog);

jest.mock('../../../src/webview/state/useDesignerStore', () => ({
  useDesignerStore: () => ({
    project: defaultProject,
    findCatalog: mockFindCatalog,
    updateCatalog: mockUpdateCatalog,
    renameCatalog: mockRenameCatalog,
    addGrant: mockAddGrant,
    revokeGrant: mockRevokeGrant,
  }),
}));

jest.mock('../../../src/webview/vscode-api', () => ({
  getVsCodeApi: () => ({ postMessage: jest.fn(), getState: jest.fn(), setState: jest.fn() }),
}));

jest.mock('../../../src/webview/components/RichComment', () => ({
  RichComment: ({ text }: { text: string }) => React.createElement('span', null, text),
}));

jest.mock('../../../src/webview/components/BulkOperationsPanel', () => ({
  BulkOperationsPanel: () => React.createElement('div', null, 'BulkPanel'),
}));

describe('CatalogDetails Component', () => {
  beforeEach(() => {
    jest.clearAllMocks();
    mockFindCatalog.mockReturnValue(defaultCatalog);
  });

  test('renders catalog header with name', () => {
    render(<CatalogDetails catalogId="cat_1" />);
    expect(screen.getByText('my_catalog')).toBeInTheDocument();
  });

  test('renders CATALOG badge', () => {
    render(<CatalogDetails catalogId="cat_1" />);
    expect(screen.getByText('CATALOG')).toBeInTheDocument();
  });

  test('shows empty state when catalog not found', () => {
    mockFindCatalog.mockReturnValueOnce(null);
    render(<CatalogDetails catalogId="cat_999" />);
    expect(screen.getByText('Catalog not found')).toBeInTheDocument();
  });

  test('renders Properties section with Comment', () => {
    render(<CatalogDetails catalogId="cat_1" />);
    expect(screen.getByText('Comment:')).toBeInTheDocument();
    expect(screen.getByText('A test catalog')).toBeInTheDocument();
  });

  test('shows Comment recommended when catalog has no comment', () => {
    mockFindCatalog.mockReturnValueOnce({ ...defaultCatalog, comment: undefined });
    render(<CatalogDetails catalogId="cat_1" />);
    expect(screen.getByText('Comment recommended')).toBeInTheDocument();
  });

  test('renders tags section header', () => {
    render(<CatalogDetails catalogId="cat_1" />);
    expect(screen.getByText('Catalog Tags (Unity Catalog)')).toBeInTheDocument();
  });

  test('renders Grants section with count', () => {
    render(<CatalogDetails catalogId="cat_1" />);
    expect(screen.getByText('Grants (1)')).toBeInTheDocument();
  });

  test('has Add Grant button', () => {
    render(<CatalogDetails catalogId="cat_1" />);
    expect(screen.getByRole('button', { name: /Add Grant/i })).toBeInTheDocument();
  });

  test('has Add Tag button', () => {
    render(<CatalogDetails catalogId="cat_1" />);
    expect(screen.getByRole('button', { name: /Add Tag/i })).toBeInTheDocument();
  });

  test('has Bulk operations button', () => {
    render(<CatalogDetails catalogId="cat_1" />);
    expect(screen.getByRole('button', { name: /Bulk operations/i })).toBeInTheDocument();
  });

  test('renders grant principal and privileges when grants exist', () => {
    render(<CatalogDetails catalogId="cat_1" />);
    expect(screen.getByText('data_engineers')).toBeInTheDocument();
    expect(screen.getByText('USE CATALOG, CREATE SCHEMA')).toBeInTheDocument();
  });
});
