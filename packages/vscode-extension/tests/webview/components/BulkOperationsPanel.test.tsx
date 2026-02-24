/**
 * Unit tests for BulkOperationsPanel - bulk grants and tags within catalog/schema scope.
 */

import React from 'react';
import { render, screen, fireEvent } from '@testing-library/react';
import { describe, test, expect, jest, beforeEach } from '@jest/globals';
import { BulkOperationsPanel } from '../../../src/webview/components/BulkOperationsPanel';

const mockGetObjectsInScope = jest.fn();
const mockBuildBulkGrantOps = jest.fn();
const mockBuildBulkTableTagOps = jest.fn();
const mockBuildBulkSchemaTagOps = jest.fn();
const mockBuildBulkCatalogTagOps = jest.fn();
const mockApplyBulkOps = jest.fn();

const defaultScopeResult = {
  catalog: { id: 'cat_1', name: 'my_catalog', tags: {} },
  schemas: [{ id: 'sch_1', name: 'my_schema', tags: {} }],
  tables: [{ id: 't1', name: 'table1' }, { id: 't2', name: 'table2' }],
  views: [],
  volumes: [],
  functions: [],
  materializedViews: [],
  grantTargets: [
    { targetType: 'catalog' as const, targetId: 'cat_1' },
    { targetType: 'schema' as const, targetId: 'sch_1' },
    { targetType: 'table' as const, targetId: 't1' },
    { targetType: 'table' as const, targetId: 't2' },
  ],
};

const mockProject = {
  version: 4,
  name: 'test',
  provider: { type: 'unity', version: '1.0.0' },
  state: { catalogs: [] },
  ops: [],
  snapshots: [],
  deployments: [],
  settings: { autoIncrementVersion: true, versionPrefix: 'v' },
  latestSnapshot: null,
};

const defaultStoreReturn = {
  project: mockProject,
  getObjectsInScope: mockGetObjectsInScope,
  buildBulkGrantOps: mockBuildBulkGrantOps,
  buildBulkTableTagOps: mockBuildBulkTableTagOps,
  buildBulkSchemaTagOps: mockBuildBulkSchemaTagOps,
  buildBulkCatalogTagOps: mockBuildBulkCatalogTagOps,
  applyBulkOps: mockApplyBulkOps,
};

const mockUseDesignerStore = jest.fn(() => defaultStoreReturn);

jest.mock('../../../src/webview/state/useDesignerStore', () => ({
  useDesignerStore: () => mockUseDesignerStore(),
}));

describe('BulkOperationsPanel', () => {
  const onClose = jest.fn();

  beforeEach(() => {
    jest.clearAllMocks();
    mockUseDesignerStore.mockReturnValue(defaultStoreReturn);
    mockGetObjectsInScope.mockReturnValue(defaultScopeResult);
  });

  test('renders title and scope preview', () => {
    render(
      <BulkOperationsPanel scope="catalog" catalogId="cat_1" onClose={onClose} />
    );
    expect(screen.getByRole('heading', { name: /Bulk operations/i })).toBeInTheDocument();
    expect(screen.getByText(/1 catalog/)).toBeInTheDocument();
    expect(screen.getByText(/2 table/)).toBeInTheDocument();
  });

  test('renders operation dropdown with Add grant selected by default', () => {
    render(
      <BulkOperationsPanel scope="catalog" catalogId="cat_1" onClose={onClose} />
    );
    const dropdown = screen.getByLabelText(/Operation type/i);
    expect(dropdown).toBeInTheDocument();
    expect(screen.getByLabelText(/Principal/)).toBeInTheDocument();
    expect(screen.getByLabelText(/Privileges/)).toBeInTheDocument();
  });

  test('shows Add catalog tag option when scope is catalog', () => {
    render(
      <BulkOperationsPanel scope="catalog" catalogId="cat_1" onClose={onClose} />
    );
    const options = screen.getAllByRole('option');
    const catalogTagOption = options.find((o) => (o as HTMLOptionElement).value === 'add_catalog_tag');
    expect(catalogTagOption).toBeInTheDocument();
  });

  test('does not show Add catalog tag option when scope is schema', () => {
    mockGetObjectsInScope.mockReturnValue({
      ...defaultScopeResult,
      catalog: undefined,
      schemas: [{ id: 'sch_1', name: 's1', tags: {} }],
      grantTargets: [
        { targetType: 'schema' as const, targetId: 'sch_1' },
        { targetType: 'table' as const, targetId: 't1' },
      ],
    });
    render(
      <BulkOperationsPanel scope="schema" schemaId="sch_1" onClose={onClose} />
    );
    const options = screen.getAllByRole('option');
    const catalogTagOption = options.find((o) => (o as HTMLOptionElement).value === 'add_catalog_tag');
    expect(catalogTagOption).toBeUndefined();
  });

  test('Apply is disabled when principal or privileges empty for add_grant', () => {
    render(
      <BulkOperationsPanel scope="catalog" catalogId="cat_1" onClose={onClose} />
    );
    const applyButton = screen.getByRole('button', { name: /Apply/i });
    expect(applyButton).toBeDisabled();
    const principalInput = document.getElementById('bulk-grant-principal') ?? screen.getByLabelText(/Principal/i);
    const privilegesInput = document.getElementById('bulk-grant-privileges') ?? screen.getByLabelText(/Privileges/i);
    fireEvent.input(principalInput, { target: { value: 'data_engineers' } });
    expect(applyButton).toBeDisabled();
    fireEvent.input(privilegesInput, { target: { value: 'SELECT, MODIFY' } });
    expect(applyButton).not.toBeDisabled();
  });

  test('Apply calls applyBulkOps and onClose when grant form is valid', () => {
    const mockOps = [{ id: 'op_1', ts: '', provider: 'unity', op: 'unity.add_grant', target: 'cat_1', payload: {} }];
    mockBuildBulkGrantOps.mockReturnValue(mockOps);

    render(
      <BulkOperationsPanel scope="catalog" catalogId="cat_1" onClose={onClose} />
    );
    const principalInput = document.getElementById('bulk-grant-principal') ?? screen.getByLabelText(/Principal/i);
    const privilegesInput = document.getElementById('bulk-grant-privileges') ?? screen.getByLabelText(/Privileges/i);
    fireEvent.input(principalInput, { target: { value: 'data_engineers' } });
    fireEvent.input(privilegesInput, { target: { value: 'SELECT, MODIFY' } });
    const applyButton = screen.getByRole('button', { name: /Apply/i });
    fireEvent.click(applyButton);

    expect(mockBuildBulkGrantOps).toHaveBeenCalledWith(
      defaultScopeResult,
      'data_engineers',
      ['SELECT', 'MODIFY']
    );
    expect(mockApplyBulkOps).toHaveBeenCalledWith(mockOps);
    expect(onClose).toHaveBeenCalled();
  });

  test('Cancel calls onClose', () => {
    render(
      <BulkOperationsPanel scope="catalog" catalogId="cat_1" onClose={onClose} />
    );
    const cancelButton = screen.getByRole('button', { name: /Cancel/i });
    fireEvent.click(cancelButton);
    expect(onClose).toHaveBeenCalled();
    expect(mockApplyBulkOps).not.toHaveBeenCalled();
  });

  test('shows operation count message when scope has targets', () => {
    render(
      <BulkOperationsPanel scope="catalog" catalogId="cat_1" onClose={onClose} />
    );
    expect(screen.getByText(/4 operation/)).toBeInTheDocument();
  });

  test('shows empty scope message when grant targets is empty', () => {
    mockGetObjectsInScope.mockReturnValue({
      ...defaultScopeResult,
      grantTargets: [],
      tables: [],
      schemas: [],
      catalog: undefined,
    });
    render(
      <BulkOperationsPanel scope="schema" schemaId="sch_1" onClose={onClose} />
    );
    expect(screen.getByText(/No objects in scope for this operation/)).toBeInTheDocument();
    const applyButton = screen.getByRole('button', { name: /Apply/i });
    expect(applyButton).toBeDisabled();
  });

  test('returns null when project is null', () => {
    mockUseDesignerStore.mockReturnValueOnce({
      ...defaultStoreReturn,
      project: null,
    });
    const { container } = render(
      <BulkOperationsPanel scope="catalog" catalogId="cat_1" onClose={onClose} />
    );
    expect(container.firstChild).toBeNull();
  });
});
