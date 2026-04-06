/**
 * Unit tests for Sidebar - Tree rendering, badges, empty states, and action buttons
 */

import React from 'react';
import { render, screen, fireEvent } from '@testing-library/react';
import { describe, test, expect, jest, beforeEach } from '@jest/globals';
import { Sidebar } from '../../../src/webview/components/Sidebar';

jest.mock('../../../src/webview/components/RichComment', () => ({
  RichComment: ({ text }: { text: string }) => React.createElement('span', null, text),
}));

jest.mock('@vscode/webview-ui-toolkit/react', () => ({
  VSCodeButton: ({ children, ...props }: React.PropsWithChildren<Record<string, unknown>>) =>
    React.createElement('button', props, children),
  VSCodeDropdown: ({ children, ...props }: React.PropsWithChildren<Record<string, unknown>>) =>
    React.createElement('select', props, children),
  VSCodeOption: ({ children, ...props }: React.PropsWithChildren<Record<string, unknown>>) =>
    React.createElement('option', props, children),
  VSCodeTextField: ({ children, ...props }: React.PropsWithChildren<Record<string, unknown>>) =>
    React.createElement('input', props, children),
}));

jest.mock('../../../src/webview/utils/sqlParser', () => ({
  extractDependenciesFromView: jest.fn(() => []),
}));

jest.mock('../../../src/webview/utils/unityNames', () => ({
  validateUnityCatalogObjectName: jest.fn(() => null),
}));

jest.mock('../../../src/webview/models/unity', () => ({
  getDefaultTargetConfig: jest.fn(() => null),
}));

jest.mock('../../../src/webview/utils/useNameValidation', () => ({
  useNameValidation: jest.fn(() => ({
    validate: jest.fn(() => Promise.resolve({ valid: true, name: '', objectType: '', error: null, suggestion: null, pattern: null, description: null })),
    pending: false,
  })),
}));

jest.mock('../../../src/webview/components/NamingWarningModal', () => ({
  NamingWarningModal: () => null,
}));

const mockUseDesignerStore = jest.fn();
jest.mock('../../../src/webview/state/useDesignerStore', () => ({
  useDesignerStore: (...args: any[]) => mockUseDesignerStore(...args),
}));

const defaultProject = {
  state: {
    catalogs: [
      {
        id: 'cat_1', name: 'my_catalog',
        schemas: [
          {
            id: 'sch_1', name: 'my_schema',
            tables: [{ id: 'tbl_1', name: 'users', format: 'delta', columns: [], properties: {}, tags: {}, constraints: [], grants: [] }],
            views: [{ id: 'view_1', name: 'active_users', definition: 'SELECT...', tags: {}, properties: {}, grants: [] }],
            volumes: [{ id: 'vol_1', name: 'raw_data', volumeType: 'managed' }],
            functions: [{ id: 'fn_1', name: 'add_nums', language: 'SQL', body: 'RETURN 1', returnType: 'INT' }],
            materializedViews: [{ id: 'mv_1', name: 'summary', definition: 'SELECT...' }],
            grants: [],
          },
        ],
        grants: [],
      },
    ],
  },
  managedLocations: {},
};

const defaultStoreReturn = {
  project: defaultProject,
  provider: { id: 'unity', name: 'Unity Catalog' },
  selectedCatalogId: null,
  selectedSchemaId: null,
  selectedTableId: null,
  selectCatalog: jest.fn(),
  selectSchema: jest.fn(),
  selectTable: jest.fn(),
  addCatalog: jest.fn(),
  addSchema: jest.fn(),
  addTable: jest.fn(),
  addView: jest.fn(),
  renameCatalog: jest.fn(),
  renameSchema: jest.fn(),
  renameTable: jest.fn(),
  renameView: jest.fn(),
  dropCatalog: jest.fn(),
  dropSchema: jest.fn(),
  dropTable: jest.fn(),
  dropView: jest.fn(),
  addVolume: jest.fn(),
  renameVolume: jest.fn(),
  dropVolume: jest.fn(),
  addFunction: jest.fn(),
  renameFunction: jest.fn(),
  dropFunction: jest.fn(),
  addMaterializedView: jest.fn(),
  renameMaterializedView: jest.fn(),
  dropMaterializedView: jest.fn(),
};

describe('Sidebar Component', () => {
  beforeEach(() => {
    jest.clearAllMocks();
    mockUseDesignerStore.mockReturnValue(defaultStoreReturn);
  });

  test('renders catalog names in tree', () => {
    render(<Sidebar />);
    expect(screen.getByText('my_catalog')).toBeInTheDocument();
  });

  test('renders schema names when catalog is expanded', () => {
    const { container } = render(<Sidebar />);
    // Expand the catalog by clicking the expander
    const expander = container.querySelector('.expander') as HTMLElement;
    fireEvent.click(expander);
    expect(screen.getByText('my_schema')).toBeInTheDocument();
  });

  test('renders table names with format badges when schema is expanded', () => {
    const { container } = render(<Sidebar />);
    // Expand catalog
    fireEvent.click(container.querySelectorAll('.expander')[0]);
    // Expand schema
    fireEvent.click(container.querySelectorAll('.expander')[1]);
    expect(screen.getByText('users')).toBeInTheDocument();
    expect(screen.getByText('delta')).toBeInTheDocument();
  });

  test('renders view names with VIEW badge when schema is expanded', () => {
    const { container } = render(<Sidebar />);
    fireEvent.click(container.querySelectorAll('.expander')[0]);
    fireEvent.click(container.querySelectorAll('.expander')[1]);
    expect(screen.getByText('active_users')).toBeInTheDocument();
    expect(screen.getByText('VIEW')).toBeInTheDocument();
  });

  test('renders volume names with VOL badge when schema is expanded', () => {
    const { container } = render(<Sidebar />);
    fireEvent.click(container.querySelectorAll('.expander')[0]);
    fireEvent.click(container.querySelectorAll('.expander')[1]);
    expect(screen.getByText('raw_data')).toBeInTheDocument();
    expect(screen.getByText('VOL')).toBeInTheDocument();
  });

  test('renders function names with FN badge when schema is expanded', () => {
    const { container } = render(<Sidebar />);
    fireEvent.click(container.querySelectorAll('.expander')[0]);
    fireEvent.click(container.querySelectorAll('.expander')[1]);
    expect(screen.getByText('add_nums')).toBeInTheDocument();
    expect(screen.getByText('FN')).toBeInTheDocument();
  });

  test('renders materialized view names with MV badge when schema is expanded', () => {
    const { container } = render(<Sidebar />);
    fireEvent.click(container.querySelectorAll('.expander')[0]);
    fireEvent.click(container.querySelectorAll('.expander')[1]);
    expect(screen.getByText('summary')).toBeInTheDocument();
    expect(screen.getByText('MV')).toBeInTheDocument();
  });

  test('shows "Add Catalog" button', () => {
    render(<Sidebar />);
    // The header has a context-aware add button; with no selection it shows "+ Catalog"
    expect(screen.getByTitle('Add Catalog')).toBeInTheDocument();
  });

  test('shows empty state when no catalogs', () => {
    mockUseDesignerStore.mockReturnValue({
      ...defaultStoreReturn,
      project: { ...defaultProject, state: { catalogs: [] }, managedLocations: {} },
    });
    render(<Sidebar />);
    expect(screen.getByText('No catalogs yet')).toBeInTheDocument();
    expect(screen.getByText('Create a catalog to add schemas and tables.')).toBeInTheDocument();
  });

  test('shows loading/empty state when no project', () => {
    mockUseDesignerStore.mockReturnValue({
      ...defaultStoreReturn,
      project: null,
    });
    render(<Sidebar />);
    expect(screen.getByText('Loading...')).toBeInTheDocument();
  });

  test('has action buttons for catalog (add schema, rename, drop)', () => {
    render(<Sidebar />);
    expect(screen.getByLabelText('Add Schema')).toBeInTheDocument();
    expect(screen.getByLabelText('Rename Catalog')).toBeInTheDocument();
    expect(screen.getByLabelText('Drop Catalog')).toBeInTheDocument();
  });

  test('has action buttons for schema when expanded (add table, rename, drop)', () => {
    const { container } = render(<Sidebar />);
    fireEvent.click(container.querySelector('.expander') as HTMLElement);
    expect(screen.getByLabelText('Add Table')).toBeInTheDocument();
    expect(screen.getByLabelText('Rename Schema')).toBeInTheDocument();
    expect(screen.getByLabelText('Drop Schema')).toBeInTheDocument();
  });

  test('has action buttons for tables when expanded (rename, drop)', () => {
    const { container } = render(<Sidebar />);
    fireEvent.click(container.querySelectorAll('.expander')[0]);
    fireEvent.click(container.querySelectorAll('.expander')[1]);
    expect(screen.getByLabelText('Rename table')).toBeInTheDocument();
    expect(screen.getByLabelText('Drop table')).toBeInTheDocument();
  });

  test('has action buttons for views when expanded (rename, drop)', () => {
    const { container } = render(<Sidebar />);
    fireEvent.click(container.querySelectorAll('.expander')[0]);
    fireEvent.click(container.querySelectorAll('.expander')[1]);
    expect(screen.getByLabelText('Rename view')).toBeInTheDocument();
    expect(screen.getByLabelText('Drop view')).toBeInTheDocument();
  });

  test('has action buttons for volumes when expanded (rename, drop)', () => {
    const { container } = render(<Sidebar />);
    fireEvent.click(container.querySelectorAll('.expander')[0]);
    fireEvent.click(container.querySelectorAll('.expander')[1]);
    expect(screen.getByLabelText('Rename volume')).toBeInTheDocument();
    expect(screen.getByLabelText('Drop volume')).toBeInTheDocument();
  });

  test('has action buttons for functions when expanded (rename, drop)', () => {
    const { container } = render(<Sidebar />);
    fireEvent.click(container.querySelectorAll('.expander')[0]);
    fireEvent.click(container.querySelectorAll('.expander')[1]);
    expect(screen.getByLabelText('Rename function')).toBeInTheDocument();
    expect(screen.getByLabelText('Drop function')).toBeInTheDocument();
  });

  test('has action buttons for materialized views when expanded (rename, drop)', () => {
    const { container } = render(<Sidebar />);
    fireEvent.click(container.querySelectorAll('.expander')[0]);
    fireEvent.click(container.querySelectorAll('.expander')[1]);
    expect(screen.getByLabelText('Rename materialized view')).toBeInTheDocument();
    expect(screen.getByLabelText('Drop materialized view')).toBeInTheDocument();
  });

  test('renders provider name in header', () => {
    render(<Sidebar />);
    expect(screen.getByText('Unity Catalog')).toBeInTheDocument();
  });

  test('falls back to Schema Designer when no provider name', () => {
    mockUseDesignerStore.mockReturnValue({
      ...defaultStoreReturn,
      provider: null,
    });
    render(<Sidebar />);
    expect(screen.getByText('Schema Designer')).toBeInTheDocument();
  });

  test('renders tree container', () => {
    const { container } = render(<Sidebar />);
    expect(container.querySelector('.tree')).toBeInTheDocument();
  });

  test('renders catalog expander arrow (collapsed)', () => {
    render(<Sidebar />);
    expect(screen.getByText('▶')).toBeInTheDocument();
  });

  test('renders multiple catalogs', () => {
    const multiCatalogProject = {
      ...defaultProject,
      state: {
        catalogs: [
          { id: 'cat_1', name: 'catalog_alpha', schemas: [], grants: [] },
          { id: 'cat_2', name: 'catalog_beta', schemas: [], grants: [] },
        ],
      },
    };
    mockUseDesignerStore.mockReturnValue({
      ...defaultStoreReturn,
      project: multiCatalogProject,
    });
    render(<Sidebar />);
    expect(screen.getByText('catalog_alpha')).toBeInTheDocument();
    expect(screen.getByText('catalog_beta')).toBeInTheDocument();
  });

  test('shows context-aware add button for table when schema is selected', () => {
    mockUseDesignerStore.mockReturnValue({
      ...defaultStoreReturn,
      selectedSchemaId: 'sch_1',
    });
    render(<Sidebar />);
    expect(screen.getByTitle('Add Table')).toBeInTheDocument();
  });

  test('shows context-aware add button for schema when catalog is selected', () => {
    mockUseDesignerStore.mockReturnValue({
      ...defaultStoreReturn,
      selectedCatalogId: 'cat_1',
      selectedSchemaId: null,
    });
    render(<Sidebar />);
    expect(screen.getByTitle('Add Schema')).toBeInTheDocument();
  });

  test('shows error state when project state is invalid', () => {
    mockUseDesignerStore.mockReturnValue({
      ...defaultStoreReturn,
      project: { state: null, managedLocations: {} },
    });
    render(<Sidebar />);
    expect(screen.getByText('Error: Invalid project state')).toBeInTheDocument();
  });

  test('empty state has Add Catalog button with correct title', () => {
    mockUseDesignerStore.mockReturnValue({
      ...defaultStoreReturn,
      project: { ...defaultProject, state: { catalogs: [] }, managedLocations: {} },
    });
    render(<Sidebar />);
    const addBtns = screen.getAllByTitle('Add Catalog');
    expect(addBtns.length).toBeGreaterThan(0);
  });

  // ─── Interaction Tests ────────────────────────────────────────────────

  describe('Rename dialogs', () => {
    test('clicking Rename Catalog opens rename dialog and submitting calls renameCatalog', () => {
      render(<Sidebar />);
      fireEvent.click(screen.getByLabelText('Rename Catalog'));
      // Dialog should appear with the current name pre-filled
      expect(screen.getByText('Edit catalog')).toBeInTheDocument();
      const input = screen.getByDisplayValue('my_catalog');
      expect(input).toBeInTheDocument();
      // Change the name
      fireEvent.input(input, { target: { value: 'new_catalog' } });
      // Submit the form
      fireEvent.click(screen.getByText('Save'));
      expect(defaultStoreReturn.renameCatalog).toHaveBeenCalledWith('cat_1', 'new_catalog');
    });

    test('clicking Rename Schema opens rename dialog and submitting calls renameSchema', () => {
      const { container } = render(<Sidebar />);
      // Expand catalog to see schema actions
      fireEvent.click(container.querySelector('.expander') as HTMLElement);
      fireEvent.click(screen.getByLabelText('Rename Schema'));
      expect(screen.getByText('Edit schema')).toBeInTheDocument();
      const input = screen.getByDisplayValue('my_schema');
      fireEvent.input(input, { target: { value: 'renamed_schema' } });
      fireEvent.click(screen.getByText('Save'));
      expect(defaultStoreReturn.renameSchema).toHaveBeenCalledWith('sch_1', 'renamed_schema');
    });

    test('clicking Rename table opens rename dialog and submitting calls renameTable', () => {
      const { container } = render(<Sidebar />);
      fireEvent.click(container.querySelectorAll('.expander')[0]);
      fireEvent.click(container.querySelectorAll('.expander')[1]);
      fireEvent.click(screen.getByLabelText('Rename table'));
      expect(screen.getByText('Edit table')).toBeInTheDocument();
      const input = screen.getByDisplayValue('users');
      fireEvent.input(input, { target: { value: 'customers' } });
      fireEvent.click(screen.getByText('Save'));
      expect(defaultStoreReturn.renameTable).toHaveBeenCalledWith('tbl_1', 'customers');
    });

    test('clicking Rename view opens rename dialog and submitting calls renameView', () => {
      const { container } = render(<Sidebar />);
      fireEvent.click(container.querySelectorAll('.expander')[0]);
      fireEvent.click(container.querySelectorAll('.expander')[1]);
      fireEvent.click(screen.getByLabelText('Rename view'));
      expect(screen.getByText('Edit view')).toBeInTheDocument();
      const input = screen.getByDisplayValue('active_users');
      fireEvent.input(input, { target: { value: 'all_users' } });
      fireEvent.click(screen.getByText('Save'));
      expect(defaultStoreReturn.renameView).toHaveBeenCalledWith('view_1', 'all_users');
    });

    test('clicking Rename volume opens rename dialog and submitting calls renameVolume', () => {
      const { container } = render(<Sidebar />);
      fireEvent.click(container.querySelectorAll('.expander')[0]);
      fireEvent.click(container.querySelectorAll('.expander')[1]);
      fireEvent.click(screen.getByLabelText('Rename volume'));
      expect(screen.getByText('Edit volume')).toBeInTheDocument();
      const input = screen.getByDisplayValue('raw_data');
      fireEvent.input(input, { target: { value: 'clean_data' } });
      fireEvent.click(screen.getByText('Save'));
      expect(defaultStoreReturn.renameVolume).toHaveBeenCalledWith('vol_1', 'clean_data');
    });

    test('clicking Rename function opens rename dialog and submitting calls renameFunction', () => {
      const { container } = render(<Sidebar />);
      fireEvent.click(container.querySelectorAll('.expander')[0]);
      fireEvent.click(container.querySelectorAll('.expander')[1]);
      fireEvent.click(screen.getByLabelText('Rename function'));
      expect(screen.getByText('Edit function')).toBeInTheDocument();
      const input = screen.getByDisplayValue('add_nums');
      fireEvent.input(input, { target: { value: 'sum_nums' } });
      fireEvent.click(screen.getByText('Save'));
      expect(defaultStoreReturn.renameFunction).toHaveBeenCalledWith('fn_1', 'sum_nums');
    });

    test('clicking Rename materialized view opens rename dialog and submitting calls renameMaterializedView', () => {
      const { container } = render(<Sidebar />);
      fireEvent.click(container.querySelectorAll('.expander')[0]);
      fireEvent.click(container.querySelectorAll('.expander')[1]);
      fireEvent.click(screen.getByLabelText('Rename materialized view'));
      expect(screen.getByText('Edit materialized_view')).toBeInTheDocument();
      const input = screen.getByDisplayValue('summary');
      fireEvent.input(input, { target: { value: 'daily_summary' } });
      fireEvent.click(screen.getByText('Save'));
      expect(defaultStoreReturn.renameMaterializedView).toHaveBeenCalledWith('mv_1', 'daily_summary');
    });

    test('rename dialog Cancel button closes dialog without calling rename', () => {
      render(<Sidebar />);
      fireEvent.click(screen.getByLabelText('Rename Catalog'));
      expect(screen.getByText('Edit catalog')).toBeInTheDocument();
      fireEvent.click(screen.getByText('Cancel'));
      expect(screen.queryByText('Edit catalog')).not.toBeInTheDocument();
      expect(defaultStoreReturn.renameCatalog).not.toHaveBeenCalled();
    });

    test('rename dialog submitting unchanged name closes dialog without calling rename', () => {
      render(<Sidebar />);
      fireEvent.click(screen.getByLabelText('Rename Catalog'));
      // Submit without changing the value (which is pre-filled with current name)
      fireEvent.click(screen.getByText('Save'));
      expect(defaultStoreReturn.renameCatalog).not.toHaveBeenCalled();
    });
  });

  describe('Drop dialogs', () => {
    test('clicking Drop Catalog opens confirmation and confirming calls dropCatalog', () => {
      render(<Sidebar />);
      fireEvent.click(screen.getByLabelText('Drop Catalog'));
      expect(screen.getByText('Confirm Drop')).toBeInTheDocument();
      expect(screen.getByText(/Are you sure you want to drop catalog "my_catalog"\?/)).toBeInTheDocument();
      // Catalog drop shows cascade warning
      expect(screen.getByText('This will also drop every schema and table underneath this catalog.')).toBeInTheDocument();
      fireEvent.click(screen.getByText('Drop'));
      expect(defaultStoreReturn.dropCatalog).toHaveBeenCalledWith('cat_1');
    });

    test('clicking Drop Schema opens confirmation and confirming calls dropSchema', () => {
      const { container } = render(<Sidebar />);
      fireEvent.click(container.querySelector('.expander') as HTMLElement);
      fireEvent.click(screen.getByLabelText('Drop Schema'));
      expect(screen.getByText('Confirm Drop')).toBeInTheDocument();
      expect(screen.getByText(/Are you sure you want to drop schema "my_schema"\?/)).toBeInTheDocument();
      expect(screen.getByText('This will also drop all tables inside this schema.')).toBeInTheDocument();
      fireEvent.click(screen.getByText('Drop'));
      expect(defaultStoreReturn.dropSchema).toHaveBeenCalledWith('sch_1');
    });

    test('clicking Drop table opens confirmation and confirming calls dropTable', () => {
      const { container } = render(<Sidebar />);
      fireEvent.click(container.querySelectorAll('.expander')[0]);
      fireEvent.click(container.querySelectorAll('.expander')[1]);
      fireEvent.click(screen.getByLabelText('Drop table'));
      expect(screen.getByText('Confirm Drop')).toBeInTheDocument();
      expect(screen.getByText(/Are you sure you want to drop table "users"\?/)).toBeInTheDocument();
      fireEvent.click(screen.getByText('Drop'));
      expect(defaultStoreReturn.dropTable).toHaveBeenCalledWith('tbl_1');
    });

    test('clicking Drop view opens confirmation and confirming calls dropView', () => {
      const { container } = render(<Sidebar />);
      fireEvent.click(container.querySelectorAll('.expander')[0]);
      fireEvent.click(container.querySelectorAll('.expander')[1]);
      fireEvent.click(screen.getByLabelText('Drop view'));
      expect(screen.getByText('Confirm Drop')).toBeInTheDocument();
      expect(screen.getByText(/Are you sure you want to drop view "active_users"\?/)).toBeInTheDocument();
      fireEvent.click(screen.getByText('Drop'));
      expect(defaultStoreReturn.dropView).toHaveBeenCalledWith('view_1');
    });

    test('clicking Drop volume opens confirmation and confirming calls dropVolume', () => {
      const { container } = render(<Sidebar />);
      fireEvent.click(container.querySelectorAll('.expander')[0]);
      fireEvent.click(container.querySelectorAll('.expander')[1]);
      fireEvent.click(screen.getByLabelText('Drop volume'));
      expect(screen.getByText('Confirm Drop')).toBeInTheDocument();
      fireEvent.click(screen.getByText('Drop'));
      expect(defaultStoreReturn.dropVolume).toHaveBeenCalledWith('vol_1');
    });

    test('clicking Drop function opens confirmation and confirming calls dropFunction', () => {
      const { container } = render(<Sidebar />);
      fireEvent.click(container.querySelectorAll('.expander')[0]);
      fireEvent.click(container.querySelectorAll('.expander')[1]);
      fireEvent.click(screen.getByLabelText('Drop function'));
      expect(screen.getByText('Confirm Drop')).toBeInTheDocument();
      fireEvent.click(screen.getByText('Drop'));
      expect(defaultStoreReturn.dropFunction).toHaveBeenCalledWith('fn_1');
    });

    test('clicking Drop materialized view opens confirmation and confirming calls dropMaterializedView', () => {
      const { container } = render(<Sidebar />);
      fireEvent.click(container.querySelectorAll('.expander')[0]);
      fireEvent.click(container.querySelectorAll('.expander')[1]);
      fireEvent.click(screen.getByLabelText('Drop materialized view'));
      expect(screen.getByText('Confirm Drop')).toBeInTheDocument();
      fireEvent.click(screen.getByText('Drop'));
      expect(defaultStoreReturn.dropMaterializedView).toHaveBeenCalledWith('mv_1');
    });

    test('drop dialog Cancel button closes dialog without calling drop', () => {
      render(<Sidebar />);
      fireEvent.click(screen.getByLabelText('Drop Catalog'));
      expect(screen.getByText('Confirm Drop')).toBeInTheDocument();
      fireEvent.click(screen.getByText('Cancel'));
      expect(screen.queryByText('Confirm Drop')).not.toBeInTheDocument();
      expect(defaultStoreReturn.dropCatalog).not.toHaveBeenCalled();
    });
  });

  describe('Add Catalog dialog', () => {
    test('clicking header Add Catalog opens add dialog and submitting calls addCatalog', () => {
      render(<Sidebar />);
      fireEvent.click(screen.getByTitle('Add Catalog'));
      expect(screen.getByText('Add Catalog')).toBeInTheDocument();
      const input = screen.getByPlaceholderText('Enter catalog name');
      fireEvent.input(input, { target: { value: 'new_catalog' } });
      fireEvent.click(screen.getByText('Add'));
      expect(defaultStoreReturn.addCatalog).toHaveBeenCalledWith('new_catalog', undefined);
    });

    test('add catalog dialog Cancel closes without calling addCatalog', () => {
      render(<Sidebar />);
      fireEvent.click(screen.getByTitle('Add Catalog'));
      fireEvent.click(screen.getByText('Cancel'));
      expect(defaultStoreReturn.addCatalog).not.toHaveBeenCalled();
    });

    test('add catalog with duplicate name shows error', () => {
      render(<Sidebar />);
      fireEvent.click(screen.getByTitle('Add Catalog'));
      const input = screen.getByPlaceholderText('Enter catalog name');
      fireEvent.input(input, { target: { value: 'my_catalog' } });
      fireEvent.click(screen.getByText('Add'));
      expect(screen.getByText('Catalog "my_catalog" already exists.')).toBeInTheDocument();
      expect(defaultStoreReturn.addCatalog).not.toHaveBeenCalled();
    });
  });

  describe('Add Schema dialog', () => {
    test('clicking Add Schema on catalog opens add dialog and submitting calls addSchema', () => {
      render(<Sidebar />);
      fireEvent.click(screen.getByLabelText('Add Schema'));
      expect(screen.getByText('Add Schema')).toBeInTheDocument();
      const input = screen.getByPlaceholderText('Enter schema name');
      fireEvent.input(input, { target: { value: 'new_schema' } });
      fireEvent.click(screen.getByText('Add'));
      expect(defaultStoreReturn.addSchema).toHaveBeenCalledWith('cat_1', 'new_schema', undefined);
    });

    test('add schema with duplicate name shows error', () => {
      render(<Sidebar />);
      fireEvent.click(screen.getByLabelText('Add Schema'));
      const input = screen.getByPlaceholderText('Enter schema name');
      fireEvent.input(input, { target: { value: 'my_schema' } });
      fireEvent.click(screen.getByText('Add'));
      expect(screen.getByText('Schema "my_schema" already exists in this catalog.')).toBeInTheDocument();
      expect(defaultStoreReturn.addSchema).not.toHaveBeenCalled();
    });
  });

  describe('Add Table dialog', () => {
    test('clicking Add Table on schema opens add dialog with object type selector and submitting calls addTable', () => {
      const { container } = render(<Sidebar />);
      // Expand catalog to see schema
      fireEvent.click(container.querySelector('.expander') as HTMLElement);
      fireEvent.click(screen.getByLabelText('Add Table'));
      // The dialog should show with object type selector
      expect(screen.getByText('OBJECT TYPE')).toBeInTheDocument();
      // Table radio should be checked by default (the objectType defaults to table)
      const tableRadio = screen.getByDisplayValue('table') as HTMLInputElement;
      expect(tableRadio.checked).toBe(true);
      // Enter table name
      const input = screen.getByPlaceholderText('Enter table name');
      fireEvent.input(input, { target: { value: 'orders' } });
      fireEvent.click(screen.getByText('Add'));
      expect(defaultStoreReturn.addTable).toHaveBeenCalledWith('sch_1', 'orders', 'delta', undefined);
    });

    test('add table with duplicate name shows error', () => {
      const { container } = render(<Sidebar />);
      fireEvent.click(container.querySelector('.expander') as HTMLElement);
      fireEvent.click(screen.getByLabelText('Add Table'));
      const input = screen.getByPlaceholderText('Enter table name');
      fireEvent.input(input, { target: { value: 'users' } });
      fireEvent.click(screen.getByText('Add'));
      expect(screen.getByText('Table or view "users" already exists in this schema.')).toBeInTheDocument();
      expect(defaultStoreReturn.addTable).not.toHaveBeenCalled();
    });
  });

  describe('Add View dialog', () => {
    test('selecting View object type shows SQL textarea and submitting calls addView', () => {
      const { container } = render(<Sidebar />);
      fireEvent.click(container.querySelector('.expander') as HTMLElement);
      fireEvent.click(screen.getByLabelText('Add Table'));
      // Switch to view by clicking the radio
      const viewRadio = screen.getByDisplayValue('view') as HTMLInputElement;
      fireEvent.click(viewRadio);
      // After switching, we need the SQL textarea and view name fields
      expect(screen.getByText('SQL Definition *')).toBeInTheDocument();
      const sqlTextarea = document.getElementById('view-sql') as HTMLTextAreaElement;
      expect(sqlTextarea).toBeInTheDocument();
      // Enter SQL
      fireEvent.input(sqlTextarea, { target: { value: 'SELECT id FROM users' } });
      // Enter view name
      const nameInput = screen.getByPlaceholderText('Enter view name');
      fireEvent.input(nameInput, { target: { value: 'my_view' } });
      fireEvent.click(screen.getByText('Add'));
      expect(defaultStoreReturn.addView).toHaveBeenCalledWith(
        'sch_1',
        'my_view',
        'SELECT id FROM users',
        expect.objectContaining({ extractedDependencies: expect.any(Object) })
      );
    });
  });

  describe('Toggle catalog expand/collapse', () => {
    test('clicking expander twice collapses the catalog', () => {
      const { container } = render(<Sidebar />);
      const expander = container.querySelector('.expander') as HTMLElement;
      // Expand
      fireEvent.click(expander);
      expect(screen.getByText('my_schema')).toBeInTheDocument();
      expect(screen.getByText('▼')).toBeInTheDocument();
      // Collapse
      fireEvent.click(expander);
      expect(screen.queryByText('my_schema')).not.toBeInTheDocument();
      expect(screen.getByText('▶')).toBeInTheDocument();
    });

    test('clicking schema expander twice collapses the schema', () => {
      const { container } = render(<Sidebar />);
      // Expand catalog
      fireEvent.click(container.querySelectorAll('.expander')[0]);
      // Expand schema
      fireEvent.click(container.querySelectorAll('.expander')[1]);
      expect(screen.getByText('users')).toBeInTheDocument();
      // Collapse schema
      fireEvent.click(container.querySelectorAll('.expander')[1]);
      expect(screen.queryByText('users')).not.toBeInTheDocument();
    });
  });

  describe('Selection handling', () => {
    test('clicking on a catalog item calls selectCatalog', () => {
      const { container } = render(<Sidebar />);
      const catalogItem = container.querySelector('.tree-item') as HTMLElement;
      fireEvent.click(catalogItem);
      expect(defaultStoreReturn.selectCatalog).toHaveBeenCalledWith('cat_1');
      expect(defaultStoreReturn.selectSchema).toHaveBeenCalledWith(null);
      expect(defaultStoreReturn.selectTable).toHaveBeenCalledWith(null);
    });

    test('clicking on a schema item calls selectCatalog and selectSchema', () => {
      const { container } = render(<Sidebar />);
      // Expand catalog
      fireEvent.click(container.querySelector('.expander') as HTMLElement);
      // Click on schema item (second .tree-item)
      const schemaItem = container.querySelectorAll('.tree-item')[1] as HTMLElement;
      fireEvent.click(schemaItem);
      expect(defaultStoreReturn.selectCatalog).toHaveBeenCalledWith('cat_1');
      expect(defaultStoreReturn.selectSchema).toHaveBeenCalledWith('sch_1');
      expect(defaultStoreReturn.selectTable).toHaveBeenCalledWith(null);
    });

    test('clicking on a table item calls selectCatalog, selectSchema, and selectTable', () => {
      const { container } = render(<Sidebar />);
      fireEvent.click(container.querySelectorAll('.expander')[0]);
      fireEvent.click(container.querySelectorAll('.expander')[1]);
      // Find the table item (should contain 'users')
      const tableItem = screen.getByText('users').closest('.tree-item') as HTMLElement;
      fireEvent.click(tableItem);
      expect(defaultStoreReturn.selectCatalog).toHaveBeenCalledWith('cat_1');
      expect(defaultStoreReturn.selectSchema).toHaveBeenCalledWith('sch_1');
      expect(defaultStoreReturn.selectTable).toHaveBeenCalledWith('tbl_1');
    });

    test('clicking on a view item calls selectTable with view id', () => {
      const { container } = render(<Sidebar />);
      fireEvent.click(container.querySelectorAll('.expander')[0]);
      fireEvent.click(container.querySelectorAll('.expander')[1]);
      const viewItem = screen.getByText('active_users').closest('.tree-item') as HTMLElement;
      fireEvent.click(viewItem);
      expect(defaultStoreReturn.selectTable).toHaveBeenCalledWith('view_1');
    });

    test('clicking on a volume item calls selectTable with volume id', () => {
      const { container } = render(<Sidebar />);
      fireEvent.click(container.querySelectorAll('.expander')[0]);
      fireEvent.click(container.querySelectorAll('.expander')[1]);
      const volItem = screen.getByText('raw_data').closest('.tree-item') as HTMLElement;
      fireEvent.click(volItem);
      expect(defaultStoreReturn.selectTable).toHaveBeenCalledWith('vol_1');
    });

    test('clicking on a function item calls selectTable with function id', () => {
      const { container } = render(<Sidebar />);
      fireEvent.click(container.querySelectorAll('.expander')[0]);
      fireEvent.click(container.querySelectorAll('.expander')[1]);
      const fnItem = screen.getByText('add_nums').closest('.tree-item') as HTMLElement;
      fireEvent.click(fnItem);
      expect(defaultStoreReturn.selectTable).toHaveBeenCalledWith('fn_1');
    });

    test('clicking on a materialized view item calls selectTable with mv id', () => {
      const { container } = render(<Sidebar />);
      fireEvent.click(container.querySelectorAll('.expander')[0]);
      fireEvent.click(container.querySelectorAll('.expander')[1]);
      const mvItem = screen.getByText('summary').closest('.tree-item') as HTMLElement;
      fireEvent.click(mvItem);
      expect(defaultStoreReturn.selectTable).toHaveBeenCalledWith('mv_1');
    });

    test('selected catalog has "selected" class', () => {
      mockUseDesignerStore.mockReturnValue({
        ...defaultStoreReturn,
        selectedCatalogId: 'cat_1',
      });
      const { container } = render(<Sidebar />);
      const catalogItem = container.querySelector('.tree-item') as HTMLElement;
      expect(catalogItem.classList.contains('selected')).toBe(true);
    });

    test('selected table has "selected" class', () => {
      mockUseDesignerStore.mockReturnValue({
        ...defaultStoreReturn,
        selectedTableId: 'tbl_1',
      });
      const { container } = render(<Sidebar />);
      fireEvent.click(container.querySelectorAll('.expander')[0]);
      fireEvent.click(container.querySelectorAll('.expander')[1]);
      const tableItem = screen.getByText('users').closest('.tree-item') as HTMLElement;
      expect(tableItem.classList.contains('selected')).toBe(true);
    });
  });

  describe('Validation', () => {
    test('rename dialog shows validation error for invalid name', () => {
      const validateMock = jest.fn(() => 'Name must be lowercase') as jest.Mock;
      const unityNames = require('../../../src/webview/utils/unityNames');
      unityNames.validateUnityCatalogObjectName = validateMock;

      render(<Sidebar />);
      fireEvent.click(screen.getByLabelText('Rename Catalog'));
      const input = screen.getByDisplayValue('my_catalog');
      fireEvent.input(input, { target: { value: 'INVALID' } });
      fireEvent.click(screen.getByText('Save'));
      expect(screen.getByText('Name must be lowercase')).toBeInTheDocument();
      expect(defaultStoreReturn.renameCatalog).not.toHaveBeenCalled();

      // Reset mock
      unityNames.validateUnityCatalogObjectName = jest.fn(() => null);
    });

    test('add catalog dialog shows validation error for invalid name', () => {
      const validateMock = jest.fn(() => 'Invalid name') as jest.Mock;
      const unityNames = require('../../../src/webview/utils/unityNames');
      unityNames.validateUnityCatalogObjectName = validateMock;

      render(<Sidebar />);
      fireEvent.click(screen.getByTitle('Add Catalog'));
      const input = screen.getByPlaceholderText('Enter catalog name');
      fireEvent.input(input, { target: { value: 'BAD' } });
      fireEvent.click(screen.getByText('Add'));
      expect(screen.getByText('Invalid name')).toBeInTheDocument();
      expect(defaultStoreReturn.addCatalog).not.toHaveBeenCalled();

      // Reset mock
      unityNames.validateUnityCatalogObjectName = jest.fn(() => null);
    });
  });

  describe('Drop dialog overlay dismiss', () => {
    test('clicking the drop dialog overlay closes it', () => {
      render(<Sidebar />);
      fireEvent.click(screen.getByLabelText('Drop Catalog'));
      expect(screen.getByText('Confirm Drop')).toBeInTheDocument();
      // Click the overlay (the alertdialog element)
      const overlay = screen.getByRole('alertdialog');
      fireEvent.click(overlay);
      expect(screen.queryByText('Confirm Drop')).not.toBeInTheDocument();
    });
  });

  describe('Rename dialog overlay dismiss', () => {
    test('clicking the rename dialog overlay closes it', () => {
      render(<Sidebar />);
      fireEvent.click(screen.getByLabelText('Rename Catalog'));
      expect(screen.getByText('Edit catalog')).toBeInTheDocument();
      // Click the overlay (the dialog element)
      const overlay = screen.getByRole('dialog');
      fireEvent.click(overlay);
      expect(screen.queryByText('Edit catalog')).not.toBeInTheDocument();
    });
  });

  describe('Add dialog overlay dismiss', () => {
    test('clicking the add dialog overlay closes it', () => {
      render(<Sidebar />);
      fireEvent.click(screen.getByTitle('Add Catalog'));
      // The add dialog uses role="dialog"
      const dialogs = screen.getAllByRole('dialog');
      const overlay = dialogs[dialogs.length - 1];
      fireEvent.click(overlay);
      expect(defaultStoreReturn.addCatalog).not.toHaveBeenCalled();
    });
  });

  describe('Context-aware header add button interactions', () => {
    test('header add button opens add schema dialog when catalog is selected', () => {
      mockUseDesignerStore.mockReturnValue({
        ...defaultStoreReturn,
        selectedCatalogId: 'cat_1',
        selectedSchemaId: null,
      });
      render(<Sidebar />);
      fireEvent.click(screen.getByTitle('Add Schema'));
      expect(screen.getByText('Add Schema')).toBeInTheDocument();
    });

    test('header add button opens add table dialog when schema is selected', () => {
      mockUseDesignerStore.mockReturnValue({
        ...defaultStoreReturn,
        selectedSchemaId: 'sch_1',
      });
      render(<Sidebar />);
      fireEvent.click(screen.getByTitle('Add Table'));
      expect(screen.getByText('OBJECT TYPE')).toBeInTheDocument();
    });
  });

  describe('Empty state interactions', () => {
    test('clicking Add Catalog in empty state opens add catalog dialog', () => {
      mockUseDesignerStore.mockReturnValue({
        ...defaultStoreReturn,
        project: { ...defaultProject, state: { catalogs: [] }, managedLocations: {} },
      });
      render(<Sidebar />);
      // There are two Add Catalog buttons in empty state (header + empty state button)
      const addBtns = screen.getAllByTitle('Add Catalog');
      // Click the empty-state one (last one)
      fireEvent.click(addBtns[addBtns.length - 1]);
      expect(screen.getByPlaceholderText('Enter catalog name')).toBeInTheDocument();
    });
  });

  describe('Add Volume dialog', () => {
    test('selecting Volume object type and submitting calls addVolume', () => {
      const { container } = render(<Sidebar />);
      fireEvent.click(container.querySelector('.expander') as HTMLElement);
      fireEvent.click(screen.getByLabelText('Add Table'));
      // Switch to volume by clicking the radio
      const volumeRadio = screen.getByDisplayValue('volume') as HTMLInputElement;
      fireEvent.click(volumeRadio);
      // Enter volume name
      const input = screen.getByPlaceholderText('Enter volume name');
      fireEvent.input(input, { target: { value: 'my_volume' } });
      fireEvent.click(screen.getByText('Add'));
      expect(defaultStoreReturn.addVolume).toHaveBeenCalledWith('sch_1', 'my_volume', 'managed', expect.any(Object));
    });
  });

  describe('Add Function dialog', () => {
    test('selecting Function object type and submitting calls addFunction', () => {
      const { container } = render(<Sidebar />);
      fireEvent.click(container.querySelector('.expander') as HTMLElement);
      fireEvent.click(screen.getByLabelText('Add Table'));
      // Switch to function by clicking the radio
      const fnRadio = screen.getByDisplayValue('function') as HTMLInputElement;
      fireEvent.click(fnRadio);
      // Enter function name
      const input = screen.getByPlaceholderText('Enter function name');
      fireEvent.input(input, { target: { value: 'my_func' } });
      fireEvent.click(screen.getByText('Add'));
      expect(defaultStoreReturn.addFunction).toHaveBeenCalledWith('sch_1', 'my_func', 'SQL', 'NULL', expect.any(Object));
    });
  });

  describe('Add Materialized View dialog', () => {
    test('selecting Materialized View type and submitting calls addMaterializedView', () => {
      const { container } = render(<Sidebar />);
      fireEvent.click(container.querySelector('.expander') as HTMLElement);
      fireEvent.click(screen.getByLabelText('Add Table'));
      // Switch to materialized_view by clicking the radio
      const mvRadio = screen.getByDisplayValue('materialized_view') as HTMLInputElement;
      fireEvent.click(mvRadio);
      // Enter name
      const input = screen.getByPlaceholderText('Enter materialized view name');
      fireEvent.input(input, { target: { value: 'my_mv' } });
      // Enter SQL definition
      const sqlTextarea = document.getElementById('mv-sql') as HTMLTextAreaElement;
      fireEvent.input(sqlTextarea, { target: { value: 'SELECT count(*) FROM users' } });
      fireEvent.click(screen.getByText('Add'));
      expect(defaultStoreReturn.addMaterializedView).toHaveBeenCalledWith(
        'sch_1', 'my_mv', expect.any(String), expect.any(Object)
      );
    });
  });
});
