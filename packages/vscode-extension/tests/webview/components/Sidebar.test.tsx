/**
 * Tests for Sidebar component - Tree navigation and add-dialog interactions.
 *
 * Coverage:
 *  - Tree rendering (empty state, catalogs, schemas, tables)
 *  - Selection (catalog / schema / table click)
 *  - Tree expansion / collapse
 *  - Context-aware header add button
 *  - Rename dialog (catalog, schema)
 *  - Drop dialog (catalog warning, schema warning, confirm, cancel)
 *  - Add Catalog dialog (tags flush, partial tag ignored)
 *  - Add Schema dialog (tags, comment, duplicate-name validation, cancel)
 *  - Add Table dialog (default delta format, iceberg switch)
 *  - Duplicate-name validation for catalog and schema
 */

import React from 'react';
import { render, screen, fireEvent } from '@testing-library/react';
import { describe, test, expect, jest, beforeEach } from '@jest/globals';
import { Sidebar } from '../../../src/webview/components/Sidebar';

// ---------------------------------------------------------------------------
// Shared mock helpers
// ---------------------------------------------------------------------------

const mockAddCatalog = jest.fn();
const mockAddSchema = jest.fn();
const mockAddTable = jest.fn();
const mockSelectCatalog = jest.fn();
const mockSelectSchema = jest.fn();
const mockSelectTable = jest.fn();

const minimalProject = (extraCatalogs: any[] = []) => ({
  state: { catalogs: extraCatalogs },
  provider: { type: 'unity', version: '1.0.0' },
  managedLocations: {},
  externalLocations: {},
  environments: {},
  settings: {},
});

const minimalProvider = {
  id: 'unity',
  name: 'Unity Catalog',
  version: '1.0.0',
  capabilities: {
    hierarchy: {
      levels: [
        { displayName: 'Catalog' },
        { displayName: 'Schema' },
        { displayName: 'Table' },
      ],
    },
  },
};

jest.mock('../../../src/webview/state/useDesignerStore', () => ({
  useDesignerStore: jest.fn(),
}));

// Pull the mocked factory so individual tests can override the return value.
import { useDesignerStore } from '../../../src/webview/state/useDesignerStore';
const mockUseDesignerStore = useDesignerStore as jest.MockedFunction<typeof useDesignerStore>;

const mockRenameCatalog = jest.fn();
const mockRenameSchema = jest.fn();
const mockRenameTable = jest.fn();
const mockDropCatalog = jest.fn();
const mockDropSchema = jest.fn();
const mockDropTable = jest.fn();

function makeStoreMock(overrides: Record<string, any> = {}) {
  return {
    project: minimalProject(),
    provider: minimalProvider,
    selectedCatalogId: null,
    selectedSchemaId: null,
    selectedTableId: null,
    selectCatalog: mockSelectCatalog,
    selectSchema: mockSelectSchema,
    selectTable: mockSelectTable,
    addCatalog: mockAddCatalog,
    addSchema: mockAddSchema,
    addTable: mockAddTable,
    addVolume: jest.fn(),
    addFunction: jest.fn(),
    addMaterializedView: jest.fn(),
    addView: jest.fn(),
    renameCatalog: mockRenameCatalog,
    renameSchema: mockRenameSchema,
    renameTable: mockRenameTable,
    renameView: jest.fn(),
    renameVolume: jest.fn(),
    renameFunction: jest.fn(),
    renameMaterializedView: jest.fn(),
    dropCatalog: mockDropCatalog,
    dropSchema: mockDropSchema,
    dropTable: mockDropTable,
    dropView: jest.fn(),
    dropVolume: jest.fn(),
    dropFunction: jest.fn(),
    dropMaterializedView: jest.fn(),
    findCatalog: jest.fn(),
    findSchema: jest.fn(),
    findTable: jest.fn(),
    findView: jest.fn(),
    findVolume: jest.fn(),
    findFunction: jest.fn(),
    findMaterializedView: jest.fn(),
    buildBulkGrantOps: jest.fn(),
    buildBulkTableTagOps: jest.fn(),
    buildBulkSchemaTagOps: jest.fn(),
    buildBulkCatalogTagOps: jest.fn(),
    emitOps: jest.fn(),
    ...overrides,
  };
}

// ---------------------------------------------------------------------------
// Reusable project fixtures
// ---------------------------------------------------------------------------

const CAT_ID = 'cat_1';
const SCH_ID = 'sch_1';
const TBL_ID = 'tbl_1';

function projectWithCatalog(schemas: any[] = []) {
  return minimalProject([
    { id: CAT_ID, name: 'my_catalog', schemas, grants: [], tags: {} },
  ]);
}

function projectWithSchema(tables: any[] = [], views: any[] = []) {
  return projectWithCatalog([
    {
      id: SCH_ID,
      name: 'my_schema',
      tables,
      views,
      volumes: [],
      functions: [],
      materializedViews: [],
      grants: [],
      tags: {},
    },
  ]);
}

function projectWithTable() {
  return projectWithSchema([
    { id: TBL_ID, name: 'my_table', columns: [], grants: [], tags: {}, constraints: [] },
  ]);
}

// ---------------------------------------------------------------------------
// Helpers for interacting with the add-dialog inputs
// ---------------------------------------------------------------------------

/** Simulate a user typing into a VSCodeTextField (rendered as <input> in tests). */
function typeInto(input: HTMLElement, value: string) {
  // onInput handler reads e.target.value — fireEvent.input is the right event.
  Object.defineProperty(input, 'value', { writable: true, value });
  fireEvent.input(input, { target: { value } });
}

/** Open the "Add Catalog" dialog. Works whether the empty-state or header button is used. */
function openAddCatalogDialog(container: HTMLElement) {
  // In the empty state there are two "Add Catalog" buttons (header + empty-state body).
  // Use the empty-state primary button which is always present when there are no catalogs.
  const btn = container.querySelector('.sidebar-empty-state__button') as HTMLElement
    ?? (screen.getAllByTitle('Add Catalog')[0] as HTMLElement);
  fireEvent.click(btn);
}

/** Click the tag "+" button that is inside the dialog form. */
function clickTagPlusButton(container: HTMLElement) {
  const plusBtn = container.querySelector('[data-testid="add-tag-btn"]') as HTMLElement;
  fireEvent.click(plusBtn);
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

describe('Sidebar Component', () => {
  beforeEach(() => {
    jest.clearAllMocks();
    mockUseDesignerStore.mockReturnValue(makeStoreMock() as any);
  });

  test('renders sidebar container', () => {
    const { container } = render(<Sidebar />);
    expect(container.querySelector('.sidebar')).not.toBeNull();
  });

  test('renders without crashing with empty state', () => {
    const { container } = render(<Sidebar />);
    expect(container).toBeTruthy();
  });

  // -------------------------------------------------------------------------
  // Add Catalog dialog
  // -------------------------------------------------------------------------

  describe('Add Catalog dialog', () => {
    test('calls addCatalog with tags when user presses "+" before confirming', () => {
      const { container } = render(<Sidebar />);

      openAddCatalogDialog(container);

      const nameInput = screen.getByPlaceholderText('Enter catalog name') as HTMLInputElement;
      typeInto(nameInput, 'my_catalog');

      const [keyInput, valueInput] = screen.getAllByPlaceholderText(/Key|Value/i) as HTMLInputElement[];
      typeInto(keyInput, 'env');
      typeInto(valueInput, 'production');

      // Explicitly press "+" to commit the tag
      clickTagPlusButton(container);

      fireEvent.submit(container.querySelector('form.modal-content') as HTMLFormElement);

      expect(mockAddCatalog).toHaveBeenCalledWith(
        'my_catalog',
        expect.objectContaining({ tags: { env: 'production' } })
      );
    });

    test('flushes pending tag input when user confirms without pressing "+"', () => {
      const { container } = render(<Sidebar />);

      openAddCatalogDialog(container);

      const nameInput = screen.getByPlaceholderText('Enter catalog name') as HTMLInputElement;
      typeInto(nameInput, 'my_catalog');

      const [keyInput, valueInput] = screen.getAllByPlaceholderText(/Key|Value/i) as HTMLInputElement[];
      typeInto(keyInput, 'env');
      typeInto(valueInput, 'production');

      // Confirm WITHOUT pressing "+" — the pending tag must still be registered
      fireEvent.submit(container.querySelector('form.modal-content') as HTMLFormElement);

      expect(mockAddCatalog).toHaveBeenCalledWith(
        'my_catalog',
        expect.objectContaining({ tags: { env: 'production' } })
      );
    });

    test('does not include partial tag when only key is filled', () => {
      const { container } = render(<Sidebar />);

      openAddCatalogDialog(container);

      const nameInput = screen.getByPlaceholderText('Enter catalog name') as HTMLInputElement;
      typeInto(nameInput, 'my_catalog');

      // Only fill key, leave value empty — incomplete pair should be ignored
      const [keyInput] = screen.getAllByPlaceholderText(/Key|Value/i) as HTMLInputElement[];
      typeInto(keyInput, 'env');

      fireEvent.submit(container.querySelector('form.modal-content') as HTMLFormElement);

      const call = mockAddCatalog.mock.calls[0] as any[];
      expect(call[1]?.tags).toBeUndefined();
    });
  });

  // -------------------------------------------------------------------------
  // Add Schema dialog
  // -------------------------------------------------------------------------

  describe('Add Schema dialog', () => {
    function renderWithCatalog() {
      const catalogId = 'cat_1';
      const project = minimalProject([
        { id: catalogId, name: 'my_catalog', schemas: [], grants: [], tags: {} },
      ]);
      mockUseDesignerStore.mockReturnValue(makeStoreMock({ project }) as any);
      const result = render(<Sidebar />);
      return { ...result, catalogId };
    }

    test('calls addSchema with tags when user presses "+" before confirming', () => {
      const { container, catalogId } = renderWithCatalog();

      fireEvent.click(screen.getByLabelText('Add Schema'));

      const nameInput = screen.getByPlaceholderText('Enter schema name') as HTMLInputElement;
      typeInto(nameInput, 'my_schema');

      const [keyInput, valueInput] = screen.getAllByPlaceholderText(/Key|Value/i) as HTMLInputElement[];
      typeInto(keyInput, 'tier');
      typeInto(valueInput, 'gold');

      // Explicitly press "+" to commit the tag
      clickTagPlusButton(container);

      fireEvent.submit(container.querySelector('form.modal-content') as HTMLFormElement);

      expect(mockAddSchema).toHaveBeenCalledWith(
        catalogId,
        'my_schema',
        expect.objectContaining({ tags: { tier: 'gold' } })
      );
    });

    test('flushes pending tag input when user confirms without pressing "+"', () => {
      const { container, catalogId } = renderWithCatalog();

      fireEvent.click(screen.getByLabelText('Add Schema'));

      const nameInput = screen.getByPlaceholderText('Enter schema name') as HTMLInputElement;
      typeInto(nameInput, 'my_schema');

      const [keyInput, valueInput] = screen.getAllByPlaceholderText(/Key|Value/i) as HTMLInputElement[];
      typeInto(keyInput, 'tier');
      typeInto(valueInput, 'gold');

      // Confirm WITHOUT pressing "+"
      fireEvent.submit(container.querySelector('form.modal-content') as HTMLFormElement);

      expect(mockAddSchema).toHaveBeenCalledWith(
        catalogId,
        'my_schema',
        expect.objectContaining({ tags: { tier: 'gold' } })
      );
    });

    test('merges pending tag with already-confirmed tags', () => {
      const { container, catalogId } = renderWithCatalog();

      fireEvent.click(screen.getByLabelText('Add Schema'));

      const nameInput = screen.getByPlaceholderText('Enter schema name') as HTMLInputElement;
      typeInto(nameInput, 'my_schema');

      const [keyInput, valueInput] = screen.getAllByPlaceholderText(/Key|Value/i) as HTMLInputElement[];

      // First tag: confirmed via "+"
      typeInto(keyInput, 'domain');
      typeInto(valueInput, 'sales');
      clickTagPlusButton(container);

      // Second tag: NOT confirmed via "+" — must still appear in the emitted op
      typeInto(keyInput, 'tier');
      typeInto(valueInput, 'silver');

      fireEvent.submit(container.querySelector('form.modal-content') as HTMLFormElement);

      expect(mockAddSchema).toHaveBeenCalledWith(
        catalogId,
        'my_schema',
        expect.objectContaining({ tags: { domain: 'sales', tier: 'silver' } })
      );
    });

    test('includes comment in addSchema options', () => {
      const { container, catalogId } = renderWithCatalog();

      fireEvent.click(screen.getByLabelText('Add Schema'));

      const nameInput = screen.getByPlaceholderText('Enter schema name') as HTMLInputElement;
      typeInto(nameInput, 'my_schema');

      const commentInput = screen.getByPlaceholderText('Optional description') as HTMLInputElement;
      typeInto(commentInput, 'A useful schema');

      fireEvent.submit(container.querySelector('form.modal-content') as HTMLFormElement);

      expect(mockAddSchema).toHaveBeenCalledWith(
        catalogId,
        'my_schema',
        expect.objectContaining({ comment: 'A useful schema' })
      );
    });

    test('shows duplicate-name error and does not call addSchema', () => {
      // Schema 'existing' already exists in the catalog
      const project = minimalProject([
        {
          id: CAT_ID,
          name: 'my_catalog',
          schemas: [{ id: 'sch_existing', name: 'existing', tables: [], views: [], volumes: [], functions: [], materializedViews: [], grants: [], tags: {} }],
          grants: [],
          tags: {},
        },
      ]);
      mockUseDesignerStore.mockReturnValue(makeStoreMock({ project }) as any);
      const { container } = render(<Sidebar />);

      fireEvent.click(screen.getByLabelText('Add Schema'));

      const nameInput = screen.getByPlaceholderText('Enter schema name') as HTMLInputElement;
      typeInto(nameInput, 'existing');

      fireEvent.submit(container.querySelector('form.modal-content') as HTMLFormElement);

      expect(mockAddSchema).not.toHaveBeenCalled();
      expect(screen.getByText(/already exists/i)).toBeTruthy();
    });

    test('cancel button closes the add schema dialog', () => {
      const { container } = renderWithCatalog();

      fireEvent.click(screen.getByLabelText('Add Schema'));
      expect(container.querySelector('form.modal-content')).not.toBeNull();

      fireEvent.click(screen.getByRole('button', { name: /cancel/i }));

      expect(container.querySelector('form.modal-content')).toBeNull();
      expect(mockAddSchema).not.toHaveBeenCalled();
    });
  });

  // -------------------------------------------------------------------------
  // Tree rendering
  // -------------------------------------------------------------------------

  describe('Tree rendering', () => {
    test('shows empty-state message when there are no catalogs', () => {
      render(<Sidebar />);
      expect(screen.getByText('No catalogs yet')).toBeTruthy();
    });

    test('renders catalog name in the tree', () => {
      mockUseDesignerStore.mockReturnValue(
        makeStoreMock({ project: projectWithCatalog() }) as any
      );
      render(<Sidebar />);
      expect(screen.getByText('my_catalog')).toBeTruthy();
    });

    test('renders schema name when catalog is expanded', () => {
      mockUseDesignerStore.mockReturnValue(
        makeStoreMock({ project: projectWithSchema() }) as any
      );
      const { container } = render(<Sidebar />);

      // Expand the catalog
      fireEvent.click(container.querySelector('.expander') as HTMLElement);

      expect(screen.getByText('my_schema')).toBeTruthy();
    });

    test('renders table name when catalog and schema are both expanded', () => {
      mockUseDesignerStore.mockReturnValue(
        makeStoreMock({ project: projectWithTable() }) as any
      );
      const { container } = render(<Sidebar />);

      // Expand catalog first, then re-query for the schema expander that appeared.
      fireEvent.click(container.querySelectorAll('.expander')[0]);
      fireEvent.click(container.querySelectorAll('.expander')[1]);

      expect(screen.getByText('my_table')).toBeTruthy();
    });

    test('provider name is shown in the sidebar header', () => {
      render(<Sidebar />);
      expect(screen.getByText('Unity Catalog')).toBeTruthy();
    });
  });

  // -------------------------------------------------------------------------
  // Selection
  // -------------------------------------------------------------------------

  describe('Selection', () => {
    test('clicking a catalog calls selectCatalog and resets schema/table selection', () => {
      mockUseDesignerStore.mockReturnValue(
        makeStoreMock({ project: projectWithCatalog() }) as any
      );
      render(<Sidebar />);

      fireEvent.click(screen.getByText('my_catalog'));

      expect(mockSelectCatalog).toHaveBeenCalledWith(CAT_ID);
      expect(mockSelectSchema).toHaveBeenCalledWith(null);
      expect(mockSelectTable).toHaveBeenCalledWith(null);
    });

    test('clicking a schema calls selectSchema and resets table selection', () => {
      mockUseDesignerStore.mockReturnValue(
        makeStoreMock({ project: projectWithSchema() }) as any
      );
      const { container } = render(<Sidebar />);

      fireEvent.click(container.querySelector('.expander') as HTMLElement);
      fireEvent.click(screen.getByText('my_schema'));

      expect(mockSelectSchema).toHaveBeenCalledWith(SCH_ID);
      expect(mockSelectTable).toHaveBeenCalledWith(null);
    });

    test('clicking a table calls selectTable', () => {
      mockUseDesignerStore.mockReturnValue(
        makeStoreMock({ project: projectWithTable() }) as any
      );
      const { container } = render(<Sidebar />);

      fireEvent.click(container.querySelectorAll('.expander')[0]);
      fireEvent.click(container.querySelectorAll('.expander')[1]);

      fireEvent.click(screen.getByText('my_table'));

      expect(mockSelectTable).toHaveBeenCalledWith(TBL_ID);
    });

    test('selected catalog gets the "selected" css class', () => {
      mockUseDesignerStore.mockReturnValue(
        makeStoreMock({ project: projectWithCatalog(), selectedCatalogId: CAT_ID }) as any
      );
      const { container } = render(<Sidebar />);

      const catalogItem = container.querySelector('.tree-item.selected');
      expect(catalogItem).not.toBeNull();
      expect(catalogItem?.textContent).toContain('my_catalog');
    });
  });

  // -------------------------------------------------------------------------
  // Tree expansion / collapse
  // -------------------------------------------------------------------------

  describe('Tree expansion', () => {
    test('schemas are hidden before expanding the catalog', () => {
      mockUseDesignerStore.mockReturnValue(
        makeStoreMock({ project: projectWithSchema() }) as any
      );
      render(<Sidebar />);
      expect(screen.queryByText('my_schema')).toBeNull();
    });

    test('expanding the catalog reveals its schemas', () => {
      mockUseDesignerStore.mockReturnValue(
        makeStoreMock({ project: projectWithSchema() }) as any
      );
      const { container } = render(<Sidebar />);

      fireEvent.click(container.querySelector('.expander') as HTMLElement);
      expect(screen.getByText('my_schema')).toBeTruthy();
    });

    test('collapsing a catalog hides its schemas again', () => {
      mockUseDesignerStore.mockReturnValue(
        makeStoreMock({ project: projectWithSchema() }) as any
      );
      const { container } = render(<Sidebar />);

      const expander = container.querySelector('.expander') as HTMLElement;
      fireEvent.click(expander); // expand
      fireEvent.click(expander); // collapse

      expect(screen.queryByText('my_schema')).toBeNull();
    });
  });

  // -------------------------------------------------------------------------
  // Context-aware header add button
  // -------------------------------------------------------------------------

  describe('Context-aware header add button', () => {
    test('shows "+ Catalog" button in the header when nothing is selected', () => {
      render(<Sidebar />);
      // Both the header button and the empty-state button share this title when no catalogs exist.
      expect(screen.getAllByTitle('Add Catalog').length).toBeGreaterThan(0);
    });

    test('shows "+ Schema" when a catalog is selected', () => {
      mockUseDesignerStore.mockReturnValue(
        makeStoreMock({ project: projectWithCatalog(), selectedCatalogId: CAT_ID }) as any
      );
      render(<Sidebar />);
      expect(screen.getAllByTitle('Add Schema').length).toBeGreaterThan(0);
    });

    test('shows "+ Table" when a schema is selected', () => {
      mockUseDesignerStore.mockReturnValue(
        makeStoreMock({
          project: projectWithSchema(),
          selectedCatalogId: CAT_ID,
          selectedSchemaId: SCH_ID,
        }) as any
      );
      render(<Sidebar />);
      expect(screen.getAllByTitle('Add Table').length).toBeGreaterThan(0);
    });
  });

  // -------------------------------------------------------------------------
  // Rename dialog
  // -------------------------------------------------------------------------

  describe('Rename dialog', () => {
    test('clicking "Rename Catalog" opens the rename dialog', () => {
      mockUseDesignerStore.mockReturnValue(
        makeStoreMock({ project: projectWithCatalog() }) as any
      );
      const { container } = render(<Sidebar />);

      fireEvent.click(screen.getByLabelText('Rename Catalog'));

      expect(container.querySelector('form.modal-content')).not.toBeNull();
      expect(screen.getByText(/Edit catalog/i)).toBeTruthy();
    });

    test('renaming a catalog calls renameCatalog with the new name', () => {
      mockUseDesignerStore.mockReturnValue(
        makeStoreMock({ project: projectWithCatalog() }) as any
      );
      const { container } = render(<Sidebar />);

      fireEvent.click(screen.getByLabelText('Rename Catalog'));

      const nameInput = container.querySelector('#rename-name-input') as HTMLInputElement;
      typeInto(nameInput, 'renamed_catalog');

      fireEvent.submit(container.querySelector('form.modal-content') as HTMLFormElement);

      expect(mockRenameCatalog).toHaveBeenCalledWith(CAT_ID, 'renamed_catalog');
    });

    test('submitting the same name closes the dialog without calling renameCatalog', () => {
      mockUseDesignerStore.mockReturnValue(
        makeStoreMock({ project: projectWithCatalog() }) as any
      );
      const { container } = render(<Sidebar />);

      fireEvent.click(screen.getByLabelText('Rename Catalog'));

      const nameInput = container.querySelector('#rename-name-input') as HTMLInputElement;
      typeInto(nameInput, 'my_catalog'); // same name

      fireEvent.submit(container.querySelector('form.modal-content') as HTMLFormElement);

      expect(mockRenameCatalog).not.toHaveBeenCalled();
      expect(container.querySelector('form.modal-content')).toBeNull();
    });

    test('clicking "Rename Schema" opens the rename dialog for the schema', () => {
      mockUseDesignerStore.mockReturnValue(
        makeStoreMock({ project: projectWithSchema() }) as any
      );
      const { container } = render(<Sidebar />);

      // Expand catalog first so the schema row is visible
      fireEvent.click(container.querySelector('.expander') as HTMLElement);
      fireEvent.click(screen.getByLabelText('Rename Schema'));

      expect(screen.getByText(/Edit schema/i)).toBeTruthy();
    });

    test('renaming a schema calls renameSchema with the new name', () => {
      mockUseDesignerStore.mockReturnValue(
        makeStoreMock({ project: projectWithSchema() }) as any
      );
      const { container } = render(<Sidebar />);

      fireEvent.click(container.querySelector('.expander') as HTMLElement);
      fireEvent.click(screen.getByLabelText('Rename Schema'));

      const nameInput = container.querySelector('#rename-name-input') as HTMLInputElement;
      typeInto(nameInput, 'renamed_schema');

      fireEvent.submit(container.querySelector('form.modal-content') as HTMLFormElement);

      expect(mockRenameSchema).toHaveBeenCalledWith(SCH_ID, 'renamed_schema');
    });

    test('cancel button on rename dialog closes it without renaming', () => {
      mockUseDesignerStore.mockReturnValue(
        makeStoreMock({ project: projectWithCatalog() }) as any
      );
      const { container } = render(<Sidebar />);

      fireEvent.click(screen.getByLabelText('Rename Catalog'));
      fireEvent.click(screen.getByRole('button', { name: /cancel/i }));

      expect(container.querySelector('form.modal-content')).toBeNull();
      expect(mockRenameCatalog).not.toHaveBeenCalled();
    });
  });

  // -------------------------------------------------------------------------
  // Drop dialog
  // -------------------------------------------------------------------------

  describe('Drop dialog', () => {
    test('clicking "Drop Catalog" opens the drop confirmation dialog', () => {
      mockUseDesignerStore.mockReturnValue(
        makeStoreMock({ project: projectWithCatalog() }) as any
      );
      render(<Sidebar />);

      fireEvent.click(screen.getByLabelText('Drop Catalog'));

      expect(screen.getByText('Confirm Drop')).toBeTruthy();
      expect(screen.getByText(/drop catalog "my_catalog"/i)).toBeTruthy();
    });

    test('drop catalog dialog shows cascade warning', () => {
      mockUseDesignerStore.mockReturnValue(
        makeStoreMock({ project: projectWithCatalog() }) as any
      );
      render(<Sidebar />);

      fireEvent.click(screen.getByLabelText('Drop Catalog'));

      expect(screen.getByText(/also drop every schema/i)).toBeTruthy();
    });

    test('confirming drop calls dropCatalog and closes the dialog', () => {
      mockUseDesignerStore.mockReturnValue(
        makeStoreMock({ project: projectWithCatalog() }) as any
      );
      const { container } = render(<Sidebar />);

      fireEvent.click(screen.getByLabelText('Drop Catalog'));
      fireEvent.submit(container.querySelector('form.modal-content') as HTMLFormElement);

      expect(mockDropCatalog).toHaveBeenCalledWith(CAT_ID);
      expect(container.querySelector('form.modal-content')).toBeNull();
    });

    test('canceling the drop dialog does not call dropCatalog', () => {
      mockUseDesignerStore.mockReturnValue(
        makeStoreMock({ project: projectWithCatalog() }) as any
      );
      render(<Sidebar />);

      fireEvent.click(screen.getByLabelText('Drop Catalog'));
      fireEvent.click(screen.getByRole('button', { name: /cancel/i }));

      expect(mockDropCatalog).not.toHaveBeenCalled();
    });

    test('clicking "Drop Schema" shows schema cascade warning', () => {
      mockUseDesignerStore.mockReturnValue(
        makeStoreMock({ project: projectWithSchema() }) as any
      );
      const { container } = render(<Sidebar />);

      fireEvent.click(container.querySelector('.expander') as HTMLElement);
      fireEvent.click(screen.getByLabelText('Drop Schema'));

      expect(screen.getByText(/also drop all tables/i)).toBeTruthy();
    });

    test('confirming drop schema calls dropSchema', () => {
      mockUseDesignerStore.mockReturnValue(
        makeStoreMock({ project: projectWithSchema() }) as any
      );
      const { container } = render(<Sidebar />);

      fireEvent.click(container.querySelector('.expander') as HTMLElement);
      fireEvent.click(screen.getByLabelText('Drop Schema'));
      fireEvent.submit(container.querySelector('form.modal-content') as HTMLFormElement);

      expect(mockDropSchema).toHaveBeenCalledWith(SCH_ID);
    });
  });

  // -------------------------------------------------------------------------
  // Add Table dialog
  // -------------------------------------------------------------------------

  describe('Add Table dialog', () => {
    function renderWithSchema() {
      mockUseDesignerStore.mockReturnValue(
        makeStoreMock({ project: projectWithSchema() }) as any
      );
      const result = render(<Sidebar />);
      // Expand catalog so the schema row with its "Add Table" button is visible
      fireEvent.click(result.container.querySelector('.expander') as HTMLElement);
      return result;
    }

    test('opens Add Table dialog when clicking "Add Table" on a schema row', () => {
      const { container } = renderWithSchema();

      fireEvent.click(screen.getByLabelText('Add Table'));

      expect(container.querySelector('form.modal-content')).not.toBeNull();
      expect(screen.getByPlaceholderText('Enter table name')).toBeTruthy();
    });

    test('calls addTable with delta format by default', () => {
      const { container } = renderWithSchema();

      fireEvent.click(screen.getByLabelText('Add Table'));

      const nameInput = screen.getByPlaceholderText('Enter table name') as HTMLInputElement;
      typeInto(nameInput, 'events');

      fireEvent.submit(container.querySelector('form.modal-content') as HTMLFormElement);

      // No options filled → options arg is undefined
      expect(mockAddTable).toHaveBeenCalledWith(SCH_ID, 'events', 'delta', undefined);
    });

    test('calls addTable with iceberg format when the format dropdown is changed', () => {
      const { container } = renderWithSchema();

      fireEvent.click(screen.getByLabelText('Add Table'));

      const nameInput = screen.getByPlaceholderText('Enter table name') as HTMLInputElement;
      typeInto(nameInput, 'events');

      // The format selector is a VSCodeDropdown (rendered as <select>), driven by onInput.
      const formatSelect = container.querySelector('#table-format-select') as HTMLSelectElement;
      Object.defineProperty(formatSelect, 'value', { writable: true, value: 'iceberg' });
      fireEvent.input(formatSelect, { target: { value: 'iceberg' } });

      fireEvent.submit(container.querySelector('form.modal-content') as HTMLFormElement);

      expect(mockAddTable).toHaveBeenCalledWith(SCH_ID, 'events', 'iceberg', undefined);
    });
  });

  // -------------------------------------------------------------------------
  // Duplicate-name validation for catalogs
  // -------------------------------------------------------------------------

  describe('Duplicate name validation', () => {
    test('shows error and does not call addCatalog when catalog name already exists', () => {
      mockUseDesignerStore.mockReturnValue(
        makeStoreMock({ project: projectWithCatalog() }) as any
      );
      const { container } = render(<Sidebar />);

      // Header "Add Catalog" button (no empty-state button when catalogs exist)
      fireEvent.click(screen.getAllByTitle('Add Catalog')[0]);

      const nameInput = screen.getByPlaceholderText('Enter catalog name') as HTMLInputElement;
      typeInto(nameInput, 'my_catalog'); // already exists

      fireEvent.submit(container.querySelector('form.modal-content') as HTMLFormElement);

      expect(mockAddCatalog).not.toHaveBeenCalled();
      expect(screen.getByText(/already exists/i)).toBeTruthy();
    });
  });
});

