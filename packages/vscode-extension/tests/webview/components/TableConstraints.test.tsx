/**
 * Tests for TableConstraints component - Managing table constraints
 */

import React from 'react';
import { render, screen, fireEvent } from '@testing-library/react';
import { describe, test, expect, jest, beforeEach } from '@jest/globals';
import { TableConstraints } from '../../../src/webview/components/TableConstraints';

// Mock functions
const mockAddConstraint = jest.fn();
const mockUpdateConstraint = jest.fn();
const mockDropConstraint = jest.fn();

// Default project with constraints for testing
const defaultProject = {
  state: {
    catalogs: [{
      id: 'cat_1', name: 'my_catalog',
      schemas: [{
        id: 'sch_1', name: 'my_schema',
        tables: [{
          id: 'tbl_1', name: 'my_table', format: 'delta',
          columns: [
            { id: 'col_1', name: 'id', type: 'INT', nullable: false },
            { id: 'col_2', name: 'name', type: 'STRING', nullable: true },
            { id: 'col_3', name: 'email', type: 'STRING', nullable: true },
          ],
          properties: {}, tags: {},
          constraints: [
            { id: 'cst_1', name: 'pk_id', type: 'primary_key', columns: ['col_1'] },
            { id: 'cst_2', name: 'chk_name', type: 'check', columns: ['name'], expression: 'name IS NOT NULL' },
          ],
          grants: [],
        }, {
          id: 'tbl_2', name: 'other_table', format: 'delta',
          columns: [
            { id: 'ocol_1', name: 'ref_id', type: 'INT', nullable: false },
          ],
          properties: {}, tags: {},
          constraints: [],
          grants: [],
        }],
        views: [], grants: [],
      }],
      grants: [],
    }],
  },
};

let currentProject: typeof defaultProject | null = defaultProject;

jest.mock('../../../src/webview/state/useDesignerStore', () => ({
  useDesignerStore: () => ({
    project: currentProject,
    addConstraint: mockAddConstraint,
    updateConstraint: mockUpdateConstraint,
    dropConstraint: mockDropConstraint,
  }),
}));

describe('TableConstraints Component', () => {
  beforeEach(() => {
    jest.clearAllMocks();
    currentProject = defaultProject;
  });

  // ---------------------------------------------------------------
  // 1. Returns null when table not found
  // ---------------------------------------------------------------
  test('returns null when table is not found', () => {
    const { container } = render(<TableConstraints tableId="nonexistent_table" />);
    expect(container.innerHTML).toBe('');
  });

  test('returns null when project is null', () => {
    currentProject = null;
    const { container } = render(<TableConstraints tableId="tbl_1" />);
    expect(container.innerHTML).toBe('');
  });

  // ---------------------------------------------------------------
  // 2. Renders heading "Constraints (N)"
  // ---------------------------------------------------------------
  test('renders heading with constraint count', () => {
    render(<TableConstraints tableId="tbl_1" />);
    expect(screen.getByText('Constraints (2)')).toBeInTheDocument();
  });

  // ---------------------------------------------------------------
  // 3. Renders existing constraint names and types
  // ---------------------------------------------------------------
  test('renders existing constraint names', () => {
    render(<TableConstraints tableId="tbl_1" />);
    expect(screen.getByText('pk_id')).toBeInTheDocument();
    expect(screen.getByText('chk_name')).toBeInTheDocument();
  });

  // ---------------------------------------------------------------
  // 4. Shows constraint columns in definition
  // ---------------------------------------------------------------
  test('shows constraint column names in the definition', () => {
    render(<TableConstraints tableId="tbl_1" />);
    // PRIMARY KEY definition resolves col_1 -> "id"
    expect(screen.getByText('PRIMARY KEY (id)')).toBeInTheDocument();
  });

  // ---------------------------------------------------------------
  // 5. Shows check expression for check constraints
  // ---------------------------------------------------------------
  test('shows check expression for check constraints', () => {
    render(<TableConstraints tableId="tbl_1" />);
    expect(screen.getByText('CHECK (name IS NOT NULL)')).toBeInTheDocument();
  });

  // ---------------------------------------------------------------
  // 6. Has Add Constraint button
  // ---------------------------------------------------------------
  test('has Add Constraint button', () => {
    render(<TableConstraints tableId="tbl_1" />);
    expect(screen.getByText('+ Add Constraint')).toBeInTheDocument();
  });

  // ---------------------------------------------------------------
  // 7. Has edit/delete buttons per constraint
  // ---------------------------------------------------------------
  test('has edit and delete buttons per constraint', () => {
    render(<TableConstraints tableId="tbl_1" />);
    const editButtons = screen.getAllByTitle('Edit constraint');
    const dropButtons = screen.getAllByTitle('Drop constraint');
    expect(editButtons).toHaveLength(2);
    expect(dropButtons).toHaveLength(2);
  });

  // ---------------------------------------------------------------
  // 8. Shows empty state when no constraints
  // ---------------------------------------------------------------
  test('shows empty state when no constraints', () => {
    render(<TableConstraints tableId="tbl_2" />);
    expect(screen.getByText('No constraints defined.')).toBeInTheDocument();
    expect(screen.getByText(/Add PRIMARY KEY, FOREIGN KEY, or CHECK constraints/)).toBeInTheDocument();
    expect(screen.getByText('Constraints (0)')).toBeInTheDocument();
  });

  // ---------------------------------------------------------------
  // 9. Renders PRIMARY KEY badge
  // ---------------------------------------------------------------
  test('renders PRIMARY KEY badge', () => {
    render(<TableConstraints tableId="tbl_1" />);
    expect(screen.getByText('PRIMARY KEY')).toBeInTheDocument();
  });

  // ---------------------------------------------------------------
  // 10. Renders CHECK badge
  // ---------------------------------------------------------------
  test('renders CHECK badge', () => {
    render(<TableConstraints tableId="tbl_1" />);
    expect(screen.getByText('CHECK')).toBeInTheDocument();
  });

  // ---------------------------------------------------------------
  // 11. Renders FOREIGN KEY badge
  // ---------------------------------------------------------------
  test('renders FOREIGN KEY badge', () => {
    const projectWithFK = {
      state: {
        catalogs: [{
          id: 'cat_1', name: 'my_catalog',
          schemas: [{
            id: 'sch_1', name: 'my_schema',
            tables: [{
              id: 'tbl_fk', name: 'orders', format: 'delta',
              columns: [
                { id: 'fk_col_1', name: 'user_id', type: 'INT', nullable: false },
              ],
              properties: {}, tags: {},
              constraints: [
                {
                  id: 'cst_fk_1', name: 'fk_user', type: 'foreign_key',
                  columns: ['fk_col_1'],
                  parentTable: 'tbl_parent',
                  parentColumns: ['parent_col_1'],
                },
              ],
              grants: [],
            }, {
              id: 'tbl_parent', name: 'users', format: 'delta',
              columns: [
                { id: 'parent_col_1', name: 'id', type: 'INT', nullable: false },
              ],
              properties: {}, tags: {},
              constraints: [],
              grants: [],
            }],
            views: [], grants: [],
          }],
          grants: [],
        }],
      },
    };
    currentProject = projectWithFK as typeof defaultProject;

    render(<TableConstraints tableId="tbl_fk" />);
    expect(screen.getByText('FOREIGN KEY')).toBeInTheDocument();
  });

  // ---------------------------------------------------------------
  // Foreign key definition resolves parent table and column names
  // ---------------------------------------------------------------
  test('renders foreign key definition with resolved parent table and columns', () => {
    const projectWithFK = {
      state: {
        catalogs: [{
          id: 'cat_1', name: 'my_catalog',
          schemas: [{
            id: 'sch_1', name: 'my_schema',
            tables: [{
              id: 'tbl_fk', name: 'orders', format: 'delta',
              columns: [
                { id: 'fk_col_1', name: 'user_id', type: 'INT', nullable: false },
              ],
              properties: {}, tags: {},
              constraints: [
                {
                  id: 'cst_fk_1', name: 'fk_user', type: 'foreign_key',
                  columns: ['fk_col_1'],
                  parentTable: 'tbl_parent',
                  parentColumns: ['parent_col_1'],
                },
              ],
              grants: [],
            }, {
              id: 'tbl_parent', name: 'users', format: 'delta',
              columns: [
                { id: 'parent_col_1', name: 'id', type: 'INT', nullable: false },
              ],
              properties: {}, tags: {},
              constraints: [],
              grants: [],
            }],
            views: [], grants: [],
          }],
          grants: [],
        }],
      },
    };
    currentProject = projectWithFK as typeof defaultProject;

    render(<TableConstraints tableId="tbl_fk" />);
    // Should resolve column names and parent table qualified name
    expect(screen.getByText('FOREIGN KEY (user_id) REFERENCES my_catalog.my_schema.users (id)')).toBeInTheDocument();
  });

  // ---------------------------------------------------------------
  // Table header row renders column labels
  // ---------------------------------------------------------------
  test('renders table header with TYPE, NAME, DEFINITION, ACTIONS', () => {
    render(<TableConstraints tableId="tbl_1" />);
    expect(screen.getByText('TYPE')).toBeInTheDocument();
    expect(screen.getByText('NAME')).toBeInTheDocument();
    expect(screen.getByText('DEFINITION')).toBeInTheDocument();
    expect(screen.getByText('ACTIONS')).toBeInTheDocument();
  });

  // ---------------------------------------------------------------
  // Constraint type badge has correct CSS class
  // ---------------------------------------------------------------
  test('constraint type badge has correct CSS class', () => {
    const { container } = render(<TableConstraints tableId="tbl_1" />);
    const pkBadge = container.querySelector('.constraint-type-badge.primary_key');
    const checkBadge = container.querySelector('.constraint-type-badge.check');
    expect(pkBadge).not.toBeNull();
    expect(checkBadge).not.toBeNull();
  });

  // ---------------------------------------------------------------
  // Clicking "+ Add Constraint" opens the add dialog
  // ---------------------------------------------------------------
  test('clicking Add Constraint button opens add dialog', () => {
    render(<TableConstraints tableId="tbl_1" />);
    fireEvent.click(screen.getByText('+ Add Constraint'));
    // The dialog should appear with heading "Add Constraint"
    expect(screen.getByText('Add Constraint', { selector: 'h2' })).toBeInTheDocument();
    // Dialog should contain type dropdown options
    expect(screen.getByText('Constraint type')).toBeInTheDocument();
    expect(screen.getByText('Constraint name (optional)')).toBeInTheDocument();
  });

  // ---------------------------------------------------------------
  // Add dialog shows column checkboxes
  // ---------------------------------------------------------------
  test('add dialog shows column checkboxes for non-check types', () => {
    render(<TableConstraints tableId="tbl_1" />);
    fireEvent.click(screen.getByText('+ Add Constraint'));
    // Default type is primary_key, so columns should be visible
    expect(screen.getByText('Columns \u2014 select one or more')).toBeInTheDocument();
    expect(screen.getByText('id (INT)')).toBeInTheDocument();
    expect(screen.getByText('name (STRING)')).toBeInTheDocument();
    expect(screen.getByText('email (STRING)')).toBeInTheDocument();
  });

  // ---------------------------------------------------------------
  // Add dialog shows TIMESERIES checkbox for primary_key
  // ---------------------------------------------------------------
  test('add dialog shows TIMESERIES checkbox for primary_key type', () => {
    render(<TableConstraints tableId="tbl_1" />);
    fireEvent.click(screen.getByText('+ Add Constraint'));
    expect(screen.getByText('TIMESERIES (for time-series data)')).toBeInTheDocument();
  });

  // ---------------------------------------------------------------
  // Add dialog shows informational note
  // ---------------------------------------------------------------
  test('add dialog shows informational note about Unity Catalog', () => {
    render(<TableConstraints tableId="tbl_1" />);
    fireEvent.click(screen.getByText('+ Add Constraint'));
    expect(screen.getByText(/Unity Catalog constraints are informational only/)).toBeInTheDocument();
  });

  // ---------------------------------------------------------------
  // Add dialog has Cancel button that closes dialog
  // ---------------------------------------------------------------
  test('add dialog Cancel button closes the dialog', () => {
    render(<TableConstraints tableId="tbl_1" />);
    fireEvent.click(screen.getByText('+ Add Constraint'));
    expect(screen.getByText('Add Constraint', { selector: 'h2' })).toBeInTheDocument();
    // Click Cancel
    fireEvent.click(screen.getByText('Cancel'));
    expect(screen.queryByText('Add Constraint', { selector: 'h2' })).not.toBeInTheDocument();
  });

  // ---------------------------------------------------------------
  // Clicking edit button opens edit dialog
  // ---------------------------------------------------------------
  test('clicking edit button opens edit dialog with constraint data', () => {
    render(<TableConstraints tableId="tbl_1" />);
    const editButtons = screen.getAllByTitle('Edit constraint');
    fireEvent.click(editButtons[0]); // Edit the primary key constraint
    expect(screen.getByText('Edit Constraint')).toBeInTheDocument();
    expect(screen.getByText('Save Changes')).toBeInTheDocument();
  });

  // ---------------------------------------------------------------
  // Clicking drop button opens drop confirmation dialog
  // ---------------------------------------------------------------
  test('clicking drop button opens drop confirmation dialog', () => {
    render(<TableConstraints tableId="tbl_1" />);
    const dropButtons = screen.getAllByTitle('Drop constraint');
    fireEvent.click(dropButtons[0]); // Drop the first constraint
    expect(screen.getByText('Drop Constraint')).toBeInTheDocument();
    expect(screen.getByText(/Are you sure you want to drop constraint/)).toBeInTheDocument();
    // pk_id appears both in the table row and the dialog's <strong> tag
    const pkMatches = screen.getAllByText('pk_id');
    expect(pkMatches.length).toBeGreaterThanOrEqual(2);
  });

  // ---------------------------------------------------------------
  // Confirming drop calls dropConstraint
  // ---------------------------------------------------------------
  test('confirming drop calls dropConstraint with correct args', () => {
    render(<TableConstraints tableId="tbl_1" />);
    const dropButtons = screen.getAllByTitle('Drop constraint');
    fireEvent.click(dropButtons[0]);
    // Click the "Drop" button in the confirmation dialog
    fireEvent.click(screen.getByText('Drop'));
    expect(mockDropConstraint).toHaveBeenCalledWith('tbl_1', 'cst_1');
  });

  // ---------------------------------------------------------------
  // Canceling drop dialog closes it without calling dropConstraint
  // ---------------------------------------------------------------
  test('canceling drop dialog does not call dropConstraint', () => {
    render(<TableConstraints tableId="tbl_1" />);
    const dropButtons = screen.getAllByTitle('Drop constraint');
    fireEvent.click(dropButtons[0]);
    fireEvent.click(screen.getByText('Cancel'));
    expect(mockDropConstraint).not.toHaveBeenCalled();
  });

  // ---------------------------------------------------------------
  // Unnamed constraint shows "unnamed" label
  // ---------------------------------------------------------------
  test('shows "unnamed" for constraint without a name', () => {
    const projectUnnamed = {
      state: {
        catalogs: [{
          id: 'cat_1', name: 'my_catalog',
          schemas: [{
            id: 'sch_1', name: 'my_schema',
            tables: [{
              id: 'tbl_unnamed', name: 'test_table', format: 'delta',
              columns: [
                { id: 'col_1', name: 'id', type: 'INT', nullable: false },
              ],
              properties: {}, tags: {},
              constraints: [
                { id: 'cst_no_name', name: '', type: 'primary_key', columns: ['col_1'] },
              ],
              grants: [],
            }],
            views: [], grants: [],
          }],
          grants: [],
        }],
      },
    };
    currentProject = projectUnnamed as typeof defaultProject;

    render(<TableConstraints tableId="tbl_unnamed" />);
    expect(screen.getByText('unnamed')).toBeInTheDocument();
  });

  // ---------------------------------------------------------------
  // PRIMARY KEY with TIMESERIES displays TIMESERIES label
  // ---------------------------------------------------------------
  test('renders PRIMARY KEY with TIMESERIES in definition', () => {
    const projectTimeseries = {
      state: {
        catalogs: [{
          id: 'cat_1', name: 'my_catalog',
          schemas: [{
            id: 'sch_1', name: 'my_schema',
            tables: [{
              id: 'tbl_ts', name: 'events', format: 'delta',
              columns: [
                { id: 'col_ts', name: 'event_time', type: 'TIMESTAMP', nullable: false },
              ],
              properties: {}, tags: {},
              constraints: [
                { id: 'cst_ts', name: 'pk_ts', type: 'primary_key', columns: ['col_ts'], timeseries: true },
              ],
              grants: [],
            }],
            views: [], grants: [],
          }],
          grants: [],
        }],
      },
    };
    currentProject = projectTimeseries as typeof defaultProject;

    render(<TableConstraints tableId="tbl_ts" />);
    expect(screen.getByText('PRIMARY KEY (event_time) TIMESERIES')).toBeInTheDocument();
  });

  // ---------------------------------------------------------------
  // PRIMARY KEY with multiple columns
  // ---------------------------------------------------------------
  test('renders PRIMARY KEY with multiple columns', () => {
    const projectMultiCol = {
      state: {
        catalogs: [{
          id: 'cat_1', name: 'my_catalog',
          schemas: [{
            id: 'sch_1', name: 'my_schema',
            tables: [{
              id: 'tbl_mc', name: 'composite', format: 'delta',
              columns: [
                { id: 'mc_1', name: 'a', type: 'INT', nullable: false },
                { id: 'mc_2', name: 'b', type: 'INT', nullable: false },
              ],
              properties: {}, tags: {},
              constraints: [
                { id: 'cst_mc', name: 'pk_ab', type: 'primary_key', columns: ['mc_1', 'mc_2'] },
              ],
              grants: [],
            }],
            views: [], grants: [],
          }],
          grants: [],
        }],
      },
    };
    currentProject = projectMultiCol as typeof defaultProject;

    render(<TableConstraints tableId="tbl_mc" />);
    expect(screen.getByText('PRIMARY KEY (a, b)')).toBeInTheDocument();
  });

  // ---------------------------------------------------------------
  // Foreign key with unresolvable parent table shows raw ID
  // ---------------------------------------------------------------
  test('foreign key with unresolvable parent table shows raw table ID', () => {
    const projectFKUnresolved = {
      state: {
        catalogs: [{
          id: 'cat_1', name: 'my_catalog',
          schemas: [{
            id: 'sch_1', name: 'my_schema',
            tables: [{
              id: 'tbl_fku', name: 'orders', format: 'delta',
              columns: [
                { id: 'fku_col', name: 'user_id', type: 'INT', nullable: false },
              ],
              properties: {}, tags: {},
              constraints: [
                {
                  id: 'cst_fku', name: 'fk_missing', type: 'foreign_key',
                  columns: ['fku_col'],
                  parentTable: 'nonexistent_tbl',
                  parentColumns: ['some_col'],
                },
              ],
              grants: [],
            }],
            views: [], grants: [],
          }],
          grants: [],
        }],
      },
    };
    currentProject = projectFKUnresolved as typeof defaultProject;

    render(<TableConstraints tableId="tbl_fku" />);
    // When parent table is not found, it falls back to the raw parentTable value
    expect(screen.getByText(/FOREIGN KEY \(user_id\) REFERENCES nonexistent_tbl/)).toBeInTheDocument();
  });

  // ---------------------------------------------------------------
  // Select all / Clear buttons in add dialog
  // ---------------------------------------------------------------
  test('add dialog has Select all and Clear buttons for columns', () => {
    render(<TableConstraints tableId="tbl_1" />);
    fireEvent.click(screen.getByText('+ Add Constraint'));
    expect(screen.getByText('Select all')).toBeInTheDocument();
    expect(screen.getByText('Clear')).toBeInTheDocument();
  });

  // ---------------------------------------------------------------
  // Check constraint with empty expression
  // ---------------------------------------------------------------
  test('renders check constraint with empty expression gracefully', () => {
    const projectCheckEmpty = {
      state: {
        catalogs: [{
          id: 'cat_1', name: 'my_catalog',
          schemas: [{
            id: 'sch_1', name: 'my_schema',
            tables: [{
              id: 'tbl_ce', name: 'test', format: 'delta',
              columns: [
                { id: 'ce_col', name: 'val', type: 'INT', nullable: true },
              ],
              properties: {}, tags: {},
              constraints: [
                { id: 'cst_ce', name: 'empty_check', type: 'check', columns: [], expression: '' },
              ],
              grants: [],
            }],
            views: [], grants: [],
          }],
          grants: [],
        }],
      },
    };
    currentProject = projectCheckEmpty as typeof defaultProject;

    render(<TableConstraints tableId="tbl_ce" />);
    expect(screen.getByText('CHECK ()')).toBeInTheDocument();
  });

  // ---------------------------------------------------------------
  // Column ID fallback when column not found
  // ---------------------------------------------------------------
  test('falls back to column ID when column not found in table', () => {
    const projectMissingCol = {
      state: {
        catalogs: [{
          id: 'cat_1', name: 'my_catalog',
          schemas: [{
            id: 'sch_1', name: 'my_schema',
            tables: [{
              id: 'tbl_missing_col', name: 'broken', format: 'delta',
              columns: [],
              properties: {}, tags: {},
              constraints: [
                { id: 'cst_mc', name: 'pk_ghost', type: 'primary_key', columns: ['deleted_col_id'] },
              ],
              grants: [],
            }],
            views: [], grants: [],
          }],
          grants: [],
        }],
      },
    };
    currentProject = projectMissingCol as typeof defaultProject;

    render(<TableConstraints tableId="tbl_missing_col" />);
    expect(screen.getByText('PRIMARY KEY (deleted_col_id)')).toBeInTheDocument();
  });

  // ---------------------------------------------------------------
  // Table without constraints array
  // ---------------------------------------------------------------
  test('handles table with undefined constraints array', () => {
    const projectNoConstraints = {
      state: {
        catalogs: [{
          id: 'cat_1', name: 'my_catalog',
          schemas: [{
            id: 'sch_1', name: 'my_schema',
            tables: [{
              id: 'tbl_nc', name: 'bare', format: 'delta',
              columns: [
                { id: 'nc_col', name: 'x', type: 'INT', nullable: false },
              ],
              properties: {}, tags: {},
              grants: [],
              // constraints intentionally omitted
            }],
            views: [], grants: [],
          }],
          grants: [],
        }],
      },
    };
    currentProject = projectNoConstraints as typeof defaultProject;

    render(<TableConstraints tableId="tbl_nc" />);
    expect(screen.getByText('Constraints (0)')).toBeInTheDocument();
    expect(screen.getByText('No constraints defined.')).toBeInTheDocument();
  });

  // ---------------------------------------------------------------
  // Foreign key without parentColumns
  // ---------------------------------------------------------------
  test('renders foreign key without parentColumns gracefully', () => {
    const projectFKNoCols = {
      state: {
        catalogs: [{
          id: 'cat_1', name: 'my_catalog',
          schemas: [{
            id: 'sch_1', name: 'my_schema',
            tables: [{
              id: 'tbl_fknc', name: 'orders', format: 'delta',
              columns: [
                { id: 'fknc_col', name: 'uid', type: 'INT', nullable: false },
              ],
              properties: {}, tags: {},
              constraints: [
                {
                  id: 'cst_fknc', name: 'fk_no_parent_cols', type: 'foreign_key',
                  columns: ['fknc_col'],
                  parentTable: 'unknown_table',
                },
              ],
              grants: [],
            }],
            views: [], grants: [],
          }],
          grants: [],
        }],
      },
    };
    currentProject = projectFKNoCols as typeof defaultProject;

    render(<TableConstraints tableId="tbl_fknc" />);
    expect(screen.getByText(/FOREIGN KEY \(uid\) REFERENCES unknown_table/)).toBeInTheDocument();
  });

  // ---------------------------------------------------------------
  // Edit dialog Cancel button closes dialog
  // ---------------------------------------------------------------
  test('edit dialog Cancel button closes the dialog', () => {
    render(<TableConstraints tableId="tbl_1" />);
    const editButtons = screen.getAllByTitle('Edit constraint');
    fireEvent.click(editButtons[0]);
    expect(screen.getByText('Edit Constraint')).toBeInTheDocument();
    fireEvent.click(screen.getByText('Cancel'));
    expect(screen.queryByText('Edit Constraint')).not.toBeInTheDocument();
  });

  // ---------------------------------------------------------------
  // The section has the correct CSS class
  // ---------------------------------------------------------------
  test('root element has table-constraints-section class', () => {
    const { container } = render(<TableConstraints tableId="tbl_1" />);
    expect(container.querySelector('.table-constraints-section')).not.toBeNull();
  });

  // ---------------------------------------------------------------
  // Constraints table renders correct number of rows
  // ---------------------------------------------------------------
  test('constraints table renders correct number of body rows', () => {
    const { container } = render(<TableConstraints tableId="tbl_1" />);
    const rows = container.querySelectorAll('.constraints-table tbody tr');
    expect(rows).toHaveLength(2);
  });
});
