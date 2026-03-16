/**
 * Tests for ColumnGrid component - Inline column editing
 */

import React from 'react';
import { render, screen, within, fireEvent } from '@testing-library/react';
import { describe, test, expect, jest, beforeEach } from '@jest/globals';
import { ColumnGrid } from '../../../src/webview/components/ColumnGrid';
import type { Column } from '../../../src/webview/models/unity';

// Mock the Zustand store
const mockAddColumn = jest.fn();
const mockRenameColumn = jest.fn();
const mockDropColumn = jest.fn();
const mockChangeColumnType = jest.fn();
const mockSetColumnNullable = jest.fn();
const mockSetColumnComment = jest.fn();
const mockReorderColumns = jest.fn();
const mockSetColumnTag = jest.fn();
const mockUnsetColumnTag = jest.fn();

jest.mock('../../../src/webview/state/useDesignerStore', () => ({
  useDesignerStore: () => ({
    project: null,
    addColumn: mockAddColumn,
    renameColumn: mockRenameColumn,
    dropColumn: mockDropColumn,
    changeColumnType: mockChangeColumnType,
    setColumnNullable: mockSetColumnNullable,
    setColumnComment: mockSetColumnComment,
    reorderColumns: mockReorderColumns,
    setColumnTag: mockSetColumnTag,
    unsetColumnTag: mockUnsetColumnTag,
  }),
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

const columns: Column[] = [
  {
    id: 'col_1',
    name: 'id',
    type: 'INT',
    nullable: false,
    tags: { team: 'analytics', env: 'prod', pii: 'true' },
  },
  {
    id: 'col_2',
    name: 'name',
    type: 'STRING',
    nullable: true,
    comment: 'User name',
  },
  {
    id: 'col_3',
    name: 'age',
    type: 'INT',
    nullable: true,
  },
];

describe('ColumnGrid', () => {
  beforeEach(() => {
    jest.clearAllMocks();
  });

  // 1. Renders column names in the grid
  test('renders column names in the grid', () => {
    render(<ColumnGrid tableId="tbl_1" columns={columns} />);

    expect(screen.getByText('id')).toBeInTheDocument();
    expect(screen.getByText('name')).toBeInTheDocument();
    expect(screen.getByText('age')).toBeInTheDocument();
  });

  // 2. Renders column types
  test('renders column types', () => {
    render(<ColumnGrid tableId="tbl_1" columns={columns} />);

    // INT appears twice (id and age columns)
    const intElements = screen.getAllByText('INT');
    expect(intElements).toHaveLength(2);
    expect(screen.getByText('STRING')).toBeInTheDocument();
  });

  // 3. Shows nullable indicator via checkboxes
  test('shows nullable indicator as checkboxes', () => {
    render(<ColumnGrid tableId="tbl_1" columns={columns} />);

    const checkboxes = screen.getAllByRole('checkbox');
    // All 3 columns have a nullable checkbox
    expect(checkboxes).toHaveLength(3);

    // col_1 (id) is NOT nullable -> unchecked
    expect(checkboxes[0]).not.toBeChecked();
    // col_2 (name) is nullable -> checked
    expect(checkboxes[1]).toBeChecked();
    // col_3 (age) is nullable -> checked
    expect(checkboxes[2]).toBeChecked();
  });

  // 4. Shows column comment
  test('shows column comment', () => {
    render(<ColumnGrid tableId="tbl_1" columns={columns} />);

    expect(screen.getByText('User name')).toBeInTheDocument();
  });

  // 5. Shows tag badges (first 2 tags + "+N" for more)
  test('shows first 2 tag badges and +N indicator for extra tags', () => {
    render(<ColumnGrid tableId="tbl_1" columns={columns} />);

    // col_1 has 3 tags: team, env, pii - only first 2 shown as badges
    const tagBadges = document.querySelectorAll('.tag-badge');
    expect(tagBadges).toHaveLength(2);
    expect(tagBadges[0].textContent).toBe('team');
    expect(tagBadges[1].textContent).toBe('env');

    // "+1" for the remaining tag
    const moreIndicator = document.querySelector('.tag-more');
    expect(moreIndicator).not.toBeNull();
    expect(moreIndicator!.textContent).toBe('+1');
  });

  // 6. Shows em-dash when no tags
  test('shows em-dash when column has no tags', () => {
    render(<ColumnGrid tableId="tbl_1" columns={columns} />);

    // col_2 and col_3 have no tags, so each should show a "—" placeholder
    const noTagsSpans = document.querySelectorAll('.no-tags');
    expect(noTagsSpans).toHaveLength(2);
    expect(noTagsSpans[0].textContent).toBe('—');
  });

  // 7. Has Add Column button
  test('has Add Column button when columns exist', () => {
    render(<ColumnGrid tableId="tbl_1" columns={columns} />);

    const addButtons = screen.getAllByText('Add column');
    // The secondary "Add column" button below the table
    expect(addButtons.length).toBeGreaterThanOrEqual(1);
  });

  // 8. Has edit/delete action buttons per column
  test('has edit and delete action buttons per column', () => {
    render(<ColumnGrid tableId="tbl_1" columns={columns} />);

    const editButtons = screen.getAllByTitle('Edit column');
    const dropButtons = screen.getAllByTitle('Drop column');

    expect(editButtons).toHaveLength(3);
    expect(dropButtons).toHaveLength(3);
  });

  // 9. Shows empty state when no columns
  test('shows empty state when no columns are provided', () => {
    render(<ColumnGrid tableId="tbl_1" columns={[]} />);

    expect(screen.getByText('No columns defined yet')).toBeInTheDocument();
    expect(
      screen.getByText('Start by adding your first column. You can always reorder or refine it later.')
    ).toBeInTheDocument();
    // The empty state still has an "Add column" button
    expect(screen.getByText('Add column')).toBeInTheDocument();
  });

  // 10. Column header names
  test('renders correct column header names', () => {
    render(<ColumnGrid tableId="tbl_1" columns={columns} />);

    expect(screen.getByText('Name')).toBeInTheDocument();
    expect(screen.getByText('Type')).toBeInTheDocument();
    expect(screen.getByText('Nullable')).toBeInTheDocument();
    expect(screen.getByText('Comment')).toBeInTheDocument();
    expect(screen.getByText('Tags')).toBeInTheDocument();
    expect(screen.getByText('Actions')).toBeInTheDocument();
  });

  // 11. Renders multiple columns correctly (one row per column in tbody)
  test('renders multiple columns as separate table rows', () => {
    const { container } = render(<ColumnGrid tableId="tbl_1" columns={columns} />);

    const tbody = container.querySelector('tbody');
    expect(tbody).not.toBeNull();
    const rows = tbody!.querySelectorAll('tr');
    expect(rows).toHaveLength(3);
  });

  // Additional coverage: nullable checkboxes are disabled in view mode
  test('nullable checkboxes are disabled when not editing', () => {
    render(<ColumnGrid tableId="tbl_1" columns={columns} />);

    const checkboxes = screen.getAllByRole('checkbox');
    checkboxes.forEach((cb) => {
      expect(cb).toBeDisabled();
    });
  });

  // Additional coverage: drag handles shown for non-editing rows
  test('renders drag handles for each column row', () => {
    const { container } = render(<ColumnGrid tableId="tbl_1" columns={columns} />);

    const dragHandles = container.querySelectorAll('.drag-handle');
    expect(dragHandles).toHaveLength(3);
    // Each drag handle shows the drag icon text
    dragHandles.forEach((handle) => {
      expect(handle.textContent).toContain('⋮⋮');
    });
  });

  // Additional coverage: rows are draggable
  test('column rows are draggable', () => {
    const { container } = render(<ColumnGrid tableId="tbl_1" columns={columns} />);

    const tbody = container.querySelector('tbody');
    const rows = tbody!.querySelectorAll('tr');
    rows.forEach((row) => {
      expect(row.getAttribute('draggable')).toBe('true');
    });
  });

  // Additional coverage: comment column shows em-dash when no comment
  test('shows em-dash placeholder for columns without comments', () => {
    render(<ColumnGrid tableId="tbl_1" columns={columns} />);

    // col_1 has no comment, col_3 has no comment -> 2 empty comment placeholders
    const emptySpans = document.querySelectorAll('.empty');
    expect(emptySpans).toHaveLength(2);
    emptySpans.forEach((span) => {
      expect(span.textContent).toBe('—');
    });
  });

  // Additional coverage: tag badges have correct title attributes
  test('tag badges have tooltip with tag name and value', () => {
    render(<ColumnGrid tableId="tbl_1" columns={columns} />);

    const tagBadges = document.querySelectorAll('.tag-badge');
    expect(tagBadges[0].getAttribute('title')).toBe('team: analytics');
    expect(tagBadges[1].getAttribute('title')).toBe('env: prod');
  });

  // Additional coverage: renders with a single column that has exactly 2 tags
  test('shows exactly 2 tag badges with no +N when column has 2 tags', () => {
    const twoTagColumns: Column[] = [
      {
        id: 'col_x',
        name: 'email',
        type: 'STRING',
        nullable: false,
        tags: { pii: 'true', classification: 'sensitive' },
      },
    ];

    render(<ColumnGrid tableId="tbl_1" columns={twoTagColumns} />);

    const tagBadges = document.querySelectorAll('.tag-badge');
    expect(tagBadges).toHaveLength(2);

    // No "+N" indicator since there are exactly 2 tags
    const moreIndicator = document.querySelector('.tag-more');
    expect(moreIndicator).toBeNull();
  });

  // Additional coverage: empty state does not show the secondary Add column button
  test('empty state does not render the secondary Add column button outside the table', () => {
    const { container } = render(<ColumnGrid tableId="tbl_1" columns={[]} />);

    // The .add-column-button (secondary button below table) should not exist
    const addColumnBtn = container.querySelector('.add-column-button');
    expect(addColumnBtn).toBeNull();
  });

  // Additional coverage: column grid container class
  test('renders with column-grid class on the container div', () => {
    const { container } = render(<ColumnGrid tableId="tbl_1" columns={columns} />);

    expect(container.querySelector('.column-grid')).not.toBeNull();
  });

  // --- Interaction Tests ---

  // 1. Edit mode toggle: clicking "Edit column" enters edit mode with inline inputs
  test('clicking Edit column enters edit mode with inline inputs', () => {
    render(<ColumnGrid tableId="tbl_1" columns={columns} />);

    const editButtons = screen.getAllByTitle('Edit column');
    fireEvent.click(editButtons[0]); // Edit the first column (id)

    // Should show a text input with the column name
    const nameInput = screen.getByDisplayValue('id');
    expect(nameInput).toBeInTheDocument();
    expect(nameInput.tagName).toBe('INPUT');

    // Should show a select for the column type
    const typeSelect = screen.getByDisplayValue('INT');
    expect(typeSelect).toBeInTheDocument();
    expect(typeSelect.tagName).toBe('SELECT');

    // Should show Save and Cancel buttons
    expect(screen.getByTitle('Save changes')).toBeInTheDocument();
    expect(screen.getByTitle('Cancel')).toBeInTheDocument();

    // Edit and Drop buttons for this row should be gone
    // (only 2 edit buttons remain for the other 2 columns)
    expect(screen.getAllByTitle('Edit column')).toHaveLength(2);
    expect(screen.getAllByTitle('Drop column')).toHaveLength(2);
  });

  // 2. Save edit: change values and click save calls store functions
  test('save edit calls renameColumn, changeColumnType, setColumnNullable, setColumnComment', () => {
    render(<ColumnGrid tableId="tbl_1" columns={columns} />);

    // Enter edit mode for col_1 (id, INT, nullable=false, no comment)
    const editButtons = screen.getAllByTitle('Edit column');
    fireEvent.click(editButtons[0]);

    // Change name
    const nameInput = screen.getByDisplayValue('id');
    fireEvent.change(nameInput, { target: { value: 'user_id' } });

    // Change type
    const typeSelect = screen.getByDisplayValue('INT');
    fireEvent.change(typeSelect, { target: { value: 'BIGINT' } });

    // Toggle nullable (currently false -> true)
    const nullableCheckbox = screen.getByTitle('Toggle nullable');
    fireEvent.click(nullableCheckbox);

    // Change comment
    const commentInput = screen.getByPlaceholderText('Optional comment');
    fireEvent.change(commentInput, { target: { value: 'Primary key' } });

    // Click save
    fireEvent.click(screen.getByTitle('Save changes'));

    expect(mockRenameColumn).toHaveBeenCalledWith('tbl_1', 'col_1', 'user_id');
    expect(mockChangeColumnType).toHaveBeenCalledWith('tbl_1', 'col_1', 'BIGINT');
    expect(mockSetColumnNullable).toHaveBeenCalledWith('tbl_1', 'col_1', true);
    expect(mockSetColumnComment).toHaveBeenCalledWith('tbl_1', 'col_1', 'Primary key');
  });

  // 3. Cancel edit: reverts to view mode without calling store
  test('cancel edit reverts to view mode without calling store functions', () => {
    render(<ColumnGrid tableId="tbl_1" columns={columns} />);

    const editButtons = screen.getAllByTitle('Edit column');
    fireEvent.click(editButtons[0]);

    // Change name
    const nameInput = screen.getByDisplayValue('id');
    fireEvent.change(nameInput, { target: { value: 'changed_name' } });

    // Click cancel
    fireEvent.click(screen.getByTitle('Cancel'));

    // Should be back in view mode
    expect(screen.getAllByTitle('Edit column')).toHaveLength(3);
    expect(screen.getAllByTitle('Drop column')).toHaveLength(3);

    // Store should NOT have been called
    expect(mockRenameColumn).not.toHaveBeenCalled();
    expect(mockChangeColumnType).not.toHaveBeenCalled();
    expect(mockSetColumnNullable).not.toHaveBeenCalled();
    expect(mockSetColumnComment).not.toHaveBeenCalled();
  });

  // 4. Drop column dialog: click Drop column → confirm → dropColumn called
  test('drop column dialog confirms and calls dropColumn', () => {
    render(<ColumnGrid tableId="tbl_1" columns={columns} />);

    const dropButtons = screen.getAllByTitle('Drop column');
    fireEvent.click(dropButtons[1]); // Drop the second column (name)

    // Confirmation dialog should appear
    expect(screen.getByText('Confirm Drop Column')).toBeInTheDocument();
    expect(screen.getByText(/Are you sure you want to drop column "name"\?/)).toBeInTheDocument();

    // Click Drop to confirm
    fireEvent.click(screen.getByText('Drop'));

    expect(mockDropColumn).toHaveBeenCalledWith('tbl_1', 'col_2');
  });

  // 4b. Drop column dialog: cancel closes dialog without calling dropColumn
  test('drop column dialog cancel does not call dropColumn', () => {
    render(<ColumnGrid tableId="tbl_1" columns={columns} />);

    const dropButtons = screen.getAllByTitle('Drop column');
    fireEvent.click(dropButtons[0]);

    expect(screen.getByText('Confirm Drop Column')).toBeInTheDocument();

    // Click Cancel
    fireEvent.click(screen.getByText('Cancel'));

    expect(mockDropColumn).not.toHaveBeenCalled();
    // Dialog should be closed
    expect(screen.queryByText('Confirm Drop Column')).not.toBeInTheDocument();
  });

  // 5. Add column dialog: open, fill in, submit
  test('add column dialog opens, accepts input, and calls addColumn', () => {
    render(<ColumnGrid tableId="tbl_1" columns={columns} />);

    // Click "Add column" button (the secondary one below the table)
    const addButtons = screen.getAllByText('Add column');
    fireEvent.click(addButtons[0]);

    // Dialog should appear
    expect(screen.getByText('Add Column')).toBeInTheDocument();

    // Fill in name via onInput (the mock uses onChange but component uses onInput)
    const nameInput = screen.getByPlaceholderText('Enter column name');
    fireEvent.input(nameInput, { target: { value: 'email' } });

    // Change type
    const typeDropdown = screen.getByDisplayValue('STRING');
    fireEvent.input(typeDropdown, { target: { value: 'INT' } });

    // Uncheck nullable - the add dialog has a native checkbox for nullable
    // Find the checkbox in the add dialog that is not disabled (all grid checkboxes are disabled)
    const allCheckboxes = screen.getAllByRole('checkbox');
    const addDialogCheckbox = allCheckboxes.find(
      (cb) => !(cb as HTMLInputElement).disabled
    ) as HTMLInputElement;
    expect(addDialogCheckbox).toBeDefined();
    expect(addDialogCheckbox.checked).toBe(true); // default is nullable=true
    fireEvent.click(addDialogCheckbox);

    // Fill in comment
    const commentInput = screen.getByPlaceholderText('Column description');
    fireEvent.input(commentInput, { target: { value: 'User email' } });

    // Click "Add" to submit
    fireEvent.click(screen.getByText('Add'));

    expect(mockAddColumn).toHaveBeenCalledWith('tbl_1', 'email', 'INT', false, 'User email', {});
  });

  // 5b. Add column dialog: cancel closes dialog
  test('add column dialog cancel closes without calling addColumn', () => {
    render(<ColumnGrid tableId="tbl_1" columns={columns} />);

    const addButtons = screen.getAllByText('Add column');
    fireEvent.click(addButtons[0]);

    expect(screen.getByText('Add Column')).toBeInTheDocument();

    // Click Cancel in the dialog
    const cancelButtons = screen.getAllByText('Cancel');
    // The dialog Cancel button
    fireEvent.click(cancelButtons[cancelButtons.length - 1]);

    expect(mockAddColumn).not.toHaveBeenCalled();
    expect(screen.queryByText('Add Column')).not.toBeInTheDocument();
  });

  // 6. Tags dialog: click manage tags button in edit mode opens tags dialog
  test('tags dialog opens when clicking manage tags in edit mode', () => {
    render(<ColumnGrid tableId="tbl_1" columns={columns} />);

    // Enter edit mode for col_1 (which has tags)
    const editButtons = screen.getAllByTitle('Edit column');
    fireEvent.click(editButtons[0]);

    // Click the "Manage tags" button
    const manageTagsBtn = screen.getByTitle('Manage tags');
    fireEvent.click(manageTagsBtn);

    // Tags dialog should appear
    expect(screen.getByText('Column Tags: id')).toBeInTheDocument();
    expect(screen.getByText(/Tags are used for governance/)).toBeInTheDocument();
  });

  // 7. Nullable checkbox toggle in edit mode
  test('nullable checkbox toggles in edit mode', () => {
    render(<ColumnGrid tableId="tbl_1" columns={columns} />);

    // Enter edit mode for col_2 (nullable=true)
    const editButtons = screen.getAllByTitle('Edit column');
    fireEvent.click(editButtons[1]);

    // Find the enabled nullable checkbox (Toggle nullable title)
    const nullableCheckbox = screen.getByTitle('Toggle nullable') as HTMLInputElement;
    expect(nullableCheckbox).not.toBeDisabled();
    expect(nullableCheckbox.checked).toBe(true);

    // Toggle it
    fireEvent.click(nullableCheckbox);

    // Save and verify setColumnNullable is called with false
    fireEvent.click(screen.getByTitle('Save changes'));
    expect(mockSetColumnNullable).toHaveBeenCalledWith('tbl_1', 'col_2', false);
  });

  // 8. Save without changes does not call store functions
  test('save without changes does not call any store functions', () => {
    render(<ColumnGrid tableId="tbl_1" columns={columns} />);

    const editButtons = screen.getAllByTitle('Edit column');
    fireEvent.click(editButtons[1]); // Edit col_2 (name, STRING, nullable=true, comment='User name')

    // Immediately save without changes
    fireEvent.click(screen.getByTitle('Save changes'));

    expect(mockRenameColumn).not.toHaveBeenCalled();
    expect(mockChangeColumnType).not.toHaveBeenCalled();
    expect(mockSetColumnNullable).not.toHaveBeenCalled();
    expect(mockSetColumnComment).not.toHaveBeenCalled();
  });

  // 9. Edit mode hides drag handle
  test('edit mode hides drag handle for the editing row', () => {
    const { container } = render(<ColumnGrid tableId="tbl_1" columns={columns} />);

    const editButtons = screen.getAllByTitle('Edit column');
    fireEvent.click(editButtons[0]);

    const dragHandles = container.querySelectorAll('.drag-handle');
    // First drag handle should be empty (editing row)
    expect(dragHandles[0].textContent).not.toContain('⋮⋮');
    // Others still have drag handles
    expect(dragHandles[1].textContent).toContain('⋮⋮');
    expect(dragHandles[2].textContent).toContain('⋮⋮');
  });

  // 10. Add column from empty state
  test('add column from empty state opens add dialog', () => {
    render(<ColumnGrid tableId="tbl_1" columns={[]} />);

    fireEvent.click(screen.getByText('Add column'));

    expect(screen.getByText('Add Column')).toBeInTheDocument();
    expect(screen.getByPlaceholderText('Enter column name')).toBeInTheDocument();
  });

  // 11. Comment input shows in edit mode with existing comment
  test('edit mode shows existing comment in the input', () => {
    render(<ColumnGrid tableId="tbl_1" columns={columns} />);

    // Edit col_2 which has comment 'User name'
    const editButtons = screen.getAllByTitle('Edit column');
    fireEvent.click(editButtons[1]);

    const commentInput = screen.getByDisplayValue('User name');
    expect(commentInput).toBeInTheDocument();
    expect(commentInput.getAttribute('placeholder')).toBe('Optional comment');
  });

  // 12. Edit mode row is not draggable
  test('editing row is not draggable', () => {
    const { container } = render(<ColumnGrid tableId="tbl_1" columns={columns} />);

    const editButtons = screen.getAllByTitle('Edit column');
    fireEvent.click(editButtons[0]);

    const tbody = container.querySelector('tbody');
    const rows = tbody!.querySelectorAll('tr');
    // First row (editing) should not be draggable
    expect(rows[0].getAttribute('draggable')).toBe('false');
    // Other rows remain draggable
    expect(rows[1].getAttribute('draggable')).toBe('true');
    expect(rows[2].getAttribute('draggable')).toBe('true');
  });
});
