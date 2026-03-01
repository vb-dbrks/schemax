import React, { useState } from 'react';
import { VSCodeButton, VSCodeDropdown, VSCodeOption, VSCodeTextField } from '@vscode/webview-ui-toolkit/react';
import type { Constraint, Table, UnityCatalog, UnityColumn } from '../models/unity';
import { useDesignerStore } from '../state/useDesignerStore';

// Codicon icons - theme-aware and vector-based
const IconEdit: React.FC = () => (
  <i slot="start" className="codicon codicon-edit" aria-hidden="true"></i>
);

const IconTrash: React.FC = () => (
  <i slot="start" className="codicon codicon-trash" aria-hidden="true"></i>
);

interface TableConstraintsProps {
  tableId: string;
}

interface ConstraintFormData {
  type: 'primary_key' | 'foreign_key' | 'check';
  name?: string;
  columns?: string[];
  timeseries?: boolean;
  parentTable?: string;
  parentColumns?: string[];
  expression?: string;
}

export const TableConstraints: React.FC<TableConstraintsProps> = ({ tableId }) => {
  const { project, addConstraint, updateConstraint, dropConstraint } = useDesignerStore();
  const [addDialog, setAddDialog] = useState(false);
  const [editDialog, setEditDialog] = useState<{constraintId: string, constraint: Constraint} | null>(null);
  const [dropDialog, setDropDialog] = useState<{constraintId: string, name?: string} | null>(null);
  const [pkWarningDialog, setPkWarningDialog] = useState(false);

  // Form state for adding/editing constraints (including type selection)
  const [formData, setFormData] = useState<ConstraintFormData>({ type: 'primary_key' });

  // Find the table
  const table = React.useMemo(() => {
    if (!project) return null;
    for (const catalog of project.state.catalogs as UnityCatalog[]) {
      for (const schema of catalog.schemas) {
        const t = schema.tables.find((t: Table) => t.id === tableId);
        if (t) return t;
      }
    }
    return null;
  }, [project, tableId]);

  // Get all tables for foreign key reference (excluding current table)
  const allTables = React.useMemo(() => {
    if (!project) return [];
    const tables: Array<{id: string, name: string, catalogName: string, schemaName: string, columns: UnityColumn[]}> = [];
    for (const catalog of project.state.catalogs as UnityCatalog[]) {
      for (const schema of catalog.schemas) {
        for (const t of schema.tables) {
          if (t.id !== tableId) { // Exclude current table
            tables.push({
              id: t.id,
              name: t.name,
              catalogName: catalog.name,
              schemaName: schema.name,
              columns: t.columns || []
            });
          }
        }
      }
    }
    return tables;
  }, [project, tableId]);

  if (!table) return null;

  const constraints = table.constraints || [];

  // Check if PRIMARY KEY already exists
  const hasPrimaryKey = constraints.some(c => c.type === 'primary_key');

  // Handle add constraint button click - just open dialog, no validation yet
  const handleAddConstraintClick = () => {
    setFormData({ type: 'primary_key' });
    setAddDialog(true);
  };

  const handleAddConstraint = () => {
    if (!addDialog || !formData.type) return;

    // Validate PRIMARY KEY: if user is trying to add a PRIMARY KEY and one already exists
    if (formData.type === 'primary_key' && hasPrimaryKey) {
      // Close add dialog and show warning
      setAddDialog(false);
      setPkWarningDialog(true);
      setFormData({ type: 'primary_key' });
      return;
    }

    const constraint: Omit<Constraint, 'id'> = {
      type: formData.type,
      name: formData.name?.trim() || '',
      columns: formData.columns || [],
    };

    // Add type-specific fields
    if (formData.type === 'primary_key') {
      if (formData.timeseries) constraint.timeseries = true;
    } else if (formData.type === 'foreign_key') {
      constraint.parentTable = formData.parentTable;
      constraint.parentColumns = formData.parentColumns;
    } else if (formData.type === 'check') {
      constraint.expression = formData.expression;
    }

    addConstraint(tableId, constraint);
    setAddDialog(false);
    setFormData({ type: 'primary_key' });
  };

  const handleDropConstraint = () => {
    if (!dropDialog) return;
    dropConstraint(tableId, dropDialog.constraintId);
    setDropDialog(null);
  };

  const handleOpenEditDialog = (constraint: Constraint) => {
    // Populate form data from existing constraint
    const editFormData: ConstraintFormData = {
      type: constraint.type,
      name: constraint.name || '',
      columns: constraint.columns || [],
    };

    if (constraint.type === 'primary_key') {
      editFormData.timeseries = constraint.timeseries || false;
    } else if (constraint.type === 'foreign_key') {
      editFormData.parentTable = constraint.parentTable || '';
      editFormData.parentColumns = constraint.parentColumns || [];
    } else if (constraint.type === 'check') {
      editFormData.expression = constraint.expression || '';
    }

    setFormData(editFormData);
    setEditDialog({ constraintId: constraint.id, constraint });
  };

  const handleSaveEdit = () => {
    if (!editDialog) return;

    const constraint: Omit<Constraint, 'id'> = {
      type: formData.type,
      name: formData.name?.trim() || '',
      columns: formData.columns || [],
    };

    // Add type-specific fields
    if (formData.type === 'primary_key') {
      if (formData.timeseries) constraint.timeseries = true;
    } else if (formData.type === 'foreign_key') {
      constraint.parentTable = formData.parentTable;
      constraint.parentColumns = formData.parentColumns;
    } else if (formData.type === 'check') {
      constraint.expression = formData.expression;
    }

    // Use updateConstraint which batches drop and add as a single atomic operation
    updateConstraint(tableId, editDialog.constraintId, constraint);

    setEditDialog(null);
    setFormData({ type: 'primary_key' });
  };

  const getConstraintDisplay = (constraint: Constraint): string => {
    switch (constraint.type) {
      case 'primary_key':
        return `PRIMARY KEY (${constraint.columns.map(colId => {
          const col = table.columns.find(c => c.id === colId);
          return col?.name || colId;
        }).join(', ')})${constraint.timeseries ? ' TIMESERIES' : ''}`;
      case 'foreign_key': {
        const childColumns = constraint.columns.map(colId => {
          const col = table.columns.find(c => c.id === colId);
          return col?.name || colId;
        }).join(', ');
        
        // Look up parent table name from ID
        const parentTableInfo = allTables.find(t => t.id === constraint.parentTable);
        const parentTableName = parentTableInfo 
          ? `${parentTableInfo.catalogName}.${parentTableInfo.schemaName}.${parentTableInfo.name}`
          : constraint.parentTable || '?';
        
        // Look up parent column names from IDs
        const parentColumns = (constraint.parentColumns || []).map(colId => {
          if (parentTableInfo) {
            const col = parentTableInfo.columns.find((c: UnityColumn) => c.id === colId);
            return col?.name || colId;
          }
          return colId;
        }).join(', ');
        
        return `FOREIGN KEY (${childColumns}) REFERENCES ${parentTableName}${parentColumns ? ` (${parentColumns})` : ''}`;
      }
      case 'check':
        return `CHECK (${constraint.expression || ''})`;
      default:
        return constraint.type;
    }
  };

  return (
    <div className="table-constraints-section">
      <h3>Constraints ({constraints.length})</h3>

      {constraints.length === 0 ? (
        <div className="empty-constraints">
          <p>No constraints defined.</p>
          <p className="hint">Add PRIMARY KEY, FOREIGN KEY, or CHECK constraints (informational only in Unity Catalog).</p>
        </div>
      ) : (
        <table className="constraints-table">
          <thead>
            <tr>
              <th>TYPE</th>
              <th>NAME</th>
              <th>DEFINITION</th>
              <th style={{ width: '120px' }}>ACTIONS</th>
            </tr>
          </thead>
          <tbody>
            {constraints.map((constraint) => (
              <tr key={constraint.id}>
                <td>
                  <span className={`constraint-type-badge ${constraint.type}`}>
                    {constraint.type.replace('_', ' ').toUpperCase()}
                  </span>
                </td>
                <td>{constraint.name || <span className="unnamed">unnamed</span>}</td>
                <td className="constraint-def">{getConstraintDisplay(constraint)}</td>
                <td className="actions-cell">
                  <VSCodeButton
                    appearance="icon"
                    onClick={() => handleOpenEditDialog(constraint)}
                    title="Edit constraint"
                  >
                    <IconEdit />
                  </VSCodeButton>
                  <VSCodeButton
                    appearance="icon"
                    onClick={() => setDropDialog({constraintId: constraint.id, name: constraint.name})}
                    title="Drop constraint"
                  >
                    <IconTrash />
                  </VSCodeButton>
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      )}

      <div className="constraint-actions">
        <button
          className="add-constraint-btn"
          onClick={handleAddConstraintClick}
        >
          + Add Constraint
        </button>
      </div>

      {/* Add Constraint Dialog */}
      {addDialog && (
        <div className="modal-overlay" onClick={() => setAddDialog(false)}>
          <div className="modal-content" onClick={(e) => e.stopPropagation()}>
            <h2>Add Constraint</h2>
            
            <div className="modal-body">
              <div className="constraint-form-field">
                <label className="constraint-form-label">Constraint type</label>
                <VSCodeDropdown
                  value={formData.type || 'primary_key'}
                  style={{ width: '100%' }}
                  onInput={(e) => setFormData({
                    ...formData,
                    type: (e.target as HTMLSelectElement).value as 'primary_key' | 'foreign_key' | 'check'
                  })}
                >
                  <VSCodeOption value="primary_key">PRIMARY KEY</VSCodeOption>
                  <VSCodeOption value="foreign_key">FOREIGN KEY</VSCodeOption>
                  <VSCodeOption value="check">CHECK</VSCodeOption>
                </VSCodeDropdown>
              </div>

              <div className="constraint-form-field">
                <label className="constraint-form-label">Constraint name (optional)</label>
                <VSCodeTextField
                  value={formData.name || ''}
                  placeholder="e.g., orders_pk"
                  style={{ width: '100%' }}
                  onInput={(e) => setFormData({...formData, name: (e.target as HTMLInputElement).value})}
                />
              </div>

              {formData.type !== 'check' && (
                <div className="constraint-form-field">
                  <label className="constraint-form-label">Columns — select one or more</label>
                  <div className="constraint-columns-list" role="group" aria-label="Select columns">
                    <div className="constraint-columns-list-actions">
                      <button
                        type="button"
                        className="constraint-list-link"
                        onClick={() => setFormData({...formData, columns: table.columns.map(c => c.id)})}
                      >
                        Select all
                      </button>
                      <span className="constraint-list-sep">·</span>
                      <button
                        type="button"
                        className="constraint-list-link"
                        onClick={() => setFormData({...formData, columns: []})}
                      >
                        Clear
                      </button>
                    </div>
                    <ul className="constraint-columns-checkbox-list">
                      {table.columns.map(col => (
                        <li key={col.id}>
                          <label className="constraint-checkbox-label">
                            <input
                              type="checkbox"
                              checked={(formData.columns || []).includes(col.id)}
                              onChange={(e) => {
                                const current = formData.columns || [];
                                const next = e.target.checked
                                  ? [...current, col.id]
                                  : current.filter((id: string) => id !== col.id);
                                setFormData({...formData, columns: next});
                              }}
                            />
                            <span>{col.name} ({col.type})</span>
                          </label>
                        </li>
                      ))}
                    </ul>
                  </div>
                </div>
              )}

              {formData.type === 'primary_key' && (
                <label className="checkbox-label constraint-form-field">
                  <input
                    type="checkbox"
                    checked={formData.timeseries || false}
                    onChange={(e) => setFormData({...formData, timeseries: e.target.checked})}
                  />
                  <span>TIMESERIES (for time-series data)</span>
                </label>
              )}

              {formData.type === 'foreign_key' && (
                <>
                  <div className="constraint-form-field">
                    <label className="constraint-form-label">References table</label>
                    <VSCodeDropdown
                      value={formData.parentTable || ''}
                      style={{ width: '100%' }}
                      onInput={(e) => setFormData({
                        ...formData,
                        parentTable: (e.target as HTMLSelectElement).value,
                        parentColumns: []
                      })}
                    >
                      <VSCodeOption value="">— Select table —</VSCodeOption>
                      {allTables.map(t => (
                        <VSCodeOption key={t.id} value={t.id}>
                          {t.catalogName}.{t.schemaName}.{t.name}
                        </VSCodeOption>
                      ))}
                    </VSCodeDropdown>
                  </div>

                  {formData.parentTable && (
                    <div className="constraint-form-field">
                      <label className="constraint-form-label">Parent columns — select one or more</label>
                      <div className="constraint-columns-list" role="group" aria-label="Select parent columns">
                        <div className="constraint-columns-list-actions">
                          <button
                            type="button"
                            className="constraint-list-link"
                            onClick={() => {
                              const parentCols = allTables.find(t => t.id === formData.parentTable)?.columns || [];
                              setFormData({...formData, parentColumns: parentCols.map((c: UnityColumn) => c.id)});
                            }}
                          >
                            Select all
                          </button>
                          <span className="constraint-list-sep">·</span>
                          <button
                            type="button"
                            className="constraint-list-link"
                            onClick={() => setFormData({...formData, parentColumns: []})}
                          >
                            Clear
                          </button>
                        </div>
                        <ul className="constraint-columns-checkbox-list">
                          {(allTables.find(t => t.id === formData.parentTable)?.columns || []).map((col: UnityColumn) => (
                            <li key={col.id}>
                              <label className="constraint-checkbox-label">
                                <input
                                  type="checkbox"
                                  checked={(formData.parentColumns || []).includes(col.id)}
                                  onChange={(e) => {
                                    const current = formData.parentColumns || [];
                                    const next = e.target.checked
                                      ? [...current, col.id]
                                      : current.filter((id: string) => id !== col.id);
                                    setFormData({...formData, parentColumns: next});
                                  }}
                                />
                                <span>{col.name} ({col.type})</span>
                              </label>
                            </li>
                          ))}
                        </ul>
                      </div>
                    </div>
                  )}
                </>
              )}

              {formData.type === 'check' && (
                <div className="constraint-form-field">
                  <label className="constraint-form-label">CHECK expression</label>
                  <VSCodeTextField
                    value={formData.expression || ''}
                    placeholder="e.g., age > 0"
                    style={{ width: '100%' }}
                    onInput={(e) => setFormData({...formData, expression: (e.target as HTMLInputElement).value})}
                  />
                  <p className="hint">SQL expression that must be true</p>
                </div>
              )}

              <p className="hint" style={{ marginTop: '16px', fontStyle: 'italic' }}>
                Note: Unity Catalog constraints are informational only (not enforced). They are used for query optimization and documentation purposes.
              </p>
            </div>

            <div className="modal-buttons">
              <VSCodeButton
                onClick={handleAddConstraint}
                disabled={
                  (formData.type !== 'check' && (!formData.columns || formData.columns.length === 0)) ||
                  (formData.type === 'check' && !formData.expression) ||
                  (formData.type === 'foreign_key' && !formData.parentTable)
                }
              >
                Add Constraint
              </VSCodeButton>
              <VSCodeButton
                appearance="secondary"
                onClick={() => { setAddDialog(false); setFormData({ type: 'primary_key' }); }}
              >
                Cancel
              </VSCodeButton>
            </div>
          </div>
        </div>
      )}

      {/* Edit Constraint Dialog */}
      {editDialog && (
        <div className="modal-overlay" onClick={() => { setEditDialog(null); setFormData({ type: 'primary_key' }); }}>
          <div className="modal-content" onClick={(e) => e.stopPropagation()}>
            <h2>Edit Constraint</h2>
            
            <div className="modal-body">
              <div className="constraint-form-field">
                <label className="constraint-form-label">Constraint type</label>
                <VSCodeDropdown
                  value={formData.type || 'primary_key'}
                  style={{ width: '100%' }}
                  onInput={(e) => setFormData({
                    ...formData,
                    type: (e.target as HTMLSelectElement).value as 'primary_key' | 'foreign_key' | 'check'
                  })}
                >
                  <VSCodeOption value="primary_key">PRIMARY KEY</VSCodeOption>
                  <VSCodeOption value="foreign_key">FOREIGN KEY</VSCodeOption>
                  <VSCodeOption value="check">CHECK</VSCodeOption>
                </VSCodeDropdown>
              </div>

              <div className="constraint-form-field">
                <label className="constraint-form-label">Constraint name (optional)</label>
                <VSCodeTextField
                  value={formData.name || ''}
                  placeholder="e.g., orders_pk"
                  style={{ width: '100%' }}
                  onInput={(e) => setFormData({...formData, name: (e.target as HTMLInputElement).value})}
                />
              </div>

              {formData.type !== 'check' && (
                <div className="constraint-form-field">
                  <label className="constraint-form-label">Columns — select one or more</label>
                  <div className="constraint-columns-list" role="group" aria-label="Select columns">
                    <div className="constraint-columns-list-actions">
                      <button
                        type="button"
                        className="constraint-list-link"
                        onClick={() => setFormData({...formData, columns: table.columns.map(c => c.id)})}
                      >
                        Select all
                      </button>
                      <span className="constraint-list-sep">·</span>
                      <button
                        type="button"
                        className="constraint-list-link"
                        onClick={() => setFormData({...formData, columns: []})}
                      >
                        Clear
                      </button>
                    </div>
                    <ul className="constraint-columns-checkbox-list">
                      {table.columns.map(col => (
                        <li key={col.id}>
                          <label className="constraint-checkbox-label">
                            <input
                              type="checkbox"
                              checked={(formData.columns || []).includes(col.id)}
                              onChange={(e) => {
                                const current = formData.columns || [];
                                const next = e.target.checked
                                  ? [...current, col.id]
                                  : current.filter((id: string) => id !== col.id);
                                setFormData({...formData, columns: next});
                              }}
                            />
                            <span>{col.name} ({col.type})</span>
                          </label>
                        </li>
                      ))}
                    </ul>
                  </div>
                </div>
              )}

              {formData.type === 'primary_key' && (
                <label className="checkbox-label constraint-form-field">
                  <input
                    type="checkbox"
                    checked={formData.timeseries || false}
                    onChange={(e) => setFormData({...formData, timeseries: e.target.checked})}
                  />
                  <span>TIMESERIES (for time-series data)</span>
                </label>
              )}

              {formData.type === 'foreign_key' && (
                <>
                  <div className="constraint-form-field">
                    <label className="constraint-form-label">References table</label>
                    <VSCodeDropdown
                      value={formData.parentTable || ''}
                      style={{ width: '100%' }}
                      onInput={(e) => setFormData({
                        ...formData,
                        parentTable: (e.target as HTMLSelectElement).value,
                        parentColumns: []
                      })}
                    >
                      <VSCodeOption value="">— Select table —</VSCodeOption>
                      {allTables.map(t => (
                        <VSCodeOption key={t.id} value={t.id}>
                          {t.catalogName}.{t.schemaName}.{t.name}
                        </VSCodeOption>
                      ))}
                    </VSCodeDropdown>
                  </div>

                  {formData.parentTable && (
                    <div className="constraint-form-field">
                      <label className="constraint-form-label">Parent columns — select one or more</label>
                      <div className="constraint-columns-list" role="group" aria-label="Select parent columns">
                        <div className="constraint-columns-list-actions">
                          <button
                            type="button"
                            className="constraint-list-link"
                            onClick={() => {
                              const parentCols = allTables.find(t => t.id === formData.parentTable)?.columns || [];
                              setFormData({...formData, parentColumns: parentCols.map((c: UnityColumn) => c.id)});
                            }}
                          >
                            Select all
                          </button>
                          <span className="constraint-list-sep">·</span>
                          <button
                            type="button"
                            className="constraint-list-link"
                            onClick={() => setFormData({...formData, parentColumns: []})}
                          >
                            Clear
                          </button>
                        </div>
                        <ul className="constraint-columns-checkbox-list">
                          {(allTables.find(t => t.id === formData.parentTable)?.columns || []).map((col: UnityColumn) => (
                            <li key={col.id}>
                              <label className="constraint-checkbox-label">
                                <input
                                  type="checkbox"
                                  checked={(formData.parentColumns || []).includes(col.id)}
                                  onChange={(e) => {
                                    const current = formData.parentColumns || [];
                                    const next = e.target.checked
                                      ? [...current, col.id]
                                      : current.filter((id: string) => id !== col.id);
                                    setFormData({...formData, parentColumns: next});
                                  }}
                                />
                                <span>{col.name} ({col.type})</span>
                              </label>
                            </li>
                          ))}
                        </ul>
                      </div>
                    </div>
                  )}
                </>
              )}

              {formData.type === 'check' && (
                <div className="constraint-form-field">
                  <label className="constraint-form-label">CHECK expression</label>
                  <VSCodeTextField
                    value={formData.expression || ''}
                    placeholder="e.g., age > 0"
                    style={{ width: '100%' }}
                    onInput={(e) => setFormData({...formData, expression: (e.target as HTMLInputElement).value})}
                  />
                  <p className="hint">SQL expression that must be true</p>
                </div>
              )}

              <p className="hint" style={{ marginTop: '16px', fontStyle: 'italic' }}>
                Note: Unity Catalog constraints are informational only (not enforced). They are used for query optimization and documentation purposes.
              </p>
            </div>

            <div className="modal-buttons">
              <VSCodeButton
                onClick={handleSaveEdit}
                disabled={
                  (formData.type !== 'check' && (!formData.columns || formData.columns.length === 0)) ||
                  (formData.type === 'check' && !formData.expression) ||
                  (formData.type === 'foreign_key' && !formData.parentTable)
                }
              >
                Save Changes
              </VSCodeButton>
              <VSCodeButton
                appearance="secondary"
                onClick={() => { setEditDialog(null); setFormData({ type: 'primary_key' }); }}
              >
                Cancel
              </VSCodeButton>
            </div>
          </div>
        </div>
      )}

      {/* Drop Confirmation Dialog */}
      {dropDialog && (
        <div className="modal-overlay" onClick={() => setDropDialog(null)}>
          <div className="modal-content" onClick={(e) => e.stopPropagation()}>
            <h2>Drop Constraint</h2>
            <p className="warning-text">
              Are you sure you want to drop constraint <strong>{dropDialog.name || 'unnamed'}</strong>?
            </p>
            <div className="modal-buttons">
              <VSCodeButton onClick={handleDropConstraint}>
                Drop
              </VSCodeButton>
              <VSCodeButton appearance="secondary" onClick={() => setDropDialog(null)}>
                Cancel
              </VSCodeButton>
            </div>
          </div>
        </div>
      )}

      {/* PRIMARY KEY Warning Dialog */}
      {pkWarningDialog && (
        <div className="modal-overlay" onClick={() => setPkWarningDialog(false)}>
          <div className="modal-content" onClick={(e) => e.stopPropagation()}>
            <h2>Primary Key Already Exists</h2>
            <p style={{ lineHeight: '1.6', marginBottom: '16px' }}>
              This table already has a PRIMARY KEY constraint. 
              Databricks allows only one PRIMARY KEY per table.
            </p>
            <p style={{ lineHeight: '1.6', marginBottom: '16px' }}>
              To add more columns to the existing PRIMARY KEY, use the edit button (
              <i className="codicon codicon-edit" style={{ fontSize: '12px' }}></i>
              ) next to the constraint.
            </p>
            <p style={{ lineHeight: '1.6' }}>
              To create a new PRIMARY KEY with different columns, first drop the existing constraint using the delete button (
              <i className="codicon codicon-trash" style={{ fontSize: '12px' }}></i>
              ).
            </p>
            <div className="modal-buttons">
              <VSCodeButton onClick={() => setPkWarningDialog(false)}>
                OK
              </VSCodeButton>
            </div>
          </div>
        </div>
      )}
    </div>
  );
};
