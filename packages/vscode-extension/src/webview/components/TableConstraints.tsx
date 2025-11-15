import React, { useState } from 'react';
import { VSCodeButton } from '@vscode/webview-ui-toolkit/react';
import { Constraint, Table } from '../../providers/unity/models';
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

export const TableConstraints: React.FC<TableConstraintsProps> = ({ tableId }) => {
  const { project, addConstraint, dropConstraint } = useDesignerStore();
  const [addDialog, setAddDialog] = useState(false);
  const [editDialog, setEditDialog] = useState<{constraintId: string, constraint: Constraint} | null>(null);
  const [dropDialog, setDropDialog] = useState<{constraintId: string, name?: string} | null>(null);

  // Form state for adding/editing constraints (including type selection)
  const [formData, setFormData] = useState<any>({ type: 'primary_key' });

  // Find the table
  const table = React.useMemo(() => {
    if (!project) return null;
    for (const catalog of (project as any).state.catalogs) {
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
    const tables: Array<{id: string, name: string, catalogName: string, schemaName: string, columns: any[]}> = [];
    for (const catalog of (project as any).state.catalogs) {
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

  const handleAddConstraint = () => {
    if (!addDialog || !formData.type) return;

    const constraint: Omit<Constraint, 'id'> = {
      type: formData.type,
      name: formData.name || undefined,
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
    const editFormData: any = {
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

    // Drop old constraint and add new one
    dropConstraint(tableId, editDialog.constraintId);

    const constraint: Omit<Constraint, 'id'> = {
      type: formData.type,
      name: formData.name || undefined,
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
            const col = parentTableInfo.columns.find((c: any) => c.id === colId);
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
          onClick={() => setAddDialog(true)}
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
              <label>
                Constraint Type:
                <select
                  value={formData.type || 'primary_key'}
                  onChange={(e) => setFormData({...formData, type: e.target.value as 'primary_key' | 'foreign_key' | 'check'})}
                  style={{ width: '100%', padding: '6px', marginTop: '4px' }}
                >
                  <option value="primary_key">PRIMARY KEY</option>
                  <option value="foreign_key">FOREIGN KEY</option>
                  <option value="check">CHECK</option>
                </select>
              </label>

              <label>
                Constraint Name (optional):
                <input
                  type="text"
                  value={formData.name || ''}
                  onChange={(e) => setFormData({...formData, name: e.target.value})}
                  placeholder="e.g., orders_pk"
                />
              </label>

              {formData.type !== 'check' && (
                <label>
                  Columns:
                  <select
                    multiple
                    value={formData.columns || []}
                    onChange={(e) => {
                      const selected = Array.from(e.target.selectedOptions, option => option.value);
                      setFormData({...formData, columns: selected});
                    }}
                    size={Math.min(table.columns.length, 5)}
                  >
                    {table.columns.map(col => (
                      <option key={col.id} value={col.id}>{col.name} ({col.type})</option>
                    ))}
                  </select>
                  <p className="hint">Hold Cmd/Ctrl to select multiple columns</p>
                </label>
              )}

              {formData.type === 'primary_key' && (
                <label className="checkbox-label">
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
                  <label>
                    References Table:
                    <select
                      value={formData.parentTable || ''}
                      onChange={(e) => setFormData({...formData, parentTable: e.target.value, parentColumns: []})}
                      style={{ width: '100%', padding: '6px', marginTop: '4px' }}
                    >
                      <option value="">-- Select Parent Table --</option>
                      {allTables.map(t => (
                        <option key={t.id} value={t.id}>
                          {t.catalogName}.{t.schemaName}.{t.name}
                        </option>
                      ))}
                    </select>
                  </label>

                  {formData.parentTable && (
                    <label>
                      Parent Columns:
                      <select
                        multiple
                        value={formData.parentColumns || []}
                        onChange={(e) => {
                          const selected = Array.from(e.target.selectedOptions, option => option.value);
                          setFormData({...formData, parentColumns: selected});
                        }}
                        size={Math.min(allTables.find(t => t.id === formData.parentTable)?.columns?.length || 5, 5)}
                      >
                        {(allTables.find(t => t.id === formData.parentTable)?.columns || []).map((col: any) => (
                          <option key={col.id} value={col.id}>{col.name} ({col.type})</option>
                        ))}
                      </select>
                      <p className="hint">Hold Cmd/Ctrl to select multiple columns</p>
                    </label>
                  )}
                </>
              )}

              {formData.type === 'check' && (
                <label>
                  CHECK Expression:
                  <input
                    type="text"
                    value={formData.expression || ''}
                    onChange={(e) => setFormData({...formData, expression: e.target.value})}
                    placeholder="e.g., age > 0"
                  />
                  <p className="hint">SQL expression that must be true</p>
                </label>
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
              <label>
                Constraint Type:
                <select
                  value={formData.type || 'primary_key'}
                  onChange={(e) => setFormData({...formData, type: e.target.value as 'primary_key' | 'foreign_key' | 'check'})}
                  style={{ width: '100%', padding: '6px', marginTop: '4px' }}
                >
                  <option value="primary_key">PRIMARY KEY</option>
                  <option value="foreign_key">FOREIGN KEY</option>
                  <option value="check">CHECK</option>
                </select>
              </label>

              <label>
                Constraint Name (optional):
                <input
                  type="text"
                  value={formData.name || ''}
                  onChange={(e) => setFormData({...formData, name: e.target.value})}
                  placeholder="e.g., orders_pk"
                />
              </label>

              {formData.type !== 'check' && (
                <label>
                  Columns:
                  <select
                    multiple
                    value={formData.columns || []}
                    onChange={(e) => {
                      const selected = Array.from(e.target.selectedOptions, option => option.value);
                      setFormData({...formData, columns: selected});
                    }}
                    size={Math.min(table.columns.length, 5)}
                  >
                    {table.columns.map(col => (
                      <option key={col.id} value={col.id}>{col.name} ({col.type})</option>
                    ))}
                  </select>
                  <p className="hint">Hold Cmd/Ctrl to select multiple columns</p>
                </label>
              )}

              {formData.type === 'primary_key' && (
                <label className="checkbox-label">
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
                  <label>
                    References Table:
                    <select
                      value={formData.parentTable || ''}
                      onChange={(e) => setFormData({...formData, parentTable: e.target.value, parentColumns: []})}
                      style={{ width: '100%', padding: '6px', marginTop: '4px' }}
                    >
                      <option value="">-- Select Parent Table --</option>
                      {allTables.map(t => (
                        <option key={t.id} value={t.id}>
                          {t.catalogName}.{t.schemaName}.{t.name}
                        </option>
                      ))}
                    </select>
                  </label>

                  {formData.parentTable && (
                    <label>
                      Parent Columns:
                      <select
                        multiple
                        value={formData.parentColumns || []}
                        onChange={(e) => {
                          const selected = Array.from(e.target.selectedOptions, option => option.value);
                          setFormData({...formData, parentColumns: selected});
                        }}
                        size={Math.min(allTables.find(t => t.id === formData.parentTable)?.columns?.length || 5, 5)}
                      >
                        {(allTables.find(t => t.id === formData.parentTable)?.columns || []).map((col: any) => (
                          <option key={col.id} value={col.id}>{col.name} ({col.type})</option>
                        ))}
                      </select>
                      <p className="hint">Hold Cmd/Ctrl to select multiple columns</p>
                    </label>
                  )}
                </>
              )}

              {formData.type === 'check' && (
                <label>
                  CHECK Expression:
                  <input
                    type="text"
                    value={formData.expression || ''}
                    onChange={(e) => setFormData({...formData, expression: e.target.value})}
                    placeholder="e.g., age > 0"
                  />
                  <p className="hint">SQL expression that must be true</p>
                </label>
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
    </div>
  );
};

