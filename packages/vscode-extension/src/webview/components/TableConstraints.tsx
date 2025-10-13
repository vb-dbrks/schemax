import React, { useState } from 'react';
import { Constraint, Table } from '../../shared/model';
import { useDesignerStore } from '../state/useDesignerStore';

interface TableConstraintsProps {
  tableId: string;
}

export const TableConstraints: React.FC<TableConstraintsProps> = ({ tableId }) => {
  const { project, addConstraint, dropConstraint } = useDesignerStore();
  const [addDialog, setAddDialog] = useState<{type: 'primary_key' | 'foreign_key' | 'check'} | null>(null);
  const [dropDialog, setDropDialog] = useState<{constraintId: string, name?: string} | null>(null);

  // Form state for adding constraints
  const [formData, setFormData] = useState<any>({});

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

  if (!table) return null;

  const constraints = table.constraints || [];

  const handleAddConstraint = () => {
    if (!addDialog) return;

    const constraint: Omit<Constraint, 'id'> = {
      type: addDialog.type,
      name: formData.name || undefined,
      columns: formData.columns || [],
    };

    // Add type-specific fields
    if (addDialog.type === 'primary_key') {
      if (formData.timeseries) constraint.timeseries = true;
    } else if (addDialog.type === 'foreign_key') {
      constraint.parentTable = formData.parentTable;
      constraint.parentColumns = formData.parentColumns;
      if (formData.matchFull) constraint.matchFull = true;
      if (formData.onUpdate) constraint.onUpdate = 'NO_ACTION';
      if (formData.onDelete) constraint.onDelete = 'NO_ACTION';
    } else if (addDialog.type === 'check') {
      constraint.expression = formData.expression;
    }

    // Add constraint options
    if (formData.notEnforced) constraint.notEnforced = true;
    if (formData.deferrable) constraint.deferrable = true;
    if (formData.initiallyDeferred) constraint.initiallyDeferred = true;
    if (formData.rely) constraint.rely = true;

    addConstraint(tableId, constraint);
    setAddDialog(null);
    setFormData({});
  };

  const handleDropConstraint = () => {
    if (!dropDialog) return;
    dropConstraint(tableId, dropDialog.constraintId);
    setDropDialog(null);
  };

  const getConstraintDisplay = (constraint: Constraint): string => {
    switch (constraint.type) {
      case 'primary_key':
        return `PRIMARY KEY (${constraint.columns.map(colId => {
          const col = table.columns.find(c => c.id === colId);
          return col?.name || colId;
        }).join(', ')})${constraint.timeseries ? ' TIMESERIES' : ''}`;
      case 'foreign_key':
        return `FOREIGN KEY (${constraint.columns.map(colId => {
          const col = table.columns.find(c => c.id === colId);
          return col?.name || colId;
        }).join(', ')}) REFERENCES ${constraint.parentTable || '?'}`;
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
          <p className="hint">Add PRIMARY KEY, FOREIGN KEY, or CHECK constraints for data integrity.</p>
        </div>
      ) : (
        <table className="constraints-table">
          <thead>
            <tr>
              <th>Type</th>
              <th>Name</th>
              <th>Definition</th>
              <th>Options</th>
              <th style={{ width: '100px' }}>Actions</th>
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
                <td>
                  {constraint.rely && <span className="option-badge">RELY</span>}
                  {constraint.notEnforced && <span className="option-badge">NOT ENFORCED</span>}
                  {constraint.deferrable && <span className="option-badge">DEFERRABLE</span>}
                </td>
                <td>
                  <button
                    className="delete-btn"
                    onClick={() => setDropDialog({constraintId: constraint.id, name: constraint.name})}
                    title="Drop Constraint"
                  >
                    Drop
                  </button>
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      )}

      <div className="constraint-actions">
        <button
          className="add-constraint-btn"
          onClick={() => setAddDialog({type: 'primary_key'})}
        >
          + PRIMARY KEY
        </button>
        <button
          className="add-constraint-btn"
          onClick={() => setAddDialog({type: 'foreign_key'})}
        >
          + FOREIGN KEY
        </button>
        <button
          className="add-constraint-btn"
          onClick={() => setAddDialog({type: 'check'})}
        >
          + CHECK
        </button>
      </div>

      {/* Add Constraint Dialog */}
      {addDialog && (
        <div className="modal-overlay" onClick={() => setAddDialog(null)}>
          <div className="modal-content" onClick={(e) => e.stopPropagation()}>
            <h2>Add {addDialog.type.replace('_', ' ').toUpperCase()} Constraint</h2>
            
            <div className="modal-body">
              <label>
                Constraint Name (optional):
                <input
                  type="text"
                  value={formData.name || ''}
                  onChange={(e) => setFormData({...formData, name: e.target.value})}
                  placeholder="e.g., orders_pk"
                />
              </label>

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

              {addDialog.type === 'primary_key' && (
                <label className="checkbox-label">
                  <input
                    type="checkbox"
                    checked={formData.timeseries || false}
                    onChange={(e) => setFormData({...formData, timeseries: e.target.checked})}
                  />
                  <span>TIMESERIES (for time-series data)</span>
                </label>
              )}

              {addDialog.type === 'foreign_key' && (
                <>
                  <label>
                    Parent Table ID:
                    <input
                      type="text"
                      value={formData.parentTable || ''}
                      onChange={(e) => setFormData({...formData, parentTable: e.target.value})}
                      placeholder="tbl_..."
                    />
                    <p className="hint">Reference to parent table ID</p>
                  </label>

                  <label>
                    Parent Columns (comma-separated IDs):
                    <input
                      type="text"
                      value={formData.parentColumnsStr || ''}
                      onChange={(e) => {
                        setFormData({
                          ...formData,
                          parentColumnsStr: e.target.value,
                          parentColumns: e.target.value.split(',').map(s => s.trim()).filter(Boolean)
                        });
                      }}
                      placeholder="col_..., col_..."
                    />
                  </label>

                  <label className="checkbox-label">
                    <input
                      type="checkbox"
                      checked={formData.matchFull || false}
                      onChange={(e) => setFormData({...formData, matchFull: e.target.checked})}
                    />
                    <span>MATCH FULL</span>
                  </label>
                </>
              )}

              {addDialog.type === 'check' && (
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

              <div className="constraint-options">
                <h4>Constraint Options</h4>
                <label className="checkbox-label">
                  <input
                    type="checkbox"
                    checked={formData.notEnforced || false}
                    onChange={(e) => setFormData({...formData, notEnforced: e.target.checked})}
                  />
                  <span>NOT ENFORCED</span>
                </label>
                <label className="checkbox-label">
                  <input
                    type="checkbox"
                    checked={formData.deferrable || false}
                    onChange={(e) => setFormData({...formData, deferrable: e.target.checked})}
                  />
                  <span>DEFERRABLE</span>
                </label>
                <label className="checkbox-label">
                  <input
                    type="checkbox"
                    checked={formData.initiallyDeferred || false}
                    onChange={(e) => setFormData({...formData, initiallyDeferred: e.target.checked})}
                  />
                  <span>INITIALLY DEFERRED</span>
                </label>
                <label className="checkbox-label">
                  <input
                    type="checkbox"
                    checked={formData.rely || false}
                    onChange={(e) => setFormData({...formData, rely: e.target.checked})}
                  />
                  <span>RELY (for query optimization)</span>
                </label>
              </div>
            </div>

            <div className="modal-actions">
              <button className="cancel-btn" onClick={() => { setAddDialog(null); setFormData({}); }}>
                Cancel
              </button>
              <button
                className="confirm-btn"
                onClick={handleAddConstraint}
                disabled={!formData.columns || formData.columns.length === 0 || (addDialog.type === 'check' && !formData.expression)}
              >
                Add Constraint
              </button>
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
            <div className="modal-actions">
              <button className="cancel-btn" onClick={() => setDropDialog(null)}>
                Cancel
              </button>
              <button className="confirm-btn delete" onClick={handleDropConstraint}>
                Drop
              </button>
            </div>
          </div>
        </div>
      )}
    </div>
  );
};

