import React, { useState } from 'react';
import { VSCodeButton } from '@vscode/webview-ui-toolkit/react';
import { Table, RowFilter, ColumnMask } from '../../providers/unity/models';
import { useDesignerStore } from '../state/useDesignerStore';
import { parsePrivileges } from '../utils/grants';

const IconEdit: React.FC = () => (
  <i slot="start" className="codicon codicon-edit" aria-hidden="true"></i>
);
const IconTrash: React.FC = () => (
  <i slot="start" className="codicon codicon-trash" aria-hidden="true"></i>
);

interface SecurityGovernanceProps {
  tableId: string;
}

export const SecurityGovernance: React.FC<SecurityGovernanceProps> = ({ tableId }) => {
  const { project, addRowFilter, updateRowFilter, removeRowFilter, addColumnMask, updateColumnMask, removeColumnMask, addGrant, revokeGrant } = useDesignerStore();

  const [addFilterDialog, setAddFilterDialog] = useState(false);
  const [addMaskDialog, setAddMaskDialog] = useState(false);
  const [addGrantDialog, setAddGrantDialog] = useState(false);
  const [editingGrant, setEditingGrant] = useState<{ principal: string; privileges: string[] } | null>(null);
  const [deleteGrantDialog, setDeleteGrantDialog] = useState<{ principal: string } | null>(null);
  const [grantForm, setGrantForm] = useState({ principal: '', privileges: '' });
  const [editFilterDialog, setEditFilterDialog] = useState<RowFilter | null>(null);
  const [editMaskDialog, setEditMaskDialog] = useState<ColumnMask | null>(null);
  const [deleteFilterDialog, setDeleteFilterDialog] = useState<{filterId: string, name: string} | null>(null);
  const [deleteMaskDialog, setDeleteMaskDialog] = useState<{maskId: string, name: string} | null>(null);

  const [filterForm, setFilterForm] = useState({name: '', udfExpression: '', enabled: true, description: ''});
  const [maskForm, setMaskForm] = useState({columnId: '', name: '', maskFunction: '', enabled: true, description: ''});

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

  const rowFilters = table.rowFilters || [];
  const columnMasks = table.columnMasks || [];
  const grants = table.grants || [];

  const handleAddFilter = () => {
    if (!filterForm.name || !filterForm.udfExpression) return;
    addRowFilter(tableId, filterForm.name, filterForm.udfExpression, filterForm.enabled, filterForm.description);
    setAddFilterDialog(false);
    setFilterForm({name: '', udfExpression: '', enabled: true, description: ''});
  };

  const handleUpdateFilter = () => {
    if (!editFilterDialog) return;
    updateRowFilter(tableId, editFilterDialog.id, {
      name: filterForm.name,
      udfExpression: filterForm.udfExpression,
      enabled: filterForm.enabled,
      description: filterForm.description
    });
    setEditFilterDialog(null);
    setFilterForm({name: '', udfExpression: '', enabled: true, description: ''});
  };

  const handleDeleteFilter = () => {
    if (!deleteFilterDialog) return;
    removeRowFilter(tableId, deleteFilterDialog.filterId);
    setDeleteFilterDialog(null);
  };

  const handleAddMask = () => {
    if (!maskForm.columnId || !maskForm.name || !maskForm.maskFunction) return;
    addColumnMask(tableId, maskForm.columnId, maskForm.name, maskForm.maskFunction, maskForm.enabled, maskForm.description);
    setAddMaskDialog(false);
    setMaskForm({columnId: '', name: '', maskFunction: '', enabled: true, description: ''});
  };

  const handleUpdateMask = () => {
    if (!editMaskDialog) return;
    updateColumnMask(tableId, editMaskDialog.id, {
      name: maskForm.name,
      maskFunction: maskForm.maskFunction,
      enabled: maskForm.enabled,
      description: maskForm.description
    });
    setEditMaskDialog(null);
    setMaskForm({columnId: '', name: '', maskFunction: '', enabled: true, description: ''});
  };

  const handleDeleteMask = () => {
    if (!deleteMaskDialog) return;
    removeColumnMask(tableId, deleteMaskDialog.maskId);
    setDeleteMaskDialog(null);
  };

  const getColumnName = (columnId: string): string => {
    const col = table.columns.find(c => c.id === columnId);
    return col?.name || columnId;
  };

  const handleAddGrant = () => {
    const principal = grantForm.principal.trim();
    const privs = parsePrivileges(grantForm.privileges);
    if (!principal || privs.length === 0) return;
    if (editingGrant) revokeGrant('table', tableId, editingGrant.principal);
    addGrant('table', tableId, principal, privs);
    setAddGrantDialog(false);
    setEditingGrant(null);
    setGrantForm({ principal: '', privileges: '' });
  };

  const handleRevokeGrant = (principal: string) => {
    revokeGrant('table', tableId, principal);
    setDeleteGrantDialog(null);
  };

  return (
    <div className="security-governance-section">
      <h3>Security & Governance</h3>

      {/* Row Filters */}
      <div className="row-filters-subsection">
        <h4>Row Filters ({rowFilters.length})</h4>
        {rowFilters.length === 0 ? (
          <div className="empty-filters">
            <p>No row filters defined.</p>
            <p className="hint">Add row-level security filters to control data access based on user identity.</p>
          </div>
        ) : (
          <table className="filters-table">
            <thead>
              <tr>
                <th>Status</th>
                <th>Name</th>
                <th>UDF Expression</th>
                <th>Description</th>
                <th style={{ width: '150px' }}>Actions</th>
              </tr>
            </thead>
            <tbody>
              {rowFilters.map((filter) => (
                <tr key={filter.id}>
                  <td>
                    <span className={`status-badge ${filter.enabled ? 'enabled' : 'disabled'}`}>
                      {filter.enabled ? 'Enabled' : 'Disabled'}
                    </span>
                  </td>
                  <td>{filter.name}</td>
                  <td className="code-cell">{filter.udfExpression}</td>
                  <td>{filter.description || <span className="unnamed">—</span>}</td>
                  <td>
                    <button
                      className="edit-btn-small"
                      onClick={() => {
                        setEditFilterDialog(filter);
                        setFilterForm({
                          name: filter.name,
                          udfExpression: filter.udfExpression,
                          enabled: filter.enabled,
                          description: filter.description || ''
                        });
                      }}
                      title="Edit Filter"
                    >
                      Edit
                    </button>
                    <button
                      className="delete-btn-small"
                      onClick={() => setDeleteFilterDialog({filterId: filter.id, name: filter.name})}
                      title="Remove Filter"
                    >
                      Remove
                    </button>
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        )}
        <button className="add-filter-btn" onClick={() => setAddFilterDialog(true)}>
          + Add Row Filter
        </button>
      </div>

      {/* Column Masks */}
      <div className="column-masks-subsection">
        <h4>Column Masks ({columnMasks.length})</h4>
        {columnMasks.length === 0 ? (
          <div className="empty-masks">
            <p>No column masks defined.</p>
            <p className="hint">Add column-level data masking to redact sensitive information.</p>
          </div>
        ) : (
          <table className="masks-table">
            <thead>
              <tr>
                <th>Status</th>
                <th>Name</th>
                <th>Column</th>
                <th>Mask Function</th>
                <th>Description</th>
                <th style={{ width: '150px' }}>Actions</th>
              </tr>
            </thead>
            <tbody>
              {columnMasks.map((mask) => (
                <tr key={mask.id}>
                  <td>
                    <span className={`status-badge ${mask.enabled ? 'enabled' : 'disabled'}`}>
                      {mask.enabled ? 'Enabled' : 'Disabled'}
                    </span>
                  </td>
                  <td>{mask.name}</td>
                  <td>{getColumnName(mask.columnId)}</td>
                  <td className="code-cell">{mask.maskFunction}</td>
                  <td>{mask.description || <span className="unnamed">—</span>}</td>
                  <td>
                    <button
                      className="edit-btn-small"
                      onClick={() => {
                        setEditMaskDialog(mask);
                        setMaskForm({
                          columnId: mask.columnId,
                          name: mask.name,
                          maskFunction: mask.maskFunction,
                          enabled: mask.enabled,
                          description: mask.description || ''
                        });
                      }}
                      title="Edit Mask"
                    >
                      Edit
                    </button>
                    <button
                      className="delete-btn-small"
                      onClick={() => setDeleteMaskDialog({maskId: mask.id, name: mask.name})}
                      title="Remove Mask"
                    >
                      Remove
                    </button>
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        )}
        <button className="add-mask-btn" onClick={() => setAddMaskDialog(true)}>
          + Add Column Mask
        </button>
      </div>

      {/* Table Grants - same layout as catalog/schema/view */}
      <div className="table-properties-section">
        <h3>Grants ({grants.length})</h3>
        {grants.length === 0 && !addGrantDialog ? (
          <div className="empty-properties">
            <p>No grants defined. Grant privileges (e.g. SELECT, MODIFY) to users or groups.</p>
          </div>
        ) : (
          <table className="properties-table">
            <thead>
              <tr>
                <th>Principal</th>
                <th>Privileges</th>
                <th>Actions</th>
              </tr>
            </thead>
            <tbody>
              {grants.map((g) => (
                <tr key={g.principal}>
                  <td>{g.principal}</td>
                  <td>{(g.privileges || []).join(', ')}</td>
                  <td>
                    <VSCodeButton
                      appearance="icon"
                      onClick={() => { setEditingGrant({ principal: g.principal, privileges: g.privileges || [] }); setGrantForm({ principal: g.principal, privileges: (g.privileges || []).join(', ') }); setAddGrantDialog(true); }}
                      title="Edit grant"
                    >
                      <IconEdit />
                    </VSCodeButton>
                    <VSCodeButton
                      appearance="icon"
                      onClick={() => setDeleteGrantDialog({ principal: g.principal })}
                      title="Revoke all"
                    >
                      <IconTrash />
                    </VSCodeButton>
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        )}
        <button className="add-property-btn" onClick={() => { setEditingGrant(null); setGrantForm({ principal: '', privileges: '' }); setAddGrantDialog(true); }}>
          + Add Grant
        </button>
      </div>

      {/* Add Row Filter Dialog */}
      {addFilterDialog && (
        <div className="modal-overlay" onClick={() => setAddFilterDialog(false)}>
          <div className="modal-content" onClick={(e) => e.stopPropagation()}>
            <h2>Add Row Filter</h2>
            <div className="modal-body">
              <label>
                Name:
                <input
                  type="text"
                  value={filterForm.name}
                  onChange={(e) => setFilterForm({...filterForm, name: e.target.value})}
                  placeholder="e.g., region_filter"
                />
              </label>

              <label>
                UDF Expression:
                <textarea
                  value={filterForm.udfExpression}
                  onChange={(e) => setFilterForm({...filterForm, udfExpression: e.target.value})}
                  placeholder="e.g., region = current_user()"
                  rows={3}
                />
                <p className="hint">SQL expression returning boolean (true = show row)</p>
              </label>

              <label>
                Description (optional):
                <input
                  type="text"
                  value={filterForm.description}
                  onChange={(e) => setFilterForm({...filterForm, description: e.target.value})}
                  placeholder="Filter description"
                />
              </label>

              <label className="checkbox-label">
                <input
                  type="checkbox"
                  checked={filterForm.enabled}
                  onChange={(e) => setFilterForm({...filterForm, enabled: e.target.checked})}
                />
                <span>Enabled</span>
              </label>
            </div>
            <div className="modal-actions">
              <button className="cancel-btn" onClick={() => { setAddFilterDialog(false); setFilterForm({name: '', udfExpression: '', enabled: true, description: ''}); }}>
                Cancel
              </button>
              <button
                className="confirm-btn"
                onClick={handleAddFilter}
                disabled={!filterForm.name || !filterForm.udfExpression}
              >
                Add Filter
              </button>
            </div>
          </div>
        </div>
      )}

      {/* Edit Row Filter Dialog */}
      {editFilterDialog && (
        <div className="modal-overlay" onClick={() => setEditFilterDialog(null)}>
          <div className="modal-content" onClick={(e) => e.stopPropagation()}>
            <h2>Edit Row Filter</h2>
            <div className="modal-body">
              <label>
                Name:
                <input
                  type="text"
                  value={filterForm.name}
                  onChange={(e) => setFilterForm({...filterForm, name: e.target.value})}
                />
              </label>

              <label>
                UDF Expression:
                <textarea
                  value={filterForm.udfExpression}
                  onChange={(e) => setFilterForm({...filterForm, udfExpression: e.target.value})}
                  rows={3}
                />
              </label>

              <label>
                Description (optional):
                <input
                  type="text"
                  value={filterForm.description}
                  onChange={(e) => setFilterForm({...filterForm, description: e.target.value})}
                />
              </label>

              <label className="checkbox-label">
                <input
                  type="checkbox"
                  checked={filterForm.enabled}
                  onChange={(e) => setFilterForm({...filterForm, enabled: e.target.checked})}
                />
                <span>Enabled</span>
              </label>
            </div>
            <div className="modal-actions">
              <button className="cancel-btn" onClick={() => { setEditFilterDialog(null); setFilterForm({name: '', udfExpression: '', enabled: true, description: ''}); }}>
                Cancel
              </button>
              <button
                className="confirm-btn"
                onClick={handleUpdateFilter}
              >
                Update Filter
              </button>
            </div>
          </div>
        </div>
      )}

      {/* Delete Row Filter Dialog */}
      {deleteFilterDialog && (
        <div className="modal-overlay" onClick={() => setDeleteFilterDialog(null)}>
          <div className="modal-content" onClick={(e) => e.stopPropagation()}>
            <h2>Remove Row Filter</h2>
            <p className="warning-text">
              Are you sure you want to remove row filter <strong>{deleteFilterDialog.name}</strong>?
            </p>
            <div className="modal-actions">
              <button className="cancel-btn" onClick={() => setDeleteFilterDialog(null)}>
                Cancel
              </button>
              <button className="confirm-btn delete" onClick={handleDeleteFilter}>
                Remove
              </button>
            </div>
          </div>
        </div>
      )}

      {/* Add Column Mask Dialog */}
      {addMaskDialog && (
        <div className="modal-overlay" onClick={() => setAddMaskDialog(false)}>
          <div className="modal-content" onClick={(e) => e.stopPropagation()}>
            <h2>Add Column Mask</h2>
            <div className="modal-body">
              <label>
                Column:
                <select
                  value={maskForm.columnId}
                  onChange={(e) => setMaskForm({...maskForm, columnId: e.target.value})}
                >
                  <option value="">Select column...</option>
                  {table.columns.map(col => (
                    <option key={col.id} value={col.id}>{col.name} ({col.type})</option>
                  ))}
                </select>
              </label>

              <label>
                Name:
                <input
                  type="text"
                  value={maskForm.name}
                  onChange={(e) => setMaskForm({...maskForm, name: e.target.value})}
                  placeholder="e.g., email_redact"
                />
              </label>

              <label>
                Mask Function:
                <textarea
                  value={maskForm.maskFunction}
                  onChange={(e) => setMaskForm({...maskForm, maskFunction: e.target.value})}
                  placeholder="e.g., REDACT_EMAIL(email)"
                  rows={3}
                />
                <p className="hint">SQL UDF that returns same type as column</p>
              </label>

              <label>
                Description (optional):
                <input
                  type="text"
                  value={maskForm.description}
                  onChange={(e) => setMaskForm({...maskForm, description: e.target.value})}
                  placeholder="Mask description"
                />
              </label>

              <label className="checkbox-label">
                <input
                  type="checkbox"
                  checked={maskForm.enabled}
                  onChange={(e) => setMaskForm({...maskForm, enabled: e.target.checked})}
                />
                <span>Enabled</span>
              </label>
            </div>
            <div className="modal-actions">
              <button className="cancel-btn" onClick={() => { setAddMaskDialog(false); setMaskForm({columnId: '', name: '', maskFunction: '', enabled: true, description: ''}); }}>
                Cancel
              </button>
              <button
                className="confirm-btn"
                onClick={handleAddMask}
                disabled={!maskForm.columnId || !maskForm.name || !maskForm.maskFunction}
              >
                Add Mask
              </button>
            </div>
          </div>
        </div>
      )}

      {/* Edit Column Mask Dialog */}
      {editMaskDialog && (
        <div className="modal-overlay" onClick={() => setEditMaskDialog(null)}>
          <div className="modal-content" onClick={(e) => e.stopPropagation()}>
            <h2>Edit Column Mask</h2>
            <div className="modal-body">
              <label>
                Column:
                <input
                  type="text"
                  value={getColumnName(maskForm.columnId)}
                  disabled
                />
              </label>

              <label>
                Name:
                <input
                  type="text"
                  value={maskForm.name}
                  onChange={(e) => setMaskForm({...maskForm, name: e.target.value})}
                />
              </label>

              <label>
                Mask Function:
                <textarea
                  value={maskForm.maskFunction}
                  onChange={(e) => setMaskForm({...maskForm, maskFunction: e.target.value})}
                  rows={3}
                />
              </label>

              <label>
                Description (optional):
                <input
                  type="text"
                  value={maskForm.description}
                  onChange={(e) => setMaskForm({...maskForm, description: e.target.value})}
                />
              </label>

              <label className="checkbox-label">
                <input
                  type="checkbox"
                  checked={maskForm.enabled}
                  onChange={(e) => setMaskForm({...maskForm, enabled: e.target.checked})}
                />
                <span>Enabled</span>
              </label>
            </div>
            <div className="modal-actions">
              <button className="cancel-btn" onClick={() => { setEditMaskDialog(null); setMaskForm({columnId: '', name: '', maskFunction: '', enabled: true, description: ''}); }}>
                Cancel
              </button>
              <button
                className="confirm-btn"
                onClick={handleUpdateMask}
              >
                Update Mask
              </button>
            </div>
          </div>
        </div>
      )}

      {/* Delete Column Mask Dialog */}
      {deleteMaskDialog && (
        <div className="modal-overlay" onClick={() => setDeleteMaskDialog(null)}>
          <div className="modal-content" onClick={(e) => e.stopPropagation()}>
            <h2>Remove Column Mask</h2>
            <p className="warning-text">
              Are you sure you want to remove column mask <strong>{deleteMaskDialog.name}</strong>?
            </p>
            <div className="modal-actions">
              <button className="cancel-btn" onClick={() => setDeleteMaskDialog(null)}>
                Cancel
              </button>
              <button className="confirm-btn delete" onClick={handleDeleteMask}>
                Remove
              </button>
            </div>
          </div>
        </div>
      )}

      {/* Add / Edit Grant Dialog */}
      {addGrantDialog && (
        <div className="modal-overlay" onClick={() => { setAddGrantDialog(false); setEditingGrant(null); }}>
          <div className="modal-content" onClick={(e) => e.stopPropagation()}>
            <h2>{editingGrant ? 'Edit Grant' : 'Add Grant'}</h2>
            <div className="modal-body">
              <label>
                Principal (user, group, or service principal):
                <input
                  type="text"
                  value={grantForm.principal}
                  onChange={(e) => setGrantForm({ ...grantForm, principal: e.target.value })}
                  placeholder="e.g. data_engineers or user@domain.com"
                  readOnly={!!editingGrant}
                />
              </label>
              <label>
                Privileges (comma-separated; e.g. SELECT, MODIFY):
                <input
                  type="text"
                  value={grantForm.privileges}
                  onChange={(e) => setGrantForm({ ...grantForm, privileges: e.target.value })}
                  placeholder="e.g. SELECT, MODIFY"
                />
              </label>
            </div>
            <div className="modal-actions">
              <button className="cancel-btn" onClick={() => { setAddGrantDialog(false); setEditingGrant(null); setGrantForm({ principal: '', privileges: '' }); }}>
                Cancel
              </button>
              <button
                className="confirm-btn"
                onClick={handleAddGrant}
                disabled={!grantForm.principal.trim() || !grantForm.privileges.trim()}
              >
                {editingGrant ? 'Save' : 'Add Grant'}
              </button>
            </div>
          </div>
        </div>
      )}

      {/* Delete Grant Dialog */}
      {deleteGrantDialog && (
        <div className="modal-overlay" onClick={() => setDeleteGrantDialog(null)}>
          <div className="modal-content" onClick={(e) => e.stopPropagation()}>
            <h2>Revoke Grant</h2>
            <p className="warning-text">
              Revoke all privileges for <strong>{deleteGrantDialog.principal}</strong> on this table?
            </p>
            <div className="modal-actions">
              <button className="cancel-btn" onClick={() => setDeleteGrantDialog(null)}>
                Cancel
              </button>
              <button className="confirm-btn delete" onClick={() => handleRevokeGrant(deleteGrantDialog.principal)}>
                Revoke
              </button>
            </div>
          </div>
        </div>
      )}
    </div>
  );
};

