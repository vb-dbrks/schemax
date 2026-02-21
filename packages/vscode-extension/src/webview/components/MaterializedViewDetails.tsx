import React, { useState } from 'react';
import { VSCodeButton } from '@vscode/webview-ui-toolkit/react';
import { useDesignerStore } from '../state/useDesignerStore';
import { parsePrivileges } from '../utils/grants';
import { extractDependenciesFromView } from '../../providers/base/sql-parser';
import './ViewDetails.css';

const IconEdit: React.FC = () => (
  <i slot="start" className="codicon codicon-edit" aria-hidden="true"></i>
);
const IconTrash: React.FC = () => (
  <i slot="start" className="codicon codicon-trash" aria-hidden="true"></i>
);

interface MaterializedViewDetailsProps {
  materializedViewId: string;
}

export const MaterializedViewDetails: React.FC<MaterializedViewDetailsProps> = ({ materializedViewId }) => {
  const { findMaterializedView, updateMaterializedView, addGrant, revokeGrant } = useDesignerStore();
  const info = findMaterializedView(materializedViewId);
  const [isEditing, setIsEditing] = useState(false);
  const [editedDefinition, setEditedDefinition] = useState('');
  const [editingProps, setEditingProps] = useState(false);
  const [editComment, setEditComment] = useState('');
  const [editSchedule, setEditSchedule] = useState('');
  const [addGrantDialog, setAddGrantDialog] = useState(false);
  const [editingGrant, setEditingGrant] = useState<{ principal: string; privileges: string[] } | null>(null);
  const [revokeGrantDialog, setRevokeGrantDialog] = useState<{ principal: string } | null>(null);
  const [grantForm, setGrantForm] = useState({ principal: '', privileges: '' });
  const [isEditingDeps, setIsEditingDeps] = useState(false);
  const [editedTables, setEditedTables] = useState<string[]>([]);
  const [editedViews, setEditedViews] = useState<string[]>([]);
  const [newTableName, setNewTableName] = useState('');
  const [newViewName, setNewViewName] = useState('');

  if (!info) {
    return (
      <div className="view-details">
        <div className="empty-state"><p>Materialized view not found</p></div>
      </div>
    );
  }

  const { catalog, schema, mv } = info;
  const m = mv as {
    id: string;
    name: string;
    definition?: string;
    comment?: string;
    refreshSchedule?: string;
    extractedDependencies?: { tables?: string[]; views?: string[] };
    grants?: { principal: string; privileges: string[] }[];
  };

  const handleSaveDefinition = () => {
    if (editedDefinition.trim()) {
      // Strip CREATE MATERIALIZED VIEW ... AS prefix if user pasted full DDL (store only SELECT)
      const createMVPattern = /^CREATE\s+(?:OR\s+REPLACE\s+)?MATERIALIZED\s+VIEW\s+(?:IF\s+NOT\s+EXISTS\s+)?(?:[\w.]+\.)?\w+\s+AS\s+([\s\S]+)/i;
      const match = editedDefinition.trim().match(createMVPattern);
      const definitionToStore = match ? match[1].trim() : editedDefinition.trim();
      const deps = extractDependenciesFromView(definitionToStore);
      updateMaterializedView(materializedViewId, definitionToStore, deps);
      setIsEditing(false);
    }
  };

  const startEditProps = () => {
    setEditComment(m.comment ?? '');
    setEditSchedule(m.refreshSchedule ?? '');
    setEditingProps(true);
  };

  const saveProps = () => {
    const deps = m.definition ? extractDependenciesFromView(m.definition) : undefined;
    updateMaterializedView(
      materializedViewId,
      m.definition ?? 'SELECT 1',
      deps,
      { comment: editComment.trim() || undefined, refreshSchedule: editSchedule.trim() || undefined }
    );
    setEditingProps(false);
  };

  const cancelEditProps = () => {
    setEditComment(m.comment ?? '');
    setEditSchedule(m.refreshSchedule ?? '');
    setEditingProps(false);
  };

  const startEditDeps = () => {
    const deps = m.extractedDependencies ?? { tables: [], views: [] };
    setEditedTables(Array.isArray(deps.tables) ? [...deps.tables] : []);
    setEditedViews(Array.isArray(deps.views) ? [...deps.views] : []);
    setNewTableName('');
    setNewViewName('');
    setIsEditingDeps(true);
  };

  const saveDeps = () => {
    updateMaterializedView(
      materializedViewId,
      m.definition ?? 'SELECT 1',
      { tables: editedTables, views: editedViews },
      { comment: m.comment, refreshSchedule: m.refreshSchedule }
    );
    setIsEditingDeps(false);
  };

  const cancelEditDeps = () => setIsEditingDeps(false);

  const addTableDep = () => {
    const name = newTableName.trim();
    if (name && !editedTables.includes(name)) {
      setEditedTables([...editedTables, name]);
      setNewTableName('');
    }
  };

  const addViewDep = () => {
    const name = newViewName.trim();
    if (name && !editedViews.includes(name)) {
      setEditedViews([...editedViews, name]);
      setNewViewName('');
    }
  };

  const removeTableDep = (index: number) => {
    setEditedTables(editedTables.filter((_, i) => i !== index));
  };

  const removeViewDep = (index: number) => {
    setEditedViews(editedViews.filter((_, i) => i !== index));
  };

  const grants = m.grants ?? [];

  return (
    <div className="view-details">
      <div className="view-header">
        <div className="view-title">
          <i className="codicon codicon-symbol-array" aria-hidden="true" />
          <h2>{catalog.name}.{schema.name}.{m.name}</h2>
        </div>
        <span className="view-badge" style={{ background: 'var(--vscode-charts-green)', color: 'white' }}>MATERIALIZED VIEW</span>
      </div>

      <div className="view-section">
        <div className="section-header">
          <h3>Properties</h3>
          {!editingProps ? (
            <VSCodeButton appearance="secondary" onClick={startEditProps}>Edit</VSCodeButton>
          ) : (
            <>
              <VSCodeButton appearance="primary" onClick={saveProps}>Save</VSCodeButton>
              <VSCodeButton appearance="secondary" onClick={cancelEditProps}>Cancel</VSCodeButton>
            </>
          )}
        </div>
        <dl className="properties-list">
          <dt>Refresh schedule</dt>
          <dd>
            {editingProps ? (
              <input
                type="text"
                value={editSchedule}
                onChange={(e) => setEditSchedule(e.target.value)}
                placeholder="EVERY 1 DAY"
                style={{ width: '100%', padding: '6px 8px', fontFamily: 'monospace' }}
              />
            ) : (
              <code>{m.refreshSchedule || '—'}</code>
            )}
          </dd>
          <dt>Comment</dt>
          <dd>
            {editingProps ? (
              <textarea
                value={editComment}
                onChange={(e) => setEditComment(e.target.value)}
                rows={2}
                placeholder="Optional description"
                style={{ width: '100%', padding: '6px 8px', fontSize: '13px', resize: 'vertical' }}
              />
            ) : (
              m.comment ?? '—'
            )}
          </dd>
        </dl>
      </div>

      <div className="view-section">
        <div className="section-header">
          <h3>SQL Definition</h3>
          {!isEditing ? (
            <VSCodeButton appearance="secondary" onClick={() => { setEditedDefinition(m.definition ?? ''); setIsEditing(true); }}>Edit</VSCodeButton>
          ) : (
            <>
              <VSCodeButton appearance="primary" onClick={handleSaveDefinition}>Save</VSCodeButton>
              <VSCodeButton appearance="secondary" onClick={() => setIsEditing(false)}>Cancel</VSCodeButton>
            </>
          )}
        </div>
        {isEditing ? (
          <textarea value={editedDefinition} onChange={(e) => setEditedDefinition(e.target.value)} rows={12} style={{ width: '100%', fontFamily: 'monospace' }} />
        ) : (
          <pre className="code-block" style={{ whiteSpace: 'pre-wrap', wordBreak: 'break-word' }}>{m.definition ?? 'SELECT 1'}</pre>
        )}
      </div>

      {/* Dependencies Section (editable) */}
      <div className="view-section">
        <div className="section-header">
          <h3>Dependencies</h3>
          {!isEditingDeps && (
            <VSCodeButton appearance="secondary" onClick={startEditDeps}>
              <IconEdit />
              Edit
            </VSCodeButton>
          )}
        </div>
        <p className="dependencies-help">
          Dependencies are derived from the SQL when you save the definition. You can also add or
          remove entries below to control creation order. Use object names as in the project (e.g.
          table name or schema.table).
        </p>
        {isEditingDeps ? (
          <div className="dependencies-container">
            <div className="dependency-group">
              <h4>Tables</h4>
              {editedTables.map((name, i) => (
                <div key={i} className="dependency-row">
                  <code>{name}</code>
                  <VSCodeButton appearance="icon" onClick={() => removeTableDep(i)} title="Remove">
                    <IconTrash />
                  </VSCodeButton>
                </div>
              ))}
              <div className="dependency-add">
                <input
                  type="text"
                  value={newTableName}
                  onChange={(e) => setNewTableName(e.target.value)}
                  placeholder="Table name"
                  onKeyDown={(e) => e.key === 'Enter' && (e.preventDefault(), addTableDep())}
                  style={{ flex: 1, padding: '4px 8px', fontFamily: 'monospace' }}
                />
                <VSCodeButton appearance="secondary" onClick={addTableDep}>
                  Add table
                </VSCodeButton>
              </div>
            </div>
            <div className="dependency-group">
              <h4>Views</h4>
              {editedViews.map((name, i) => (
                <div key={i} className="dependency-row">
                  <code>{name}</code>
                  <VSCodeButton appearance="icon" onClick={() => removeViewDep(i)} title="Remove">
                    <IconTrash />
                  </VSCodeButton>
                </div>
              ))}
              <div className="dependency-add">
                <input
                  type="text"
                  value={newViewName}
                  onChange={(e) => setNewViewName(e.target.value)}
                  placeholder="View name"
                  onKeyDown={(e) => e.key === 'Enter' && (e.preventDefault(), addViewDep())}
                  style={{ flex: 1, padding: '4px 8px', fontFamily: 'monospace' }}
                />
                <VSCodeButton appearance="secondary" onClick={addViewDep}>
                  Add view
                </VSCodeButton>
              </div>
            </div>
            <div className="edit-actions">
              <VSCodeButton onClick={saveDeps}>
                <i className="codicon codicon-check"></i> Save
              </VSCodeButton>
              <VSCodeButton appearance="secondary" onClick={cancelEditDeps}>
                <i className="codicon codicon-close"></i> Cancel
              </VSCodeButton>
            </div>
          </div>
        ) : (
          <div className="dependencies-container">
            {(m.extractedDependencies?.tables?.length ?? 0) > 0 && (
              <div className="dependency-group">
                <h4>Dependent Tables</h4>
                <ul className="dependency-list">
                  {(m.extractedDependencies?.tables ?? []).map((table: string, i: number) => (
                    <li key={i}>
                      <i className="codicon codicon-table"></i>
                      <code>{table}</code>
                    </li>
                  ))}
                </ul>
              </div>
            )}
            {(m.extractedDependencies?.views?.length ?? 0) > 0 && (
              <div className="dependency-group">
                <h4>Dependent Views</h4>
                <ul className="dependency-list">
                  {(m.extractedDependencies?.views ?? []).map((v: string, i: number) => (
                    <li key={i}>
                      <svg xmlns="http://www.w3.org/2000/svg" width="1em" height="1em" fill="none" viewBox="0 0 16 16" style={{ marginRight: '4px' }}>
                        <path fill="currentColor" fillRule="evenodd" d="M1.75 1a.75.75 0 0 0-.75.75v12.5c0 .414.336.75.75.75H4v-1.5H2.5V7H5v2h1.5V7h3v2H11V7h2.5v2H15V1.75a.75.75 0 0 0-.75-.75zM13.5 5.5v-3h-11v3z" clipRule="evenodd" />
                        <path fill="currentColor" fillRule="evenodd" d="M11.75 10a.75.75 0 0 0-.707.5H9.957a.75.75 0 0 0-.708-.5H5.75a.75.75 0 0 0-.75.75v1.75a2.5 2.5 0 0 0 5 0V12h1v.5a2.5 2.5 0 0 0 5 0v-1.75a.75.75 0 0 0-.75-.75zm.75 2.5v-1h2v1a1 1 0 1 1-2 0m-6-1v1a1 1 0 1 0 2 0v-1z" clipRule="evenodd" />
                      </svg>
                      <code>{v}</code>
                    </li>
                  ))}
                </ul>
              </div>
            )}
            {(!m.extractedDependencies?.tables?.length && !m.extractedDependencies?.views?.length) && (
              <p className="no-dependencies">No dependencies detected. Edit to add table or view names.</p>
            )}
          </div>
        )}
      </div>

      {/* Grants */}
      <div className="table-properties-section">
        <h3>Grants ({grants.length})</h3>
        {grants.length === 0 && !addGrantDialog ? (
          <div className="empty-properties">
            <p>No grants defined. Grant privileges (e.g. SELECT) to users or groups.</p>
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
                    <VSCodeButton appearance="icon" onClick={() => { setEditingGrant({ principal: g.principal, privileges: g.privileges || [] }); setGrantForm({ principal: g.principal, privileges: (g.privileges || []).join(', ') }); setAddGrantDialog(true); }} title="Edit grant">
                      <IconEdit />
                    </VSCodeButton>
                    <VSCodeButton appearance="icon" onClick={() => setRevokeGrantDialog({ principal: g.principal })} title="Revoke all">
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

      {addGrantDialog && (
        <div className="modal-overlay" onClick={() => { setAddGrantDialog(false); setEditingGrant(null); }} style={{ position: 'fixed', inset: 0, background: 'rgba(0,0,0,0.5)', display: 'flex', alignItems: 'center', justifyContent: 'center', zIndex: 1000 }}>
          <div className="modal-content" onClick={(e) => e.stopPropagation()} style={{ background: 'var(--vscode-editor-background)', padding: '16px', borderRadius: '8px', minWidth: '320px' }}>
            <h3>{editingGrant ? 'Edit Grant' : 'Add Grant'}</h3>
            <label>Principal</label>
            <input type="text" value={grantForm.principal} onChange={(e) => setGrantForm({ ...grantForm, principal: e.target.value })} placeholder="e.g. data_engineers" style={{ width: '100%', marginBottom: '8px' }} readOnly={!!editingGrant} />
            <label>Privileges (comma-separated, e.g. SELECT)</label>
            <input type="text" value={grantForm.privileges} onChange={(e) => setGrantForm({ ...grantForm, privileges: e.target.value })} placeholder="SELECT" style={{ width: '100%', marginBottom: '12px' }} />
            <div style={{ display: 'flex', gap: '8px', justifyContent: 'flex-end' }}>
              <VSCodeButton appearance="secondary" onClick={() => { setAddGrantDialog(false); setEditingGrant(null); setGrantForm({ principal: '', privileges: '' }); }}>Cancel</VSCodeButton>
              <VSCodeButton
                onClick={() => {
                  const principal = grantForm.principal.trim();
                  const privs = parsePrivileges(grantForm.privileges);
                  if (principal && privs.length > 0) {
                    if (editingGrant) revokeGrant('materialized_view', materializedViewId, editingGrant.principal);
                    addGrant('materialized_view', materializedViewId, principal, privs);
                    setAddGrantDialog(false);
                    setEditingGrant(null);
                    setGrantForm({ principal: '', privileges: '' });
                  }
                }}
                disabled={!grantForm.principal.trim() || !grantForm.privileges.trim()}
              >
                {editingGrant ? 'Save' : 'Add Grant'}
              </VSCodeButton>
            </div>
          </div>
        </div>
      )}

      {revokeGrantDialog && (
        <div className="modal-overlay" onClick={() => setRevokeGrantDialog(null)} style={{ position: 'fixed', inset: 0, background: 'rgba(0,0,0,0.5)', display: 'flex', alignItems: 'center', justifyContent: 'center', zIndex: 1000 }}>
          <div className="modal-content" onClick={(e) => e.stopPropagation()} style={{ background: 'var(--vscode-editor-background)', padding: '16px', borderRadius: '8px', minWidth: '320px' }}>
            <h3>Revoke Grant</h3>
            <p>Revoke all privileges for <strong>{revokeGrantDialog.principal}</strong> on this materialized view?</p>
            <div style={{ display: 'flex', gap: '8px', justifyContent: 'flex-end' }}>
              <VSCodeButton appearance="secondary" onClick={() => setRevokeGrantDialog(null)}>Cancel</VSCodeButton>
              <VSCodeButton onClick={() => { revokeGrant('materialized_view', materializedViewId, revokeGrantDialog.principal); setRevokeGrantDialog(null); }}>Revoke</VSCodeButton>
            </div>
          </div>
        </div>
      )}
    </div>
  );
};
