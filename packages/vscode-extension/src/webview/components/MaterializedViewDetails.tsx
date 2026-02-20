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
    grants?: { principal: string; privileges: string[] }[];
  };

  const handleSaveDefinition = () => {
    if (editedDefinition.trim()) {
      const deps = extractDependenciesFromView(editedDefinition);
      updateMaterializedView(materializedViewId, editedDefinition.trim(), deps);
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
