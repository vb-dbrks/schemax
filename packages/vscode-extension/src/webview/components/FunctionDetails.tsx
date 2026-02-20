import React, { useState } from 'react';
import { VSCodeButton } from '@vscode/webview-ui-toolkit/react';
import { useDesignerStore } from '../state/useDesignerStore';
import { parsePrivileges } from '../utils/grants';
import './ViewDetails.css';

const IconEdit: React.FC = () => (
  <i slot="start" className="codicon codicon-edit" aria-hidden="true"></i>
);
const IconTrash: React.FC = () => (
  <i slot="start" className="codicon codicon-trash" aria-hidden="true"></i>
);

interface FunctionDetailsProps {
  functionId: string;
}

export const FunctionDetails: React.FC<FunctionDetailsProps> = ({ functionId }) => {
  const { findFunction, updateFunction, addGrant, revokeGrant } = useDesignerStore();
  const info = findFunction(functionId);
  const [editingBody, setEditingBody] = useState(false);
  const [editBody, setEditBody] = useState('');
  const [editReturnType, setEditReturnType] = useState('');
  const [editComment, setEditComment] = useState('');
  const [addGrantDialog, setAddGrantDialog] = useState(false);
  const [editingGrant, setEditingGrant] = useState<{ principal: string; privileges: string[] } | null>(null);
  const [revokeGrantDialog, setRevokeGrantDialog] = useState<{ principal: string } | null>(null);
  const [grantForm, setGrantForm] = useState({ principal: '', privileges: '' });

  if (!info) {
    return (
      <div className="view-details">
        <div className="empty-state"><p>Function not found</p></div>
      </div>
    );
  }

  const { catalog, schema, func } = info;
  const f = func as {
    id: string;
    name: string;
    language?: string;
    returnType?: string;
    body?: string;
    comment?: string;
    parameters?: unknown[];
    grants?: { principal: string; privileges: string[] }[];
  };

  const startEdit = () => {
    setEditBody(f.body ?? '');
    setEditReturnType(f.returnType ?? 'STRING');
    setEditComment(f.comment ?? '');
    setEditingBody(true);
  };

  const saveEdit = () => {
    updateFunction(functionId, {
      body: editBody.trim() || 'NULL',
      returnType: editReturnType.trim() || 'STRING',
      comment: editComment.trim() || undefined,
    });
    setEditingBody(false);
  };

  const cancelEdit = () => {
    setEditBody(f.body ?? '');
    setEditReturnType(f.returnType ?? 'STRING');
    setEditComment(f.comment ?? '');
    setEditingBody(false);
  };

  const grants = f.grants ?? [];

  return (
    <div className="view-details">
      <div className="view-header">
        <div className="view-title">
          <i className="codicon codicon-symbol-method" aria-hidden="true" />
          <h2>{catalog.name}.{schema.name}.{f.name}</h2>
        </div>
        <span className="view-badge" style={{ background: 'var(--vscode-charts-orange)', color: 'white' }}>FUNCTION</span>
      </div>

      <div className="view-section">
        <div className="section-header">
          <h3>Properties</h3>
          {!editingBody ? (
            <VSCodeButton appearance="secondary" onClick={startEdit}>Edit</VSCodeButton>
          ) : (
            <>
              <VSCodeButton appearance="primary" onClick={saveEdit}>Save</VSCodeButton>
              <VSCodeButton appearance="secondary" onClick={cancelEdit}>Cancel</VSCodeButton>
            </>
          )}
        </div>
        <dl className="properties-list">
          <dt>Language</dt>
          <dd>{f.language ?? 'SQL'}</dd>
          <dt>Return type</dt>
          <dd>
            {editingBody ? (
              <input
                type="text"
                value={editReturnType}
                onChange={(e) => setEditReturnType(e.target.value)}
                placeholder="STRING"
                style={{ width: '100%', padding: '6px 8px', fontFamily: 'monospace' }}
              />
            ) : (
              <code>{f.returnType ?? 'STRING'}</code>
            )}
          </dd>
          <dt>Comment</dt>
          <dd>
            {editingBody ? (
              <textarea
                value={editComment}
                onChange={(e) => setEditComment(e.target.value)}
                rows={2}
                placeholder="Optional description"
                style={{ width: '100%', padding: '6px 8px', fontSize: '13px', resize: 'vertical' }}
              />
            ) : (
              f.comment ?? '—'
            )}
          </dd>
        </dl>
      </div>

      <div className="view-section">
        <div className="section-header">
          <h3>Body</h3>
        </div>
        {editingBody ? (
          <textarea
            value={editBody}
            onChange={(e) => setEditBody(e.target.value)}
            rows={8}
            style={{ width: '100%', fontFamily: 'monospace', fontSize: '13px', padding: '8px' }}
            placeholder="SQL expression or Python code"
          />
        ) : (
          <pre className="code-block" style={{ whiteSpace: 'pre-wrap', wordBreak: 'break-word' }}>{f.body ?? 'NULL'}</pre>
        )}
      </div>

      {/* Grants */}
      <div className="table-properties-section">
        <h3>Grants ({grants.length})</h3>
        {grants.length === 0 && !addGrantDialog ? (
          <div className="empty-properties">
            <p>No grants defined. Grant privileges (e.g. EXECUTE) to users or groups.</p>
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
        <button className="add-property-btn" onClick={() => setAddGrantDialog(true)}>
          + Add Grant
        </button>
      </div>

      {addGrantDialog && (
        <div className="modal-overlay" onClick={() => { setAddGrantDialog(false); setEditingGrant(null); }} style={{ position: 'fixed', inset: 0, background: 'rgba(0,0,0,0.5)', display: 'flex', alignItems: 'center', justifyContent: 'center', zIndex: 1000 }}>
          <div className="modal-content" onClick={(e) => e.stopPropagation()} style={{ background: 'var(--vscode-editor-background)', padding: '16px', borderRadius: '8px', minWidth: '320px' }}>
            <h3>{editingGrant ? 'Edit Grant' : 'Add Grant'}</h3>
            <label>Principal</label>
            <input type="text" value={grantForm.principal} onChange={(e) => setGrantForm({ ...grantForm, principal: e.target.value })} placeholder="e.g. data_engineers" style={{ width: '100%', marginBottom: '8px' }} readOnly={!!editingGrant} />
            <label>Privileges (comma-separated, e.g. EXECUTE)</label>
            <input type="text" value={grantForm.privileges} onChange={(e) => setGrantForm({ ...grantForm, privileges: e.target.value })} placeholder="EXECUTE" style={{ width: '100%', marginBottom: '12px' }} />
            <div style={{ display: 'flex', gap: '8px', justifyContent: 'flex-end' }}>
              <VSCodeButton appearance="secondary" onClick={() => { setAddGrantDialog(false); setEditingGrant(null); setGrantForm({ principal: '', privileges: '' }); }}>Cancel</VSCodeButton>
              <VSCodeButton
                onClick={() => {
                  const principal = grantForm.principal.trim();
                  const privs = parsePrivileges(grantForm.privileges);
                  if (principal && privs.length > 0) {
                    if (editingGrant) revokeGrant('function', functionId, editingGrant.principal);
                    addGrant('function', functionId, principal, privs);
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
            <p>Revoke all privileges for <strong>{revokeGrantDialog.principal}</strong> on this function?</p>
            <div style={{ display: 'flex', gap: '8px', justifyContent: 'flex-end' }}>
              <VSCodeButton appearance="secondary" onClick={() => setRevokeGrantDialog(null)}>Cancel</VSCodeButton>
              <VSCodeButton onClick={() => { revokeGrant('function', functionId, revokeGrantDialog.principal); setRevokeGrantDialog(null); }}>Revoke</VSCodeButton>
            </div>
          </div>
        </div>
      )}
    </div>
  );
};
