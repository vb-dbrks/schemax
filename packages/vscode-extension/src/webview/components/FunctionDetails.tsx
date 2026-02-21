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
  const [commentDialog, setCommentDialog] = useState(false);
  const [commentInput, setCommentInput] = useState('');
  const [returnTypeDialog, setReturnTypeDialog] = useState(false);
  const [returnTypeInput, setReturnTypeInput] = useState('');
  const [bodyDialog, setBodyDialog] = useState(false);
  const [bodyInput, setBodyInput] = useState('');
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

  const openCommentDialog = () => {
    setCommentInput(f.comment ?? '');
    setCommentDialog(true);
  };

  const saveComment = () => {
    updateFunction(functionId, { comment: commentInput.trim() || undefined });
    setCommentDialog(false);
  };

  const openReturnTypeDialog = () => {
    setReturnTypeInput(f.returnType ?? 'STRING');
    setReturnTypeDialog(true);
  };

  const saveReturnType = () => {
    updateFunction(functionId, { returnType: returnTypeInput.trim() || 'STRING' });
    setReturnTypeDialog(false);
  };

  const openBodyDialog = () => {
    setBodyInput(f.body ?? '');
    setBodyDialog(true);
  };

  const saveBody = () => {
    updateFunction(functionId, { body: bodyInput.trim() || 'NULL' });
    setBodyDialog(false);
  };

  const openAddGrant = () => {
    setEditingGrant(null);
    setGrantForm({ principal: '', privileges: '' });
    setAddGrantDialog(true);
  };

  const openEditGrant = (principal: string, privileges: string[]) => {
    setEditingGrant({ principal, privileges });
    setGrantForm({ principal, privileges: privileges.join(', ') });
    setAddGrantDialog(true);
  };

  const saveGrant = () => {
    const principal = grantForm.principal.trim();
    const privs = parsePrivileges(grantForm.privileges);
    if (!principal || privs.length === 0) return;
    if (editingGrant) revokeGrant('function', functionId, editingGrant.principal);
    addGrant('function', functionId, principal, privs);
    setAddGrantDialog(false);
    setEditingGrant(null);
    setGrantForm({ principal: '', privileges: '' });
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

      {/* Properties - table-inspired: one section, property rows with pencil → modal */}
      <div className="view-section">
        <h3>Properties</h3>
        <div className="table-properties">
          <div className="property-row">
            <label>Language</label>
            <div className="property-value">
              <span>{f.language ?? 'SQL'}</span>
            </div>
          </div>
          <div className="property-row">
            <label>Return type</label>
            <div className="property-value">
              <code>{f.returnType ?? 'STRING'}</code>
              <VSCodeButton appearance="icon" onClick={openReturnTypeDialog} title="Edit return type">
                <IconEdit />
              </VSCodeButton>
            </div>
          </div>
          <div className="property-row">
            <label>Comment</label>
            <div className="property-value">
              {f.comment ? (
                <span style={{ flex: 1 }}>{f.comment}</span>
              ) : (
                <span className="inline-warning" style={{ flex: 1 }}>
                  <span className="inline-warning__dot" aria-hidden="true" />
                  Comment recommended
                </span>
              )}
              <VSCodeButton appearance="icon" onClick={openCommentDialog} title="Edit comment">
                <IconEdit />
              </VSCodeButton>
            </div>
          </div>
          <div className="property-row">
            <label>Body</label>
            <div className="property-value" style={{ alignItems: 'flex-start', flex: 1 }}>
              <pre className="code-block" style={{ flex: 1, margin: 0, whiteSpace: 'pre-wrap', wordBreak: 'break-word', maxHeight: '120px', overflow: 'auto' }}>{f.body ?? 'NULL'}</pre>
              <VSCodeButton appearance="icon" onClick={openBodyDialog} title="Edit body">
                <IconEdit />
              </VSCodeButton>
            </div>
          </div>
        </div>
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
                    <VSCodeButton appearance="icon" onClick={() => openEditGrant(g.principal, g.privileges || [])} title="Edit grant">
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
        <button className="add-property-btn" onClick={openAddGrant}>
          + Add Grant
        </button>
      </div>

      {/* Comment modal */}
      {commentDialog && (
        <div className="modal" onClick={() => setCommentDialog(false)} role="dialog">
          <div className="modal-content" onClick={(e) => e.stopPropagation()}>
            <h3>Set Function Comment</h3>
            <textarea
              value={commentInput}
              onChange={(e) => setCommentInput(e.target.value)}
              placeholder="Optional description"
              rows={3}
              style={{ width: '100%', marginBottom: '12px', padding: '8px', resize: 'vertical' }}
              autoFocus
            />
            <div className="modal-buttons">
              <VSCodeButton onClick={saveComment}>Set</VSCodeButton>
              <VSCodeButton appearance="secondary" onClick={() => setCommentDialog(false)}>Cancel</VSCodeButton>
            </div>
          </div>
        </div>
      )}

      {/* Return type modal */}
      {returnTypeDialog && (
        <div className="modal" onClick={() => setReturnTypeDialog(false)} role="dialog">
          <div className="modal-content" onClick={(e) => e.stopPropagation()}>
            <h3>Set Return Type</h3>
            <input
              type="text"
              value={returnTypeInput}
              onChange={(e) => setReturnTypeInput(e.target.value)}
              placeholder="STRING"
              style={{ width: '100%', marginBottom: '12px', padding: '8px', fontFamily: 'monospace' }}
              autoFocus
            />
            <div className="modal-buttons">
              <VSCodeButton onClick={saveReturnType}>Set</VSCodeButton>
              <VSCodeButton appearance="secondary" onClick={() => setReturnTypeDialog(false)}>Cancel</VSCodeButton>
            </div>
          </div>
        </div>
      )}

      {/* Body modal */}
      {bodyDialog && (
        <div className="modal" onClick={() => setBodyDialog(false)} role="dialog">
          <div className="modal-content" onClick={(e) => e.stopPropagation()} style={{ minWidth: '400px', maxWidth: '90vw' }}>
            <h3>Set Function Body</h3>
            <textarea
              value={bodyInput}
              onChange={(e) => setBodyInput(e.target.value)}
              placeholder="SQL expression or Python code"
              rows={12}
              style={{ width: '100%', marginBottom: '12px', padding: '8px', fontFamily: 'monospace', fontSize: '13px', resize: 'vertical' }}
              autoFocus
            />
            <div className="modal-buttons">
              <VSCodeButton onClick={saveBody}>Set</VSCodeButton>
              <VSCodeButton appearance="secondary" onClick={() => setBodyDialog(false)}>Cancel</VSCodeButton>
            </div>
          </div>
        </div>
      )}

      {/* Add / Edit Grant modal */}
      {addGrantDialog && (
        <div className="modal-overlay" onClick={() => { setAddGrantDialog(false); setEditingGrant(null); }} style={{ position: 'fixed', inset: 0, background: 'rgba(0,0,0,0.5)', display: 'flex', alignItems: 'center', justifyContent: 'center', zIndex: 1000 }}>
          <div className="modal-content" onClick={(e) => e.stopPropagation()} style={{ background: 'var(--vscode-editor-background)', padding: '16px', borderRadius: '8px', minWidth: '320px' }}>
            <h3>{editingGrant ? 'Edit Grant' : 'Add Grant'}</h3>
            <label>Principal</label>
            <input type="text" value={grantForm.principal} onChange={(e) => setGrantForm({ ...grantForm, principal: e.target.value })} placeholder="e.g. data_engineers" style={{ width: '100%', marginBottom: '8px' }} readOnly={!!editingGrant} />
            <label>Privileges (comma-separated, e.g. EXECUTE)</label>
            <input type="text" value={grantForm.privileges} onChange={(e) => setGrantForm({ ...grantForm, privileges: e.target.value })} placeholder="EXECUTE" style={{ width: '100%', marginBottom: '12px' }} />
            <div style={{ display: 'flex', gap: '8px', justifyContent: 'flex-end' }}>
              <VSCodeButton appearance="secondary" onClick={() => { setAddGrantDialog(false); setEditingGrant(null); }}>Cancel</VSCodeButton>
              <VSCodeButton onClick={saveGrant} disabled={!grantForm.principal.trim() || !grantForm.privileges.trim()}>
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
