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

interface VolumeDetailsProps {
  volumeId: string;
}

export const VolumeDetails: React.FC<VolumeDetailsProps> = ({ volumeId }) => {
  const { findVolume, updateVolume, addGrant, revokeGrant } = useDesignerStore();
  const info = findVolume(volumeId);
  const [commentDialog, setCommentDialog] = useState(false);
  const [commentInput, setCommentInput] = useState('');
  const [locationDialog, setLocationDialog] = useState(false);
  const [locationInput, setLocationInput] = useState('');
  const [addGrantDialog, setAddGrantDialog] = useState(false);
  const [editingGrant, setEditingGrant] = useState<{ principal: string; privileges: string[] } | null>(null);
  const [revokeGrantDialog, setRevokeGrantDialog] = useState<{ principal: string } | null>(null);
  const [grantForm, setGrantForm] = useState({ principal: '', privileges: '' });

  if (!info) {
    return (
      <div className="view-details">
        <div className="empty-state"><p>Volume not found</p></div>
      </div>
    );
  }

  const { catalog, schema, volume } = info;
  const vol = volume as {
    id: string;
    name: string;
    volumeType?: string;
    comment?: string;
    location?: string;
    grants?: { principal: string; privileges: string[] }[];
  };

  const openCommentDialog = () => {
    setCommentInput(vol.comment ?? '');
    setCommentDialog(true);
  };

  const saveComment = () => {
    updateVolume(volumeId, { comment: commentInput.trim() || undefined });
    setCommentDialog(false);
  };

  const openLocationDialog = () => {
    setLocationInput(vol.location ?? '');
    setLocationDialog(true);
  };

  const saveLocation = () => {
    updateVolume(volumeId, { location: locationInput.trim() || undefined });
    setLocationDialog(false);
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
    if (editingGrant) {
      revokeGrant('volume', volumeId, editingGrant.principal);
    }
    addGrant('volume', volumeId, principal, privs);
    setAddGrantDialog(false);
    setEditingGrant(null);
    setGrantForm({ principal: '', privileges: '' });
  };

  const grants = vol.grants ?? [];

  return (
    <div className="view-details">
      <div className="view-header">
        <div className="view-title">
          <i className="codicon codicon-folder" aria-hidden="true" />
          <h2>{catalog.name}.{schema.name}.{vol.name}</h2>
        </div>
        <span className="view-badge" style={{ background: 'var(--vscode-charts-purple)', color: 'white' }}>VOLUME</span>
      </div>

      {/* Properties - table-inspired: property rows with pencil to edit */}
      <div className="view-section">
        <h3>Properties</h3>
        <div className="table-properties">
          <div className="property-row">
            <label>Type</label>
            <div className="property-value">
              <span>{vol.volumeType === 'external' ? 'External' : 'Managed'}</span>
            </div>
          </div>
          {vol.volumeType === 'external' && (
            <div className="property-row">
              <label>Location</label>
              <div className="property-value">
                <code style={{ flex: 1, wordBreak: 'break-all' }}>{vol.location || '—'}</code>
                <VSCodeButton appearance="icon" onClick={openLocationDialog} title="Edit location">
                  <IconEdit />
                </VSCodeButton>
              </div>
            </div>
          )}
          <div className="property-row">
            <label>Comment</label>
            <div className="property-value">
              {vol.comment ? (
                <span style={{ flex: 1 }}>{vol.comment}</span>
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
        </div>
      </div>

      {/* Grants */}
      <div className="table-properties-section">
        <h3>Grants ({grants.length})</h3>
        {grants.length === 0 && !addGrantDialog ? (
          <div className="empty-properties">
            <p>No grants defined. Grant privileges (e.g. READ VOLUME, WRITE VOLUME) to users or groups.</p>
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
                      onClick={() => openEditGrant(g.principal, g.privileges || [])}
                      title="Edit grant"
                    >
                      <IconEdit />
                    </VSCodeButton>
                    <VSCodeButton
                      appearance="icon"
                      onClick={() => setRevokeGrantDialog({ principal: g.principal })}
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
        <button className="add-property-btn" onClick={openAddGrant}>
          + Add Grant
        </button>
      </div>

      {/* Comment modal - table-style: modal with Set/Cancel at bottom */}
      {commentDialog && (
        <div className="modal" onClick={() => setCommentDialog(false)} role="dialog">
          <div className="modal-content" onClick={(e) => e.stopPropagation()}>
            <h3>Set Volume Comment</h3>
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

      {/* Location modal (external only) */}
      {locationDialog && (
        <div className="modal" onClick={() => setLocationDialog(false)} role="dialog">
          <div className="modal-content" onClick={(e) => e.stopPropagation()}>
            <h3>Set Volume Location</h3>
            <input
              type="text"
              value={locationInput}
              onChange={(e) => setLocationInput(e.target.value)}
              placeholder="abfss://... or s3://..."
              style={{ width: '100%', marginBottom: '12px', padding: '8px', fontFamily: 'monospace' }}
              autoFocus
            />
            <div className="modal-buttons">
              <VSCodeButton onClick={saveLocation}>Set</VSCodeButton>
              <VSCodeButton appearance="secondary" onClick={() => setLocationDialog(false)}>Cancel</VSCodeButton>
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
            <input
              type="text"
              value={grantForm.principal}
              onChange={(e) => setGrantForm({ ...grantForm, principal: e.target.value })}
              placeholder="e.g. data_engineers"
              style={{ width: '100%', marginBottom: '8px' }}
              readOnly={!!editingGrant}
            />
            <label>Privileges (comma-separated; e.g. READ VOLUME, WRITE VOLUME)</label>
            <input
              type="text"
              value={grantForm.privileges}
              onChange={(e) => setGrantForm({ ...grantForm, privileges: e.target.value })}
              placeholder="READ VOLUME, WRITE VOLUME"
              style={{ width: '100%', marginBottom: '8px' }}
            />
            <p className="modal-field-hint" style={{ marginTop: 0, marginBottom: '12px' }}>
              Use commas to separate privileges. &quot;READ VOLUME&quot; is one privilege, not two.
            </p>
            <div style={{ display: 'flex', gap: '8px', justifyContent: 'flex-end' }}>
              <VSCodeButton appearance="secondary" onClick={() => { setAddGrantDialog(false); setEditingGrant(null); }}>Cancel</VSCodeButton>
              <VSCodeButton
                onClick={saveGrant}
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
            <p>Revoke all privileges for <strong>{revokeGrantDialog.principal}</strong> on this volume?</p>
            <div style={{ display: 'flex', gap: '8px', justifyContent: 'flex-end' }}>
              <VSCodeButton appearance="secondary" onClick={() => setRevokeGrantDialog(null)}>Cancel</VSCodeButton>
              <VSCodeButton onClick={() => { revokeGrant('volume', volumeId, revokeGrantDialog.principal); setRevokeGrantDialog(null); }}>Revoke</VSCodeButton>
            </div>
          </div>
        </div>
      )}
    </div>
  );
};
