import React, { useState, useEffect } from 'react';
import { useDesignerStore } from '../state/useDesignerStore';
import { VSCodeButton, VSCodeTextField, VSCodeDropdown, VSCodeOption } from '@vscode/webview-ui-toolkit/react';
import { validateUnityCatalogObjectName } from '../utils/unityNames';
import { RichComment } from './RichComment';

// Codicon icons - theme-aware and vector-based
const IconEditInline: React.FC = () => (
  <i slot="start" className="codicon codicon-edit" aria-hidden="true"></i>
);

const IconEdit: React.FC = () => (
  <i slot="start" className="codicon codicon-edit" aria-hidden="true"></i>
);

const IconTrash: React.FC = () => (
  <i slot="start" className="codicon codicon-trash" aria-hidden="true"></i>
);

interface SchemaDetailsProps {
  schemaId: string;
}

export const SchemaDetails: React.FC<SchemaDetailsProps> = ({ schemaId }) => {
  const { project, findSchema, updateSchema, renameSchema, addGrant, revokeGrant } = useDesignerStore();
  const schemaInfo = findSchema(schemaId);
  const schema = schemaInfo?.schema;
  const catalog = schemaInfo?.catalog;

  const MANAGED_LOCATION_DEFAULT = '__default__';
  const [managedLocationName, setManagedLocationName] = useState(
    schema?.managedLocationName ? schema.managedLocationName : MANAGED_LOCATION_DEFAULT
  );
  const [tags, setTags] = useState<Record<string, string>>(schema?.tags || {});
  const [copySuccess, setCopySuccess] = useState(false);
  const [renameDialog, setRenameDialog] = useState(false);
  const [newName, setNewName] = useState('');
  const [renameError, setRenameError] = useState<string | null>(null);
  const [commentDialog, setCommentDialog] = useState<{schemaId: string, comment: string} | null>(null);
  const [editingTag, setEditingTag] = useState<string | null>(null);
  const [editTagValue, setEditTagValue] = useState('');
  const [isAdding, setIsAdding] = useState(false);
  const [newTagName, setNewTagName] = useState('');
  const [newTagValue, setNewTagValue] = useState('');
  const [deleteDialog, setDeleteDialog] = useState<string | null>(null);
  const [addGrantDialog, setAddGrantDialog] = useState(false);
  const [revokeGrantDialog, setRevokeGrantDialog] = useState<{ principal: string } | null>(null);
  const [grantForm, setGrantForm] = useState({ principal: '', privileges: '' });

  // Update local state when schema changes
  useEffect(() => {
    if (schema) {
      setManagedLocationName(schema.managedLocationName ? schema.managedLocationName : MANAGED_LOCATION_DEFAULT);
      setTags(schema.tags || {});
    }
  }, [schema]);

  if (!schema || !catalog) {
    return (
      <div className="table-designer">
        <div className="empty-state">
          <h2>Schema not found</h2>
          <p>The selected schema could not be found.</p>
        </div>
      </div>
    );
  }

  const handleManagedLocationChange = (newLocation: string) => {
    const isDefault = newLocation === MANAGED_LOCATION_DEFAULT || newLocation === '';
    const value = isDefault ? null : newLocation; // null survives JSON so reducer can clear
    setManagedLocationName(isDefault ? MANAGED_LOCATION_DEFAULT : newLocation);
    updateSchema(schemaId, { managedLocationName: value });
  };

  const handleSetComment = () => {
    setCommentDialog({schemaId: schema.id, comment: schema.comment || ''});
  };

  const handleAddTag = () => {
    if (newTagName.trim() && newTagValue.trim()) {
      // Immediately persist the tag via operation
      const updatedTags = { ...tags, [newTagName]: newTagValue };
      updateSchema(schemaId, { tags: updatedTags });
      setTags(updatedTags);
      setNewTagName('');
      setNewTagValue('');
      setIsAdding(false);
    }
  };

  const handleCancelAdd = () => {
    setIsAdding(false);
    setNewTagName('');
    setNewTagValue('');
  };

  const handleRemoveTag = (tagName: string) => {
    const newTags = { ...tags };
    delete newTags[tagName];
    // Immediately persist the removal via operation
    updateSchema(schemaId, { tags: newTags });
    setTags(newTags);
    setDeleteDialog(null);
  };

  const handleStartEditTag = (tagName: string, tagValue: string) => {
    setEditingTag(tagName);
    setEditTagValue(tagValue);
  };

  const handleSaveEditTag = (oldTagName: string) => {
    if (!editTagValue.trim()) return;
    const newTags = { ...tags };
    newTags[oldTagName] = editTagValue;
    // Immediately persist the tag update via operation
    updateSchema(schemaId, { tags: newTags });
    setTags(newTags);
    setEditingTag(null);
    setEditTagValue('');
  };

  const handleCancelEditTag = () => {
    setEditingTag(null);
    setEditTagValue('');
  };

  const handleCopySchemaName = () => {
    const fullName = `${catalog.name}.${schema.name}`;
    navigator.clipboard.writeText(fullName).then(() => {
      setCopySuccess(true);
      setTimeout(() => setCopySuccess(false), 2000);
    });
  };

  const handleOpenRenameDialog = () => {
    setNewName(schema.name);
    setRenameDialog(true);
    // Auto-focus the input field after a short delay
    setTimeout(() => {
      const input = document.getElementById('rename-schema-input') as any;
      if (input && input.shadowRoot) {
        const inputElement = input.shadowRoot.querySelector('input');
        if (inputElement) inputElement.focus();
      }
    }, 100);
  };

  const handleCloseRenameDialog = () => {
    setRenameDialog(false);
    setNewName('');
    setRenameError(null);
  };

  const handleConfirmRename = (e: React.FormEvent) => {
    e.preventDefault();
    const trimmedName = newName.trim();
    const nameError = validateUnityCatalogObjectName(trimmedName);
    if (nameError) {
      setRenameError(nameError);
      return;
    }
    if (trimmedName && trimmedName !== schema.name) {
      renameSchema(schemaId, trimmedName);
    }
    handleCloseRenameDialog();
  };

  return (
    <div className="table-designer">
      <div className="table-header">
        <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
          <div style={{ display: 'flex', alignItems: 'center', gap: '6px' }}>
            <h2 style={{ marginBottom: 0 }}>{catalog.name}.{schema.name}</h2>
            <button
              onClick={handleCopySchemaName}
              title={copySuccess ? 'Copied!' : 'Copy schema name'}
              style={{
                background: 'transparent',
                border: 'none',
                cursor: 'pointer',
                padding: '2px',
                display: 'flex',
                alignItems: 'center',
                justifyContent: 'center',
                color: copySuccess ? 'var(--vscode-testing-iconPassed)' : 'var(--vscode-foreground)',
                opacity: copySuccess ? 1 : 0.6,
                height: '20px',
                width: '20px',
              }}
            >
              <i className={`codicon ${copySuccess ? 'codicon-check' : 'codicon-copy'}`} style={{ fontSize: '14px' }}></i>
            </button>
            <button
              onClick={handleOpenRenameDialog}
              title="Edit schema name"
              style={{
                background: 'transparent',
                border: 'none',
                cursor: 'pointer',
                padding: '2px',
                display: 'flex',
                alignItems: 'center',
                justifyContent: 'center',
                color: 'var(--vscode-foreground)',
                opacity: 0.6,
                height: '20px',
                width: '20px',
              }}
            >
              <i className="codicon codicon-edit" style={{ fontSize: '14px' }}></i>
            </button>
          </div>
        </div>
        <div className="table-metadata">
          <span className="badge">SCHEMA</span>
        </div>
      </div>

      {/* Comment */}
      <div className="table-properties">
        <div className="property-row">
          <label>Comment:</label>
          <div className="property-value">
            {schema.comment ? (
              <RichComment text={schema.comment} />
            ) : (
              <span className="inline-warning">
                <span className="inline-warning__dot" aria-hidden="true" />
                Comment recommended
              </span>
            )}
            <VSCodeButton appearance="icon" type="button" onClick={handleSetComment}>
              <IconEditInline />
            </VSCodeButton>
          </div>
        </div>
      </div>

      {/* Managed Location */}
      <div className="table-properties">
        <div className="property-row">
          <label>
            Managed Location
            <span className="info-icon" title="Storage location for managed tables"> ℹ️</span>
          </label>
          <div className="property-value">
            <VSCodeDropdown
              value={managedLocationName}
              style={{ width: '100%' }}
              onInput={(e: Event) => {
                const target = e.target as HTMLSelectElement;
                handleManagedLocationChange(target.value);
              }}
            >
              <VSCodeOption value={MANAGED_LOCATION_DEFAULT}>— Default —</VSCodeOption>
              {Object.entries(project?.managedLocations || {}).map(([name, location]: [string, any]) => (
                <VSCodeOption key={name} value={name}>
                  {name} {location.description && `(${location.description})`}
                </VSCodeOption>
              ))}
            </VSCodeDropdown>
          </div>
        </div>
        <div style={{ marginTop: '8px', fontSize: '11px', color: 'var(--vscode-descriptionForeground)' }}>
          Changing managed location may be rejected by Unity Catalog depending on object state and permissions.
        </div>
        {managedLocationName !== MANAGED_LOCATION_DEFAULT && managedLocationName && project?.managedLocations?.[managedLocationName] && (
          <div style={{ marginTop: '8px', fontSize: '11px', color: 'var(--vscode-descriptionForeground)' }}>
            <strong>Paths:</strong>
            <div style={{ marginTop: '4px' }}>
              {Object.entries(project.managedLocations[managedLocationName].paths || {}).map(([env, path]) => (
                <div key={env} style={{ marginLeft: '8px' }}>
                  <span style={{ fontWeight: 500 }}>{env}:</span> <code>{path}</code>
                </div>
              ))}
            </div>
          </div>
        )}
      </div>

      {/* Tags */}
      <div className="table-properties-section">
        <h3>Schema Tags (Unity Catalog)</h3>
        
        {Object.keys(tags).length === 0 && !isAdding ? (
          <div className="empty-properties">
            <p>No schema tags defined</p>
          </div>
        ) : (
          <table className="properties-table">
            <thead>
              <tr>
                <th>Tag Name</th>
                <th>Value</th>
                <th>Actions</th>
              </tr>
            </thead>
            <tbody>
              {Object.entries(tags).map(([tagName, tagValue]) => (
                <tr key={tagName}>
                  <td><code>{tagName}</code></td>
                  <td>
                    {editingTag === tagName ? (
                      <input
                        type="text"
                        value={editTagValue}
                        onChange={(e) => setEditTagValue(e.target.value)}
                        autoFocus
                      />
                    ) : (
                      String(tagValue)
                    )}
                  </td>
                  <td className="actions-cell">
                    {editingTag === tagName ? (
                      <>
                        <button 
                          className="action-button-save"
                          onClick={() => handleSaveEditTag(tagName)}
                          title="Save"
                        >
                          ✓
                        </button>
                        <button 
                          className="action-button-cancel"
                          onClick={handleCancelEditTag}
                          title="Cancel"
                        >
                          ✕
                        </button>
                      </>
                    ) : (
                      <>
                        <VSCodeButton
                          appearance="icon"
                          onClick={() => handleStartEditTag(tagName, String(tagValue))}
                          title="Edit tag"
                        >
                          <IconEdit />
                        </VSCodeButton>
                        <VSCodeButton
                          appearance="icon"
                          onClick={() => setDeleteDialog(tagName)}
                          title="Delete tag"
                        >
                          <IconTrash />
                        </VSCodeButton>
                      </>
                    )}
                  </td>
                </tr>
              ))}
              
              {isAdding && (
                <tr className="adding-row">
                  <td>
                    <input
                      type="text"
                      value={newTagName}
                      onChange={(e) => setNewTagName(e.target.value)}
                      placeholder="e.g., data_classification"
                      autoFocus
                    />
                  </td>
                  <td>
                    <input
                      type="text"
                      value={newTagValue}
                      onChange={(e) => setNewTagValue(e.target.value)}
                      placeholder="e.g., confidential"
                    />
                  </td>
                  <td className="actions-cell">
                    <button 
                      className="action-button-save"
                      onClick={handleAddTag}
                      title="Add tag"
                    >
                      ✓
                    </button>
                    <button 
                      className="action-button-cancel"
                      onClick={handleCancelAdd}
                      title="Cancel"
                    >
                      ✕
                    </button>
                  </td>
                </tr>
              )}
            </tbody>
          </table>
        )}
        
        {!isAdding && (
          <button 
            className="add-property-btn"
            onClick={() => setIsAdding(true)}
          >
            + Add Tag
          </button>
        )}

        {deleteDialog && (
          <div className="modal-overlay" onClick={() => setDeleteDialog(null)}>
            <div className="modal-content" onClick={(e) => e.stopPropagation()}>
              <h3>Delete Schema Tag</h3>
              <p>Are you sure you want to delete tag <code>{deleteDialog}</code>?</p>
              <p className="warning-text">This will generate an UNSET TAGS operation.</p>
              <div className="modal-buttons">
                <button onClick={() => handleRemoveTag(deleteDialog)} style={{ backgroundColor: 'var(--vscode-errorForeground)' }}>
                  Delete
                </button>
                <button onClick={() => setDeleteDialog(null)}>Cancel</button>
              </div>
            </div>
          </div>
        )}
      </div>

      {/* Grants */}
      <div className="table-properties-section">
        <h3>Grants ({schema.grants?.length ?? 0})</h3>
        {(!schema.grants || schema.grants.length === 0) && !addGrantDialog ? (
          <div className="empty-properties">
            <p>No grants defined. Grant privileges (e.g. USE SCHEMA, CREATE TABLE) to users or groups.</p>
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
              {(schema.grants || []).map((g) => (
                <tr key={g.principal}>
                  <td>{g.principal}</td>
                  <td>{(g.privileges || []).join(', ')}</td>
                  <td>
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
        <div className="modal-overlay" onClick={() => setAddGrantDialog(false)}>
          <div className="modal-content" onClick={(e) => e.stopPropagation()}>
            <h3>Add Grant</h3>
            <div className="modal-body">
              <label>Principal</label>
              <input type="text" value={grantForm.principal} onChange={(e) => setGrantForm({ ...grantForm, principal: e.target.value })} placeholder="e.g. data_engineers" />
              <label>Privileges (comma-separated, e.g. USE SCHEMA, CREATE TABLE)</label>
              <input type="text" value={grantForm.privileges} onChange={(e) => setGrantForm({ ...grantForm, privileges: e.target.value })} placeholder="USE SCHEMA, CREATE TABLE" />
            </div>
            <div className="modal-buttons">
              <button onClick={() => { setAddGrantDialog(false); setGrantForm({ principal: '', privileges: '' }); }}>Cancel</button>
              <button
                onClick={() => {
                  const principal = grantForm.principal.trim();
                  const privs = grantForm.privileges.split(/[\s,]+/).filter(Boolean);
                  if (principal && privs.length > 0) { addGrant('schema', schemaId, principal, privs); setAddGrantDialog(false); setGrantForm({ principal: '', privileges: '' }); }
                }}
                disabled={!grantForm.principal.trim() || !grantForm.privileges.trim()}
              >
                Add Grant
              </button>
            </div>
          </div>
        </div>
      )}

      {revokeGrantDialog && (
        <div className="modal-overlay" onClick={() => setRevokeGrantDialog(null)}>
          <div className="modal-content" onClick={(e) => e.stopPropagation()}>
            <h3>Revoke Grant</h3>
            <p>Revoke all privileges for <strong>{revokeGrantDialog.principal}</strong> on this schema?</p>
            <div className="modal-buttons">
              <button onClick={() => { revokeGrant('schema', schemaId, revokeGrantDialog.principal); setRevokeGrantDialog(null); }} style={{ backgroundColor: 'var(--vscode-errorForeground)' }}>Revoke</button>
              <button onClick={() => setRevokeGrantDialog(null)}>Cancel</button>
            </div>
          </div>
        </div>
      )}

      {/* Rename Dialog */}
      {renameDialog && (
        <div className="modal-overlay" onClick={handleCloseRenameDialog}>
          <form
            className="modal-content modal-surface"
            onClick={(e) => e.stopPropagation()}
            onSubmit={handleConfirmRename}
          >
            <h3>Rename Schema</h3>
            
            <div className="modal-field-group">
              <label htmlFor="rename-schema-input">Name</label>
              <VSCodeTextField
                id="rename-schema-input"
                value={newName}
                placeholder="Schema name"
                onInput={(e: Event) => {
                  const target = e.target as HTMLInputElement;
                  setNewName(target.value);
                  setRenameError(null);
                }}
                style={{ width: '100%' }}
              />
              {renameError && <p className="form-error">{renameError}</p>}
            </div>

            <div className="modal-actions">
              <VSCodeButton type="button" appearance="secondary" onClick={handleCloseRenameDialog}>
                Cancel
              </VSCodeButton>
              <VSCodeButton type="submit">
                Rename
              </VSCodeButton>
            </div>
          </form>
        </div>
      )}

      {/* Comment Dialog */}
      {commentDialog && (
        <div className="modal" onClick={() => setCommentDialog(null)}>
          <div className="modal-content" onClick={(e) => e.stopPropagation()}>
            <h3>Set Schema Comment</h3>
            <input
              type="text"
              defaultValue={commentDialog.comment}
              autoFocus
              onKeyDown={(e) => {
                if (e.key === 'Enter') {
                  updateSchema(commentDialog.schemaId, { comment: (e.target as HTMLInputElement).value });
                  setCommentDialog(null);
                } else if (e.key === 'Escape') {
                  setCommentDialog(null);
                }
              }}
              id="schema-comment-input"
            />
            <div className="modal-buttons">
              <button onClick={() => {
                const input = document.getElementById('schema-comment-input') as HTMLInputElement;
                updateSchema(commentDialog.schemaId, { comment: input.value });
                setCommentDialog(null);
              }}>Set</button>
              <button onClick={() => setCommentDialog(null)}>Cancel</button>
            </div>
          </div>
        </div>
      )}
    </div>
  );
};
