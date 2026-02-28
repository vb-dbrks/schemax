import React, { useState, useEffect } from 'react';
import { useDesignerStore } from '../state/useDesignerStore';
import { VSCodeButton, VSCodeTextField, VSCodeDropdown, VSCodeOption } from '@vscode/webview-ui-toolkit/react';
import { validateUnityCatalogObjectName } from '../utils/unityNames';
import { parsePrivileges } from '../utils/grants';
import { RichComment } from './RichComment';
import { BulkOperationsPanel } from './BulkOperationsPanel';
import type { NamingStandardsRule, NamingRuleObjectType } from '../providers/unity/models';

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

const IconPlus: React.FC = () => (
  <i slot="start" className="codicon codicon-add" aria-hidden="true"></i>
);

const OBJECT_TYPE_LABELS: Record<NamingRuleObjectType, string> = {
  schema: 'Schema',
  table: 'Table',
  view: 'View',
  materialized_view: 'Materialized view',
  column: 'Column',
  volume: 'Volume',
  function: 'Function',
};

function newRuleId(): string {
  return `rule_${Date.now()}_${Math.random().toString(36).slice(2)}`;
}

interface CatalogDetailsProps {
  catalogId: string;
}

export const CatalogDetails: React.FC<CatalogDetailsProps> = ({ catalogId }) => {
  const { project, findCatalog, updateCatalog, renameCatalog, addGrant, revokeGrant } = useDesignerStore();
  const catalog = findCatalog(catalogId);

  const MANAGED_LOCATION_DEFAULT = '__default__';
  const [managedLocationName, setManagedLocationName] = useState(
    catalog?.managedLocationName ? catalog.managedLocationName : MANAGED_LOCATION_DEFAULT
  );
  const [tags, setTags] = useState<Record<string, string>>(catalog?.tags || {});
  const [copySuccess, setCopySuccess] = useState(false);
  const [renameDialog, setRenameDialog] = useState(false);
  const [newName, setNewName] = useState('');
  const [renameError, setRenameError] = useState<string | null>(null);
  const [commentDialog, setCommentDialog] = useState<{catalogId: string, comment: string} | null>(null);
  const [editingTag, setEditingTag] = useState<string | null>(null);
  const [editTagValue, setEditTagValue] = useState('');
  const [isAdding, setIsAdding] = useState(false);
  const [newTagName, setNewTagName] = useState('');
  const [newTagValue, setNewTagValue] = useState('');
  const [deleteDialog, setDeleteDialog] = useState<string | null>(null);
  const [addGrantDialog, setAddGrantDialog] = useState(false);
  const [editingGrant, setEditingGrant] = useState<{ principal: string; privileges: string[] } | null>(null);
  const [revokeGrantDialog, setRevokeGrantDialog] = useState<{ principal: string } | null>(null);
  const [grantForm, setGrantForm] = useState({ principal: '', privileges: '' });
  const [showBulkPanel, setShowBulkPanel] = useState(false);
  const [addRuleForm, setAddRuleForm] = useState(false);
  const [newRuleObjectType, setNewRuleObjectType] = useState<NamingRuleObjectType>('schema');
  const [newRulePattern, setNewRulePattern] = useState('');
  const [deleteRuleId, setDeleteRuleId] = useState<string | null>(null);
  const [editingRuleId, setEditingRuleId] = useState<string | null>(null);
  const [editRuleObjectType, setEditRuleObjectType] = useState<NamingRuleObjectType>('schema');
  const [editRulePattern, setEditRulePattern] = useState('');
  const [namingPatternError, setNamingPatternError] = useState<string | null>(null);

  // Update local state when catalog changes
  useEffect(() => {
    if (catalog) {
      setManagedLocationName(catalog.managedLocationName ? catalog.managedLocationName : MANAGED_LOCATION_DEFAULT);
      setTags(catalog.tags || {});
    }
  }, [catalog]);

  if (!catalog) {
    return (
      <div className="table-designer">
        <div className="empty-state">
          <h2>Catalog not found</h2>
          <p>The selected catalog could not be found.</p>
        </div>
      </div>
    );
  }

  const handleManagedLocationChange = (newLocation: string) => {
    const isDefault = newLocation === MANAGED_LOCATION_DEFAULT || newLocation === '';
    const value = isDefault ? null : newLocation; // null survives JSON so reducer can clear
    setManagedLocationName(isDefault ? MANAGED_LOCATION_DEFAULT : newLocation);
    updateCatalog(catalogId, { managedLocationName: value });
  };

  const handleSetComment = () => {
    setCommentDialog({catalogId: catalog.id, comment: catalog.comment || ''});
  };

  const handleAddTag = () => {
    if (newTagName.trim() && newTagValue.trim()) {
      // Immediately persist the tag via operation
      const updatedTags = { ...tags, [newTagName]: newTagValue };
      updateCatalog(catalogId, { tags: updatedTags });
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
    updateCatalog(catalogId, { tags: newTags });
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
    updateCatalog(catalogId, { tags: newTags });
    setTags(newTags);
    setEditingTag(null);
    setEditTagValue('');
  };

  const handleCancelEditTag = () => {
    setEditingTag(null);
    setEditTagValue('');
  };

  const rules: NamingStandardsRule[] = catalog?.namingStandards?.rules ?? [];

  const handleAddNamingRule = () => {
    const pattern = newRulePattern.trim();
    if (!pattern) {
      setNamingPatternError('Pattern (regex) cannot be empty.');
      return;
    }
    setNamingPatternError(null);
    const newRule: NamingStandardsRule = {
      id: newRuleId(),
      objectType: newRuleObjectType,
      pattern,
      enabled: true,
    };
    const nextRules = [...rules, newRule];
    updateCatalog(catalogId, {
      namingStandards: { ...catalog?.namingStandards, rules: nextRules },
    });
    setAddRuleForm(false);
    setNewRuleObjectType('schema');
    setNewRulePattern('');
  };

  const handleStartEditRule = (rule: NamingStandardsRule) => {
    setEditingRuleId(rule.id);
    setEditRuleObjectType(rule.objectType);
    setEditRulePattern(rule.pattern ?? '');
  };

  const handleSaveEditRule = (ruleId: string) => {
    const pattern = editRulePattern.trim();
    if (!pattern) {
      setNamingPatternError('Pattern (regex) cannot be empty.');
      return;
    }
    setNamingPatternError(null);
    const nextRules = rules.map((r) =>
      r.id === ruleId ? { ...r, objectType: editRuleObjectType, pattern } : r
    );
    updateCatalog(catalogId, {
      namingStandards: { ...catalog?.namingStandards, rules: nextRules },
    });
    setEditingRuleId(null);
    setEditRuleObjectType('schema');
    setEditRulePattern('');
  };

  const handleCancelEditRule = () => {
    setEditingRuleId(null);
    setEditRuleObjectType('schema');
    setEditRulePattern('');
    setNamingPatternError(null);
  };

  const handleRemoveNamingRule = (ruleId: string) => {
    const nextRules = rules.filter((r) => r.id !== ruleId);
    updateCatalog(catalogId, {
      namingStandards: { ...catalog?.namingStandards, rules: nextRules },
    });
    setDeleteRuleId(null);
  };

  const handleCopyCatalogName = () => {
    const fullName = catalog.name;
    navigator.clipboard.writeText(fullName).then(() => {
      setCopySuccess(true);
      setTimeout(() => setCopySuccess(false), 2000);
    });
  };

  const handleOpenRenameDialog = () => {
    setNewName(catalog.name);
    setRenameDialog(true);
    // Auto-focus the input field after a short delay
    setTimeout(() => {
      const input = document.getElementById('rename-catalog-input') as any;
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
    if (trimmedName && trimmedName !== catalog.name) {
      renameCatalog(catalogId, trimmedName);
    }
    handleCloseRenameDialog();
  };

  return (
    <div className="table-designer">
      <div className="table-header">
        <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
          <div style={{ display: 'flex', alignItems: 'center', gap: '6px' }}>
            <h2 style={{ marginBottom: 0 }}>{catalog.name}</h2>
            <button
              onClick={handleCopyCatalogName}
              title={copySuccess ? 'Copied!' : 'Copy catalog name'}
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
              title="Edit catalog name"
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
          <VSCodeButton
            appearance="secondary"
            onClick={() => setShowBulkPanel(true)}
            title="Apply grants or tags in bulk to this catalog and all objects under it"
          >
            Bulk operations
          </VSCodeButton>
        </div>
        <div className="table-metadata">
          <span className="badge">CATALOG</span>
        </div>
      </div>

      {/* Comment */}
      <div className="table-properties">
        <div className="property-row">
          <label>Comment:</label>
          <div className="property-value">
            {catalog.comment ? (
              <RichComment text={catalog.comment} />
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
      <div className="table-properties" style={{ marginTop: '24px' }}>
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
        <h3>Catalog Tags (Unity Catalog)</h3>
        
        {Object.keys(tags).length === 0 && !isAdding ? (
          <div className="empty-properties">
            <p>No catalog tags defined</p>
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
      </div>

      {/* Grants */}
      <div className="table-properties-section">
        <h3>Grants ({catalog.grants?.length ?? 0})</h3>
        {(!catalog.grants || catalog.grants.length === 0) && !addGrantDialog ? (
          <div className="empty-properties">
            <p>No grants defined. Grant privileges (e.g. USE CATALOG, CREATE SCHEMA) to users or groups.</p>
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
              {(catalog.grants || []).map((g) => (
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

      {/* Naming Standards (catalog-level) */}
      <div className="table-properties-section">
        <h3>Naming Standards</h3>

        {rules.length === 0 && !addRuleForm ? (
          <div className="empty-properties">
            <p>No naming standards configured for this catalog.</p>
          </div>
        ) : (
          <table className="properties-table">
            <thead>
              <tr>
                <th>Object</th>
                <th>Pattern (regex)</th>
                <th>Actions</th>
              </tr>
            </thead>
            <tbody>
              {rules.map((rule) => (
                <tr key={rule.id}>
                  {editingRuleId === rule.id ? (
                    <>
                      <td>
                        <VSCodeDropdown
                          value={editRuleObjectType}
                          style={{ minWidth: '140px' }}
                          onInput={(e: Event) => {
                            const target = e.target as HTMLSelectElement;
                            setEditRuleObjectType(target.value as NamingRuleObjectType);
                          }}
                        >
                          {(Object.keys(OBJECT_TYPE_LABELS) as NamingRuleObjectType[]).map((key) => (
                            <VSCodeOption key={key} value={key}>
                              {OBJECT_TYPE_LABELS[key]}
                            </VSCodeOption>
                          ))}
                        </VSCodeDropdown>
                      </td>
                      <td>
                        <input
                          type="text"
                          value={editRulePattern}
                          placeholder="^[a-z][a-z0-9_]*$"
                          onChange={(e) => {
                            setEditRulePattern(e.target.value);
                            setNamingPatternError(null);
                          }}
                        />
                      </td>
                      <td className="actions-cell">
                        <button
                          className="action-button-save"
                          onClick={() => handleSaveEditRule(rule.id)}
                          title="Save"
                        >
                          ✓
                        </button>
                        <button
                          className="action-button-cancel"
                          onClick={handleCancelEditRule}
                          title="Cancel"
                        >
                          ✕
                        </button>
                      </td>
                    </>
                  ) : (
                    <>
                      <td><code>{OBJECT_TYPE_LABELS[rule.objectType]}</code></td>
                      <td>
                        <code title={rule.pattern || ''}>{rule.pattern ?? '—'}</code>
                      </td>
                      <td className="actions-cell">
                        <VSCodeButton
                          appearance="icon"
                          onClick={() => handleStartEditRule(rule)}
                          title="Edit rule"
                        >
                          <IconEdit />
                        </VSCodeButton>
                        <VSCodeButton
                          appearance="icon"
                          onClick={() => setDeleteRuleId(rule.id)}
                          title="Delete rule"
                        >
                          <IconTrash />
                        </VSCodeButton>
                      </td>
                    </>
                  )}
                </tr>
              ))}
              {addRuleForm && (
                <tr className="adding-row">
                  <td>
                    <VSCodeDropdown
                      value={newRuleObjectType}
                      style={{ minWidth: '140px' }}
                      onInput={(e: Event) => {
                        const target = e.target as HTMLSelectElement;
                        setNewRuleObjectType(target.value as NamingRuleObjectType);
                      }}
                    >
                      {(Object.keys(OBJECT_TYPE_LABELS) as NamingRuleObjectType[]).map((key) => (
                        <VSCodeOption key={key} value={key}>
                          {OBJECT_TYPE_LABELS[key]}
                        </VSCodeOption>
                      ))}
                    </VSCodeDropdown>
                  </td>
                  <td>
                    <input
                      type="text"
                      value={newRulePattern}
                      placeholder="^[a-z][a-z0-9_]*$"
                      onChange={(e) => {
                        setNewRulePattern(e.target.value);
                        setNamingPatternError(null);
                      }}
                    />
                  </td>
                  <td className="actions-cell">
                    <button
                      className="action-button-save"
                      onClick={handleAddNamingRule}
                      title="Add rule"
                    >
                      ✓
                    </button>
                    <button
                      className="action-button-cancel"
                      onClick={() => {
                        setAddRuleForm(false);
                        setNewRulePattern('');
                        setNamingPatternError(null);
                      }}
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

        {!addRuleForm && !editingRuleId && (
          <button
            className="add-property-btn"
            onClick={() => setAddRuleForm(true)}
          >
            + Add Rule
          </button>
        )}
        {deleteRuleId && (
          <div className="modal-overlay" onClick={() => setDeleteRuleId(null)}>
            <div className="modal-content" onClick={(e) => e.stopPropagation()}>
              <h3>Delete naming rule</h3>
              <p>Remove this rule from the catalog naming standards?</p>
              <div className="modal-buttons">
                <button onClick={() => handleRemoveNamingRule(deleteRuleId)} style={{ backgroundColor: 'var(--vscode-errorForeground)' }}>
                  Delete
                </button>
                <button onClick={() => setDeleteRuleId(null)}>Cancel</button>
              </div>
            </div>
          </div>
        )}
      </div>

      {namingPatternError && (
        <div className="modal-overlay" onClick={() => setNamingPatternError(null)}>
          <div className="modal-content" onClick={(e) => e.stopPropagation()}>
            <h3>Invalid naming rule</h3>
            <p>{namingPatternError}</p>
            <div className="modal-buttons">
              <button onClick={() => setNamingPatternError(null)}>OK</button>
            </div>
          </div>
        </div>
      )}

      {deleteDialog && (
        <div className="modal-overlay" onClick={() => setDeleteDialog(null)}>
          <div className="modal-content" onClick={(e) => e.stopPropagation()}>
            <h3>Delete Catalog Tag</h3>
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

      {addGrantDialog && (
        <div className="modal-overlay" onClick={() => { setAddGrantDialog(false); setEditingGrant(null); }}>
          <div className="modal-content" onClick={(e) => e.stopPropagation()}>
            <h3>{editingGrant ? 'Edit Grant' : 'Add Grant'}</h3>
            <div className="modal-body">
              <label>Principal (user, group, or service principal)</label>
              <input
                type="text"
                value={grantForm.principal}
                onChange={(e) => setGrantForm({ ...grantForm, principal: e.target.value })}
                placeholder="e.g. data_engineers"
                readOnly={!!editingGrant}
              />
              <label>Privileges (comma-separated, e.g. USE CATALOG, CREATE SCHEMA)</label>
              <input
                type="text"
                value={grantForm.privileges}
                onChange={(e) => setGrantForm({ ...grantForm, privileges: e.target.value })}
                placeholder="USE CATALOG, CREATE SCHEMA"
              />
            </div>
            <div className="modal-buttons">
              <button onClick={() => { setAddGrantDialog(false); setEditingGrant(null); setGrantForm({ principal: '', privileges: '' }); }}>Cancel</button>
              <button
                onClick={() => {
                  const principal = grantForm.principal.trim();
                  const privs = parsePrivileges(grantForm.privileges);
                  if (principal && privs.length > 0) {
                    if (editingGrant) revokeGrant('catalog', catalogId, editingGrant.principal);
                    addGrant('catalog', catalogId, principal, privs);
                    setAddGrantDialog(false);
                    setEditingGrant(null);
                    setGrantForm({ principal: '', privileges: '' });
                  }
                }}
                disabled={!grantForm.principal.trim() || !grantForm.privileges.trim()}
              >
                {editingGrant ? 'Save' : 'Add Grant'}
              </button>
            </div>
          </div>
        </div>
      )}

      {revokeGrantDialog && (
        <div className="modal-overlay" onClick={() => setRevokeGrantDialog(null)}>
          <div className="modal-content" onClick={(e) => e.stopPropagation()}>
            <h3>Revoke Grant</h3>
            <p>Revoke all privileges for <strong>{revokeGrantDialog.principal}</strong> on this catalog?</p>
            <div className="modal-buttons">
              <button onClick={() => { revokeGrant('catalog', catalogId, revokeGrantDialog.principal); setRevokeGrantDialog(null); }} style={{ backgroundColor: 'var(--vscode-errorForeground)' }}>Revoke</button>
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
            <h3>Rename Catalog</h3>
            
            <div className="modal-field-group">
              <label htmlFor="rename-catalog-input">Name</label>
              <VSCodeTextField
                id="rename-catalog-input"
                value={newName}
                placeholder="Catalog name"
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
            <h3>Set Catalog Comment</h3>
            <input
              type="text"
              defaultValue={commentDialog.comment}
              autoFocus
              onKeyDown={(e) => {
                if (e.key === 'Enter') {
                  updateCatalog(commentDialog.catalogId, { comment: (e.target as HTMLInputElement).value });
                  setCommentDialog(null);
                } else if (e.key === 'Escape') {
                  setCommentDialog(null);
                }
              }}
              id="catalog-comment-input"
            />
            <div className="modal-buttons">
              <button onClick={() => {
                const input = document.getElementById('catalog-comment-input') as HTMLInputElement;
                updateCatalog(commentDialog.catalogId, { comment: input.value });
                setCommentDialog(null);
              }}>Set</button>
              <button onClick={() => setCommentDialog(null)}>Cancel</button>
            </div>
          </div>
        </div>
      )}

      {showBulkPanel && (
        <BulkOperationsPanel
          scope="catalog"
          catalogId={catalogId}
          onClose={() => setShowBulkPanel(false)}
        />
      )}
    </div>
  );
};
