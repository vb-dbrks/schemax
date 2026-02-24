import React, { useState } from 'react';
import {
  VSCodeButton,
  VSCodeDropdown,
  VSCodeOption,
  VSCodeTextField,
} from '@vscode/webview-ui-toolkit/react';
import { Column } from '../../providers/unity/models';
import { useDesignerStore } from '../state/useDesignerStore';
import { validateUnityCatalogObjectName } from '../utils/unityNames';
import { validateNameInCatalog, findCatalogForRename } from '../utils/namingStandards';

interface ColumnGridProps {
  tableId: string;
  columns: Column[];
}

// Codicon icons - theme-aware and vector-based
const IconPlusSmall: React.FC = () => (
  <i slot="start" className="codicon codicon-add" aria-hidden="true"></i>
);

const IconEdit: React.FC = () => (
  <i slot="start" className="codicon codicon-edit" aria-hidden="true"></i>
);

const IconTrash: React.FC = () => (
  <i slot="start" className="codicon codicon-trash" aria-hidden="true"></i>
);

const IconCheck: React.FC = () => (
  <i slot="start" className="codicon codicon-check" aria-hidden="true"></i>
);

const IconClose: React.FC = () => (
  <i slot="start" className="codicon codicon-close" aria-hidden="true"></i>
);

export const ColumnGrid: React.FC<ColumnGridProps> = ({ tableId, columns }) => {
  const {
    project,
    addColumn,
    renameColumn,
    dropColumn,
    changeColumnType,
    setColumnNullable,
    setColumnComment,
    reorderColumns,
    setColumnTag,
    unsetColumnTag,
  } = useDesignerStore();

  const [draggedIndex, setDraggedIndex] = useState<number | null>(null);
  const [editingColId, setEditingColId] = useState<string | null>(null);
  const [editValues, setEditValues] = useState<{name: string, type: string, nullable: boolean, comment: string}>({name: '', type: '', nullable: true, comment: ''});
  const [dropDialog, setDropDialog] = useState<{colId: string, name: string} | null>(null);
  const [addDialog, setAddDialog] = useState(false);
  const [addColForm, setAddColForm] = useState({
    name: '',
    type: 'STRING',
    nullable: true,
    comment: '',
  });
  const [addColError, setAddColError] = useState<string | null>(null);
  const [columnEditError, setColumnEditError] = useState<string | null>(null);
  const [addColumnTags, setAddColumnTags] = useState<Record<string, string>>({});
  const [addTagInput, setAddTagInput] = useState({ tagName: '', tagValue: '' });
  const [tagsDialog, setTagsDialog] = useState<{colId: string, name: string} | null>(null);
  const [editingColumnTag, setEditingColumnTag] = useState<string | null>(null);
  const [editColumnTagValue, setEditColumnTagValue] = useState('');
  const [isAddingColumnTag, setIsAddingColumnTag] = useState(false);
  const [newColumnTagName, setNewColumnTagName] = useState('');
  const [newColumnTagValue, setNewColumnTagValue] = useState('');

  const handleDragStart = (index: number) => {
    setDraggedIndex(index);
  };

  const handleDragOver = (e: React.DragEvent, index: number) => {
    e.preventDefault();
  };

  const handleDragEnd = () => {
    if (draggedIndex === null) return;
    setDraggedIndex(null);
  };

  const handleDrop = (e: React.DragEvent, dropIndex: number) => {
    e.preventDefault();
    if (draggedIndex === null || draggedIndex === dropIndex) return;

    const newOrder = [...columns];
    const [draggedCol] = newOrder.splice(draggedIndex, 1);
    newOrder.splice(dropIndex, 0, draggedCol);

    reorderColumns(tableId, newOrder.map(c => c.id));
    setDraggedIndex(null);
  };

  const handleEditColumn = (col: Column) => {
    setEditingColId(col.id);
    setColumnEditError(null);
    setEditValues({
      name: col.name,
      type: col.type,
      nullable: col.nullable,
      comment: col.comment || ''
    });
  };

  const handleSaveColumn = (colId: string) => {
    const col = columns.find(c => c.id === colId);
    if (!col) return;

    setColumnEditError(null);
    const trimmedName = editValues.name.trim();
    if (editValues.name !== col.name) {
      const nameErr = validateUnityCatalogObjectName(trimmedName);
      if (nameErr) {
        setColumnEditError(nameErr);
        return;
      }
      const catalogs = (project?.state as { catalogs?: unknown[] })?.catalogs ?? [];
      const catalogForTable = findCatalogForRename(catalogs as any, 'table', tableId);
      if (catalogForTable?.namingStandards?.applyToRenames) {
        const nr = validateNameInCatalog(trimmedName, 'column', catalogForTable);
        if (!nr.valid) {
          setColumnEditError(nr.error + (nr.suggestion ? ` Did you mean: ${nr.suggestion}?` : ''));
          return;
        }
      }
      const isDuplicate = columns.some(
        c => c.id !== colId && c.name === trimmedName
      );
      if (isDuplicate) {
        setColumnEditError(`A column named "${trimmedName}" already exists.`);
        return;
      }
      renameColumn(tableId, colId, trimmedName);
    }
    if (editValues.type !== col.type) {
      changeColumnType(tableId, colId, editValues.type);
    }
    if (editValues.nullable !== col.nullable) {
      setColumnNullable(tableId, colId, editValues.nullable);
    }
    if (editValues.comment !== (col.comment || '')) {
      setColumnComment(tableId, colId, editValues.comment);
    }

    setEditingColId(null);
  };

  const handleCancelEdit = () => {
    setEditingColId(null);
    setColumnEditError(null);
  };

  const handleDropColumn = (colId: string, name: string) => {
    setDropDialog({colId, name});
  };

  const handleAddColumn = (name: string, type: string, nullable: boolean, comment: string) => {
    if (!name || !type) return;
    setAddColError(null);
    const trimmedName = name.trim();
    const nameErr = validateUnityCatalogObjectName(trimmedName);
    if (nameErr) {
      setAddColError(nameErr);
      return;
    }
    const catalogs = (project?.state as { catalogs?: unknown[] })?.catalogs ?? [];
    const catalogForTable = findCatalogForRename(catalogs as any, 'table', tableId);
    if (catalogForTable) {
      const nr = validateNameInCatalog(trimmedName, 'column', catalogForTable);
      if (!nr.valid) {
        setAddColError(nr.error + (nr.suggestion ? ` Did you mean: ${nr.suggestion}?` : ''));
        return;
      }
    }
    const isDuplicate = columns.some(c => c.name === trimmedName);
    if (isDuplicate) {
      setAddColError(`A column named "${trimmedName}" already exists.`);
      return;
    }
    addColumn(tableId, trimmedName, type, nullable, comment || undefined, addColumnTags);
    setAddDialog(false);
    setAddColForm({ name: '', type: 'STRING', nullable: true, comment: '' });
    setAddColumnTags({});
    setAddTagInput({ tagName: '', tagValue: '' });
  };

  const closeAddColumnDialog = () => {
    setAddDialog(false);
    setAddColError(null);
    setAddColForm({ name: '', type: 'STRING', nullable: true, comment: '' });
    setAddColumnTags({});
    setAddTagInput({ tagName: '', tagValue: '' });
  };
  
  const handleAddTagToNewColumn = () => {
    if (addTagInput.tagName && addTagInput.tagValue) {
      setAddColumnTags({...addColumnTags, [addTagInput.tagName]: addTagInput.tagValue});
      setAddTagInput({tagName: '', tagValue: ''});
    }
  };
  
  const handleRemoveTagFromNewColumn = (tagName: string) => {
    const newTags = {...addColumnTags};
    delete newTags[tagName];
    setAddColumnTags(newTags);
  };

  return (
    <div className="column-grid">
      <table>
        <thead>
          <tr>
            <th style={{width: '30px'}}></th>
            <th>Name</th>
            <th>Type</th>
            <th style={{width: '80px'}}>Nullable</th>
            <th style={{width: '100px'}}>Tags</th>
            <th>Comment</th>
            <th style={{width: '180px'}}>Actions</th>
          </tr>
        </thead>
        <tbody>
          {columns.length === 0 ? (
            <tr>
              <td colSpan={7}>
                <div className="column-grid__empty">
                  <div className="column-grid__empty-copy">
                    <span className="column-grid__empty-title">No columns defined yet</span>
                    <span className="column-grid__empty-hint">
                      Start by adding your first column. You can always reorder or refine it later.
                    </span>
                  </div>
                  <VSCodeButton appearance="primary" type="button" onClick={() => setAddDialog(true)}>
                    <IconPlusSmall />
                    Add column
                  </VSCodeButton>
                </div>
              </td>
            </tr>
          ) : (
            columns.map((col, index) => {
              const isEditing = editingColId === col.id;
              
              return (
                <tr
                  key={col.id}
                  draggable={!isEditing}
                  onDragStart={() => handleDragStart(index)}
                  onDragOver={(e) => handleDragOver(e, index)}
                  onDrop={(e) => handleDrop(e, index)}
                  onDragEnd={handleDragEnd}
                  className={draggedIndex === index ? 'dragging' : ''}
                >
                  <td className="drag-handle">{!isEditing && '⋮⋮'}</td>
                  
                  <td>
                    {isEditing ? (
                      <>
                        <input
                          type="text"
                          value={editValues.name}
                          onChange={(e) => {
                            setEditValues({ ...editValues, name: e.target.value });
                            setColumnEditError(null);
                          }}
                          autoFocus
                          style={{ width: '100%' }}
                        />
                        {columnEditError && (
                          <p className="column-name-error" style={{ fontSize: '11px', color: 'var(--vscode-errorForeground)', marginTop: '4px', marginBottom: 0 }}>
                            {columnEditError}
                          </p>
                        )}
                      </>
                    ) : (
                      col.name
                    )}
                  </td>
                  
                  <td>
                    {isEditing ? (
                      <select
                        value={editValues.type}
                        onChange={(e) => setEditValues({...editValues, type: e.target.value})}
                        style={{width: '100%'}}
                      >
                        <option value="STRING">STRING</option>
                        <option value="INT">INT</option>
                        <option value="BIGINT">BIGINT</option>
                        <option value="DOUBLE">DOUBLE</option>
                        <option value="BOOLEAN">BOOLEAN</option>
                        <option value="DATE">DATE</option>
                        <option value="TIMESTAMP">TIMESTAMP</option>
                        <option value="DECIMAL(10,2)">DECIMAL(10,2)</option>
                        <option value="ARRAY<STRING>">ARRAY&lt;STRING&gt;</option>
                        <option value="MAP<STRING,STRING>">MAP&lt;STRING,STRING&gt;</option>
                      </select>
                    ) : (
                      col.type
                    )}
                  </td>
                  
                  <td style={{textAlign: 'center'}}>
                    <input
                      type="checkbox"
                      checked={isEditing ? editValues.nullable : col.nullable}
                      onChange={() => {
                        if (isEditing) {
                          setEditValues({...editValues, nullable: !editValues.nullable});
                        }
                      }}
                      disabled={!isEditing}
                      title={isEditing ? "Toggle nullable" : "Click Edit to change nullable"}
                    />
                  </td>
                  
                  <td>
                    <div style={{display: 'flex', alignItems: 'center', gap: '8px'}}>
                      {col.tags && Object.keys(col.tags).length > 0 ? (
                        <div className="column-tags">
                          {Object.keys(col.tags).slice(0, 2).map(tagName => (
                            <span key={tagName} className="tag-badge" title={`${tagName}: ${col.tags![tagName]}`}>
                              {tagName}
                            </span>
                          ))}
                          {Object.keys(col.tags).length > 2 && <span className="tag-more">+{Object.keys(col.tags).length - 2}</span>}
                        </div>
                      ) : (
                        <span className="no-tags">—</span>
                      )}
                      {isEditing && (
                        <VSCodeButton
                          appearance="icon"
                          onClick={() => setTagsDialog({colId: col.id, name: col.name})}
                          title="Manage tags"
                        >
                          <IconEdit />
                        </VSCodeButton>
                      )}
                    </div>
                  </td>
                  
                  <td>
                    {isEditing ? (
                      <input
                        type="text"
                        value={editValues.comment}
                        onChange={(e) => setEditValues({...editValues, comment: e.target.value})}
                        placeholder="Optional comment"
                        style={{width: '100%'}}
                      />
                    ) : (
                      col.comment || <span className="empty">—</span>
                    )}
                  </td>
                  
                  <td className="actions-cell">
                    {isEditing ? (
                      <>
                        <VSCodeButton
                          appearance="icon"
                          onClick={() => handleSaveColumn(col.id)}
                          title="Save changes"
                        >
                          <IconCheck />
                        </VSCodeButton>
                        <VSCodeButton
                          appearance="icon"
                          onClick={handleCancelEdit}
                          title="Cancel"
                        >
                          <IconClose />
                        </VSCodeButton>
                      </>
                    ) : (
                      <>
                        <VSCodeButton
                          appearance="icon"
                          onClick={() => handleEditColumn(col)}
                          title="Edit column"
                        >
                          <IconEdit />
                        </VSCodeButton>
                        <VSCodeButton
                          appearance="icon"
                          onClick={() => handleDropColumn(col.id, col.name)}
                          title="Drop column"
                        >
                          <IconTrash />
                        </VSCodeButton>
                      </>
                    )}
                  </td>
                </tr>
              );
            })
          )}
        </tbody>
      </table>

      {columns.length > 0 && (
        <VSCodeButton appearance="secondary" type="button" onClick={() => setAddDialog(true)} className="add-column-button">
          <IconPlusSmall />
          Add column
        </VSCodeButton>
      )}

      {dropDialog && (
        <div className="modal" onClick={() => setDropDialog(null)}>
          <div className="modal-content" onClick={(e) => e.stopPropagation()}>
            <h3>Confirm Drop Column</h3>
            <p>Are you sure you want to drop column "{dropDialog.name}"?</p>
            <div className="modal-buttons">
              <button onClick={() => {
                try {
                  dropColumn(tableId, dropDialog.colId);
                  setDropDialog(null);
                } catch (error) {
                  console.error('Failed to drop column:', error);
                  alert(`Failed to drop column: ${error instanceof Error ? error.message : 'Unknown error'}`);
                  setDropDialog(null);
                }
              }} style={{backgroundColor: 'var(--vscode-errorForeground)'}}>Drop</button>
              <button onClick={() => setDropDialog(null)}>Cancel</button>
            </div>
          </div>
        </div>
      )}

      {/* Add Column Dialog — styled like Add Constraint */}
      {addDialog && (
        <div className="modal-overlay" onClick={closeAddColumnDialog}>
          <div className="modal-content" onClick={(e) => e.stopPropagation()}>
            <h2>Add Column</h2>

            <div className="modal-body">
              <div className="constraint-form-field">
                <label className="constraint-form-label">Name</label>
                <VSCodeTextField
                  value={addColForm.name}
                  placeholder="Enter column name"
                  style={{ width: '100%' }}
                  onInput={(e) => {
                    setAddColForm({ ...addColForm, name: (e.target as HTMLInputElement).value });
                    setAddColError(null);
                  }}
                />
                {addColError && (
                  <p className="hint column-name-error" style={{ color: 'var(--vscode-errorForeground)', marginTop: '4px' }}>
                    {addColError}
                  </p>
                )}
              </div>

              <div className="constraint-form-field">
                <label className="constraint-form-label">Type</label>
                <VSCodeDropdown
                  value={addColForm.type}
                  style={{ width: '100%' }}
                  onInput={(e) =>
                    setAddColForm({
                      ...addColForm,
                      type: (e.target as HTMLSelectElement).value,
                    })
                  }
                >
                  <VSCodeOption value="STRING">STRING</VSCodeOption>
                  <VSCodeOption value="INT">INT</VSCodeOption>
                  <VSCodeOption value="BIGINT">BIGINT</VSCodeOption>
                  <VSCodeOption value="DOUBLE">DOUBLE</VSCodeOption>
                  <VSCodeOption value="DECIMAL">DECIMAL</VSCodeOption>
                  <VSCodeOption value="BOOLEAN">BOOLEAN</VSCodeOption>
                  <VSCodeOption value="DATE">DATE</VSCodeOption>
                  <VSCodeOption value="TIMESTAMP">TIMESTAMP</VSCodeOption>
                  <VSCodeOption value="BINARY">BINARY</VSCodeOption>
                  <VSCodeOption value="ARRAY">ARRAY</VSCodeOption>
                  <VSCodeOption value="MAP">MAP</VSCodeOption>
                  <VSCodeOption value="STRUCT">STRUCT</VSCodeOption>
                </VSCodeDropdown>
              </div>

              <label className="checkbox-label constraint-form-field">
                <input
                  type="checkbox"
                  checked={addColForm.nullable}
                  onChange={(e) =>
                    setAddColForm({ ...addColForm, nullable: e.target.checked })
                  }
                />
                <span>Nullable</span>
              </label>

              <div className="constraint-form-field">
                <label className="constraint-form-label">Comment (optional)</label>
                <VSCodeTextField
                  value={addColForm.comment}
                  placeholder="Column description"
                  style={{ width: '100%' }}
                  onInput={(e) =>
                    setAddColForm({
                      ...addColForm,
                      comment: (e.target as HTMLInputElement).value,
                    })
                  }
                />
              </div>

              <div
                className="constraint-form-field"
                style={{
                  borderTop: '1px solid var(--vscode-panel-border)',
                  paddingTop: '14px',
                  marginTop: '4px',
                }}
              >
                <label className="constraint-form-label">Tags (optional)</label>
                {Object.keys(addColumnTags).length > 0 && (
                  <ul className="add-column-tags-list">
                    {Object.entries(addColumnTags).map(([tagName, tagValue]) => (
                      <li key={tagName} className="add-column-tag-item">
                        <code>{tagName}: {tagValue}</code>
                        <button
                          type="button"
                          className="constraint-list-link add-column-tag-remove"
                          onClick={() => handleRemoveTagFromNewColumn(tagName)}
                        >
                          Remove
                        </button>
                      </li>
                    ))}
                  </ul>
                )}
                <div className="add-column-tag-input-row">
                  <VSCodeTextField
                    value={addTagInput.tagName}
                    placeholder="Tag name (e.g., PII)"
                    style={{ flex: 1 }}
                    onInput={(e) =>
                      setAddTagInput({
                        ...addTagInput,
                        tagName: (e.target as HTMLInputElement).value,
                      })
                    }
                  />
                  <VSCodeTextField
                    value={addTagInput.tagValue}
                    placeholder="Tag value (e.g., true)"
                    style={{ flex: 1 }}
                    onInput={(e) =>
                      setAddTagInput({
                        ...addTagInput,
                        tagValue: (e.target as HTMLInputElement).value,
                      })
                    }
                  />
                  <VSCodeButton
                    appearance="secondary"
                    onClick={handleAddTagToNewColumn}
                    disabled={!addTagInput.tagName || !addTagInput.tagValue}
                  >
                    + Add Tag
                  </VSCodeButton>
                </div>
              </div>
            </div>

            <div className="modal-buttons">
              <VSCodeButton
                onClick={() =>
                  handleAddColumn(
                    addColForm.name,
                    addColForm.type,
                    addColForm.nullable,
                    addColForm.comment
                  )
                }
                disabled={!addColForm.name.trim()}
              >
                Add
              </VSCodeButton>
              <VSCodeButton appearance="secondary" onClick={closeAddColumnDialog}>
                Cancel
              </VSCodeButton>
            </div>
          </div>
        </div>
      )}

      {/* Tags Dialog */}
      {tagsDialog && (() => {
        const col = columns.find(c => c.id === tagsDialog.colId);
        const tags = col?.tags || {};
        return (
          <div className="modal-overlay" onClick={() => {
            setTagsDialog(null);
            setIsAddingColumnTag(false);
            setNewColumnTagName('');
            setNewColumnTagValue('');
            setEditingColumnTag(null);
          }}>
            <div className="modal-content" onClick={(e) => e.stopPropagation()} style={{ minWidth: '600px' }}>
              <h2>Column Tags: {tagsDialog.name}</h2>
              <p style={{ color: 'var(--vscode-descriptionForeground)', fontSize: '12px', marginBottom: '16px' }}>
                Tags are used for governance, discovery, and attribute-based access control (ABAC)
              </p>
              
              <div className="modal-body">
                {Object.entries(tags).length === 0 && !isAddingColumnTag ? (
                  <div className="empty-properties" style={{ padding: '20px', textAlign: 'center' }}>
                    <p>No column tags defined</p>
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
                            {editingColumnTag === tagName ? (
                              <input
                                type="text"
                                value={editColumnTagValue}
                                onChange={(e) => setEditColumnTagValue(e.target.value)}
                                autoFocus
                              />
                            ) : (
                              String(tagValue)
                            )}
                          </td>
                          <td className="actions-cell">
                            {editingColumnTag === tagName ? (
                              <>
                                <VSCodeButton
                                  appearance="icon"
                                  onClick={() => {
                                    if (editColumnTagValue.trim()) {
                                      setColumnTag(tableId, tagsDialog.colId, tagName, editColumnTagValue);
                                      setEditingColumnTag(null);
                                      setEditColumnTagValue('');
                                    }
                                  }}
                                  title="Save"
                                >
                                  <IconCheck />
                                </VSCodeButton>
                                <VSCodeButton
                                  appearance="icon"
                                  onClick={() => {
                                    setEditingColumnTag(null);
                                    setEditColumnTagValue('');
                                  }}
                                  title="Cancel"
                                >
                                  <IconClose />
                                </VSCodeButton>
                              </>
                            ) : (
                              <>
                                <VSCodeButton
                                  appearance="icon"
                                  onClick={() => {
                                    setEditingColumnTag(tagName);
                                    setEditColumnTagValue(String(tagValue));
                                  }}
                                  title="Edit tag"
                                >
                                  <IconEdit />
                                </VSCodeButton>
                                <VSCodeButton
                                  appearance="icon"
                                  onClick={() => {
                                    unsetColumnTag(tableId, tagsDialog.colId, tagName);
                                  }}
                                  title="Remove tag"
                                >
                                  <IconTrash />
                                </VSCodeButton>
                              </>
                            )}
                          </td>
                        </tr>
                      ))}
                      
                      {isAddingColumnTag && (
                        <tr className="adding-row">
                          <td>
                            <input
                              type="text"
                              value={newColumnTagName}
                              onChange={(e) => setNewColumnTagName(e.target.value)}
                              placeholder="e.g., PII"
                              autoFocus
                            />
                          </td>
                          <td>
                            <input
                              type="text"
                              value={newColumnTagValue}
                              onChange={(e) => setNewColumnTagValue(e.target.value)}
                              placeholder="e.g., sensitive"
                            />
                          </td>
                          <td className="actions-cell">
                            <VSCodeButton
                              appearance="icon"
                              onClick={() => {
                                if (newColumnTagName.trim() && newColumnTagValue.trim()) {
                                  setColumnTag(tableId, tagsDialog.colId, newColumnTagName, newColumnTagValue);
                                  setNewColumnTagName('');
                                  setNewColumnTagValue('');
                                  setIsAddingColumnTag(false);
                                }
                              }}
                              title="Add tag"
                            >
                              <IconCheck />
                            </VSCodeButton>
                            <VSCodeButton
                              appearance="icon"
                              onClick={() => {
                                setIsAddingColumnTag(false);
                                setNewColumnTagName('');
                                setNewColumnTagValue('');
                              }}
                              title="Cancel"
                            >
                              <IconClose />
                            </VSCodeButton>
                          </td>
                        </tr>
                      )}
                    </tbody>
                  </table>
                )}
                
                {!isAddingColumnTag && (
                  <button 
                    className="add-property-btn"
                    onClick={() => setIsAddingColumnTag(true)}
                    style={{ marginTop: '12px' }}
                  >
                    + Add Tag
                  </button>
                )}
              </div>
              
              <div className="modal-actions">
                <VSCodeButton onClick={() => {
                  setTagsDialog(null);
                  setIsAddingColumnTag(false);
                  setNewColumnTagName('');
                  setNewColumnTagValue('');
                  setEditingColumnTag(null);
                }}>
                  Close
                </VSCodeButton>
              </div>
            </div>
          </div>
        );
      })()}
    </div>
  );
};
