import React, { useState } from 'react';
import { VSCodeButton } from '@vscode/webview-ui-toolkit/react';
import { Column } from '../../providers/unity/models';
import { useDesignerStore } from '../state/useDesignerStore';

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

export const ColumnGrid: React.FC<ColumnGridProps> = ({ tableId, columns }) => {
  const {
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
  const [addColumnTags, setAddColumnTags] = useState<Record<string, string>>({});
  const [addTagInput, setAddTagInput] = useState({tagName: '', tagValue: ''});
  const [tagsDialog, setTagsDialog] = useState<{colId: string, name: string} | null>(null);
  const [tagForm, setTagForm] = useState({tagName: '', tagValue: ''});

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

    // Save changes
    if (editValues.name !== col.name) {
      renameColumn(tableId, colId, editValues.name);
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
  };

  const handleDropColumn = (colId: string, name: string) => {
    setDropDialog({colId, name});
  };

  const handleAddColumn = (name: string, type: string, nullable: boolean, comment: string) => {
    if (!name || !type) {
      return;
    }
    
    // Add column with comment and tags
    addColumn(tableId, name, type, nullable, comment || undefined, addColumnTags);
    
    // Reset state
    setAddDialog(false);
    setAddColumnTags({});
    setAddTagInput({tagName: '', tagValue: ''});
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
                      <input
                        type="text"
                        value={editValues.name}
                        onChange={(e) => setEditValues({...editValues, name: e.target.value})}
                        autoFocus
                        style={{width: '100%'}}
                      />
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
                        <button 
                          onClick={() => setTagsDialog({colId: col.id, name: col.name})} 
                          title="Manage tags"
                          style={{padding: '2px 6px', fontSize: '11px', whiteSpace: 'nowrap'}}
                        >
                          🏷️ Manage
                        </button>
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
                        <button onClick={() => handleSaveColumn(col.id)} title="Save changes">
                          ✓ Save
                        </button>
                        <button onClick={handleCancelEdit} title="Cancel">
                          ✕ Cancel
                        </button>
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

      {addDialog && (
        <div className="modal" onClick={() => setAddDialog(false)}>
          <div className="modal-content" onClick={(e) => e.stopPropagation()}>
            <h3>Add Column</h3>
            <label>Name:</label>
            <input
              type="text"
              placeholder="Enter column name"
              autoFocus
              id="add-col-name"
            />
            <label style={{marginTop: '12px'}}>Type:</label>
            <select id="add-col-type" defaultValue="STRING">
              <option value="STRING">STRING</option>
              <option value="INT">INT</option>
              <option value="BIGINT">BIGINT</option>
              <option value="DOUBLE">DOUBLE</option>
              <option value="DECIMAL">DECIMAL</option>
              <option value="BOOLEAN">BOOLEAN</option>
              <option value="DATE">DATE</option>
              <option value="TIMESTAMP">TIMESTAMP</option>
              <option value="BINARY">BINARY</option>
              <option value="ARRAY">ARRAY</option>
              <option value="MAP">MAP</option>
              <option value="STRUCT">STRUCT</option>
            </select>
            <label style={{marginTop: '12px', display: 'block'}}>
              <input type="checkbox" id="add-col-nullable" defaultChecked /> Nullable
            </label>
            <label style={{marginTop: '12px', display: 'block'}}>Comment (optional):</label>
            <input
              type="text"
              placeholder="Column description"
              id="add-col-comment"
            />
            
            {/* Tags Section */}
            <div style={{marginTop: '16px', borderTop: '1px solid var(--vscode-panel-border)', paddingTop: '12px'}}>
              <label style={{fontWeight: 600}}>Tags (optional):</label>
              {Object.keys(addColumnTags).length > 0 && (
                <div style={{marginTop: '8px', marginBottom: '8px'}}>
                  {Object.entries(addColumnTags).map(([tagName, tagValue]) => (
                    <div key={tagName} style={{
                      display: 'flex',
                      alignItems: 'center',
                      gap: '8px',
                      padding: '4px 8px',
                      backgroundColor: 'var(--vscode-editor-background)',
                      borderRadius: '3px',
                      marginBottom: '4px'
                    }}>
                      <code style={{flex: 1}}>{tagName}: {tagValue}</code>
                      <button
                        onClick={() => handleRemoveTagFromNewColumn(tagName)}
                        style={{
                          padding: '2px 6px',
                          fontSize: '11px',
                          color: 'var(--vscode-errorForeground)'
                        }}
                      >
                        Remove
                      </button>
                    </div>
                  ))}
                </div>
              )}
              <div style={{display: 'flex', gap: '8px', marginTop: '8px'}}>
                <input
                  type="text"
                  placeholder="Tag name (e.g., PII)"
                  value={addTagInput.tagName}
                  onChange={(e) => setAddTagInput({...addTagInput, tagName: e.target.value})}
                  style={{flex: 1, marginBottom: 0}}
                />
                <input
                  type="text"
                  placeholder="Tag value (e.g., true)"
                  value={addTagInput.tagValue}
                  onChange={(e) => setAddTagInput({...addTagInput, tagValue: e.target.value})}
                  style={{flex: 1, marginBottom: 0}}
                />
                <button
                  onClick={handleAddTagToNewColumn}
                  disabled={!addTagInput.tagName || !addTagInput.tagValue}
                  style={{padding: '8px 12px', whiteSpace: 'nowrap'}}
                >
                  + Add Tag
                </button>
              </div>
            </div>
            
            <div className="modal-buttons">
              <button onClick={() => {
                const name = (document.getElementById('add-col-name') as HTMLInputElement).value;
                const type = (document.getElementById('add-col-type') as HTMLSelectElement).value;
                const nullable = (document.getElementById('add-col-nullable') as HTMLInputElement).checked;
                const comment = (document.getElementById('add-col-comment') as HTMLInputElement).value;
                handleAddColumn(name, type, nullable, comment);
              }}>Add</button>
              <button onClick={() => {
                setAddDialog(false);
                setAddColumnTags({});
                setAddTagInput({tagName: '', tagValue: ''});
              }}>Cancel</button>
            </div>
          </div>
        </div>
      )}

      {/* Tags Dialog */}
      {tagsDialog && (() => {
        const col = columns.find(c => c.id === tagsDialog.colId);
        const tags = col?.tags || {};
        return (
          <div className="modal-overlay" onClick={() => setTagsDialog(null)}>
            <div className="modal-content" onClick={(e) => e.stopPropagation()}>
              <h2>Manage Tags: {tagsDialog.name}</h2>
              <div className="modal-body">
                <div className="tags-list">
                  {Object.entries(tags).length === 0 ? (
                    <p className="no-tags-msg">No tags defined.</p>
                  ) : (
                    <table className="tags-table">
                      <thead>
                        <tr>
                          <th>Tag Name</th>
                          <th>Tag Value</th>
                          <th style={{width: '80px'}}>Actions</th>
                        </tr>
                      </thead>
                      <tbody>
                        {Object.entries(tags).map(([tagName, tagValue]) => (
                          <tr key={tagName}>
                            <td>{tagName}</td>
                            <td>{String(tagValue)}</td>
                            <td>
                              <button
                                className="delete-btn-small"
                                onClick={() => {
                                  unsetColumnTag(tableId, tagsDialog.colId, tagName);
                                }}
                                title="Remove tag"
                              >
                                Remove
                              </button>
                            </td>
                          </tr>
                        ))}
                      </tbody>
                    </table>
                  )}
                </div>
                
                <div className="add-tag-form">
                  <h4>Add New Tag</h4>
                  <label>
                    Tag Name:
                    <input
                      type="text"
                      value={tagForm.tagName}
                      onChange={(e) => setTagForm({...tagForm, tagName: e.target.value})}
                      placeholder="e.g., PII"
                    />
                  </label>
                  <label>
                    Tag Value:
                    <input
                      type="text"
                      value={tagForm.tagValue}
                      onChange={(e) => setTagForm({...tagForm, tagValue: e.target.value})}
                      placeholder="e.g., sensitive"
                    />
                  </label>
                  <button
                    className="add-tag-btn"
                    onClick={() => {
                      if (tagForm.tagName && tagForm.tagValue) {
                        setColumnTag(tableId, tagsDialog.colId, tagForm.tagName, tagForm.tagValue);
                        setTagForm({tagName: '', tagValue: ''});
                      }
                    }}
                    disabled={!tagForm.tagName || !tagForm.tagValue}
                  >
                    Add Tag
                  </button>
                </div>
              </div>
              <div className="modal-actions">
                <button className="confirm-btn" onClick={() => {
                  setTagsDialog(null);
                  setTagForm({tagName: '', tagValue: ''});
                }}>
                  Close
                </button>
              </div>
            </div>
          </div>
        );
      })()}
    </div>
  );
};
