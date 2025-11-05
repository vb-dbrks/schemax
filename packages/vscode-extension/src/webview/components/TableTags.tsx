import React, { useState } from 'react';
import { useDesignerStore } from '../state/useDesignerStore';

interface TableTagsProps {
  tableId: string;
}

export function TableTags({ tableId }: TableTagsProps) {
  const { project, setTableTag, unsetTableTag } = useDesignerStore();
  const [isAdding, setIsAdding] = useState(false);
  const [newTagName, setNewTagName] = useState('');
  const [newTagValue, setNewTagValue] = useState('');
  const [error, setError] = useState('');
  const [deleteDialog, setDeleteDialog] = useState<string | null>(null);

  // Find the table
  const table = React.useMemo(() => {
    const projectWithState = project as any;
    if (!projectWithState?.state?.catalogs) return null;
    for (const catalog of projectWithState.state.catalogs) {
      for (const schema of catalog.schemas) {
        const found = schema.tables.find((t: any) => t.id === tableId);
        if (found) return found;
      }
    }
    return null;
  }, [project, tableId]);

  if (!table) return null;

  const tags = table.tags || {};
  const tagEntries = Object.entries(tags);

  const handleAddTag = () => {
    if (!newTagName.trim()) {
      setError('Tag name cannot be empty');
      return;
    }
    if (!newTagValue.trim()) {
      setError('Tag value cannot be empty');
      return;
    }
    if (tags[newTagName]) {
      setError(`Tag "${newTagName}" already exists`);
      return;
    }

    setTableTag(tableId, newTagName, newTagValue);
    setNewTagName('');
    setNewTagValue('');
    setIsAdding(false);
    setError('');
  };

  const handleCancelAdd = () => {
    setIsAdding(false);
    setNewTagName('');
    setNewTagValue('');
    setError('');
  };

  const confirmDelete = () => {
    if (deleteDialog) {
      unsetTableTag(tableId, deleteDialog);
      setDeleteDialog(null);
    }
  };

  return (
    <div className="table-properties-section">
      <h3>Table Tags (Unity Catalog)</h3>
      <p className="hint" style={{marginBottom: '12px'}}>
        Tags are used for governance, discovery, and attribute-based access control (ABAC)
      </p>
      
      {error && <div className="error-message">{error}</div>}
      
      {tagEntries.length === 0 && !isAdding ? (
        <div className="empty-properties">
          <p>No table tags defined</p>
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
            {tagEntries.map(([tagName, tagValue]) => (
              <tr key={tagName}>
                <td><code>{tagName}</code></td>
                <td>{String(tagValue)}</td>
                <td className="actions-cell">
                  <button 
                    onClick={() => setDeleteDialog(tagName)}
                    style={{ color: 'var(--vscode-errorForeground)' }}
                  >
                    🗑️ Delete
                  </button>
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
                  <button onClick={handleAddTag}>✓ Add</button>
                  <button onClick={handleCancelAdd}>✕ Cancel</button>
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
        <div className="modal" onClick={() => setDeleteDialog(null)}>
          <div className="modal-content" onClick={(e) => e.stopPropagation()}>
            <h3>Delete Table Tag</h3>
            <p>Are you sure you want to delete tag <code>{deleteDialog}</code>?</p>
            <p className="warning-text">This will generate an UNSET TAGS operation.</p>
            <div className="modal-buttons">
              <button onClick={confirmDelete} style={{ backgroundColor: 'var(--vscode-errorForeground)' }}>
                Delete
              </button>
              <button onClick={() => setDeleteDialog(null)}>Cancel</button>
            </div>
          </div>
        </div>
      )}
    </div>
  );
}

