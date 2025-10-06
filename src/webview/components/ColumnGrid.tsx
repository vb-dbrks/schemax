import React, { useState } from 'react';
import { Column } from '../../shared/model';
import { useDesignerStore } from '../state/useDesignerStore';

interface ColumnGridProps {
  tableId: string;
  columns: Column[];
}

export const ColumnGrid: React.FC<ColumnGridProps> = ({ tableId, columns }) => {
  const {
    renameColumn,
    dropColumn,
    changeColumnType,
    setColumnNullable,
    setColumnComment,
    reorderColumns,
  } = useDesignerStore();

  const [draggedIndex, setDraggedIndex] = useState<number | null>(null);
  const [editingColId, setEditingColId] = useState<string | null>(null);
  const [editValues, setEditValues] = useState<{name: string, type: string, comment: string}>({name: '', type: '', comment: ''});
  const [dropDialog, setDropDialog] = useState<{colId: string, name: string} | null>(null);

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

  return (
    <div className="column-grid">
      <table>
        <thead>
          <tr>
            <th style={{width: '30px'}}></th>
            <th>Name</th>
            <th>Type</th>
            <th style={{width: '80px'}}>Nullable</th>
            <th>Comment</th>
            <th style={{width: '150px'}}>Actions</th>
          </tr>
        </thead>
        <tbody>
          {columns.length === 0 ? (
            <tr>
              <td colSpan={6} className="empty-state">
                No columns yet. Click "Add Column" to create one.
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
                      checked={col.nullable}
                      onChange={() => setColumnNullable(tableId, col.id, !col.nullable)}
                      disabled={isEditing}
                      title="Toggle nullable"
                    />
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
                        <button onClick={() => handleEditColumn(col)} title="Edit column">
                          ✏️ Edit
                        </button>
                        <button 
                          onClick={() => handleDropColumn(col.id, col.name)} 
                          title="Drop column"
                          style={{color: 'var(--vscode-errorForeground)'}}
                        >
                          🗑️ Drop
                        </button>
                      </>
                    )}
                  </td>
                </tr>
              );
            })
          )}
        </tbody>
      </table>

      {dropDialog && (
        <div className="modal" onClick={() => setDropDialog(null)}>
          <div className="modal-content" onClick={(e) => e.stopPropagation()}>
            <h3>Confirm Drop Column</h3>
            <p>Are you sure you want to drop column "{dropDialog.name}"?</p>
            <div className="modal-buttons">
              <button onClick={() => {
                dropColumn(tableId, dropDialog.colId);
                setDropDialog(null);
              }} style={{backgroundColor: 'var(--vscode-errorForeground)'}}>Drop</button>
              <button onClick={() => setDropDialog(null)}>Cancel</button>
            </div>
          </div>
        </div>
      )}
    </div>
  );
};
