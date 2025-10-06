import React, { useState } from 'react';
import { useDesignerStore } from '../state/useDesignerStore';
import { Column } from '../../shared/model';

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
    project,
  } = useDesignerStore();

  const [draggedIndex, setDraggedIndex] = useState<number | null>(null);

  const [renameDialog, setRenameDialog] = useState<{colId: string, name: string} | null>(null);
  const [dropDialog, setDropDialog] = useState<{colId: string, name: string} | null>(null);
  const [typeDialog, setTypeDialog] = useState<{colId: string, type: string} | null>(null);
  const [commentDialog, setCommentDialog] = useState<{colId: string, comment: string} | null>(null);

  const handleRenameColumn = (colId: string, currentName: string) => {
    setRenameDialog({colId, name: currentName});
  };

  const handleDropColumn = (colId: string, name: string) => {
    setDropDialog({colId, name});
  };

  const handleChangeType = (colId: string, currentType: string) => {
    setTypeDialog({colId, type: currentType});
  };

  const handleToggleNullable = (colId: string, currentNullable: boolean) => {
    setColumnNullable(tableId, colId, !currentNullable);
  };

  const handleSetComment = (colId: string, currentComment?: string) => {
    setCommentDialog({colId, comment: currentComment || ''});
  };

  const handleDragStart = (index: number) => {
    setDraggedIndex(index);
  };

  const handleDragOver = (e: React.DragEvent, index: number) => {
    e.preventDefault();
    if (draggedIndex === null || draggedIndex === index) return;

    const newColumns = [...columns];
    const draggedItem = newColumns[draggedIndex];
    newColumns.splice(draggedIndex, 1);
    newColumns.splice(index, 0, draggedItem);

    const newOrder = newColumns.map((col) => col.id);
    reorderColumns(tableId, newOrder);
    setDraggedIndex(index);
  };

  const handleDragEnd = () => {
    setDraggedIndex(null);
  };

  // Check if table needs column mapping
  const needsColumnMapping = project?.ops.some(
    (op) => 
      (op.op === 'rename_column' || op.op === 'drop_column') &&
      op.payload.tableId === tableId
  );

  return (
    <div className="column-grid">
      {needsColumnMapping && (
        <div className="warning-badge">
          ⚠️ Requires delta.columnMapping.mode=name
        </div>
      )}
      <table>
        <thead>
          <tr>
            <th></th>
            <th>Name</th>
            <th>Type</th>
            <th>Nullable</th>
            <th>Comment</th>
            <th>Actions</th>
          </tr>
        </thead>
        <tbody>
          {columns.length === 0 ? (
            <tr>
              <td colSpan={6} style={{ textAlign: 'center', color: '#888' }}>
                No columns yet. Add a column to get started.
              </td>
            </tr>
          ) : (
            columns.map((col, index) => (
              <tr
                key={col.id}
                draggable
                onDragStart={() => handleDragStart(index)}
                onDragOver={(e) => handleDragOver(e, index)}
                onDragEnd={handleDragEnd}
                className={draggedIndex === index ? 'dragging' : ''}
              >
                <td className="drag-handle">⋮⋮</td>
                <td>{col.name}</td>
                <td>{col.type}</td>
                <td>
                  <input
                    type="checkbox"
                    checked={col.nullable}
                    onChange={() => handleToggleNullable(col.id, col.nullable)}
                  />
                </td>
                <td className="comment-cell">{col.comment || <span className="empty">—</span>}</td>
                <td className="actions-cell">
                  <button onClick={() => handleRenameColumn(col.id, col.name)} title="Rename">
                    ✏️
                  </button>
                  <button onClick={() => handleChangeType(col.id, col.type)} title="Change type">
                    🔧
                  </button>
                  <button onClick={() => handleSetComment(col.id, col.comment)} title="Set comment">
                    💬
                  </button>
                  <button onClick={() => handleDropColumn(col.id, col.name)} title="Drop column">
                    🗑️
                  </button>
                </td>
              </tr>
            ))
          )}
        </tbody>
      </table>

      {renameDialog && (
        <div className="modal" onClick={() => setRenameDialog(null)}>
          <div className="modal-content" onClick={(e) => e.stopPropagation()}>
            <h3>Rename Column</h3>
            <input
              type="text"
              defaultValue={renameDialog.name}
              autoFocus
              onKeyDown={(e) => {
                if (e.key === 'Enter') {
                  renameColumn(tableId, renameDialog.colId, (e.target as HTMLInputElement).value);
                  setRenameDialog(null);
                } else if (e.key === 'Escape') {
                  setRenameDialog(null);
                }
              }}
              id="rename-col-input"
            />
            <div className="modal-buttons">
              <button onClick={() => {
                const input = document.getElementById('rename-col-input') as HTMLInputElement;
                renameColumn(tableId, renameDialog.colId, input.value);
                setRenameDialog(null);
              }}>Rename</button>
              <button onClick={() => setRenameDialog(null)}>Cancel</button>
            </div>
          </div>
        </div>
      )}

      {typeDialog && (
        <div className="modal" onClick={() => setTypeDialog(null)}>
          <div className="modal-content" onClick={(e) => e.stopPropagation()}>
            <h3>Change Column Type</h3>
            <select defaultValue={typeDialog.type} autoFocus id="type-select">
              <option value="STRING">STRING</option>
              <option value="INT">INT</option>
              <option value="BIGINT">BIGINT</option>
              <option value="DOUBLE">DOUBLE</option>
              <option value="BOOLEAN">BOOLEAN</option>
              <option value="DATE">DATE</option>
              <option value="TIMESTAMP">TIMESTAMP</option>
              <option value="DECIMAL(10,2)">DECIMAL(10,2)</option>
            </select>
            <div className="modal-buttons">
              <button onClick={() => {
                const select = document.getElementById('type-select') as HTMLSelectElement;
                changeColumnType(tableId, typeDialog.colId, select.value);
                setTypeDialog(null);
              }}>Change</button>
              <button onClick={() => setTypeDialog(null)}>Cancel</button>
            </div>
          </div>
        </div>
      )}

      {commentDialog && (
        <div className="modal" onClick={() => setCommentDialog(null)}>
          <div className="modal-content" onClick={(e) => e.stopPropagation()}>
            <h3>Set Column Comment</h3>
            <input
              type="text"
              defaultValue={commentDialog.comment}
              autoFocus
              onKeyDown={(e) => {
                if (e.key === 'Enter') {
                  setColumnComment(tableId, commentDialog.colId, (e.target as HTMLInputElement).value);
                  setCommentDialog(null);
                } else if (e.key === 'Escape') {
                  setCommentDialog(null);
                }
              }}
              id="comment-input"
            />
            <div className="modal-buttons">
              <button onClick={() => {
                const input = document.getElementById('comment-input') as HTMLInputElement;
                setColumnComment(tableId, commentDialog.colId, input.value);
                setCommentDialog(null);
              }}>Set</button>
              <button onClick={() => setCommentDialog(null)}>Cancel</button>
            </div>
          </div>
        </div>
      )}

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

