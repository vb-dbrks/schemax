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

  const handleRenameColumn = (colId: string, currentName: string) => {
    const newName = prompt('Rename column:', currentName);
    if (newName && newName !== currentName) {
      renameColumn(tableId, colId, newName);
    }
  };

  const handleDropColumn = (colId: string, name: string) => {
    if (confirm(`Drop column "${name}"?`)) {
      dropColumn(tableId, colId);
    }
  };

  const handleChangeType = (colId: string, currentType: string) => {
    const newType = prompt('Change column type:', currentType);
    if (newType && newType !== currentType) {
      changeColumnType(tableId, colId, newType);
    }
  };

  const handleToggleNullable = (colId: string, currentNullable: boolean) => {
    setColumnNullable(tableId, colId, !currentNullable);
  };

  const handleSetComment = (colId: string, currentComment?: string) => {
    const newComment = prompt('Set column comment:', currentComment || '');
    if (newComment !== null) {
      setColumnComment(tableId, colId, newComment);
    }
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
    </div>
  );
};

