import React from 'react';
import { useDesignerStore } from '../state/useDesignerStore';
import { ColumnGrid } from './ColumnGrid';
import { TableProperties } from './TableProperties';

export const TableDesigner: React.FC = () => {
  const { selectedTableId, findTable, setTableComment } = useDesignerStore();
  const [commentDialog, setCommentDialog] = React.useState<{tableId: string, comment: string} | null>(null);

  if (!selectedTableId) {
    return (
      <div className="table-designer">
        <div className="empty-state">
          <h2>No table selected</h2>
          <p>Select a table from the sidebar or create a new one.</p>
        </div>
      </div>
    );
  }

  const result = findTable(selectedTableId);
  if (!result) {
    return (
      <div className="table-designer">
        <div className="empty-state">
          <h2>Table not found</h2>
        </div>
      </div>
    );
  }

  const { catalog, schema, table } = result;

  const handleSetComment = () => {
    setCommentDialog({tableId: table.id, comment: table.comment || ''});
  };

  return (
    <div className="table-designer">
      <div className="table-header">
        <h2>
          {catalog.name}.{schema.name}.{table.name}
        </h2>
        <div className="table-metadata">
          <span className="badge">{table.format}</span>
          {table.columnMapping && <span className="badge">columnMapping: {table.columnMapping}</span>}
        </div>
      </div>

      <div className="table-properties">
        <div className="property-row">
          <label>Comment:</label>
          <div className="property-value">
            {table.comment ? (
              <span>{table.comment}</span>
            ) : (
              <span className="warning">⚠️ Comment recommended</span>
            )}
            <button onClick={handleSetComment}>✏️ Edit</button>
          </div>
        </div>
      </div>

      <div className="columns-section">
        <h3>Columns ({table.columns.length})</h3>
        <ColumnGrid tableId={table.id} columns={table.columns} />
      </div>

      <div className="properties-section">
        <TableProperties tableId={table.id} />
      </div>

      {commentDialog && (
        <div className="modal" onClick={() => setCommentDialog(null)}>
          <div className="modal-content" onClick={(e) => e.stopPropagation()}>
            <h3>Set Table Comment</h3>
            <input
              type="text"
              defaultValue={commentDialog.comment}
              autoFocus
              onKeyDown={(e) => {
                if (e.key === 'Enter') {
                  setTableComment(commentDialog.tableId, (e.target as HTMLInputElement).value);
                  setCommentDialog(null);
                } else if (e.key === 'Escape') {
                  setCommentDialog(null);
                }
              }}
              id="table-comment-input"
            />
            <div className="modal-buttons">
              <button onClick={() => {
                const input = document.getElementById('table-comment-input') as HTMLInputElement;
                setTableComment(commentDialog.tableId, input.value);
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

