import React from 'react';
import { useDesignerStore } from '../state/useDesignerStore';
import { ColumnGrid } from './ColumnGrid';

export const TableDesigner: React.FC = () => {
  const { selectedTableId, findTable, setTableComment } = useDesignerStore();

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
    const newComment = prompt('Set table comment:', table.comment || '');
    if (newComment !== null) {
      setTableComment(table.id, newComment);
    }
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
    </div>
  );
};

