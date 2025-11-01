import React from 'react';
import { VSCodeButton } from '@vscode/webview-ui-toolkit/react';
import { useDesignerStore } from '../state/useDesignerStore';
import { ColumnGrid } from './ColumnGrid';
import { TableProperties } from './TableProperties';
import { TableConstraints } from './TableConstraints';
import { SecurityGovernance } from './SecurityGovernance';

const IconEditInline: React.FC = () => (
  <svg slot="start" width="14" height="14" viewBox="0 0 16 16" fill="currentColor" aria-hidden="true">
    <path fillRule="evenodd" d="M11.414 1.586a2 2 0 0 1 2.828 2.828l-.793.793-2.828-2.828zm-2.121 2.121-7 7A2 2 0 0 0 2 12.414V14a1 1 0 0 0 1 1h1.586a2 2 0 0 0 1.414-.586l7-7z" clipRule="evenodd" />
  </svg>
);

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
          <span className="badge">{table.format.toUpperCase()}</span>
          <span className="badge">{table.external ? 'External' : 'Managed'}</span>
          
          {table.external && table.externalLocationName && (
            <>
              <span className="badge-info">📁 Location: {table.externalLocationName}</span>
              {table.path && (
                <span className="badge-info">Path: {table.path}</span>
              )}
            </>
          )}
          
          {table.partitionColumns && table.partitionColumns.length > 0 && (
            <span className="badge-info">⚡ Partitioned: {table.partitionColumns.join(', ')}</span>
          )}
          
          {table.clusterColumns && table.clusterColumns.length > 0 && (
            <span className="badge-info">🔷 Clustered: {table.clusterColumns.join(', ')}</span>
          )}
          
          {table.columnMapping && <span className="badge">Column Mapping: {table.columnMapping}</span>}
        </div>
      </div>

      {/* Show resolved location for external tables */}
      {table.external && table.externalLocationName && project?.environments?.[project?.activeEnvironment || ''] && (
        <div className="table-properties">
          <div className="property-row">
            <label>Resolved Location ({project.activeEnvironment || 'default'}):</label>
            <div className="property-value">
              {(() => {
                const envConfig = project.environments[project.activeEnvironment || ''];
                const extLoc = envConfig?.externalLocations?.[table.externalLocationName];
                if (!extLoc) {
                  return <code style={{color: 'var(--vscode-errorForeground)'}}>Location "{table.externalLocationName}" not found</code>;
                }
                const basePath = extLoc.path;
                const fullPath = table.path ? `${basePath}/${table.path}` : basePath;
                return <code>{fullPath}</code>;
              })()}
            </div>
          </div>
        </div>
      )}

      <div className="table-properties">
        <div className="property-row">
          <label>Comment:</label>
          <div className="property-value">
            {table.comment ? (
              <span>{table.comment}</span>
            ) : (
              <span className="inline-warning">
                <span className="inline-warning__dot" aria-hidden="true" />
                Comment recommended
              </span>
            )}
            <VSCodeButton appearance="secondary" type="button" onClick={handleSetComment}>
              <IconEditInline />
              Edit
            </VSCodeButton>
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

      <div className="constraints-section">
        <TableConstraints tableId={table.id} />
      </div>

      <div className="security-section">
        <SecurityGovernance tableId={table.id} />
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

