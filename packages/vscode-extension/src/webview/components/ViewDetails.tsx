import React, { useState, useMemo } from 'react';
import { VSCodeButton } from '@vscode/webview-ui-toolkit/react';
import { useDesignerStore } from '../state/useDesignerStore';
import { extractDependenciesFromView } from '../../providers/base/sql-parser';
import './ViewDetails.css';

interface ViewDetailsProps {
  viewId: string;
}

export const ViewDetails: React.FC<ViewDetailsProps> = ({ viewId }) => {
  const { project, updateView } = useDesignerStore();
  const [isEditingSQL, setIsEditingSQL] = useState(false);
  const [editedSQL, setEditedSQL] = useState('');

  // Find view
  const viewInfo = useMemo(() => {
    if (!project?.state?.catalogs) return null;
    
    for (const catalog of project.state.catalogs) {
      for (const schema of catalog.schemas || []) {
        for (const view of (schema as any).views || []) {
          if (view.id === viewId) {
            return { catalog, schema, view };
          }
        }
      }
    }
    return null;
  }, [project, viewId]);

  if (!viewInfo) {
    return (
      <div className="view-details">
        <div className="empty-state">
          <p>View not found</p>
        </div>
      </div>
    );
  }

  const { catalog, schema, view } = viewInfo;

  const handleEditSQL = () => {
    setEditedSQL(view.definition);
    setIsEditingSQL(true);
  };

  const handleSaveSQL = () => {
    if (editedSQL.trim()) {
      let cleanSQL = editedSQL.trim();
      
      // Strip CREATE VIEW/CREATE OR REPLACE VIEW prefix if user accidentally included it
      // Only the SELECT statement should be stored in the definition field
      const createViewPattern = /^CREATE\s+(OR\s+REPLACE\s+)?VIEW(\s+IF\s+NOT\s+EXISTS)?\s+(`[^`]+`|[\w.]+)(\s+COMMENT\s+[^\s]+)?\s+AS\s+/i;
      cleanSQL = cleanSQL.replace(createViewPattern, '');
      
      // Re-extract dependencies from updated SQL
      const dependencies = extractDependenciesFromView(cleanSQL);
      
      updateView(viewId, cleanSQL, dependencies);
      setIsEditingSQL(false);
    }
  };

  const handleCancelEdit = () => {
    setEditedSQL('');
    setIsEditingSQL(false);
  };

  return (
    <div className="view-details">
      {/* Header */}
      <div className="view-header">
        <div className="view-title">
          <svg xmlns="http://www.w3.org/2000/svg" width="1em" height="1em" fill="none" viewBox="0 0 16 16" aria-hidden="true" focusable="false">
            <path fill="currentColor" fillRule="evenodd" d="M1.75 1a.75.75 0 0 0-.75.75v12.5c0 .414.336.75.75.75H4v-1.5H2.5V7H5v2h1.5V7h3v2H11V7h2.5v2H15V1.75a.75.75 0 0 0-.75-.75zM13.5 5.5v-3h-11v3z" clipRule="evenodd"></path>
            <path fill="currentColor" fillRule="evenodd" d="M11.75 10a.75.75 0 0 0-.707.5H9.957a.75.75 0 0 0-.708-.5H5.75a.75.75 0 0 0-.75.75v1.75a2.5 2.5 0 0 0 5 0V12h1v.5a2.5 2.5 0 0 0 5 0v-1.75a.75.75 0 0 0-.75-.75zm.75 2.5v-1h2v1a1 1 0 1 1-2 0m-6-1v1a1 1 0 1 0 2 0v-1z" clipRule="evenodd"></path>
          </svg>
          <h2>{catalog.name}.{schema.name}.{view.name}</h2>
        </div>
        <span className="view-badge">VIEW</span>
      </div>

      {/* SQL Definition Section */}
      <div className="view-section">
        <div className="section-header">
          <h3>SQL Definition</h3>
          {!isEditingSQL && (
            <VSCodeButton
              appearance="secondary"
              onClick={handleEditSQL}
            >
              <i className="codicon codicon-edit"></i>
              Edit
            </VSCodeButton>
          )}
        </div>
        
        {isEditingSQL ? (
          <div className="edit-sql-container">
            <textarea
              value={editedSQL}
              onChange={(e) => setEditedSQL(e.target.value)}
              rows={12}
              placeholder="SELECT * FROM..."
              style={{
                width: '100%',
                fontFamily: 'var(--vscode-editor-font-family, monospace)',
                fontSize: '13px',
                padding: '12px',
                border: '1px solid var(--vscode-input-border)',
                background: 'var(--vscode-input-background)',
                color: 'var(--vscode-input-foreground)',
                resize: 'vertical',
                borderRadius: '4px'
              }}
            />
            <div className="edit-actions">
              <VSCodeButton onClick={handleSaveSQL}>
                <i className="codicon codicon-check"></i>
                Save
              </VSCodeButton>
              <VSCodeButton
                appearance="secondary"
                onClick={handleCancelEdit}
              >
                <i className="codicon codicon-close"></i>
                Cancel
              </VSCodeButton>
            </div>
          </div>
        ) : (
          <pre className="sql-display">
            {view.definition}
          </pre>
        )}
      </div>

      {/* Comment Section */}
      {view.comment && (
        <div className="view-section">
          <h3>Comment</h3>
          <p className="comment-text">{view.comment}</p>
        </div>
      )}

      {/* Extracted Dependencies Section */}
      {view.extractedDependencies && (
        <div className="view-section">
          <h3>Dependencies</h3>
          <div className="dependencies-container">
            {view.extractedDependencies.tables && view.extractedDependencies.tables.length > 0 && (
              <div className="dependency-group">
                <h4>Dependent Tables</h4>
                <ul className="dependency-list">
                  {view.extractedDependencies.tables.map((table: string, i: number) => (
                    <li key={i}>
                      <i className="codicon codicon-table"></i>
                      <code>{table}</code>
                    </li>
                  ))}
                </ul>
              </div>
            )}
            
            {view.extractedDependencies.views && view.extractedDependencies.views.length > 0 && (
              <div className="dependency-group">
                <h4>Dependent Views</h4>
                <ul className="dependency-list">
                  {view.extractedDependencies.views.map((v: string, i: number) => (
                    <li key={i}>
                      <svg xmlns="http://www.w3.org/2000/svg" width="1em" height="1em" fill="none" viewBox="0 0 16 16" aria-hidden="true" focusable="false" style={{ marginRight: '4px' }}>
                        <path fill="currentColor" fillRule="evenodd" d="M1.75 1a.75.75 0 0 0-.75.75v12.5c0 .414.336.75.75.75H4v-1.5H2.5V7H5v2h1.5V7h3v2H11V7h2.5v2H15V1.75a.75.75 0 0 0-.75-.75zM13.5 5.5v-3h-11v3z" clipRule="evenodd"></path>
                        <path fill="currentColor" fillRule="evenodd" d="M11.75 10a.75.75 0 0 0-.707.5H9.957a.75.75 0 0 0-.708-.5H5.75a.75.75 0 0 0-.75.75v1.75a2.5 2.5 0 0 0 5 0V12h1v.5a2.5 2.5 0 0 0 5 0v-1.75a.75.75 0 0 0-.75-.75zm.75 2.5v-1h2v1a1 1 0 1 1-2 0m-6-1v1a1 1 0 1 0 2 0v-1z" clipRule="evenodd"></path>
                      </svg>
                      <code>{v}</code>
                    </li>
                  ))}
                </ul>
              </div>
            )}

            {(!view.extractedDependencies.tables || view.extractedDependencies.tables.length === 0) &&
             (!view.extractedDependencies.views || view.extractedDependencies.views.length === 0) && (
              <p className="no-dependencies">No dependencies detected</p>
            )}
          </div>
        </div>
      )}

      {/* Properties Section */}
      {view.properties && Object.keys(view.properties).length > 0 && (
        <div className="view-section">
          <h3>Properties</h3>
          <table className="properties-table">
            <thead>
              <tr>
                <th>Key</th>
                <th>Value</th>
              </tr>
            </thead>
            <tbody>
              {Object.entries(view.properties).map(([key, value]) => (
                <tr key={key}>
                  <td><code>{key}</code></td>
                  <td><code>{value as string}</code></td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}

      {/* Tags Section */}
      {view.tags && Object.keys(view.tags).length > 0 && (
        <div className="view-section">
          <h3>Tags</h3>
          <div className="tags-container">
            {Object.entries(view.tags).map(([key, value]) => (
              <span key={key} className="tag-badge">
                <strong>{key}:</strong> {value as string}
              </span>
            ))}
          </div>
        </div>
      )}
    </div>
  );
};

