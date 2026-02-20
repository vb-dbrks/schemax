import React, { useState } from 'react';
import { VSCodeButton } from '@vscode/webview-ui-toolkit/react';
import { useDesignerStore } from '../state/useDesignerStore';
import { extractDependenciesFromView } from '../../providers/base/sql-parser';
import './ViewDetails.css';

interface MaterializedViewDetailsProps {
  materializedViewId: string;
}

export const MaterializedViewDetails: React.FC<MaterializedViewDetailsProps> = ({ materializedViewId }) => {
  const { findMaterializedView, updateMaterializedView } = useDesignerStore();
  const info = findMaterializedView(materializedViewId);
  const [isEditing, setIsEditing] = useState(false);
  const [editedDefinition, setEditedDefinition] = useState('');

  if (!info) {
    return (
      <div className="view-details">
        <div className="empty-state"><p>Materialized view not found</p></div>
      </div>
    );
  }

  const { catalog, schema, mv } = info;
  const m = mv as { id: string; name: string; definition?: string; comment?: string; refreshSchedule?: string; grants?: any[] };

  const handleSave = () => {
    if (editedDefinition.trim()) {
      const deps = extractDependenciesFromView(editedDefinition);
      updateMaterializedView(materializedViewId, editedDefinition.trim(), deps);
      setIsEditing(false);
    }
  };

  return (
    <div className="view-details">
      <div className="view-header">
        <div className="view-title">
          <i className="codicon codicon-symbol-array" aria-hidden="true" />
          <h2>{catalog.name}.{schema.name}.{m.name}</h2>
        </div>
        <span className="view-badge" style={{ background: 'var(--vscode-charts-green)' }}>MATERIALIZED VIEW</span>
      </div>
      <div className="view-section">
        <h3>Properties</h3>
        <dl className="properties-list">
          {m.refreshSchedule && (
            <>
              <dt>Refresh schedule</dt>
              <dd><code>{m.refreshSchedule}</code></dd>
            </>
          )}
          {m.comment != null && m.comment !== '' && (
            <>
              <dt>Comment</dt>
              <dd>{m.comment}</dd>
            </>
          )}
        </dl>
      </div>
      <div className="view-section">
        <div className="section-header">
          <h3>SQL Definition</h3>
          {!isEditing ? (
            <VSCodeButton appearance="secondary" onClick={() => { setEditedDefinition(m.definition ?? ''); setIsEditing(true); }}>Edit</VSCodeButton>
          ) : (
            <>
              <VSCodeButton appearance="primary" onClick={handleSave}>Save</VSCodeButton>
              <VSCodeButton appearance="secondary" onClick={() => setIsEditing(false)}>Cancel</VSCodeButton>
            </>
          )}
        </div>
        {isEditing ? (
          <textarea value={editedDefinition} onChange={(e) => setEditedDefinition(e.target.value)} rows={12} style={{ width: '100%', fontFamily: 'monospace' }} />
        ) : (
          <pre className="code-block" style={{ whiteSpace: 'pre-wrap', wordBreak: 'break-word' }}>{m.definition ?? 'SELECT 1'}</pre>
        )}
      </div>
      {m.grants && m.grants.length > 0 && (
        <div className="view-section">
          <h3>Grants</h3>
          <ul>{m.grants.map((g: any, i: number) => (
            <li key={i}>{g.principal}: {Array.isArray(g.privileges) ? g.privileges.join(', ') : ''}</li>
          ))}</ul>
        </div>
      )}
    </div>
  );
};
