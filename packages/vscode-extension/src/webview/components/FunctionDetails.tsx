import React from 'react';
import { useDesignerStore } from '../state/useDesignerStore';
import './ViewDetails.css';

interface FunctionDetailsProps {
  functionId: string;
}

export const FunctionDetails: React.FC<FunctionDetailsProps> = ({ functionId }) => {
  const { findFunction } = useDesignerStore();
  const info = findFunction(functionId);

  if (!info) {
    return (
      <div className="view-details">
        <div className="empty-state"><p>Function not found</p></div>
      </div>
    );
  }

  const { catalog, schema, func } = info;
  const f = func as { id: string; name: string; language?: string; returnType?: string; body?: string; comment?: string; parameters?: any[]; grants?: any[] };

  return (
    <div className="view-details">
      <div className="view-header">
        <div className="view-title">
          <i className="codicon codicon-symbol-method" aria-hidden="true" />
          <h2>{catalog.name}.{schema.name}.{f.name}</h2>
        </div>
        <span className="view-badge" style={{ background: 'var(--vscode-charts-orange)' }}>FUNCTION</span>
      </div>
      <div className="view-section">
        <h3>Properties</h3>
        <dl className="properties-list">
          <dt>Language</dt>
          <dd>{f.language ?? 'SQL'}</dd>
          <dt>Return type</dt>
          <dd><code>{f.returnType ?? 'STRING'}</code></dd>
          {f.comment != null && f.comment !== '' && (
            <>
              <dt>Comment</dt>
              <dd>{f.comment}</dd>
            </>
          )}
        </dl>
      </div>
      <div className="view-section">
        <h3>Body</h3>
        <pre className="code-block" style={{ whiteSpace: 'pre-wrap', wordBreak: 'break-word' }}>{f.body ?? 'NULL'}</pre>
      </div>
      {f.grants && f.grants.length > 0 && (
        <div className="view-section">
          <h3>Grants</h3>
          <ul>{f.grants.map((g: any, i: number) => (
            <li key={i}>{g.principal}: {Array.isArray(g.privileges) ? g.privileges.join(', ') : ''}</li>
          ))}</ul>
        </div>
      )}
    </div>
  );
};
