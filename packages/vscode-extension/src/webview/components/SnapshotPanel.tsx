import React from 'react';
import { useDesignerStore } from '../state/useDesignerStore';

export const SnapshotPanel: React.FC = () => {
  const { project } = useDesignerStore();
  const [expanded, setExpanded] = React.useState(true);

  if (!project) {
    return null;
  }

  const snapshots = project.snapshots || [];
  const uncommittedOps = project.ops || [];  // In v2, ops array IS the uncommitted ops

  return (
    <div className="snapshot-panel">
      <div className="snapshot-header" onClick={() => setExpanded(!expanded)}>
        <h3>
          {expanded ? '▼' : '▶'} Snapshots ({snapshots.length})
        </h3>
        {uncommittedOps.length > 0 && (
          <span className="uncommitted-badge" title={`${uncommittedOps.length} uncommitted operations`}>
            {uncommittedOps.length} uncommitted
          </span>
        )}
      </div>

      {expanded && (
        <div className="snapshot-list">
          {snapshots.length === 0 ? (
            <div className="empty-state">
              <p>No snapshots yet</p>
              <p className="hint">Run "Schematic: Create Snapshot" to create your first snapshot</p>
            </div>
          ) : (
            <div className="snapshot-timeline">
              {snapshots.slice().reverse().map((snapshot, index) => {
                const isLatest = index === 0;
                const date = new Date(snapshot.ts);
                const deployments = project.deployments?.filter(d => d.snapshotId === snapshot.id) || [];
                
                return (
                  <div key={snapshot.id} className={`snapshot-item ${isLatest ? 'latest' : ''}`}>
                    <div className="snapshot-indicator">
                      <div className="snapshot-dot" />
                      {index < snapshots.length - 1 && <div className="snapshot-line" />}
                    </div>
                    <div className="snapshot-content">
                      <div className="snapshot-version">
                        {snapshot.version}
                        {isLatest && <span className="latest-badge">Latest</span>}
                      </div>
                      <div className="snapshot-name">{snapshot.name}</div>
                      {snapshot.comment && (
                        <div className="snapshot-comment">{snapshot.comment}</div>
                      )}
                      <div className="snapshot-meta">
                        <span className="snapshot-date" title={snapshot.ts}>
                          {date.toLocaleDateString()} {date.toLocaleTimeString()}
                        </span>
                        {snapshot.createdBy && (
                          <span className="snapshot-author"> • {snapshot.createdBy}</span>
                        )}
                        <span className="snapshot-ops"> • {snapshot.opsCount || 0} ops</span>
                      </div>
                      {snapshot.tags && snapshot.tags.length > 0 && (
                        <div className="snapshot-tags">
                          {snapshot.tags.map(tag => (
                            <span key={tag} className="snapshot-tag">{tag}</span>
                          ))}
                        </div>
                      )}
                      {deployments.length > 0 && (
                        <div className="snapshot-deployments">
                          Deployed to: {deployments.map(d => d.environment).join(', ')}
                        </div>
                      )}
                    </div>
                  </div>
                );
              })}
            </div>
          )}
          
          {uncommittedOps.length > 0 && (
            <div className="uncommitted-changes">
              <div className="uncommitted-indicator">
                <div className="uncommitted-dot" />
              </div>
              <div className="uncommitted-content">
                <div className="uncommitted-title">Working Changes</div>
                <div className="uncommitted-ops">{uncommittedOps.length} uncommitted operations</div>
                <div className="uncommitted-hint">
                  Create a snapshot to save these changes
                </div>
              </div>
            </div>
          )}
        </div>
      )}
    </div>
  );
};

