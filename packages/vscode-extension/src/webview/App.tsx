import React, { useEffect } from 'react';
import { VSCodeButton, VSCodeProgressRing } from '@vscode/webview-ui-toolkit/react';
import { useDesignerStore } from './state/useDesignerStore';
import { Sidebar } from './components/Sidebar';
import { TableDesigner } from './components/TableDesigner';
import { ViewDetails } from './components/ViewDetails';
import { CatalogDetails } from './components/CatalogDetails';
import { SchemaDetails } from './components/SchemaDetails';
import { SnapshotPanel } from './components/SnapshotPanel';
import { getVsCodeApi } from './vscode-api';
import { ProjectSettingsPanel } from './components/ProjectSettingsPanel';

const vscode = getVsCodeApi();

// Codicon icons automatically adapt to VS Code themes
const IconSettings: React.FC = () => (
  <i slot="start" className="codicon codicon-settings-gear" aria-hidden="true"></i>
);

// Codicon icons automatically adapt to VS Code themes
const IconRefresh: React.FC<{ className?: string }> = ({ className = '' }) => (
  <i className={`codicon codicon-refresh ${className}`} aria-hidden="true"></i>
);

export const App: React.FC = () => {
  const { 
    project, 
    setProject, 
    setProvider, 
    provider, 
    selectedCatalogId,
    selectedSchemaId,
    selectedTableId, 
    findView 
  } = useDesignerStore();
  const [loading, setLoading] = React.useState(true);
  const [isProjectSettingsOpen, setIsProjectSettingsOpen] = React.useState(false);
  const [hasConflicts, setHasConflicts] = React.useState(false);
  const [conflictInfo, setConflictInfo] = React.useState<any>(null);
  const [hasStaleSnapshots, setHasStaleSnapshots] = React.useState(false);
  const [staleSnapshotInfo, setStaleSnapshotInfo] = React.useState<any>(null);
  const [validationResult, setValidationResult] = React.useState<{errors: string[], warnings: string[]} | null>(null);
  const [isRefreshing, setIsRefreshing] = React.useState(false);
  
  // Determine if selected object is a view
  const isViewSelected = selectedTableId ? !!findView(selectedTableId) : false;

  useEffect(() => {
    // Set up message listener from extension
    const messageHandler = (event: MessageEvent) => {
      const message = event.data;
      switch (message.type) {
        case 'project-loaded':
        case 'project-updated':
          if (message.payload.provider) {
            setProvider(message.payload.provider);
          }
          
          setProject(message.payload);
          setLoading(false);
          setIsRefreshing(false); // Stop refresh spinner
          
          // Check for conflicts
          if (message.payload.conflicts) {
            setHasConflicts(true);
            setConflictInfo(message.payload.conflicts);
          } else {
            setHasConflicts(false);
            setConflictInfo(null);
          }
          
          // Check for stale snapshots
          if (message.payload.staleSnapshots) {
            setHasStaleSnapshots(true);
            setStaleSnapshotInfo(message.payload.staleSnapshots);
          } else {
            setHasStaleSnapshots(false);
            setStaleSnapshotInfo(null);
          }
          
          // Check for validation results
          if (message.payload.validationResult) {
            setValidationResult(message.payload.validationResult);
          } else {
            setValidationResult(null);
          }
          break;
      }
    };

    window.addEventListener('message', messageHandler);

    // Request initial project load
    vscode.postMessage({ type: 'load-project' });

    return () => {
      window.removeEventListener('message', messageHandler);
    };
  }, [setProject, setProvider]);

  const pendingOps = project?.ops?.length ?? 0;
  const snapshotCount = project?.snapshots?.length ?? 0;
  const hasProjectSettings = Boolean(project);

  if (loading) {
    return (
      <div className="app app--loading">
        <VSCodeProgressRing aria-label="Loading Schematic Designer" />
        <p className="loading-copy">Loading Schematic Designer…</p>
        <p className="loading-subcopy">If this persists, open the developer tools (Help → Toggle Developer Tools).</p>
      </div>
    );
  }

  return (
    <div className="app">
      <header className="app-header">
        <div className="app-header__info">
          <div className="app-header__title-row">
            <h1 className="app-header__title">{project?.name || 'Schematic Project'}</h1>
            {hasProjectSettings && (
              <VSCodeButton
                type="button"
                appearance="secondary"
                className="project-settings-button"
                onClick={() => setIsProjectSettingsOpen(true)}
              >
                <IconSettings />
                View project settings
              </VSCodeButton>
            )}
          </div>
          <p className="app-header__meta">
            {provider ? `${provider.name} · v${provider.version}` : 'Unity Catalog'}
            {snapshotCount ? ` · ${snapshotCount} snapshot${snapshotCount === 1 ? '' : 's'}` : ''}
          </p>
        </div>
        <div className="app-header__actions">
          <div 
            className="app-header__status" 
            data-state={hasConflicts ? 'conflict' : (hasStaleSnapshots ? 'stale' : (pendingOps > 0 ? 'dirty' : 'clean'))}
            style={{ cursor: (hasConflicts || hasStaleSnapshots) ? 'pointer' : 'default' }}
            onClick={() => {
              if (hasConflicts && conflictInfo) {
                // Show conflict details (higher priority)
                vscode.postMessage({ type: 'show-conflict-details', payload: conflictInfo });
              } else if (hasStaleSnapshots && staleSnapshotInfo) {
                // Show stale snapshot details
                vscode.postMessage({ type: 'show-stale-snapshot-details', payload: staleSnapshotInfo });
              }
            }}
          >
            <span className="status-dot" aria-hidden="true" />
            <span>
              {hasConflicts 
                ? '⚠️ Rebase conflict detected' 
                : (hasStaleSnapshots
                    ? '⚠️ Stale snapshots detected'
                    : (pendingOps > 0 
                        ? `${pendingOps} pending ${pendingOps === 1 ? 'change' : 'changes'}` 
                        : 'No pending changes'
                      )
                  )
              }
            </span>
          </div>
          <button
            className="refresh-button"
            onClick={() => {
              setIsRefreshing(true);
              vscode.postMessage({ type: 'refresh-project' });
            }}
            title="Refresh project state (re-check for stale snapshots and conflicts)"
            aria-label="Refresh project"
            disabled={isRefreshing}
            data-refreshing={isRefreshing}
          >
            <IconRefresh />
          </button>
        </div>
      </header>

      {/* Validation Results Banner */}
      {validationResult && (validationResult.errors.length > 0 || validationResult.warnings.length > 0) && (
        <div className="validation-banner">
          {validationResult.errors.length > 0 && (
            <div className="validation-error">
              <i className="codicon codicon-error"></i>
              <div className="validation-content">
                <strong>Dependency Errors:</strong>
                <ul>
                  {validationResult.errors.map((error, i) => (
                    <li key={i}>{error}</li>
                  ))}
                </ul>
              </div>
            </div>
          )}
          {validationResult.warnings.length > 0 && (
            <div className="validation-warning">
              <i className="codicon codicon-warning"></i>
              <div className="validation-content">
                <strong>Dependency Warnings:</strong>
                <ul>
                  {validationResult.warnings.map((warning, i) => (
                    <li key={i}>{warning}</li>
                  ))}
                </ul>
              </div>
            </div>
          )}
        </div>
      )}

      <div className="content">
        <div className="left-panel">
          <Sidebar />
          <SnapshotPanel />
        </div>
        {/* Right panel selection logic */}
        {selectedTableId ? (
          // Table or View selected
          isViewSelected ? (
            <ViewDetails viewId={selectedTableId} />
          ) : (
            <TableDesigner />
          )
        ) : selectedSchemaId ? (
          // Schema selected (no table)
          <SchemaDetails schemaId={selectedSchemaId} />
        ) : selectedCatalogId ? (
          // Catalog selected (no schema, no table)
          <CatalogDetails catalogId={selectedCatalogId} />
        ) : (
          // Nothing selected
          <div className="right-panel empty">
            <p>Select a catalog, schema, table, or view to view details</p>
          </div>
        )}
      </div>

      {hasProjectSettings && isProjectSettingsOpen && project && (
        <ProjectSettingsPanel 
          project={project}
          onClose={() => setIsProjectSettingsOpen(false)}
        />
      )}
    </div>
  );
};

