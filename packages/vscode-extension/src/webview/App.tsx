import React, { useEffect } from 'react';
import { VSCodeButton, VSCodeProgressRing } from '@vscode/webview-ui-toolkit/react';
import { useDesignerStore } from './state/useDesignerStore';
import { Sidebar } from './components/Sidebar';
import { TableDesigner } from './components/TableDesigner';
import { SnapshotPanel } from './components/SnapshotPanel';
import { getVsCodeApi } from './vscode-api';
import { ProjectSettingsPanel } from './components/ProjectSettingsPanel';

const vscode = getVsCodeApi();

const IconSettings: React.FC = () => (
  <svg slot="start" width="16" height="16" viewBox="0 0 16 16" fill="currentColor" aria-hidden="true">
    <path fillRule="evenodd" d="M9.078 2.044a.75.75 0 0 0-1.156 0l-.52.649a4 4 0 0 0-1.236.29l-.785-.27a.75.75 0 0 0-.95.46l-.518 1.554a.75.75 0 0 0 .41.923l.742.33a4 4 0 0 0-.002 1.342l-.74.33a.75.75 0 0 0-.41.923l.518 1.554a.75.75 0 0 0 .95.46l.784-.27a4 4 0 0 0 1.237.29l.521.65a.75.75 0 0 0 1.155 0l.52-.65a4 4 0 0 0 1.237-.29l.784.27a.75.75 0 0 0 .95-.46l.518-1.554a.75.75 0 0 0-.41-.923l-.74-.33a4 4 0 0 0-.002-1.342l.742-.33a.75.75 0 0 0 .41-.923l-.518-1.554a.75.75 0 0 0-.95-.46l-.785.27a4 4 0 0 0-1.236-.29zm-1.078 1.956a2.5 2.5 0 1 1 0 5.001 2.5 2.5 0 0 1 0-5z" clipRule="evenodd" />
  </svg>
);

export const App: React.FC = () => {
  const { project, setProject, setProvider, provider } = useDesignerStore();
  const [loading, setLoading] = React.useState(true);
  const [isProjectSettingsOpen, setIsProjectSettingsOpen] = React.useState(false);

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
        <div className="app-header__status" data-state={pendingOps > 0 ? 'dirty' : 'clean'}>
          <span className="status-dot" aria-hidden="true" />
          <span>{pendingOps > 0 ? `${pendingOps} pending ${pendingOps === 1 ? 'change' : 'changes'}` : 'No pending changes'}</span>
        </div>
      </header>
      <div className="content">
        <div className="left-panel">
          <Sidebar />
          <SnapshotPanel />
        </div>
        <TableDesigner />
      </div>

      {hasProjectSettings && isProjectSettingsOpen && (
        <div className="modal" role="dialog" aria-modal="true" onClick={() => setIsProjectSettingsOpen(false)}>
          <div
            className="modal-content modal-surface environment-modal"
            onClick={(event) => event.stopPropagation()}
          >
            <ProjectSettingsPanel />
            <div className="modal-buttons">
              <VSCodeButton appearance="secondary" type="button" onClick={() => setIsProjectSettingsOpen(false)}>
                Close
              </VSCodeButton>
            </div>
          </div>
        </div>
      )}
    </div>
  );
};

