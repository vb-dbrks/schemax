import React, { useEffect } from 'react';
import { useDesignerStore } from './state/useDesignerStore';
import { Sidebar } from './components/Sidebar';
import { TableDesigner } from './components/TableDesigner';
import { SnapshotPanel } from './components/SnapshotPanel';
import { getVsCodeApi } from './vscode-api';

const vscode = getVsCodeApi();

export const App: React.FC = () => {
  const { project, setProject, setProvider } = useDesignerStore();
  const [loading, setLoading] = React.useState(true);

  useEffect(() => {
    console.log('[Schematic Webview] App mounted');
    
    // Set up message listener from extension
    const messageHandler = (event: MessageEvent) => {
      const message = event.data;
      console.log('[Schematic Webview] Received message:', message.type);
      switch (message.type) {
        case 'project-loaded':
        case 'project-updated':
          console.log('[Schematic Webview] Received project update');
          console.log('[Schematic Webview] Snapshots count:', message.payload.snapshots?.length || 0);
          console.log('[Schematic Webview] Ops count:', message.payload.ops?.length || 0);
          console.log('[Schematic Webview] Provider:', message.payload.provider);
          console.log('[Schematic Webview] Full project:', message.payload);
          
          // Set provider from payload
          if (message.payload.provider) {
            setProvider(message.payload.provider);
            console.log('[Schematic Webview] Provider set:', message.payload.provider.name);
          }
          
          setProject(message.payload);
          setLoading(false);
          break;
      }
    };

    window.addEventListener('message', messageHandler);

    // Request initial project load
    console.log('[Schematic Webview] Requesting project load');
    vscode.postMessage({ type: 'load-project' });

    return () => {
      window.removeEventListener('message', messageHandler);
    };
  }, [setProject, setProvider]);

  if (loading) {
    return (
      <div className="app" style={{ display: 'flex', alignItems: 'center', justifyContent: 'center', height: '100vh' }}>
        <div style={{ textAlign: 'center' }}>
          <h2>Loading Schematic Designer...</h2>
          <p style={{ color: '#888', marginTop: '8px' }}>If this persists, check the console (Help → Toggle Developer Tools)</p>
        </div>
      </div>
    );
  }

  return (
    <div className="app">
      <div className="content">
        <div className="left-panel">
          <Sidebar />
          <SnapshotPanel />
        </div>
        <TableDesigner />
      </div>
    </div>
  );
};

