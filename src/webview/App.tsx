import React, { useEffect } from 'react';
import { useDesignerStore } from './state/useDesignerStore';
import { Toolbar } from './components/Toolbar';
import { Sidebar } from './components/Sidebar';
import { TableDesigner } from './components/TableDesigner';

declare const acquireVsCodeApi: () => {
  postMessage: (message: any) => void;
};

const vscode = acquireVsCodeApi();

export const App: React.FC = () => {
  const { project, setProject } = useDesignerStore();
  const [loading, setLoading] = React.useState(true);

  useEffect(() => {
    console.log('[SchemaX Webview] App mounted');
    
    // Set up message listener from extension
    const messageHandler = (event: MessageEvent) => {
      const message = event.data;
      console.log('[SchemaX Webview] Received message:', message.type);
      switch (message.type) {
        case 'project-loaded':
        case 'project-updated':
          console.log('[SchemaX Webview] Setting project:', message.payload);
          setProject(message.payload);
          setLoading(false);
          break;
      }
    };

    window.addEventListener('message', messageHandler);

    // Request initial project load
    console.log('[SchemaX Webview] Requesting project load');
    vscode.postMessage({ type: 'load-project' });

    return () => {
      window.removeEventListener('message', messageHandler);
    };
  }, [setProject]);

  if (loading) {
    return (
      <div className="app" style={{ display: 'flex', alignItems: 'center', justifyContent: 'center', height: '100vh' }}>
        <div style={{ textAlign: 'center' }}>
          <h2>Loading SchemaX Designer...</h2>
          <p style={{ color: '#888', marginTop: '8px' }}>If this persists, check the console (Help → Toggle Developer Tools)</p>
        </div>
      </div>
    );
  }

  return (
    <div className="app">
      <Toolbar />
      <div className="content">
        <Sidebar />
        <TableDesigner />
      </div>
    </div>
  );
};

