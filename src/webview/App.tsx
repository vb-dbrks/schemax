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
  const { setProject } = useDesignerStore();

  useEffect(() => {
    // Set up message listener from extension
    const messageHandler = (event: MessageEvent) => {
      const message = event.data;
      switch (message.type) {
        case 'project-loaded':
        case 'project-updated':
          setProject(message.payload);
          break;
      }
    };

    window.addEventListener('message', messageHandler);

    // Request initial project load
    vscode.postMessage({ type: 'load-project' });

    return () => {
      window.removeEventListener('message', messageHandler);
    };
  }, [setProject]);

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

