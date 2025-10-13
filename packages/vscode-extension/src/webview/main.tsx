import React from 'react';
import { createRoot } from 'react-dom/client';
import { App } from './App';
import './styles.css';

console.log('[SchemaX Webview] main.tsx loaded');
console.log('[SchemaX Webview] React version:', React.version);

try {
  const container = document.getElementById('root');
  console.log('[SchemaX Webview] Root container:', container);
  
  if (container) {
    console.log('[SchemaX Webview] Creating React root...');
    const root = createRoot(container);
    console.log('[SchemaX Webview] Rendering App...');
    root.render(<App />);
    console.log('[SchemaX Webview] App rendered successfully');
  } else {
    console.error('[SchemaX Webview] ERROR: Root container not found!');
    document.body.innerHTML = '<div style="padding: 20px; color: red;">ERROR: Root container not found. Check console.</div>';
  }
} catch (error) {
  console.error('[SchemaX Webview] ERROR during initialization:', error);
  document.body.innerHTML = `<div style="padding: 20px; color: red;">ERROR: ${error}</div>`;
}

