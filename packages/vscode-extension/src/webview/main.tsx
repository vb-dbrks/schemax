import React from 'react';
import { createRoot } from 'react-dom/client';
import { App } from './App';
import './styles.css';

console.log('[Schematic Webview] main.tsx loaded');
console.log('[Schematic Webview] React version:', React.version);

try {
  const container = document.getElementById('root');
  console.log('[Schematic Webview] Root container:', container);
  
  if (container) {
    console.log('[Schematic Webview] Creating React root...');
    const root = createRoot(container);
    console.log('[Schematic Webview] Rendering App...');
    root.render(<App />);
    console.log('[Schematic Webview] App rendered successfully');
  } else {
    console.error('[Schematic Webview] ERROR: Root container not found!');
    document.body.innerHTML = '<div style="padding: 20px; color: red;">ERROR: Root container not found. Check console.</div>';
  }
} catch (error) {
  console.error('[Schematic Webview] ERROR during initialization:', error);
  document.body.innerHTML = `<div style="padding: 20px; color: red;">ERROR: ${error}</div>`;
}

