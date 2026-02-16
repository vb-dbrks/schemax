import React from 'react';
import { createRoot } from 'react-dom/client';
import { provideVSCodeDesignSystem, vsCodeButton, vsCodeDropdown, vsCodeOption, vsCodeProgressRing, vsCodeTextField } from '@vscode/webview-ui-toolkit';
import { App } from './App';
import './styles.css';

provideVSCodeDesignSystem().register(
  vsCodeButton(),
  vsCodeDropdown(),
  vsCodeOption(),
  vsCodeProgressRing(),
  vsCodeTextField()
);

const container = document.getElementById('root');

if (container) {
  const root = createRoot(container);
  root.render(<App />);
} else {
  console.error('[SchemaX Webview] ERROR: Root container not found!');
  document.body.innerHTML = '<div style="padding: 20px; color: red;">ERROR: Root container not found. Check console.</div>';
}

