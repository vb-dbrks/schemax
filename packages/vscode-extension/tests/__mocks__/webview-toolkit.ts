import React from 'react';

// Mock VS Code Webview UI Toolkit components for testing
export const VSCodeButton = ({ children, onClick, ...props }: any) => 
  React.createElement('button', { onClick, ...props }, children);

export const VSCodeTextField = ({ value, onChange, ...props }: any) => 
  React.createElement('input', { type: 'text', value, onChange, ...props });

export const VSCodeDropdown = ({ children, ...props }: any) => 
  React.createElement('select', props, children);

export const VSCodeOption = ({ children, value, ...props }: any) => 
  React.createElement('option', { value, ...props }, children);

export const VSCodeProgressRing = (props: any) => 
  React.createElement('div', { ...props, 'data-testid': 'progress-ring' }, 'Loading...');

export const VSCodeCheckbox = ({ checked, onChange, ...props }: any) => 
  React.createElement('input', { type: 'checkbox', checked, onChange, ...props });

export const provideVSCodeDesignSystem = () => ({
  register: (...args: any[]) => {},
});

export const vsCodeButton = () => {};
export const vsCodeDropdown = () => {};
export const vsCodeOption = () => {};
export const vsCodeProgressRing = () => {};
export const vsCodeTextField = () => {};
export const vsCodeCheckbox = () => {};

