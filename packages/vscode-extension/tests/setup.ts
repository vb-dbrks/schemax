/**
 * Jest setup file for React Testing Library
 */

import '@testing-library/jest-dom';

// Mock VS Code webview API for React components.
// When (global as any).__capturedOps is an array, append-ops payloads are pushed there (for E2E UI→live tests).
(global as any).__capturedOps = undefined;
(global as any).acquireVsCodeApi = () => ({
  postMessage: (msg: { type?: string; payload?: unknown }) => {
    if (msg.type === 'append-ops' && Array.isArray((global as any).__capturedOps)) {
      if (Array.isArray(msg.payload)) {
        (global as any).__capturedOps.push(...msg.payload);
      } else if (
        msg.payload &&
        typeof msg.payload === 'object' &&
        Array.isArray((msg.payload as { ops?: unknown[] }).ops)
      ) {
        (global as any).__capturedOps.push(...((msg.payload as { ops: unknown[] }).ops));
      }
    }
  },
  setState: jest.fn(),
  getState: jest.fn(),
});

// Suppress console errors in tests (optional)
const originalError = console.error;
beforeAll(() => {
  console.error = (...args: any[]) => {
    if (
      typeof args[0] === 'string' &&
      (args[0].includes('Warning: ReactDOM.render') ||
        args[0].includes('Not implemented: HTMLFormElement.prototype.submit'))
    ) {
      return;
    }
    originalError.call(console, ...args);
  };
});

afterAll(() => {
  console.error = originalError;
});
