# UI Testing Guide (VS Code Extension)

This document describes the testing strategy for the Schemax VS Code extension UI: **unit tests**, **integration tests**, and **E2E tests**.

## Overview

| Layer | Scope | Tools | Location |
|-------|--------|-------|----------|
| **Unit** | Pure utils, single components with mocked store | Jest + React Testing Library | `tests/webview/utils/*.test.ts`, `tests/webview/components/*.test.tsx` |
| **Unit (node)** | Storage, providers, no DOM | Jest (node env) | `tests/unit/**/*.test.ts`, `tests/providers/**/*.test.ts` |
| **Integration** | App shell, selection flow | Jest + RTL, mocked store | `tests/webview/integration/*.test.tsx` |
| **E2E** | Full VS Code + webview | @vscode/test-e2e or Playwright | `tests/e2e/` (see tests/e2e/README.md) |

## Running Tests

```bash
cd packages/vscode-extension
npm test
npm run test:watch
npm run test:coverage
```

Jest uses two projects: **node** (unit/providers) and **jsdom** (webview).

## Unit Tests

- **Utils**: e.g. `parsePrivileges` in `tests/webview/utils/grants.test.ts`.
- **Components**: Mock `useDesignerStore`; assert DOM and key interactions (VolumeDetails, FunctionDetails, MaterializedViewDetails, Sidebar, ColumnGrid, TableConstraints, ImportAssetsPanel).

## Integration Tests

- **App**: Renders without crashing; shows "No selection" when nothing selected; content area layout (`tests/webview/integration/App.integration.test.tsx`).

## E2E Tests

E2E is optional and not yet configured. See `tests/e2e/README.md` for how to add @vscode/test-e2e or Playwright and suggested cases (open Designer, selection flow, add object).

## Adding New Tests

- New util: `tests/webview/utils/<name>.test.ts` or `tests/unit/`.
- New component: `tests/webview/components/<Name>.test.tsx` with mocked store.
- New integration: `tests/webview/integration/<scenario>.integration.test.tsx`.

## Coverage

Run `npm run test:coverage`; thresholds are in `jest.config.js`. Report: `coverage/lcov-report/index.html`.
