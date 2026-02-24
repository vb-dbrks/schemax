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

### E2E: UI → CLI → SDK → live environment

Two complementary E2E approaches validate the full pipeline:

#### 1. Jest E2E: UI (store) → capture ops → write workspace → run CLI → live

**Location:** `tests/webview/integration/App.e2e-ui-to-live.test.tsx`

- Renders the **real Designer App** and injects `project-loaded` (empty state + provider).
- Drives the **same Zustand store** the UI uses: `addCatalog`, `addSchema`, `addTable`, `addColumn`, `addFunction` (same code path as user clicks).
- Captures `append-ops` messages via the mocked VS Code API and writes `.schemax/project.json` + `changelog.json` to a temp workspace.
- When `SCHEMAX_RUN_LIVE_COMMAND_TESTS=1` and `SCHEMAX_E2E_PROFILE` / `SCHEMAX_E2E_WAREHOUSE_ID` (or Databricks env) are set, spawns `schemax apply` against that workspace and asserts exit code 0.

So this test validates **UI components (store) → generated state → CLI → live env** using the actual Designer code path.

#### 2. Python SDK live tests: UI-equivalent state → CLI → SDK → live

**Location:** `packages/python-sdk/tests/integration/test_e2e_ui_to_live.py` and other `test_live_*.py` files.

- Build the **same .schemax state** the UI would produce (same op shapes via `OperationBuilder`, `append_ops`) and run `schemax apply` / rollback against a live workspace.
- Validate **UI-equivalent state → CLI → SDK → live** without driving the webview.

**Running:**

- **Jest E2E (with live apply):** Set `SCHEMAX_RUN_LIVE_COMMAND_TESTS=1`, `SCHEMAX_E2E_PROFILE`, `SCHEMAX_E2E_WAREHOUSE_ID` (or Databricks env), then `npm test -- App.e2e-ui-to-live` from `packages/vscode-extension`.
- **Interactive prompt:** Run `npm run test:integration` from `packages/vscode-extension`. If the required env vars are already set (e.g. `export SCHEMAX_RUN_LIVE_COMMAND_TESTS=1` or a `.env`), prompts are skipped. When any are missing and stdin is a TTY, the script prompts for each and then runs the E2E test. In CI or non-interactive runs, the test runs without live apply (same as `npm test`).
- **Python live tests:** See `packages/python-sdk/tests/integration/README_LIVE_TESTS.md` (requires `SCHEMAX_RUN_LIVE_COMMAND_TESTS=1` and Databricks config).

## Adding New Tests

- New util: `tests/webview/utils/<name>.test.ts` or `tests/unit/`.
- New component: `tests/webview/components/<Name>.test.tsx` with mocked store.
- New integration: `tests/webview/integration/<scenario>.integration.test.tsx`.

## Coverage

Run `npm run test:coverage`; thresholds are in `jest.config.js`. Report: `coverage/lcov-report/index.html`.
