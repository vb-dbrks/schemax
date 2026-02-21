# E2E Tests (VS Code Extension)

End-to-end tests run the Schemax extension inside a real VS Code (or Extension Development Host) and automate the UI. They are **optional** and not yet wired into the default `npm test` run.

## Status

- **E2E runtime**: Not configured. Unit and integration tests are run via Jest only.
- **When you add E2E**: Use this folder for fixtures and docs; add dependencies and scripts as below.

## How to Add E2E

### Option A: @vscode/test-e2e

1. Install:
   ```bash
   npm i -D @vscode/test-e2e
   ```
2. Add a launch config in `.vscode/launch.json` for "Extension E2E Tests" (see [VS Code docs](https://code.visualstudio.com/api/testing/extensions#running-tests)).
3. Create a test workspace under `tests/e2e/fixtures/` (e.g. a minimal project with `.schemax/project.json`).
4. Write a test that:
   - Runs VS Code with the extension and your test workspace.
   - Executes `schemax.openDesigner` (or opens the webview another way).
   - Asserts the webview content (e.g. "No selection" or catalog name).

### Option B: Playwright for VS Code

- Use [playwright-vscode](https://github.com/microsoft/playwright-vscode) or similar to launch VS Code and drive the UI.
- Add a `tests/e2e/` script in `package.json` that runs Playwright with a VS Code config.

## Suggested E2E Cases

1. **Smoke**: Open Designer → webview loads and shows empty state or project name.
2. **Selection**: Open Designer → expand catalog/schema in sidebar → click table/volume → detail panel shows correct component.
3. **Add object** (optional): Open Add dialog → choose Volume → fill name → Add → volume appears in tree (requires stable workspace).

## Running E2E (after setup)

Document the exact command here, e.g.:

```bash
npm run test:e2e
# or
npx @vscode/test-e2e
```

## CI

When E2E is added, add a separate CI job (e.g. `extension-e2e`) that runs E2E tests, optionally with a test workspace checked in under `tests/e2e/fixtures/`.
