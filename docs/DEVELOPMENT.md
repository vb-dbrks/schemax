# Development Guide

This guide covers building, testing, and contributing to SchemaX.

## Prerequisites

- Node.js 18 or higher
- npm 9 or higher
- Visual Studio Code 1.90.0 or higher

## Getting Started

### Clone and Install

```bash
git clone https://github.com/vb-dbrks/schemax-vscode.git
cd schemax-vscode
npm install
```

### Build

Build both the extension and webview:

```bash
npm run build
```

Or build individually:

```bash
npm run build:ext      # Extension only
npm run build:webview  # Webview only
```

### Development Mode

Start watch mode for automatic rebuilds:

```bash
npm run watch
```

This runs both extension and webview watchers concurrently.

### Testing

1. Open the project in VS Code
2. Press `F5` to launch the Extension Development Host
3. In the new window, open a test workspace
4. Press `Cmd+Shift+P` and run **SchemaX: Open Designer**
5. Check logs: View → Output → Select "SchemaX"

## Project Structure

```
schemax-vscode/
├── src/
│   ├── extension.ts              # Extension entry point
│   ├── storage-v2.ts              # File I/O and state management
│   ├── telemetry.ts               # Analytics stub
│   ├── shared/
│   │   ├── model.ts               # Data models (Zod schemas)
│   │   └── ops.ts                 # Operation definitions
│   └── webview/
│       ├── main.tsx               # Webview entry point
│       ├── App.tsx                # Main React component
│       ├── state/
│       │   └── useDesignerStore.ts # Zustand state management
│       ├── components/
│       │   ├── Toolbar.tsx        # Action buttons
│       │   ├── Sidebar.tsx        # Tree view
│       │   ├── TableDesigner.tsx  # Table editor
│       │   ├── ColumnGrid.tsx     # Column grid with inline editing
│       │   └── SnapshotPanel.tsx  # Version timeline
│       ├── styles.css             # Global styles
│       └── vscode-api.ts          # VS Code API bridge
├── dist/                          # Compiled extension
├── media/                         # Compiled webview assets
├── docs/                          # Documentation
├── esbuild.config.mjs            # Extension build config
├── vite.config.ts                # Webview build config
├── tsconfig.json                 # TypeScript config (extension)
└── tsconfig.webview.json         # TypeScript config (webview)
```

## Architecture

### Extension Host

The extension host (`src/extension.ts`) runs in Node.js and:
- Registers VS Code commands
- Manages file I/O via `storage-v2.ts`
- Creates and controls the webview
- Handles message passing with the webview

### Webview

The webview (`src/webview/`) is a React application that:
- Provides the visual designer UI
- Uses Zustand for state management
- Generates operations for every user action
- Sends operations to the extension via `postMessage`

### Communication Flow

```
User Action → Webview (React)
           → Generate Op
           → postMessage('append-ops')
           → Extension
           → Append to changelog.json
           → Apply op to state
           → postMessage('project-updated')
           → Webview updates UI
```

### Storage Architecture (V2)

SchemaX uses a snapshot-based architecture:

**Files:**
- `.schemax/project.json` - Metadata (snapshots list, settings)
- `.schemax/changelog.json` - Uncommitted operations
- `.schemax/snapshots/vX.Y.Z.json` - Full state snapshots

**Loading:**
1. Read latest snapshot file (or start empty)
2. Read changelog
3. Apply changelog operations to snapshot state
4. Result = current state

**Saving:**
- Operations append to `changelog.json`
- State changes are computed by applying operations
- Snapshots freeze the state and clear changelog

See [ARCHITECTURE.md](ARCHITECTURE.md) for detailed design information.

## Key Concepts

### Operations

Operations are immutable records of user actions:

```typescript
{
  id: "op_uuid",
  ts: "2025-10-06T...",
  op: "add_column",
  target: "col_uuid",
  payload: { tableId, colId, name, type, nullable }
}
```

All schema changes generate operations. Operations are:
- Append-only (never modified)
- Tracked with UUIDs
- Stored in `changelog.json`
- Used to generate migrations (future)

### Snapshots

Snapshots capture complete schema state:

**Metadata** (in `project.json`):
```typescript
{
  id: "snap_uuid",
  version: "v0.1.0",
  name: "Initial schema",
  file: ".schemax/snapshots/v0.1.0.json",
  opsCount: 15,
  ...
}
```

**Full Snapshot** (in `snapshots/v0.1.0.json`):
```typescript
{
  id: "snap_uuid",
  version: "v0.1.0",
  state: { catalogs: [...] },  // Complete schema
  opsIncluded: ["op_1", "op_2", ...],
  hash: "sha256...",
  ...
}
```

## Build System

### Extension Build (esbuild)

- Entry: `src/extension.ts`
- Output: `dist/extension.js`
- Target: Node.js (CommonJS)
- Externals: `vscode` module

### Webview Build (Vite)

- Entry: `src/webview/main.tsx`
- Output: `media/` directory
- Target: Browser (ES modules)
- Bundles: React, Zustand, CSS

## Coding Guidelines

### Logging

Always use the output channel for logging:

```typescript
outputChannel.appendLine('[SchemaX] Your message');
```

Never use `console.log()` in extension code (it goes to Extension Host console, not visible to users).

### File I/O

All file operations should go through `storage-v2.ts`:

```typescript
import * as storageV2 from './storage-v2';

// Good
const project = await storageV2.readProject(workspaceUri);
await storageV2.appendOps(workspaceUri, ops);

// Bad - don't access files directly
const content = await fs.readFile('schemax.project.json');
```

### State Management

The webview uses Zustand. All mutations must generate operations:

```typescript
// In useDesignerStore.ts
addColumn: (tableId, name, type, nullable) => {
  const op: Op = {
    id: `op_${uuidv4()}`,
    ts: new Date().toISOString(),
    op: 'add_column',
    target: colId,
    payload: { tableId, colId, name, type, nullable }
  };
  emitOps([op]);  // Sends to extension
}
```

### Data Validation

All external data must be validated with Zod:

```typescript
import { ProjectFile } from './shared/model';

const parsed = JSON.parse(content);
const project = ProjectFile.parse(parsed);  // Throws if invalid
```

## Testing Checklist

Before submitting a pull request, verify:

- [ ] Extension builds without errors (`npm run build`)
- [ ] No TypeScript errors
- [ ] Designer opens successfully
- [ ] Can create catalogs, schemas, tables, columns
- [ ] Can edit column properties inline
- [ ] Can create snapshots
- [ ] Snapshots persist after reloading
- [ ] Changelog clears after snapshot
- [ ] SchemaX output logs show no errors

## Debugging

### Extension Logs

View → Output → Select "SchemaX" from dropdown

Shows:
- Extension activation
- File operations
- Operation appends
- Snapshot creation
- Errors with stack traces

### Webview Console

1. With designer open: Help → Toggle Developer Tools
2. Go to Console tab
3. Look for `[SchemaX Webview]` logs

### Common Issues

**Blank webview:**
- Check webview console for JavaScript errors
- Verify `media/` directory has `index.html` and `assets/`
- Rebuild webview: `npm run build:webview`

**Operations not saving:**
- Check SchemaX output logs
- Verify `.schemax/` directory exists
- Check file permissions

**Snapshots disappearing:**
- Verify `project.json` has `snapshots` array
- Check `changelog.json` exists
- Look for errors in output logs

## Contributing

### Reporting Issues

When reporting bugs, include:
- Steps to reproduce
- Expected vs actual behavior
- SchemaX output logs
- Webview console errors (if any)
- VS Code version
- Operating system

### Pull Requests

1. Fork the repository
2. Create a feature branch: `git checkout -b feature/your-feature`
3. Make changes and test thoroughly
4. Commit with clear messages
5. Push and create a pull request

### Code Style

- Use TypeScript strict mode
- Follow existing code formatting
- Add comments for complex logic
- Keep functions focused and small
- Use descriptive variable names

## Release Process

1. Update version in `package.json`
2. Update CHANGELOG.md
3. Build: `npm run build`
4. Package: `npm run package`
5. Test the `.vsix` file
6. Create GitHub release
7. Publish to marketplace

## Resources

- [VS Code Extension API](https://code.visualstudio.com/api)
- [Webview API](https://code.visualstudio.com/api/extension-guides/webview)
- [React Documentation](https://react.dev/)
- [Zustand Documentation](https://zustand-demo.pmnd.rs/)
- [Zod Documentation](https://zod.dev/)

## Getting Help

- Check [docs/](.) for other documentation
- Review [.cursorrules](../.cursorrules) for project context
- Open an issue on GitHub
- Review existing issues and discussions

