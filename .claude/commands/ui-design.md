# UI Design Skill — SchemaX Webview

When implementing or modifying webview UI in `packages/vscode-extension/src/webview/`, follow the design system below precisely. Do not introduce CSS frameworks, new design tokens, or new component patterns.

---

## Styling System

- **No Tailwind / Bootstrap.** All styles live in `src/webview/styles.css` (3,354 lines).
- **Always use VS Code CSS variables** for color so light/dark mode works automatically.
- Never hardcode colors except for the warning yellow `#ffab00`, the error red `#f14c4c`, and the status data-attribute patterns already in `styles.css`. If these patterns are not in `style.css`, or human asks to add new colour, then add the pattern in the `style.css` but ask human for confirmation.

### Color Tokens (VS Code variables)

| Purpose | Variable |
|---|---|
| Primary text | `--vscode-foreground` |
| Secondary/muted text | `--vscode-descriptionForeground` |
| Main background | `--vscode-editor-background` |
| Panel/sidebar background | `--vscode-sideBar-background` |
| Borders & dividers | `--vscode-panel-border` |
| Input background | `--vscode-input-background` |
| Input border | `--vscode-input-border` |
| Focus ring | `--vscode-focusBorder` |
| Primary button | `--vscode-button-background` / `--vscode-button-hoverBackground` |
| Secondary button | `--vscode-button-secondaryBackground` |
| Error | `--vscode-errorForeground` / `--vscode-inputValidation-errorBackground` / `#f14c4c` |
| Warning | `--vscode-notificationsWarningIcon-foreground` / `#ffab00` |
| Success | `--vscode-charts-green` |
| Info | `--vscode-charts-blue` |
| List hover | `--vscode-list-hoverBackground` |
| List selection | `--vscode-list-activeSelectionBackground` |
| Badge | `--vscode-badge-background` / `--vscode-badge-foreground` |
| Tooltip | `--vscode-editorHoverWidget-background` / `--vscode-editorHoverWidget-border` |
| Monospace font | `--vscode-editor-font-family` |

### Spacing Scale

Use only: `4px · 6px · 8px · 12px · 16px · 20px · 24px`

- Inline element gap: 4–8px
- Form field gap: 4px (label→input); 12px (field→field)
- Section padding: 16px
- Section gap: 24px
- Modal padding: 24px
- Header padding: `16px 24px 12px`

### Border Radius

| Size | Use |
|---|---|
| `2px` | Tiny badges, code chips |
| `3px` | Inputs, tree items |
| `4px` | Buttons, small cards |
| `6px` | Panels, view sections |
| `8px` | Large cards |
| `999px` | Pills / tag badges |

### Typography

```css
body { font-size: 13px; }

Page title:     18px / 600
Section header: 14–16px / 600
Body:           13px / 400
Label:          12px / 500
Helper/hint:    11px / 400
Code:           12–13px / --vscode-editor-font-family
```

---

## Component Patterns

### Icons

Always use Codicons — never emoji or inline SVG (breaks theme).

```tsx
<i className="codicon codicon-edit" aria-hidden="true" />
<i className="codicon codicon-trash" aria-hidden="true" />
<i className="codicon codicon-add" aria-hidden="true" />
```

### Buttons

Use VS Code Webview UI Toolkit `<VSCodeButton>`. For icon-only buttons use a `<button>` with class `help-button` / `refresh-button`:

```tsx
// Primary
<VSCodeButton onClick={...}>Save</VSCodeButton>

// Secondary
<VSCodeButton appearance="secondary" onClick={...}>Cancel</VSCodeButton>

// Danger
<VSCodeButton className="danger-button" onClick={...}>Delete</VSCodeButton>

// Icon-only (28×28px)
<button className="help-button" title="Refresh">
  <i className="codicon codicon-refresh" aria-hidden="true" />
</button>
```

### Form Fields

```tsx
<div className="modal-field-group">
  <label>Column Name</label>
  <VSCodeTextField value={name} onInput={e => setName(e.target.value)} />
  <span className="modal-field-hint">Used as the physical column name in SQL.</span>
  {error && <span className="form-error">{error}</span>}
</div>
```

Key classes: `.modal-field-group`, `.modal-field-hint`, `.form-error`

For inline table editing use native `<input>` / `<select>` inside `.properties-table` — not toolkit components — to match the existing 28px-height row pattern.

### Modals

```tsx
// Always render into a portal or at root level; z-index: 1000
<div className="modal-overlay">
  <div className="modal-surface">
    <h2 className="modal-title">Add Column</h2>

    {/* fields */}

    <div className="modal-buttons">
      <VSCodeButton appearance="secondary" onClick={onCancel}>Cancel</VSCodeButton>
      <VSCodeButton onClick={onConfirm}>Add</VSCodeButton>
    </div>
  </div>
</div>
```

`modal-surface` minimum width: 420px. No `border-radius` needed — it is defined in `styles.css`.

### Panels / Sections

```tsx
<div className="view-section">
  <h3 className="view-section-title">Columns</h3>
  {/* content */}
</div>
```

### Tree / Sidebar Items

```tsx
<div
  className={`tree-item ${selected ? 'selected' : ''}`}
  onClick={handleSelect}
>
  <i className="codicon codicon-table" aria-hidden="true" />
  <span>{table.name}</span>
  <div className="actions">
    <button className="help-button" onClick={handleRename}>
      <i className="codicon codicon-edit" />
    </button>
    <button className="help-button" onClick={handleDelete}>
      <i className="codicon codicon-trash" />
    </button>
  </div>
</div>
```

`.actions` is hidden by default; `.tree-item:hover .actions` makes it visible — this is already in `styles.css`.

### Status States (data-attribute pattern)

```tsx
<div
  className="app-header__status"
  data-state={hasChanges ? 'dirty' : 'clean'}
>
  {statusLabel}
</div>
```

States: `clean` · `dirty` (orange) · `conflict` (red) · `stale` (yellow)

### Dropdowns

```tsx
<VSCodeDropdown value={value} onChange={e => setValue(e.target.value)}>
  <VSCodeOption value="delta">Delta</VSCodeOption>
  <VSCodeOption value="iceberg">Iceberg</VSCodeOption>
</VSCodeDropdown>
```

---

## State Management

All UI state and data mutations live in `useDesignerStore` (Zustand).

**Golden rule: never update local component state for data that belongs in the catalog model.** Mutate through the store, which emits ops.

```tsx
const { addColumn, renameColumn, dropColumn } = useDesignerStore();

// Every mutation emits an op through emitOps() internally
addColumn({ tableId, column });
```

For undo support, store actions call `recordUndoBatch(ops, label)` before posting to the extension. Do not implement ad-hoc undo logic in components.

---

## VS Code Message Protocol

```typescript
// Send to extension host
vscode.postMessage({ type: "append-ops", payload: { actionId, ops } });
vscode.postMessage({ type: "update-project-config", payload: project });
vscode.postMessage({ type: "run-import", payload: request });
vscode.postMessage({ type: "refresh-project" });

// Receive from extension host
window.addEventListener("message", (event) => {
  switch (event.data.type) {
    case "project-loaded":   // initial data
    case "project-updated":  // after save
    case "ops-appended":     // confirm undo batch
    case "ops-append-failed":
    case "undo-completed":
    case "undo-failed":
    case "import-result":
    case "import-progress":
  }
});
```

---

## Layout

```
.app (flex column, 100vh)
├── .app-header  (flex row, logo + title + status + actions)
└── .content  (flex row, flex: 1, overflow hidden)
    ├── .left-panel  (width: 300px, flex column)
    │   ├── .sidebar-header
    │   ├── .sidebar  (flex-1, overflow-y: auto)
    │   └── .snapshot-panel  (max-height: 40% of panel)
    └── [main content]  (flex-1, overflow-y: auto)
```

Do not change the sidebar's `300px` fixed width. Main content should be `flex: 1` with `overflow-y: auto`.

---

## Checklist for New UI Work

- [ ] Colors use VS Code CSS variables only
- [ ] Icons use Codicons
- [ ] Spacing follows the 4/6/8/12/16/20/24px scale
- [ ] Modals use `.modal-overlay` + `.modal-surface` + `.modal-buttons`
- [ ] New CSS added to `styles.css` (not inline or CSS modules)
- [ ] All data mutations go through `useDesignerStore`
- [ ] Toolbar/hover actions use `.actions` show-on-hover pattern
- [ ] `outputChannel.appendLine()` for any logging (never `console.log()`)
- [ ] Accessibility: `aria-hidden="true"` on decorative icons, meaningful `title` on icon buttons
