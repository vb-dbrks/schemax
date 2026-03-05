# SchemaX

Design and manage **Databricks Unity Catalog** schemas visually—with version control and one-click SQL generation.

**📖 [Read the full documentation](https://vb-dbrks.github.io/schemax/)** for setup, quickstart, and reference.

---

## Get started

1. **Install the extension**  
   In VS Code: Extensions (Ctrl+Shift+X / Cmd+Shift+X) → search **SchemaX** → Install.

2. **Open the designer**  
   Click the **SchemaX** icon in the left Activity Bar, then **Open SchemaX Designer**, or use the Command Palette (Ctrl+Shift+P / Cmd+Shift+P) → **SchemaX: Open Designer**.

3. **Create your schema**  
   Add a catalog, then schemas and tables. Edit columns and properties in the grid. Create snapshots and generate SQL when you’re ready.

**Optional — Python SDK:** For **import from Databricks**, **apply**, **rollback**, and **validate**, install the SchemaX CLI. Run **SchemaX: Install Python SDK** from the Command Palette (this uses the Python interpreter selected in VS Code when set), or run `pip install schemaxpy` in your own environment. The extension will prompt you when a feature needs the CLI.

---

## What you can do

- **Visual designer** — Catalogs, schemas, tables, and columns in an intuitive UI.
- **Version control** — Snapshots with semantic versions; changelog for uncommitted changes.
- **SQL generation** — Generate migration SQL from the designer.
- **Data governance** — Constraints, column tags, row filters, column masks, table properties (Delta Lake TBLPROPERTIES).
- **Import** — Bring existing Unity Catalog assets into your project (requires Python SDK).

---

## Commands

| Command | What it does |
|--------|----------------|
| **SchemaX: Open Designer** | Open the schema designer |
| **SchemaX: Create Snapshot** | Save a version snapshot |
| **SchemaX: Generate SQL Migration** | Generate SQL from your changes |
| **SchemaX: Show Last Emitted Changes** | View recent operations |
| **SchemaX: Import Existing Assets** | Import from Databricks (needs Python SDK) |
| **SchemaX: Install Python SDK** | Install `schemaxpy` for CLI features |

---

## Where things are stored

SchemaX uses a `.schemax` folder in your workspace:

- `project.json` — Project and environment settings  
- `changelog.json` — Uncommitted changes  
- `snapshots/` — Version snapshots (e.g. v0.1.0.json)

---

## Requirements

- VS Code 1.90.0 or newer  
- A workspace folder open

---

## Development / Testing

- Run tests: `npm test` (unit + integration).
- UI testing guide (unit, integration, E2E): see [TESTING-UI.md](TESTING-UI.md).

---

## Publishing (maintainers)

Two workflows publish the extension:

- **VS Code Marketplace** — `.github/workflows/publish-vscode-extension.yml` (publisher `schematic-dev`, uses `VSCE_PAT`).
- **Open VSX** — `.github/workflows/publish-openvsx.yml` (namespace `schemax`, uses `OVSX_PAT`). Used by Cursor and Antigravity.

Both run on `v*` tag push and via **Actions → Run workflow**. For Open VSX, add the `OVSX_PAT` secret (from [open-vsx.org/user-settings/tokens](https://open-vsx.org/user-settings/tokens)) to the same environment as `VSCE_PAT` (e.g. `vscode-marketplace`). Publishing is done via CI only; the workflow overrides the publisher to `schemax` for Open VSX.

---

## Links

- **Documentation**: [vb-dbrks.github.io/schemax](https://vb-dbrks.github.io/schemax/)
- **Report an issue**: [GitHub Issues](https://github.com/vb-dbrks/schemax-vscode/issues)
- **Repository**: [github.com/vb-dbrks/schemax-vscode](https://github.com/vb-dbrks/schemax-vscode)

Apache License 2.0 — see [LICENSE](../../LICENSE) for details.
