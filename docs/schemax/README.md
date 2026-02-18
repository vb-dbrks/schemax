# SchemaX Docs (Docusaurus)

Documentation site for SchemaX, built with [Docusaurus](https://docusaurus.io/), matching the setup used for DQX.

## Prerequisites

- Node.js >= 18

## Commands

```bash
npm install
npm run start    # Dev server with hot reload (default http://localhost:3000)
npm run build    # Production build → build/
npm run serve    # Serve the production build locally
```

## Structure

- `docs/` — MDX/Markdown pages (intro, guide, reference).
- `src/css/custom.css` — Global styles.
- `static/img/` — Logo and favicon.
- `docusaurus.config.ts` — Site config (title, baseUrl, theme, plugins).
- `sidebars.ts` — Sidebar is split into **For Users** and **For Contributors** (explicit config).

## GitHub Pages (same as DQX)

1. **Enable Pages**: Repo → **Settings** → **Pages** → **Source**: **GitHub Actions**.
2. **Deploy**:
   - Push a version tag (e.g. `git tag v0.2.0 && git push origin v0.2.0`), or
   - **Actions** → **Docs Release** → **Run workflow**.
3. The workflow builds the site and deploys to GitHub Pages. The site URL will be `https://<owner>.github.io/<repo>/` (e.g. `https://your-org.github.io/schemax-vscode/`).

The workflow (`.github/workflows/docs-release.yml`) sets `GITHUB_PAGES_URL` and `GITHUB_PAGES_BASE_URL` so the built site uses the correct base path. For local builds, `docusaurus.config.ts` falls back to `/schemax-vscode/`; change the fallback if your repo name differs.

## Editing

- Update content in `docs/` (use `sidebar_position` in frontmatter to order).
- This Docusaurus site is the **single source** for all user and contributor docs (quickstart, architecture, development, testing, provider contract, contributing). The sidebar is split into **For Users** and **For Contributors** to reduce cognitive load.

**Dependencies:** `package.json` uses npm `overrides` to fix the ajv ReDoS vulnerability and the mkdirp deprecation. You may still see one deprecation warning for `gauge` (from `docusaurus-lunr-search`); it is harmless and there is no maintained replacement.
