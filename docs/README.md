# SchemaX Documentation

- **Markdown reference** — `QUICKSTART.md`, `ARCHITECTURE.md`, `DEVELOPMENT.md`, `WORKFLOWS.md`, `TESTING.md`, etc. are the source for detailed content.
- **Docusaurus site** — A built documentation site (like DQX) lives in `docs/schemax/` and can be run locally or deployed.

## Run the docs site locally

```bash
cd docs/schemax
npm install
npm run start
```

Then open http://localhost:3000 (or the URL shown). Use `npm run build` for a production build and `npm run serve` to serve it.

**GitHub Pages**: Enable Pages (Settings → Pages → GitHub Actions), then push a version tag (e.g. `v0.2.0`) or run the **Docs Release** workflow manually. See [schemax/README.md](schemax/README.md) for details.
