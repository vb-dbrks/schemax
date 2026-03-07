<img src="docs/schemax/static/img/schemax_text_logo.png" width="320" alt="SchemaX" />

**SchemaX is a Git-friendly schema management platform for data catalogs, with a VS Code designer and a Python CLI/SDK.**

[![License: Apache-2.0](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)
[![codecov](https://codecov.io/gh/vb-dbrks/schemax/graph/badge.svg?token=Z5FWGAXEXS)](https://codecov.io/gh/vb-dbrks/schemax)
[![Python SDK CI](https://github.com/vb-dbrks/schemax-vscode/actions/workflows/python-sdk-ci.yml/badge.svg)](https://github.com/vb-dbrks/schemax-vscode/actions/workflows/python-sdk-ci.yml)
[![Extension CI](https://github.com/vb-dbrks/schemax-vscode/actions/workflows/extension-ci.yml/badge.svg)](https://github.com/vb-dbrks/schemax-vscode/actions/workflows/extension-ci.yml)
[![PyPI Downloads](https://static.pepy.tech/personalized-badge/schemaxpy?period=total&units=INTERNATIONAL_SYSTEM&left_color=BLACK&right_color=GREEN&left_text=downloads)](https://pepy.tech/projects/schemaxpy)

## What SchemaX Does

- Models catalog objects and governance as versioned operations + snapshots
- Generates provider-aware SQL with dependency ordering
- Applies changes safely with deployment tracking and rollback flows
- Supports live workflows via CLI and programmable workflows via Python SDK

Current provider depth is Databricks Unity Catalog. Hive support is in progress.

## Documentation

The Docusaurus site is the source of truth for product and engineering docs:

- [Documentation Home](https://vb-dbrks.github.io/schemax/)
- [Quickstart](https://vb-dbrks.github.io/schemax/docs/getting-started/quickstart/)
- [Architecture](https://vb-dbrks.github.io/schemax/docs/architecture/system-overview/)
- [CLI Reference](https://vb-dbrks.github.io/schemax/docs/reference/cli-reference/)
- [Provider Contract](https://vb-dbrks.github.io/schemax/docs/reference/provider-contract/)
- [Release Notes](https://vb-dbrks.github.io/schemax/docs/reference/release-notes/)
- [Testing Guide](https://vb-dbrks.github.io/schemax/docs/contributing/testing/)

Package-level docs:

- [Python SDK README](packages/python-sdk/README.md)
- [VS Code Extension README](packages/vscode-extension/README.md)

## Quick Start

### VS Code Extension

Install it from VS Code / Cursor / Antigravity Extension Marketplace
- https://open-vsx.org/extension/schemax/schemax-vscode
- https://marketplace.visualstudio.com/items?itemName=schematic-dev.schemax-vscode

### Python CLI

```python
pip install schemaxpy
```

Read more on how to use [SchemaX](https://vb-dbrks.github.io/schemax/)
## License

Apache 2.0. See [LICENSE](LICENSE).
