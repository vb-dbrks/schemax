# Changelog

All notable changes to the SchemaX extension will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added
- Visual schema designer for Unity Catalog objects
- Support for catalogs, schemas, tables, and columns
- **Table Properties (TBLPROPERTIES) management** with inline editing
  - Add, edit, and delete table properties
  - Reserved key validation
  - Built-in help for common Delta Lake properties
  - Full support for Unity Catalog TBLPROPERTIES specification
- Inline column editing with type, nullable, and comment
- Snapshot-based versioning system
- Changelog tracking for uncommitted changes
- Snapshot timeline panel showing version history
- Commands: Open Designer, Show Last Changes, Create Snapshot
- Drag-and-drop column reordering
- Custom modal dialogs for sandboxed webview
- Git-friendly JSON storage format
- Operation-based change tracking with UUIDs

### Changed
- Migrated from single-file (v1) to multi-file architecture (v2)
- Separated snapshots into individual files for better git diffs
- Moved all schema files into `.schemax/` directory

### Technical
- TypeScript 5.4
- React 18 with Vite bundler
- Zustand for state management
- Zod for data validation
- esbuild for extension bundling
- VS Code API 1.90.0

## [0.0.1] - 2025-10-06

### Added
- Initial development release
- Basic schema designer functionality
- File-based storage system

[Unreleased]: https://github.com/vb-dbrks/schemax-vscode/compare/v0.0.1...HEAD
[0.0.1]: https://github.com/vb-dbrks/schemax-vscode/releases/tag/v0.0.1

