# Root Makefile - delegates to packages
# Run from repo root: make fmt, make test, etc.

PYTHON_SDK  := packages/python-sdk
VSCODE_EXT  := packages/vscode-extension
DOCS_DIR    := docs/schemax

.PHONY: fmt format lint typecheck test test-ext check ci pre-commit all help
.PHONY: docs-build docs-serve clean

fmt format lint typecheck check pre-commit:
	$(MAKE) -C $(PYTHON_SDK) $@

# Run Python SDK tests only
test-python:
	$(MAKE) -C $(PYTHON_SDK) test

# Run VS Code extension (Jest/UI) tests only
test-ext:
	@echo "Running VS Code extension tests (Jest)..."
	cd $(VSCODE_EXT) && npm test

# Run all tests: Python SDK + VS Code extension
test: test-python test-ext

# Full checks: Python format/lint/typecheck/test, then extension test
all:
	$(MAKE) -C $(PYTHON_SDK) all
	$(MAKE) test-ext

# CI: same as all (format check, lint, typecheck, Python test, extension test)
# CI: Python check (no fix) + typecheck + Python test + extension test
ci:
	$(MAKE) -C $(PYTHON_SDK) ci
	$(MAKE) test-ext

docs-build:
	@echo "Building docs (Docusaurus)..."
	cd $(DOCS_DIR) && npm ci && npm run build

docs-serve:
	@echo "Serving docs at http://localhost:3000 (Ctrl+C to stop)"
	cd $(DOCS_DIR) && npm run serve

clean:
	@echo "Cleaning build artifacts and temp files..."
	rm -rf packages/vscode-extension/dist
	rm -rf packages/vscode-extension/media
	rm -rf packages/vscode-extension/coverage
	rm -rf $(DOCS_DIR)/.docusaurus
	rm -rf $(DOCS_DIR)/build
	find . -type d -name __pycache__ -exec rm -rf {} + 2>/dev/null || true
	find . -type f -name "*.pyc" -delete 2>/dev/null || true
	rm -rf $(PYTHON_SDK)/.pytest_cache
	rm -rf $(PYTHON_SDK)/.coverage
	rm -rf $(PYTHON_SDK)/htmlcov
	rm -rf $(PYTHON_SDK)/.tox
	@echo "Clean done."

help:
	@echo "SchemaX - run from repo root"
	@echo ""
	@echo "  make fmt         - Format Python code (Ruff)"
	@echo "  make format      - Same as fmt"
	@echo "  make lint        - Lint Python code (Ruff)"
	@echo "  make typecheck   - Type check (mypy)"
	@echo "  make test        - Run all tests (Python SDK + VS Code extension Jest/UI)"
	@echo "  make test-python - Run Python SDK tests only"
	@echo "  make test-ext    - Run VS Code extension (Jest/UI) tests only"
	@echo "  make check       - Format/lint check only (no fix)"
	@echo "  make ci          - Full CI checks (format, lint, typecheck, all tests)"
	@echo "  make pre-commit  - Format + lint + typecheck (no test)"
	@echo "  make all         - Format, lint, typecheck, Python test, extension test"
	@echo ""
	@echo "  make docs-build  - Build Docusaurus docs ($(DOCS_DIR))"
	@echo "  make docs-serve  - Serve docs locally (http://localhost:3000)"
	@echo "  make clean       - Remove extension/dist, docs build, coverage, __pycache__, etc."
	@echo ""
	@echo "For more targets: make -C $(PYTHON_SDK) help"
