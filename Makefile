# Root Makefile - delegates to packages
# Run from repo root: make fmt, make test, etc.

PYTHON_SDK  := packages/python-sdk
VSCODE_EXT  := packages/vscode-extension
DOCS_DIR    := docs/schemax

.PHONY: fmt format lint typecheck test test-python test-ext integration check ci pre-commit all help
.PHONY: docs-build docs-serve clean

fmt:
	$(MAKE) -C $(PYTHON_SDK) fmt
	@echo "Running VS Code extension format..."
	cd $(VSCODE_EXT) && npm run format

format:
	$(MAKE) -C $(PYTHON_SDK) format
	@echo "Running VS Code extension format..."
	cd $(VSCODE_EXT) && npm run format

lint:
	$(MAKE) -C $(PYTHON_SDK) lint
	@echo "Running VS Code extension lint..."
	cd $(VSCODE_EXT) && npm run lint

typecheck:
	$(MAKE) -C $(PYTHON_SDK) typecheck

check:
	$(MAKE) -C $(PYTHON_SDK) check
	@echo "Running VS Code extension lint..."
	cd $(VSCODE_EXT) && npm run lint

pre-commit:
	$(MAKE) fmt
	$(MAKE) typecheck

# Run Python SDK tests only
test-python:
	$(MAKE) -C $(PYTHON_SDK) test

# Run VS Code extension (Jest/UI) tests only
test-ext:
	@echo "Running VS Code extension tests (Jest)..."
	cd $(VSCODE_EXT) && npm test

# Run all tests: Python SDK + VS Code extension
test: test-python test-ext

# Run all Python integration tests (including live Databricks when env is set)
integration:
	$(MAKE) -C $(PYTHON_SDK) integration

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
	@echo "  make fmt         - Format Python + VS Code extension code"
	@echo "  make format      - Format Python + VS Code extension code"
	@echo "  make lint        - Lint Python + VS Code extension code"
	@echo "  make typecheck   - Type check Python SDK (mypy)"
	@echo "  make test        - Run all tests (Python SDK + VS Code extension Jest/UI)"
	@echo "  make test-python - Run Python SDK tests only"
	@echo "  make test-ext    - Run VS Code extension (Jest/UI) tests only"
	@echo "  make integration - Run all Python integration tests (including live Databricks)"
	@echo "  make check       - Python check + extension lint"
	@echo "  make ci          - Full CI checks (format, lint, typecheck, tests)"
	@echo "  make pre-commit  - Format + lint + typecheck (no test)"
	@echo "  make all         - Format, lint, typecheck, Python test, extension test"
	@echo ""
	@echo "  make docs-build  - Build Docusaurus docs ($(DOCS_DIR))"
	@echo "  make docs-serve  - Serve docs locally (http://localhost:3000)"
	@echo "  make clean       - Remove extension/dist, docs build, coverage, __pycache__, etc."
	@echo ""
	@echo "For more targets: make -C $(PYTHON_SDK) help"
