# Root Makefile - delegates to packages
# Run from repo root: make fmt, make test, etc.

PYTHON_SDK := packages/python-sdk

.PHONY: fmt format lint typecheck test check ci pre-commit all help

fmt format lint typecheck test check ci pre-commit all:
	$(MAKE) -C $(PYTHON_SDK) $@

help:
	@echo "SchemaX - run from repo root"
	@echo ""
	@echo "  make fmt         - Format Python code (Ruff)"
	@echo "  make format      - Same as fmt"
	@echo "  make lint        - Lint Python code (Ruff)"
	@echo "  make typecheck   - Type check (mypy)"
	@echo "  make test        - Run Python SDK tests"
	@echo "  make check       - Format/lint check only (no fix)"
	@echo "  make ci          - Full CI checks (format, lint, typecheck, test)"
	@echo "  make pre-commit  - Format + lint + typecheck (no test)"
	@echo "  make all         - Format, lint, typecheck, test"
	@echo ""
	@echo "For more targets: make -C $(PYTHON_SDK) help"
