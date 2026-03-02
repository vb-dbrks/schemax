.PHONY: format fmt lint typecheck pylint test integration integration-coverage-upload coverage-gate-ddl-parser all check ci pre-commit install clean help

PYTEST_WORKERS ?= auto

help:
	@echo "SchemaX Python SDK - Development Commands"
	@echo ""
	@echo "Usage: make [target]"
	@echo ""
	@echo "Targets:"
	@echo "  format      - Format code with Ruff (auto-fix)"
	@echo "  fmt         - Alias for format"
	@echo "  lint        - Lint code with Ruff (auto-fix)"
	@echo "  typecheck   - Type check with mypy"
	@echo "  pylint      - Pylint (complexity, design, Databricks/Unity checks)"
	@echo "  test        - Run tests with pytest"
	@echo "  integration - Run all integration tests (including live Databricks; set env for live)"
	@echo "  integration-coverage-upload - Run integration coverage locally and upload to Codecov"
	@echo "  coverage-gate-ddl-parser - Enforce >=90% coverage for unity ddl_parser.py"
	@echo "  all         - Run all checks (format, lint, typecheck, test)"
	@echo "  check       - Run format/lint checks without modifying files (like CI)"
	@echo "  ci          - Run exact CI checks (format check + lint check + typecheck + test)"
	@echo "  pre-commit  - Format, lint, typecheck (recommended before git commit)"
	@echo "  install     - Install package in development mode"
	@echo "  clean       - Remove build artifacts and cache"
	@echo ""

format:
	@echo "🎨 Formatting with Ruff..."
	uv run ruff format src/ tests/

# Alias: fmt = format + lint (auto-fix) + pylint
fmt: format lint pylint

lint:
	@echo "🔍 Linting with Ruff..."
	uv run ruff check src/ tests/ --fix

typecheck:
	@echo "🔬 Type checking with mypy..."
	uv run mypy src/

pylint:
	@echo "🔎 Pylint (complexity, design, Databricks/Unity)..."
	uv run pylint -j 0 src/ tests/

test:
	@echo "🧪 Running tests..."
	uv run pytest tests/ -v -n $(PYTEST_WORKERS)

# Run all integration tests including live ones (Databricks).
# Live tests run only when SCHEMAX_RUN_LIVE_COMMAND_TESTS=1 (and optionally
# SCHEMAX_RUN_LIVE_IMPORT_TESTS=1). They still require profile, warehouse-id, etc.
integration:
	@echo "🧪 Running integration tests with coverage (including live when env is set)..."
	SCHEMAX_RUN_LIVE_COMMAND_TESTS=1 SCHEMAX_RUN_LIVE_IMPORT_TESTS=1 uv run pytest tests/integration/ -v -n $(PYTEST_WORKERS) \
		--cov=src/schemax \
		--cov-report=xml:coverage-integration.xml \
		--cov-report=term-missing
	@$(MAKE) coverage-gate-ddl-parser
	@if [ -n "$$CODECOV_TOKEN" ]; then \
		echo "📤 Uploading integration coverage to Codecov..."; \
		if [ ! -x "./codecov" ]; then \
			curl -Os https://uploader.codecov.io/latest/macos/codecov; \
			chmod +x ./codecov; \
		fi; \
		./codecov \
			-t "$$CODECOV_TOKEN" \
			-f coverage-integration.xml \
			-F integration \
			-n "local-integration" \
			-C "$$(git rev-parse HEAD)" \
			-B "$$(git rev-parse --abbrev-ref HEAD)"; \
	else \
		echo "ℹ️  CODECOV_TOKEN not set; skipped coverage upload."; \
	fi

integration-coverage-upload:
	@echo "📊 Running integration coverage and uploading to Codecov..."
	@test -n "$$CODECOV_TOKEN" || (echo "❌ CODECOV_TOKEN is required"; exit 1)
	uv run pytest tests/integration -n $(PYTEST_WORKERS) \
		--cov=src/schemax \
		--cov-report=xml:coverage-integration.xml \
		--cov-report=term-missing
	@$(MAKE) coverage-gate-ddl-parser
	@if [ ! -x "./codecov" ]; then \
		curl -Os https://uploader.codecov.io/latest/macos/codecov; \
		chmod +x ./codecov; \
	fi
	./codecov \
		-t "$$CODECOV_TOKEN" \
		-f coverage-integration.xml \
		-F integration \
		-n "local-integration" \
		-C "$$(git rev-parse HEAD)" \
		-B "$$(git rev-parse --abbrev-ref HEAD)"

coverage-gate-ddl-parser:
	@echo "🎯 Enforcing module coverage gate for unity ddl_parser.py (>=90%)..."
	uv run coverage report \
		--include=src/schemax/providers/unity/ddl_parser.py \
		--fail-under=90

all: format lint typecheck pylint test
	@echo ""
	@echo "✅ All checks passed! Ready to commit."

check:
	@echo "🔍 Running format check (no modifications)..."
	@uv run ruff format --check src/ tests/
	@echo "🔍 Running lint check (no modifications)..."
	@uv run ruff check src/ tests/
	@echo "🔎 Running pylint..."
	@uv run pylint -j 0 src/ tests/
	@echo ""
	@echo "✅ Format, lint, and pylint checks passed!"

ci: check typecheck test
	@echo ""
	@echo "✅ All CI checks passed!"

pre-commit: format lint typecheck pylint
	@echo ""
	@echo "✅ Pre-commit checks complete! Ready to commit."
	@echo ""
	@echo "💡 Tip: Run 'git add -A && git commit -S -m \"your message\"'"

install:
	@echo "📦 Installing package in development mode..."
	uv pip install -e ".[dev]"

clean:
	@echo "🧹 Cleaning build artifacts..."
	rm -rf build/ dist/ *.egg-info
	rm -rf .pytest_cache .mypy_cache .ruff_cache
	find . -type d -name __pycache__ -exec rm -rf {} +
	find . -type f -name "*.pyc" -delete
