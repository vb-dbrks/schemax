.PHONY: format lint typecheck test all install clean help

help:
	@echo "Schematic Python SDK - Development Commands"
	@echo ""
	@echo "Usage: make [target]"
	@echo ""
	@echo "Targets:"
	@echo "  format     - Format code with Ruff (auto-fix)"
	@echo "  lint       - Lint code with Ruff (auto-fix)"
	@echo "  typecheck  - Type check with mypy"
	@echo "  test       - Run tests with pytest"
	@echo "  all        - Run all checks (format, lint, typecheck, test)"
	@echo "  install    - Install package in development mode"
	@echo "  clean      - Remove build artifacts and cache"
	@echo ""

format:
	@echo "🎨 Formatting with Ruff..."
	ruff format src/ tests/

lint:
	@echo "🔍 Linting with Ruff..."
	ruff check src/ tests/ --fix

typecheck:
	@echo "🔬 Type checking with mypy..."
	mypy src/

test:
	@echo "🧪 Running tests..."
	pytest tests/ -v

all: format lint typecheck test
	@echo ""
	@echo "✅ All checks passed! Ready to commit."

install:
	@echo "📦 Installing package in development mode..."
	pip install -e ".[dev]"

clean:
	@echo "🧹 Cleaning build artifacts..."
	rm -rf build/ dist/ *.egg-info
	rm -rf .pytest_cache .mypy_cache .ruff_cache
	find . -type d -name __pycache__ -exec rm -rf {} +
	find . -type f -name "*.pyc" -delete

