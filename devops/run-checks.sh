#!/bin/bash

# DevOps Quality Checks Script
# Runs code formatting checks and smoke tests locally

set -e

SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"

cd "$PROJECT_ROOT"

echo "======================================"
echo "🔍 SchemaX Quality Checks"
echo "======================================"
echo ""

# Color codes
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Track failures
FAILURES=0

# ==========================================
# 1. Code Formatting Check (Ruff)
# ==========================================
echo "1️⃣  Checking Python code formatting (Ruff)..."
echo "--------------------------------------"

if command -v ruff &> /dev/null; then
    if (cd packages/python-sdk && ruff format --check src/ tests/) && \
       (cd examples/python-scripts && ruff format --check .); then
        echo -e "${GREEN}✓${NC} Python code formatting is correct"
    else
        echo -e "${RED}✗${NC} Python code formatting check failed"
        echo ""
        echo "To fix, run:"
        echo "  cd packages/python-sdk && ruff format src/ tests/"
        echo "  cd examples/python-scripts && ruff format ."
        FAILURES=$((FAILURES + 1))
    fi
else
    echo -e "${YELLOW}⚠${NC}  Ruff not installed, skipping formatting check"
    echo "Install with: pip install ruff"
fi

echo ""

# ==========================================
# 2. Python Linting Check (Ruff)
# ==========================================
echo "2️⃣  Linting Python code (Ruff)..."
echo "--------------------------------------"

if command -v ruff &> /dev/null; then
    if (cd packages/python-sdk && ruff check src/ tests/); then
        echo -e "${GREEN}✓${NC} Python linting passed"
    else
        echo -e "${RED}✗${NC} Python linting failed"
        echo ""
        echo "To fix, run:"
        echo "  cd packages/python-sdk && ruff check src/ tests/ --fix"
        FAILURES=$((FAILURES + 1))
    fi
else
    echo -e "${YELLOW}⚠${NC}  Ruff not installed, skipping linting check"
    echo "Install with: pip install ruff"
fi

echo ""

# ==========================================
# 3. Python SDK Tests (pytest)
# ==========================================
echo "3️⃣  Running Python SDK tests (pytest)..."
echo "--------------------------------------"

if command -v pytest &> /dev/null; then
    if (cd packages/python-sdk && pytest tests/ -q); then
        echo -e "${GREEN}✓${NC} Python SDK tests passed (138 tests)"
    else
        echo -e "${RED}✗${NC} Python SDK tests failed"
        echo ""
        echo "To debug, run:"
        echo "  cd packages/python-sdk && pytest tests/ -v"
        FAILURES=$((FAILURES + 1))
    fi
else
    echo -e "${YELLOW}⚠${NC}  Pytest not installed, skipping SDK tests"
    echo "Install with: pip install pytest or uv pip install -e '.[dev]'"
fi

echo ""

# ==========================================
# 4. VS Code Extension Tests (Jest)
# ==========================================
echo "4️⃣  Running VS Code Extension tests (Jest)..."
echo "--------------------------------------"

if command -v npm &> /dev/null; then
    if (cd packages/vscode-extension && npm test); then
        echo -e "${GREEN}✓${NC} VS Code Extension tests passed"
    else
        echo -e "${RED}✗${NC} VS Code Extension tests failed"
        echo ""
        echo "To debug, run:"
        echo "  cd packages/vscode-extension && npm test -- --verbose"
        FAILURES=$((FAILURES + 1))
    fi
else
    echo -e "${YELLOW}⚠${NC}  npm not installed, skipping extension tests"
    echo "Install Node.js and npm from: https://nodejs.org/"
fi

echo ""

# ==========================================
# 5. Smoke Tests
# ==========================================
echo "5️⃣  Running smoke tests..."
echo "--------------------------------------"

if [ -f "./scripts/smoke-test.sh" ]; then
    if ./scripts/smoke-test.sh; then
        echo -e "${GREEN}✓${NC} Smoke tests passed"
    else
        echo -e "${RED}✗${NC} Smoke tests failed"
        FAILURES=$((FAILURES + 1))
    fi
else
    echo -e "${RED}✗${NC} Smoke test script not found"
    FAILURES=$((FAILURES + 1))
fi

echo ""

# ==========================================
# Summary
# ==========================================
echo "======================================"
echo "📊 Summary"
echo "======================================"

if [ $FAILURES -eq 0 ]; then
    echo -e "${GREEN}✅ All checks passed!${NC}"
    echo ""
    echo "Your code is ready to commit."
    exit 0
else
    echo -e "${RED}❌ $FAILURES check(s) failed${NC}"
    echo ""
    echo "Please fix the issues above before committing."
    exit 1
fi

