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
# 1. Code Formatting Check
# ==========================================
echo "1️⃣  Checking Python code formatting..."
echo "--------------------------------------"

if command -v black &> /dev/null; then
    if black packages/python-sdk/src/ --check --line-length 100 && \
       black examples/python-scripts/ --check --line-length 100; then
        echo -e "${GREEN}✓${NC} Python code formatting is correct"
    else
        echo -e "${RED}✗${NC} Python code formatting check failed"
        echo ""
        echo "To fix, run:"
        echo "  cd packages/python-sdk && black src/ --line-length 100"
        echo "  cd examples/python-scripts && black . --line-length 100"
        FAILURES=$((FAILURES + 1))
    fi
else
    echo -e "${YELLOW}⚠${NC}  Black not installed, skipping formatting check"
    echo "Install with: pip install black"
fi

echo ""

# ==========================================
# 2. Smoke Tests
# ==========================================
echo "2️⃣  Running smoke tests..."
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

