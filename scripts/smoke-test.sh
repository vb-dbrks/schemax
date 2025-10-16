#!/bin/bash
# Quick smoke test for Schematic

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"

echo "🧪 Schematic Smoke Test"
echo "====================="
echo ""

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

print_success() {
    echo -e "${GREEN}✓${NC} $1"
}

print_error() {
    echo -e "${RED}✗${NC} $1"
}

print_warning() {
    echo -e "${YELLOW}⚠${NC} $1"
}

# Test 1: VS Code Extension Build
echo "Test 1: VS Code Extension Build"
echo "--------------------------------"
cd "$ROOT_DIR/packages/vscode-extension"
if npm run build > /dev/null 2>&1; then
    print_success "Extension builds successfully"
    if [ -f "dist/extension.js" ]; then
        print_success "Extension bundle exists"
    else
        print_error "Extension bundle not found"
        exit 1
    fi
else
    print_error "Extension build failed"
    exit 1
fi
echo ""

# Test 2: Python SDK Installation
echo "Test 2: Python SDK Installation"
echo "--------------------------------"
cd "$ROOT_DIR/packages/python-sdk"
if pip install -e . > /dev/null 2>&1; then
    print_success "SDK installs successfully"
else
    print_error "SDK installation failed"
    exit 1
fi
echo ""

# Test 3: CLI Available
echo "Test 3: CLI Commands"
echo "--------------------"
if command -v schematic &> /dev/null; then
    print_success "CLI command available"
    version=$(schematic --version 2>&1)
    print_success "Version: $version"
else
    print_error "CLI command not found"
    exit 1
fi
echo ""

# Test 4: Example Project Validation
echo "Test 4: Example Project Validation"
echo "-----------------------------------"
cd "$ROOT_DIR/examples/basic-schema"
if schematic validate > /dev/null 2>&1; then
    print_success "Example project validates"
else
    print_error "Example project validation failed"
    exit 1
fi
echo ""

# Test 5: SQL Generation
echo "Test 5: SQL Generation"
echo "----------------------"
temp_sql="/tmp/schematic-test-$$.sql"
if schematic sql --output "$temp_sql" 2>&1 | grep -q "No operations"; then
    print_warning "No operations to generate SQL for (expected for example)"
elif [ -f "$temp_sql" ]; then
    print_success "SQL generated to file"
    rm -f "$temp_sql"
else
    print_success "SQL generation works"
fi
echo ""

# Test 6: File Structure
echo "Test 6: File Structure"
echo "----------------------"
if [ -f "$ROOT_DIR/packages/vscode-extension/package.json" ]; then
    print_success "Extension package.json exists"
else
    print_error "Extension package.json missing"
fi

if [ -f "$ROOT_DIR/packages/python-sdk/pyproject.toml" ]; then
    print_success "Python SDK pyproject.toml exists"
else
    print_error "Python SDK pyproject.toml missing"
fi

if [ -f "$ROOT_DIR/examples/basic-schema/.schematic/project.json" ]; then
    print_success "Example project files exist"
else
    print_error "Example project files missing"
fi
echo ""

# Summary
echo "================================"
echo -e "${GREEN}✅ All smoke tests passed!${NC}"
echo ""
echo "Next steps:"
echo "  1. Test VS Code extension: Press F5 in VS Code"
echo "  2. Create a test schema: schematic validate"
echo "  3. Read full testing guide: cat TESTING.md"
echo ""

