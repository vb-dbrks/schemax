#!/usr/bin/env bash
set -euo pipefail

if [[ $# -ne 1 ]]; then
  echo "Usage: $0 <semver>"
  exit 1
fi

VERSION="$1"
if [[ ! "$VERSION" =~ ^[0-9]+\.[0-9]+\.[0-9]+$ ]]; then
  echo "Invalid version '$VERSION'. Expected semver like 0.2.6"
  exit 1
fi

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

check_contains() {
  local file="$1"
  local expected="$2"
  if ! rg -n --fixed-strings "$expected" "$file" >/dev/null; then
    echo "Version sync check failed: '$expected' not found in $file"
    exit 1
  fi
}

check_contains package.json "\"version\": \"$VERSION\""
check_contains package-lock.json "\"name\": \"schemax\","
check_contains package-lock.json "\"version\": \"$VERSION\""
check_contains packages/python-sdk/pyproject.toml "version = \"$VERSION\""
check_contains packages/python-sdk/src/schemax/__init__.py "__version__ = \"$VERSION\""
check_contains packages/python-sdk/src/schemax/cli.py "CLI_VERSION = \"$VERSION\""
check_contains packages/vscode-extension/package.json "\"version\": \"$VERSION\""
check_contains docs/schemax/package.json "\"version\": \"$VERSION\""
check_contains docs/schemax/package-lock.json "\"version\": \"$VERSION\""
check_contains packages/vscode-extension/src/extension.ts "const MIN_SUPPORTED_CLI_VERSION = \"$VERSION\";"
check_contains contracts/cli-envelopes/runtime_info.success.json "\"cliVersion\": \"$VERSION\""
check_contains packages/python-sdk/CHANGELOG.md "## [$VERSION] - "
check_contains packages/vscode-extension/CHANGELOG.md "## [$VERSION] - "

echo "Version sync OK for $VERSION"
