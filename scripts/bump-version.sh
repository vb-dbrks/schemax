#!/usr/bin/env bash
set -euo pipefail

if [[ $# -lt 1 || $# -gt 2 ]]; then
  echo "Usage: $0 <semver> [YYYY-MM-DD]"
  exit 1
fi

VERSION="$1"
RELEASE_DATE="${2:-$(date +%F)}"

if [[ ! "$VERSION" =~ ^[0-9]+\.[0-9]+\.[0-9]+$ ]]; then
  echo "Invalid version '$VERSION'. Expected semver like 0.2.6"
  exit 1
fi

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

echo "Bumping SchemaX release version to $VERSION (date: $RELEASE_DATE)"

# Root package metadata
perl -i -pe 's/^(\s*"version":\s*")[^"]+(",\s*)$/$1'"$VERSION"'$2/' package.json
perl -0pi -e 's/"name": "schemax",\n  "version": "[^"]+"/"name": "schemax",\n  "version": "'"$VERSION"'"/' package-lock.json
perl -0pi -e 's/"name": "schemax",\n\s+"version": "[^"]+",\n\s+"workspaces"/"name": "schemax",\n      "version": "'"$VERSION"'",\n      "workspaces"/' package-lock.json

# Python SDK
perl -i -pe 's/^(version = ")[^"]+(")$/\1'"$VERSION"'\2/' packages/python-sdk/pyproject.toml
perl -i -pe 's/^(__version__ = ")[^"]+(")$/\1'"$VERSION"'\2/' packages/python-sdk/src/schemax/__init__.py
perl -i -pe 's/^(CLI_VERSION = ")[^"]+(")$/\1'"$VERSION"'\2/' packages/python-sdk/src/schemax/cli.py
perl -i -pe 's/^(        schemax_version: str = ")[^"]+(")/\1'"$VERSION"'\2/' packages/python-sdk/src/schemax/core/deployment.py
perl -i -pe 's/(schemax_version=")[^"]+(")/\1'"$VERSION"'\2/g' \
  packages/python-sdk/src/schemax/commands/apply.py \
  packages/python-sdk/src/schemax/commands/rollback.py \
  packages/python-sdk/src/schemax/providers/unity/provider.py

# Extension + docs packages
perl -i -pe 's/^(\s*"version":\s*")[^"]+(",\s*)$/$1'"$VERSION"'$2/' packages/vscode-extension/package.json
perl -0pi -e 's/"packages\/vscode-extension": \{\n\s+"name": "schemax-vscode",\n\s+"version": "[^"]+"/"packages\/vscode-extension": {\n      "name": "schemax-vscode",\n      "version": "'"$VERSION"'"/' package-lock.json
perl -i -pe 's/^(const MIN_SUPPORTED_CLI_VERSION = ")[^"]+(";\s*)$/\1'"$VERSION"'\2/' packages/vscode-extension/src/extension.ts
perl -i -pe "s/'0\\.2\\.[0-9]+'/'$VERSION'/g if /validateRuntimeInfo\\(|\\s+'1'/" packages/vscode-extension/tests/unit/runtimeCompatibility.test.ts

perl -i -pe 's/^(\s*"version":\s*")[^"]+(",\s*)$/$1'"$VERSION"'$2/' docs/schemax/package.json
perl -0pi -e 's/"name": "schemax-docs",\n  "version": "[^"]+"/"name": "schemax-docs",\n  "version": "'"$VERSION"'"/' docs/schemax/package-lock.json
perl -0pi -e 's/"name": "schemax-docs",\n\s+"version": "[^"]+",\n\s+"dependencies"/"name": "schemax-docs",\n      "version": "'"$VERSION"'",\n      "dependencies"/' docs/schemax/package-lock.json

# CLI contract fixture
perl -0pi -e 's/"cliVersion": "[^"]+"/"cliVersion": "'"$VERSION"'"/' contracts/cli-envelopes/runtime_info.success.json

python_sdk_changelog="packages/python-sdk/CHANGELOG.md"
vscode_changelog="packages/vscode-extension/CHANGELOG.md"

if ! grep -q "^## \[$VERSION\] - " "$python_sdk_changelog"; then
  tmp_file="$(mktemp)"
  {
    echo "# Changelog"
    echo
    echo "## [$VERSION] - $RELEASE_DATE"
    echo
    echo "### Added"
    echo
    echo "- TBA"
    echo
    tail -n +3 "$python_sdk_changelog"
  } > "$tmp_file"
  mv "$tmp_file" "$python_sdk_changelog"
fi

if ! grep -q "^## \[$VERSION\] - " "$vscode_changelog"; then
  tmp_file="$(mktemp)"
  {
    echo "# Changelog"
    echo
    echo "## [$VERSION] - $RELEASE_DATE"
    echo
    echo "### Added"
    echo
    echo "- TBA"
    echo
    tail -n +3 "$vscode_changelog"
  } > "$tmp_file"
  mv "$tmp_file" "$vscode_changelog"
fi

echo "Done. Run: scripts/check-version-sync.sh $VERSION"
