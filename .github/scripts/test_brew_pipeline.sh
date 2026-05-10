#!/usr/bin/env bash
# .github/scripts/test_brew_pipeline.sh
#
# Simulates the CI brew pipeline steps locally so you can iterate
# without pushing a new tag every time.
#
# Usage:
#   ./.github/scripts/test_brew_pipeline.sh [VERSION]
#   ./.github/scripts/test_brew_pipeline.sh 0.2.8
#
# Requirements:
#   - brew (Homebrew)
#   - uv
#   - A real checksums.txt in release_assets/ (downloaded from a real GitHub release),
#     OR pass --fake to use a stub checksum for syntax testing only.
#
set -euo pipefail

VERSION="${1:-0.2.8}"
FAKE_MODE=false
if [[ "${2:-}" == "--fake" ]]; then
    FAKE_MODE=true
fi

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"

cd "${REPO_ROOT}"

echo ""
echo "════════════════════════════════════════════════════"
echo "  🧪 Verity Brew Pipeline — Local Test Harness"
echo "  VERSION: v${VERSION}"
echo "  FAKE_MODE: ${FAKE_MODE}"
echo "════════════════════════════════════════════════════"
echo ""

# --- Step 1: Prepare fake checksum if needed ---
if [[ "${FAKE_MODE}" == "true" ]]; then
    echo "⚠️  Fake mode: generating stub checksums for syntax testing"
    mkdir -p release_assets
    FAKE_HASH="aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
    printf "%s  verity-macos-universal\n"         "${FAKE_HASH}" > release_assets/checksums.txt
    printf "%s  verity-bigquery-macos-universal\n" "${FAKE_HASH}" >> release_assets/checksums.txt
    echo "   Written release_assets/checksums.txt (stub)"
else
    if [[ ! -f "release_assets/checksums.txt" ]]; then
        echo "❌ release_assets/checksums.txt not found."
        echo "   Either:"
        echo "     1. Download it from https://github.com/axel-mauroy/verity-governance-as-code/releases"
        echo "     2. Or pass --fake as second argument for syntax-only testing"
        exit 1
    fi
fi

# --- Step 2: Run the Python update script ---
echo "🐍 Step 1/2 — Running update_brew.py..."
export GITHUB_REF_NAME="v${VERSION}"
# Point to a temp file instead of the real GITHUB_ENV
TEMP_ENV=$(mktemp)
export GITHUB_ENV="${TEMP_ENV}"

uv run .github/scripts/update_brew.py

echo "   GITHUB_ENV output:"
cat "${TEMP_ENV}"
rm -f "${TEMP_ENV}"
echo ""

# --- Step 3: brew audit ---
echo "🍺 Step 2/2 — Running brew audit..."
# Register local repo as a tap so brew audit can find formulas by name
# (both macOS and Linux require formula to be discoverable by name, not path)
brew tap-new axel-mauroy/verity-governance-as-code 2>/dev/null || true
HOMEBREW_TAP_DIR=$(brew --repository axel-mauroy/verity-governance-as-code)
cp Formula/verity.rb Formula/verity-bigquery.rb "${HOMEBREW_TAP_DIR}/Formula/"
brew audit --strict --new axel-mauroy/verity-governance-as-code/verity
brew audit --strict --new axel-mauroy/verity-governance-as-code/verity-bigquery

echo ""
echo "✅ All pipeline steps passed locally!"
echo "   You can now commit and push safely."
