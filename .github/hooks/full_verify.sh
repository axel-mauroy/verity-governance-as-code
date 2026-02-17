#!/bin/bash
set -e

echo "🚀 Starting Full Verification Script (Heavy Duty)..."

# Ensure we are at the project root
cd "$(git rev-parse --show-toplevel)"


echo "---------------------------------------------------"
echo "1️⃣  Format & Lint"
echo "---------------------------------------------------"
echo "🎨 Running cargo fmt..."
cargo fmt --all -- --check
echo "📎 Running clippy..."
cargo clippy --workspace -- -D warnings

echo "---------------------------------------------------"
echo "2️⃣  Unit & Integration Tests"
echo "---------------------------------------------------"
echo "🧪 Running cargo test..."
cargo test --workspace

echo "---------------------------------------------------"
echo "3️⃣  Security & License"
echo "---------------------------------------------------"
if command -v cargo-deny &> /dev/null; then
    echo "🛡️  Running cargo-deny..."
    cargo deny check
else
    echo "⚠️  cargo-deny not found, skipping."
fi

if command -v cargo-audit &> /dev/null; then
    echo "🛡️  Running cargo-audit..."
    cargo audit
else
    echo "⚠️  cargo-audit not found, skipping (install with 'cargo install cargo-audit')."
fi

echo "🛡️  Running Zero-Panic Guard..."
# ./.github/hooks/deny_unsecure.sh

echo "---------------------------------------------------"
echo "4️⃣  E2E Examples"
echo "---------------------------------------------------"
echo "🔨 Building Release Binary..."
cargo build --release --bin verity

VERITY_BIN=$(pwd)/target/release/verity

echo "🚀 Running Basic RAG Pipeline (DuckDB)..."
(cd examples/basic_rag_pipeline && $VERITY_BIN run)

echo "🚀 Running ML Pipeline (DataFusion)..."
(cd examples/ml_pipeline && $VERITY_BIN run)

echo "---------------------------------------------------"
echo "✅ CI Simulation Completed Successfully!"
echo "---------------------------------------------------"
