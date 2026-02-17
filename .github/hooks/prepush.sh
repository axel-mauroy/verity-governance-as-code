#!/bin/sh

echo "🛡️  Verity Pre-Push Guard 🛡️"

# 1. Format Check
echo "running cargo fmt..."
cargo fmt --all -- --check
if [ $? -ne 0 ]; then
    echo "❌ Formatting failed. Run 'cargo fmt --all' and commit again."
    exit 1
fi

# 2. Clippy Check
echo "running cargo clippy..."
cargo clippy --workspace -- -D warnings
if [ $? -ne 0 ]; then
    echo "❌ Clippy failed. Fix warnings before pushing."
    exit 1
fi

# 3. Test Check
echo "running cargo test..."
cargo test --workspace
if [ $? -ne 0 ]; then
    echo "❌ Tests failed."
    exit 1
fi

echo "✅ All systems go. Pushing to remote..."
exit 0