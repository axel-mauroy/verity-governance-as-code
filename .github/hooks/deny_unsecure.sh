#!/bin/bash
# Verity Zero-Panic Guard
# Scans production code for forbidden panic-inducing patterns.
# Test code (#[cfg(test)], #[allow(clippy::unwrap_used)]) is excluded.

set -euo pipefail

TARGET_DIRS=(
    "verity-core/src"
    "verity/src"
)

FORBIDDEN_PATTERNS=(
    '\.unwrap\(\)'
    '\.expect\('
    'panic!'
)

HAS_ERRORS=0

echo "🔍 Security Audit : scanning for unsafe patterns..."

for dir in "${TARGET_DIRS[@]}"; do
    if [ ! -d "$dir" ]; then
        echo "⚠️  Directory '$dir' not found, skipping."
        continue
    fi

    for pattern in "${FORBIDDEN_PATTERNS[@]}"; do
        # Grep production Rust files, excluding test modules and test helpers
        MATCHES=$(grep -rnE "$pattern" "$dir" \
            --include='*.rs' \
            --exclude-dir=tests \
            --exclude-dir=examples \
            | grep -v '#\[cfg(test)\]' \
            | grep -v '#\[allow(clippy::unwrap_used' \
            | grep -v '#\[allow(clippy::expect_used' \
            | grep -v '// allow-panic' \
            || true)

        if [ -n "$MATCHES" ]; then
            echo ""
            echo "❌ Pattern '$pattern' found in $dir:"
            echo "$MATCHES"
            HAS_ERRORS=1
        fi
    done
done

if [ "$HAS_ERRORS" -ne 0 ]; then
    echo ""
    echo "👉 Use error handling with '?' or 'anyhow!' instead of .unwrap()."
    exit 1
fi

echo "✅ No unsafe patterns found."
exit 0