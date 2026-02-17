#!/bin/bash
# Verity Zero-Panic Guard
# Scans production code for forbidden panic-inducing patterns.
# Test code (#[cfg(test)] modules at the bottom of files) is excluded.

set -euo pipefail

TARGET_DIRS=(
    "verity-core/src"
    "verity/src"
)

# Patterns use basic grep (no -E) to avoid escaping issues
FORBIDDEN_PATTERNS=(
    '.unwrap()'
    '.expect('
    'panic!'
)

HAS_ERRORS=0

echo "🔍 Security Audit : scanning for unsafe patterns..."

# For a given Rust file, return the line number where #[cfg(test)] starts.
# Everything from that line onward is test code and should be ignored.
# Returns a very large number if no test block is found.
get_test_boundary() {
    local file="$1"
    local line
    line=$(grep -n '#\[cfg(test)\]' "$file" | head -1 | cut -d: -f1)
    echo "${line:-999999}"
}

for dir in "${TARGET_DIRS[@]}"; do
    if [ ! -d "$dir" ]; then
        echo "⚠️  Directory '$dir' not found, skipping."
        continue
    fi

    # Collect all .rs files
    while IFS= read -r file; do
        test_boundary=$(get_test_boundary "$file")

        for pattern in "${FORBIDDEN_PATTERNS[@]}"; do
            # Use grep -F (fixed string) to avoid regex escaping issues
            # -n gives line numbers so we can filter by test boundary
            while IFS=: read -r line_num line_content; do
                # Skip if inside #[cfg(test)] block
                if [ "$line_num" -ge "$test_boundary" ]; then
                    continue
                fi

                # Skip if line has an allow-panic escape hatch
                if echo "$line_content" | grep -q '// allow-panic'; then
                    continue
                fi

                # Skip unwrap_or / unwrap_or_else / unwrap_or_default (safe patterns)
                if echo "$line_content" | grep -qF '.unwrap_or'; then
                    continue
                fi

                echo "  ❌ $file:$line_num: $line_content"
                HAS_ERRORS=1
            done < <(grep -nF "$pattern" "$file" 2>/dev/null || true)
        done
    done < <(find "$dir" -name '*.rs' -type f)
done

if [ "$HAS_ERRORS" -ne 0 ]; then
    echo ""
    echo "👉 Use error handling with '?' or 'anyhow!' instead of .unwrap()."
    exit 1
fi

echo "✅ No unsafe patterns found."
exit 0