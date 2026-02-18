#!/bin/bash
# Verity Local CI - "The Gauntlet"
set -euo pipefail # -u: erreur si variable non définie, -o pipefail: capture les erreurs dans les pipes

# Couleurs pour le feedback DX
GREEN='\033[0;32m'
RED='\033[0;31m'
NC='\033[0m' # No Color

echo -e "${GREEN}🚀 Starting Verity Heavy Duty Verification...${NC}"

# Protection : S'assurer qu'on ne run pas ça n'importe où
cd "$(git rev-parse --show-toplevel)"

# 1. Verification des outils indispensables
for cmd in cargo-deny cargo-audit; do
    if ! command -v $cmd &> /dev/null; then
        echo -e "${RED}❌ Error: $cmd is not installed.${NC} Run: cargo install $cmd"
        exit 1
    fi
done

echo "--- 1️⃣ Quality Gate ---"
cargo fmt --all -- --check
# Utilisation de --all-targets pour clippy pour inclure les tests et benchmarks
cargo clippy --workspace --all-targets -- -D warnings

echo "--- 2️⃣ Security Gate ---"
cargo deny check
cargo audit
# On réactive ton hook de sécurité
chmod +x .github/hooks/deny_unsecure.sh
./.github/hooks/deny_unsecure.sh

echo "--- 3️⃣ Logic Gate ---"
# Utilisation de nextest si disponible pour plus de rapidité, sinon cargo test
if command -v cargo-nextest &> /dev/null; then
    cargo nextest run --workspace
else
    cargo test --workspace
fi

echo "--- 4️⃣ E2E & Materialization Gate ---"
# On utilise le profil dev (ou un profil custom 'ci') pour gagner du temps de compilation
# sauf si tu veux spécifiquement tester la perf des embeddings
echo "🔨 Compiling Verity CLI..."
cargo build --bin verity

VERITY_BIN="$(pwd)/target/debug/verity"

# Exécution des pipelines d'exemple avec injection d'une DB temporaire
# pour éviter de corrompre tes données de dev locales
export VERITY_DATABASE_PATH="/tmp/verity_test_$(date +%s).db"

for example in basic_rag_pipeline ml_pipeline; do
    echo -e "Testing example: ${GREEN}$example${NC}..."
    (cd "examples/$example" && "$VERITY_BIN" run)
done

echo -e "${GREEN}✅ All systems go. Ready for merge.${NC}"