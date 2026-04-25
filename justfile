# Justfile at workspace root

set dotenv-load := true

PROJECT := "dealk-data-dev"
DATASET := "verity_dev"
SA_KEY  := "$(pwd)/verity-bigquery/sa-key.json"

# Build all binaries (Core, CLI, BigQuery Connector)
build:
    @echo "🔨 Building Verity suite..."
    cargo build

# Run an example pipeline in BigQuery
# Usage: just example basic_rag_pipeline bigquery
# Usage: just example ml_pipeline datafusion
example name engine="datafusion": build
    @if [ ! -d "examples/{{name}}" ]; then echo "❌ Example '{{name}}' not found."; exit 1; fi
    @echo "🚀 Running example '{{name}}' with engine '{{engine}}'..."
    @export PATH="$(pwd)/target/debug:$PATH"; \
    export VERITY_ENGINE="{{engine}}"; \
    export GOOGLE_APPLICATION_CREDENTIALS="{{SA_KEY}}"; \
    export GOOGLE_CLOUD_PROJECT="{{PROJECT}}"; \
    export VERITY_DATASET="{{DATASET}}"; \
    cd examples/{{name}} && verity run

# Run all tests
test: build
    cargo test --all

# Clean all targets
clean:
    cargo clean
