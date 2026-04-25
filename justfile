# Justfile at workspace root

set dotenv-load := true

PROJECT := env_var_or_default("GOOGLE_CLOUD_PROJECT", "")
DATASET := env_var_or_default("VERITY_DATASET", "")
STAGING_BUCKET := env_var_or_default("VERITY_GCS_STAGING_BUCKET", "")
SA_KEY := env_var_or_default("GOOGLE_APPLICATION_CREDENTIALS", "")

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

[doc('Bootstrap GCP environment for an example (uploads CSVs to GCS then BigQuery)')]
bootstrap-gcp name:
    @if [ -z "{{PROJECT}}" ] || [ -z "{{DATASET}}" ] || [ -z "{{STAGING_BUCKET}}" ]; then \
        echo "❌ Error: Missing GCP environment variables."; \
        echo "Please copy .env.example to .env and fill in the values."; \
        exit 1; \
    fi
    @echo "🛠️  Bootstrapping GCP environment for '{{name}}'..."
    ./examples/bootstrap_gcp.py {{name}}

# Run all tests
test: build
    cargo test --all

# Clean all targets
clean:
    cargo clean
