# verity-governance-as-code

> **The Governance-First Data Transformation Engine.**  
> *Built in Rust. Compliance as Code. Zero-Trust Compilation.*

Verity addresses the structural flaws of modern data engineering in the age of RAG. In an era where a pipeline error means a PII leak into a Vector Store, "optional documentation" is no longer acceptable.

**Verity refuses to compile if governance guidelines are not met.**

---

## ✨ Key Features

| Feature | Description |
|---|---|
| **Zero-Trust Compilation** | Governance violations are hard compile errors — no data is written before policies pass |
| **Auto PII Masking** | Columns tagged `policy: hash/redact/mask_email` are automatically wrapped at compile time |
| **Parallel DAG Execution** | Kahn's topological sort groups independent models for concurrent `tokio` execution |
| **Auto-Schema Propagation** | Undocumented columns are detected and added to `schema.yml` automatically |
| **Source Generation** | `verity generate` scans `data/` and creates `models/sources.yaml` with smart merge |
| **Data Quality Tests** | `unique`, `not_null`, `row_count_anomaly`, `z_score_anomaly` run post-materialization |
| **Strict vs. Dev Modes** | `strict: true` blocks in CI/Prod; `strict: false` warns but doesn't block in Dev |
| **Data Catalog** | `verity docs` generates a JSON-LD (DCAT + PROV-O) catalog from the manifest |
| **Single Binary** | Pure Rust — `cargo install`, no Python venv, no cold start |

---

## 🛠 Quick Start

**Prerequisites:** Rust stable (2024 edition)

```bash
# Install
cargo install --path verity

# Run the full pipeline
verity run

# Run a single model
verity run --select stg_users

# Static Data Lineage Analysis (pre-flight checks for PII leaks)
verity lineage --check

# Strict mode (CI/Prod)
VERITY_STRICT=true verity run

# Scan data/ and generate/merge sources.yaml
verity generate --owner "data_team" --pii

# Generate data catalog
verity docs

# Ad-hoc SQL query
verity query "SELECT * FROM stg_users LIMIT 5"

# Clean build artifacts
verity clean
```

---

## 📂 Examples

| Example | What it demonstrates |
|---|---|
| [`basic_rag_pipeline`](examples/basic_rag_pipeline/) | Multi-domain pipeline (HR, Supply Chain, Compliance) — PII masking into staging/intermediate/marts |
| [`ml_pipeline`](examples/ml_pipeline/) | Churn prediction — versioned feature store, prediction drift monitoring, security level downgrade |

---

## 📚 Documentation

| Document | Description |
|---|---|
| [PRD](docs/PRD.md) | Product Requirements — goals, features, success metrics |
| [Implementation Guide](docs/IMPLEMENTATION_GUIDE.md) | Pipeline lifecycle, module map, config reference, extension how-tos |
| [ADRs](docs/adr/) | Architecture Decision Records — why key design choices were made |
| [Sources Generation](docs/sources_generation.md) | `verity generate` smart-merge strategy |
| [Contributing](CONTRIBUTING.md) | Setup, quality gates, architecture rules, PR process |

---

## 📝 Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md) for local setup, git workflow, quality gates (format → security → zero-panic → tests), and the PR process.

---

**Verity** — *Because compliance shouldn't be optional.*