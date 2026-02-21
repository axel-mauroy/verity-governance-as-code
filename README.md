# verity-governance-as-code
Verity is a modern framework for managing data governance and compliance as code.

> **The Governance-First Data Transformation Engine.**
> *Built in Rust. Compliance as Code. Zero-Trust Compilation.*

---

## 🚀 Mission

Verity addresses the structural flaws of modern data engineering in the age of RAG (Retrieval-Augmented Generation).
In an era where a pipeline error means a PII leak into a Vector Store, "optional documentation" is no longer acceptable.

**Verity refuses to compile if governance guidelines are not met.**

## 🧠 Core Philosophy: Zero-Trust Compilation

We apply the **"Shift-Left"** security principle to Data Governance.

### 1. Governance as Code
Metadata (Owner, Privacy Tags, Security Level) is part of the pipeline contract. PII masking is **automatically applied** at compile time — no manual SQL changes required.
-   **PII column detected?** → Automatically hashed (SHA256) or redacted via `PolicyRewriter`.
-   **Unversioned model?** → Compile Error.
-   **Missing schema contract?** → Auto-generated, flagged for review.

### 2. Atomic & Safe
-   **Atomic I/O**: All critical file operations (State, SQL Artifacts, Catalog) use **atomic writes**. Intermediate states never corrupt your data platform.
-   **Fail-Fast**: The pipeline crashes immediately upon policy violation (unless explicitly bypassed in Dev mode).

### 3. Zero Overhead
Single binary written in Rust. No Python `venv` hell. No "Cold Start".

## ✨ Key Features

| Feature | Description |
|---------|-------------|
| **Dual SQL Engine** | Supports **DuckDB** (default) and **Apache DataFusion** — selectable via config |
| **Parallel DAG Execution** | Independent layers execute concurrently via `tokio` for maximum throughput |
| **Auto PII Masking** | Columns tagged with `policy: hash` are automatically wrapped in `SHA256()` at compile time |
| **Auto-Schema Propagation** | Undocumented columns are detected and added to `schema.yml` automatically |
| **Source Generation** | `verity generate` scans data directories and creates `models/sources.yaml` with smart merge |
| **Data Quality Tests** | `unique`, `not_null`, and custom tests run after each model materialization |
| **Anomaly Detection** | Row count deviation checks (`row_count_anomaly`) embedded in post-flight compliance |
| **Strict vs. Dev Modes** | `strict: true` enforces governance in CI/Prod; `strict: false` warns but doesn't block in Dev |
| **Data Catalog** | `verity docs` generates HTML/JSON documentation from the manifest |

## 🏗 Architecture

Verity follows a **Hexagonal Architecture** (Ports & Adapters):

```
┌──────────────────────────────────────────────────────┐
│                    verity (CLI)                       │
│  run, clean, generate, docs, query, inspect           │
├──────────────────────────────────────────────────────┤
│                  verity-core                          │
│                                                      │
│  ┌─────────────────────────────────────────────────┐ │
│  │  Application Layer                              │ │
│  │  Pipeline Orchestrator, Materializer,           │ │
│  │  Validation Engine, Catalog Generator           │ │
│  ├─────────────────────────────────────────────────┤ │
│  │  Domain Layer (Pure Logic, zero I/O)            │ │
│  │  ┌──────────────┬──────────────┬──────────────┐ │ │
│  │  │  Governance  │    Graph     │   Project    │ │ │
│  │  │  Rewriter    │    Solver    │   Manifest   │ │ │
│  │  │  PII Scanner │    DAG       │   Lifecycle  │ │ │
│  │  └──────────────┴──────────────┴──────────────┘ │ │
│  ├─────────────────────────────────────────────────┤ │
│  │  Ports (Trait Interfaces)                       │ │
│  │  Connector, ManifestLoader, TemplateEngine,     │ │
│  │  SchemaSource                                   │ │
│  ├─────────────────────────────────────────────────┤ │
│  │  Infrastructure (Adapters)                      │ │
│  │  DuckDB + DataFusion Connectors,                │ │
│  │  Jinja Renderer, Config Loader, Atomic FS       │ │
│  └─────────────────────────────────────────────────┘ │
└──────────────────────────────────────────────────────┘
```

### Module Breakdown

| Module | Layer | Responsibility |
|--------|-------|----------------|
| `ports::connector` | Ports | Database abstraction trait (`execute`, `fetch_columns`, `register_source`, `materialize`, `query_scalar`) |
| `domain::governance` | Domain | `PolicyRewriter` (PII masking), `SecurityLevel`, `PiiScanner` |
| `domain::graph` | Domain | `GraphSolver` — DAG resolution and layer-based execution planning |
| `domain::project` | Domain | `Manifest`, `ManifestNode`, `NodeConfig`, `Engine` enum, lifecycle management |
| `domain::compliance` | Domain | `RowCountCheck`, anomaly detection, post-flight checks |
| `application::pipeline` | Application | Pipeline orchestration: Compile → Govern → Materialize → Validate |
| `application::materialization` | Application | `Materializer` — engine-agnostic `VIEW`/`TABLE` creation via `Connector` trait |
| `application::validation` | Application | Schema drift detection, data quality tests (`unique`, `not_null`) |
| `application::catalog` | Application | `CatalogGenerator` — HTML/JSON data catalog |
| `infrastructure::adapters` | Infrastructure | `DuckDBConnector` + `DataFusionConnector` — dual SQL engine support |
| `infrastructure::compiler` | Infrastructure | `GraphDiscovery` (manifest loading), `JinjaRenderer` (SQL templating) |
| `infrastructure::config` | Infrastructure | YAML parsing (`ProjectConfig`, `SchemaFile`, `SourceConfig`) |
| `infrastructure::fs` | Infrastructure | Atomic file writes |

## 🛠 Usage

### Prerequisites
-   Rust 2024 Edition (stable)

### Install
```bash
cargo install --path verity
```

### CLI Commands

```bash
# Run the full pipeline (uses engine from config, defaults to DuckDB)
verity run

# Run a single model
verity run --select stg_users

# Strict mode (CI/Prod)
VERITY_STRICT=true verity run

# Scan data/ and generate models/sources.yaml
verity generate

# Generate with governance metadata
verity generate --owner "data_team" --pii --security confidential

# Remove sources that no longer exist on disk
verity generate --prune

# Generate data catalog (HTML/JSON)
verity docs

# Run ad-hoc SQL queries
verity query "SELECT * FROM stg_users LIMIT 5"

# Inspect a table (schema + sample rows)
verity inspect --table users --db-path target/verity.duckdb --limit 10

# Clean build artifacts
verity clean
```

### Engine Selection

Verity supports two SQL execution engines. Set the `engine` field in `verity_project_conf.yaml`:

```yaml
# verity_project_conf.yaml
name: my_project
version: "0.1.0"
engine: duckdb       # Options: duckdb (default), datafusion
```

| Engine | Storage | Best For |
|--------|---------|----------|
| **DuckDB** (default) | Single `.duckdb` file | OLAP queries, local dev, embedded analytics |
| **DataFusion** | Parquet files in `target/data/` | Rust-native pipelines, cloud-ready, extensible |

### Project Structure
```
my_project/
├── verity_project_conf.yaml    # Project configuration & governance rules
├── config/
│   ├── policies.yml            # PII detection patterns & column policies
│   └── quality.yml             # Data quality rules
├── data/
│   └── raw/                    # Source data files (CSV, Parquet)
├── models/
│   ├── sources.yaml            # Auto-generated source definitions
│   ├── staging/                # Raw data ingestion (views)
│   │   ├── stg_users.sql
│   │   ├── stg_users.yml       # Schema contract (columns, tests, policies)
│   │   └── schema.yml          # Centralized schema (lower priority)
│   ├── features/               # Feature engineering
│   └── marts/                  # Business-ready datasets (tables)
└── target/
    ├── compiled/               # Jinja-resolved SQL (business logic only)
    └── run/                    # Governance-wrapped SQL (actually executed)
```

### Governance Policies

Policies are declared in YAML and enforced automatically:

```yaml
# In schema.yml or <model>.yml
columns:
  - name: email
    policy: hash      # → SHA256(CAST(email AS VARCHAR)) AS email
  - name: ssn
    policy: redact            # → 'REDACTED' AS ssn
  - name: phone
    policy: mask_email        # → j****@domain.com
  - name: salary_band
    policy: hash              # → SHA256 hash
```

The `PolicyRewriter` wraps your SQL in a governance CTE at compile time:
```sql
-- target/run/staging/stg_users.sql (auto-generated)
WITH verity_governance_cte AS (
    -- Your original SQL (untouched)
    SELECT user_id, email, name FROM "raw_users"
)
SELECT
    user_id,
    SHA256(CAST(email AS VARCHAR)) AS email,  -- Auto-masked
    SHA256(CAST(name AS VARCHAR)) AS name     -- Auto-masked
FROM verity_governance_cte
```

## 📂 Examples

### `examples/basic_rag_pipeline`
A multi-domain data pipeline (Human Resources, Supply Chain, Compliance) demonstrating:
-   Source registration from CSV files
-   Staging → Intermediate → Marts layer architecture
-   PII detection (email, SSN, credit card) with automatic masking
-   Data quality tests and compliance checks

### `examples/ml_pipeline`
A governance-aware ML pipeline (Churn Prediction) demonstrating:
-   Feature Store with PII-masked demographics
-   Versioned, immutable training datasets
-   Prediction drift monitoring
-   Security level management (`confidential` → `internal` downgrade via masking)

## 📝 Contributing

### Pre-Flight Checklist (Local)

1.  **Formatting**:
    ```bash
    cargo fmt --all
    ```
2.  **Code Quality** (Clippy — Deny Warnings):
    ```bash
    cargo clippy --workspace -- -D warnings
    ```
3.  **Unit Tests**:
    ```bash
    cargo test --workspace
    ```
4.  **Security Audit**:
    ```bash
    cargo audit
    ```

### Git Workflow

1.  Create a feature branch: `git checkout -b feat/my-feature`
2.  Code & Validate (Pre-Flight Checklist).
3.  Commit & Push.
4.  Open a Pull Request to `develop`.

### Pre-Push Hook
```bash
# .git/hooks/pre-push
#!/bin/sh
echo "🛡️  Verity Pre-Push Guard 🛡️"
cargo fmt --all -- --check && cargo clippy --workspace -- -D warnings && cargo test --workspace
```
Make it executable: `chmod +x .git/hooks/pre-push`

---

**Verity** — *Because compliance shouldn't be optional.*