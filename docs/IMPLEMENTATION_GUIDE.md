# Verity — Implementation Guide

> A technical deep-dive into how Verity works internally: the pipeline lifecycle, module responsibilities, and how to extend the system correctly.

---

## Table of Contents

1. [Repository Layout](#1-repository-layout)
2. [Workspace Structure](#2-workspace-structure)
3. [The Pipeline Lifecycle](#3-the-pipeline-lifecycle)
4. [Module Responsibilities](#4-module-responsibilities)
5. [Domain Layer: Pure Logic, Zero I/O](#5-domain-layer-pure-logic-zero-io)
6. [Adding a New SQL Engine](#6-adding-a-new-sql-engine)
7. [Adding a New Masking Strategy](#7-adding-a-new-masking-strategy)
8. [Adding a New Data Quality Check](#8-adding-a-new-data-quality-check)
9. [Adding a New CLI Command](#9-adding-a-new-cli-command)
10. [Adding a New Example Pipeline](#10-adding-a-new-example-pipeline)
11. [Configuration Reference](#11-configuration-reference)
12. [Build Profiles](#12-build-profiles)

---

## 1. Repository Layout

```
verity-governance-as-code/
├── verity/                    # CLI binary crate
│   └── src/
│       └── main.rs            # clap commands: run, generate, docs, query, clean
├── verity-core/               # Library crate (all engine logic)
│   └── src/
│       ├── application/       # Orchestration, Materialization, Validation, Catalog
│       ├── domain/            # Pure business logic (zero I/O)
│       │   ├── governance/    # PolicyRewriter, PiiScanner, GovernanceGuard
│       │   ├── graph/         # GraphSolver (DAG, Kahn's algorithm)
│       │   ├── project/       # Manifest, ManifestNode, lifecycle
│       │   └── compliance/    # RowCountCheck, ZScoreCheck, anomaly detection
│       ├── ports/             # Trait interfaces (Connector, ManifestLoader, etc.)
│       └── infrastructure/    # Adapters (DataFusion, Jinja, YAML, atomic FS)
├── examples/
│   ├── basic_rag_pipeline/    # Multi-domain RAG pipeline example
│   └── ml_pipeline/           # Churn prediction ML pipeline example
├── docs/
│   ├── PRD.md
│   ├── adr/                   # Architecture Decision Records
│   ├── IMPLEMENTATION_GUIDE.md   ← you are here
│   └── CONTRIBUTING.md
├── .githooks/                 # Local git hook scripts
│   ├── pre-commit
│   ├── full_verify.sh         # "The Gauntlet" — full local CI simulation
│   └── deny_unsecure.sh       # Zero-panic guard
└── .github/
    ├── workflows/ci.yml       # GitHub Actions CI definition
    └── CODEOWNERS
```

---

## 2. Workspace Structure

Verity is a **Cargo workspace** with two crates:

| Crate | Type | Role |
|---|---|---|
| `verity-core` | `lib` | All engine logic, domain, ports, infrastructure |
| `verity` | `bin` | CLI binary, wires `clap` commands to `verity-core` |

Shared dependencies are declared once in the workspace root `Cargo.toml` under `[workspace.dependencies]` and referenced in each crate's `Cargo.toml` with `{ workspace = true }`.

```toml
# Cargo.toml (workspace root)
[workspace]
members = ["verity-core", "verity"]
resolver = "2"
```

**Install the CLI locally:**
```bash
cargo install --path verity
```

### CLI Commands

```bash
# Run the full pipeline
verity run

# Run a single model
verity run --select stg_users

# Static Data Lineage Analysis
verity lineage

# Lineage Analysis as a pre-flight CI/CD security gate
verity lineage --check

# Output lineage as JSON or JSON-LD
verity lineage --format json
verity lineage --format json-ld

# Strict mode (CI/Prod)
verity run --strict
```

---

## 3. The Pipeline Lifecycle

When `verity run` is called, `application::pipeline::run_pipeline` executes the following phases in order:

```
┌─────────────────────────────────────────────────────────┐
│  Phase 1: SETUP                                         │
│  Create target/ directory. Load state.json checkpoint.  │
├─────────────────────────────────────────────────────────┤
│  Phase 2: DISCOVERY                                     │
│  ManifestLoader reads models/, sources.yaml, schema     │
│  YAML files → builds the Manifest (nodes + sources).    │
│  Single-latest-version invariant enforced here.         │
├─────────────────────────────────────────────────────────┤
│  Phase 3: SOURCE REGISTRATION                           │
│  Connector::register_source() registers each CSV/       │
│  Parquet file from sources.yaml as a named table.       │
├─────────────────────────────────────────────────────────┤
│  Phase 3.5: PLAN-LEVEL GOVERNANCE (optional)            │
│  If Connector::supports_plan_governance() → push        │
│  GovernancePolicySet to engine-level optimizer.         │
├─────────────────────────────────────────────────────────┤
│  Phase 4: DAG SCHEDULING                                │
│  GraphSolver::plan_execution() → Vec<Vec<&str>>         │
│  Layered topological sort (Kahn's algorithm).           │
│  Cycle and dangling reference detected here = ERROR.    │
├─────────────────────────────────────────────────────────┤
│  Phase 5: PARALLEL EXECUTION (per layer)                │
│  For each layer:                                        │
│    A. Jinja render (Jinja2 → SQL) → target/compiled/   │
│    B. UniversalQuoter (identifier quoting)              │
│    C. PolicyRewriter (CTE wrapper) → target/run/        │
│    D. Pre-flight linting (GovernanceLinter, strict only) │
│    E. Materializer (VIEW or TABLE via Connector)        │
│    F. Validation (schema drift → auto-patch schema)     │
│    G. Compliance (row_count_anomaly, z_score_anomaly)   │
│  Layer concurrency: buffer_unordered(8)                 │
│  Fail-fast: first model error → layer aborted.         │
├─────────────────────────────────────────────────────────┤
│  Phase 6: FINALIZE                                      │
│  Persist state.json + run_results.json (atomic writes). │
│  CHECKPOINT command sent to engine.                     │
└─────────────────────────────────────────────────────────┘
```

All file writes (compiled SQL, state, results) use `infrastructure::fs::atomic_write` — a temp-file + atomic rename pattern.

---

## 4. Module Responsibilities

### `domain/governance/`

| File | Responsibility |
|---|---|
| `rewriter.rs` | `PolicyRewriter::apply_masking` — pure function, CTE SQL wrapper |
| `scanner.rs` | `PiiScanner` — pre-compiled regex, zero-copy violations |
| `guard.rs` | `GovernanceGuard` — strict vs dev mode enforcement |
| `semantic.rs` | `SemanticGraph` — JSON-LD catalog: DCAT + PROV-O |
| `security_level.rs` | `SecurityLevel` enum (`Public`, `Internal`, `Confidential`, `Restricted`) |
| `policy.rs` | `PolicyType`, `MaskingStrategy` enums |
| `linter.rs` | `GovernanceLinter` — pre-flight sample data scan |

### `domain/graph/`

| File | Responsibility |
|---|---|
| `dag.rs` | `GraphSolver::plan_execution` — Kahn's layered topological sort |

### `domain/compliance/`

| File | Responsibility |
|---|---|
| `anomaly.rs` | `RowCountCheck`, `ModelExecutionState`, deviation validation |
| `zscore.rs` | `ZScoreCheck` — column-level drift detection |

### `ports/connector.rs`

The single engine abstraction trait. Any SQL engine adapter must implement:

```rust
#[async_trait]
pub trait Connector: Send + Sync {
    async fn execute(&self, query: &str) -> Result<(), VerityError>;
    async fn fetch_columns(&self, table_name: &str) -> Result<Vec<ColumnSchema>, VerityError>;
    async fn register_source(&self, name: &str, path: &Path) -> Result<(), VerityError>;
    async fn materialize(&self, table_name: &str, sql: &str, materialization_type: &str) -> Result<String, VerityError>;
    async fn query_scalar(&self, query: &str) -> Result<u64, VerityError>;
    fn engine_name(&self) -> &str;
    // Optional hooks:
    fn supports_plan_governance(&self) -> bool { false }
    async fn register_governance(&self, _policies: GovernancePolicySet) {}
    async fn fetch_column_averages(&self, ...) -> Result<HashMap<String, f64>, VerityError> { ... }
}
```

### `infrastructure/`

| Module | Responsibility |
|---|---|
| `adapters/` | DataFusion `Connector` implementation |
| `compiler/` | `GraphDiscovery` (manifest loading), `JinjaRenderer`, `UniversalQuoter` |
| `config/` | YAML deserialization: `ProjectConfig`, `SchemaFile`, `SourceConfig` |
| `fs.rs` | `atomic_write` — temp file + rename |

---

## 5. Domain Layer: Pure Logic, Zero I/O

**The `domain/` crate has zero I/O.** This is an architectural invariant.

- No `fs::read`, no `async`, no database calls inside `domain/`.
- All domain functions are **pure**: same inputs → same outputs.
- This makes them fast to unit test (no fixtures, no mocks).

**Example — correct:**
```rust
// ✅ Pure domain function
pub fn apply_masking(sql: &str, node: &ManifestNode) -> Result<String, DomainError>
```

**Example — violation:**
```rust
// ❌ Never do this in domain/
use std::fs;
pub fn apply_masking(sql: &str, schema_path: &Path) -> Result<String, DomainError> {
    let file = fs::read_to_string(schema_path)?;  // I/O in domain = architecture violation
    ...
}
```

If you need I/O, it belongs in `infrastructure/` or `application/`, and is injected into the domain via a port (trait).

---

## 6. Adding a New SQL Engine

1. **Create a new adapter** in `infrastructure/adapters/<engine_name>.rs`.

2. **Implement the `Connector` trait** for your engine struct:
   ```rust
   pub struct MyEngineConnector { /* engine handle */ }

   #[async_trait]
   impl Connector for MyEngineConnector {
       async fn execute(&self, query: &str) -> Result<(), VerityError> { ... }
       async fn fetch_columns(&self, table_name: &str) -> Result<Vec<ColumnSchema>, VerityError> { ... }
       async fn register_source(&self, name: &str, path: &Path) -> Result<(), VerityError> { ... }
       async fn materialize(&self, ...) -> Result<String, VerityError> { ... }
       async fn query_scalar(&self, query: &str) -> Result<u64, VerityError> { ... }
       fn engine_name(&self) -> &str { "my_engine" }
   }
   ```

3. **Register the engine variant** in `infrastructure/config/` (the `Engine` enum).

4. **Wire it up in `verity/src/main.rs`** where the `Connector` is instantiated from config.

5. **Add an E2E test** using one of the example pipelines with `engine: my_engine`.

> ⚠️ If your engine has governance-aware query planning, override `supports_plan_governance()` to return `true` and implement `register_governance()`. The pipeline will skip `PolicyRewriter` for that engine.

---

## 7. Adding a New Masking Strategy

Masking strategies are defined in **two places** that must stay in sync:

1. **Enum definition** — `domain/governance/policy.rs`:
   ```rust
   pub enum MaskingStrategy {
       Hash,
       Redact,
       MaskEmail,
       Nullify,
       Partial,
       EntityPreserving,
       MyNewStrategy,  // ← Add here
   }
   ```

2. **SQL expression** — `domain/governance/rewriter.rs`, inside `apply_masking`:
   ```rust
   MaskingStrategy::MyNewStrategy => {
       format!("my_sql_expression({}) AS {}", col.name, col.name)
   }
   ```

3. **YAML deserialization** — `infrastructure/config/` (the policy YAML parser must map the string `"my_new_strategy"` to the enum variant).

4. **Unit test** — add a test case in `rewriter.rs` tests, following the existing pattern.

---

## 8. Adding a New Data Quality Check

Built-in checks (`unique`, `not_null`) live in `application/validation.rs`. Compliance checks (`row_count_anomaly`, `z_score_anomaly`) live in `domain/compliance/`.

**Steps for a new compliance check:**

1. Add a new check struct in `domain/compliance/`:
   ```rust
   pub struct MyCheck;
   impl MyCheck {
       pub fn validate(params: &CheckParams) -> Result<(), MyCheckError> { ... }
   }
   ```

2. Register the check name in `application/pipeline.rs` inside `check_compliance()`, alongside `row_count_anomaly` and `z_score_anomaly`.

3. Add the check to the YAML schema for `quality.yml`:
   ```yaml
   - check: my_check
     params:
       my_param: 0.05
     severity: warning
   ```

4. Write a unit test in `domain/compliance/`.

---

## 9. Adding a New CLI Command

CLI commands are defined in `verity/src/main.rs` using `clap` with the `derive` feature.

1. **Add a variant** to the `Commands` enum:
   ```rust
   #[derive(Subcommand)]
   enum Commands {
       Run { ... },
       Generate { ... },
       MyNewCommand {
           #[arg(long)]
           my_flag: bool,
       },
   }
   ```

2. **Add the handler** in the `match` block:
   ```rust
   Commands::MyNewCommand { my_flag } => {
       // Call application layer function
       application::my_feature::run(my_flag).await?;
   }
   ```

3. **Implement the application function** in `verity-core/src/application/my_feature.rs`.

4. **Write an integration test** in `verity/tests/` using `assert_cmd`.

---

## 10. Adding a New Example Pipeline

Examples live in `examples/<name>/`. Each must be a self-contained Verity project:

```
examples/my_pipeline/
├── verity_project_conf.yaml    # engine, governance, quality config
├── config/
│   ├── policies.yml            # PII patterns
│   └── quality.yml             # Quality rules
├── data/
│   └── raw/                    # Source CSV or Parquet files
└── models/
    ├── sources.yaml            # Auto-generate with: verity generate
    └── staging/
        ├── stg_mymodel.sql
        └── stg_mymodel.yml
```

Add the example to the E2E gate in `.githooks/full_verify.sh`:
```bash
for example in basic_rag_pipeline ml_pipeline my_pipeline; do
    echo "Testing example: $example..."
    (cd "examples/$example" && "$VERITY_BIN" run)
done
```

And in `.github/workflows/ci.yml`:
```yaml
- name: E2E - My Pipeline
  run: cd examples/my_pipeline && ../../bin/verity run
```

---

## 11. Configuration Reference

### `verity_project_conf.yaml`

```yaml
name: my_project
version: "0.1.0"
engine: datafusion          # Only supported engine
target-path: target
config-paths: ["config"]
model-paths: ["models"]

defaults:
  staging:
    materialized: view      # view | table
  marts:
    materialized: table

governance:
  pii_detection:
    enabled: true           # Enables PiiScanner
  data_quality:
    enabled: true
  strict: false             # Override: VERITY_STRICT=true env var also works
  default_anomaly_threshold: 0.10  # 10% row count deviation allowed

  environments:
    dev:
      strict_mode: false
    uat:
      strict_mode: true
    prod:
      strict_mode: true

quality:
  defaults:
    severity: error
  rules:
    - check: unique
      severity: error
    - check: not_null
      severity: error
    - check: row_count_anomaly
      threshold: 0.08
      severity: warning
```

### Schema YAML (per-model or centralized)

```yaml
# models/staging/stg_users.yml
version: 1
latest: true

models:
  - name: stg_users
    owner: data-ops
    security_level: confidential   # public | internal | confidential | restricted
    columns:
      - name: user_id
        tests: [unique, not_null]
      - name: email
        policy: hash               # hash | redact | mask_email | nullify | partial
        tests: [not_null]
      - name: ssn
        policy: redact
    compliance:
      post_flight:
        - check: row_count_anomaly
          params:
            threshold: 0.05
          severity: error
        - check: z_score_anomaly
          params:
            column: salary
            threshold: 3.0
          severity: warning
```

---

## 12. Build Profiles

Cargo profiles are defined in the workspace root `Cargo.toml`:

| Profile | Command | Use Case |
|---|---|---|
| `dev` | `cargo build` | Local development. `opt-level=0`, full debug info. External deps compiled at `opt-level=2` for DataFusion/Arrow performance. |
| `release` | `cargo build --release` | Production binary. `opt-level=3`, `lto=thin`, symbols stripped. |
| `test` | `cargo test` | Unit tests. `opt-level=1` for a balance of speed and debuggability. |

**Tip:** For faster E2E testing during development, use the `dev` binary:
```bash
cargo build --bin verity
./target/debug/verity run
```
