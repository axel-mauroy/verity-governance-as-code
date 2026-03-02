# Product Requirements Document (PRD)
# Verity — Governance-as-Code Data Transformation Engine

**Version:** 1.0  
**Date:** 2026-03-02  
**Status:** Draft  

---

## 1. Executive Summary

Verity is a governance-first data transformation engine built in Rust. It treats data governance (PII policies, ownership, schema contracts, security levels) as first-class code artifacts that are enforced at **compile time**, not as runtime afterthoughts. The core value proposition is **Zero-Trust Compilation**: a pipeline that violates governance rules simply refuses to build.

---

## 2. Problem Statement

### 2.1 The Structural Flaws of Modern Data Engineering

| Problem | Current State | Impact |
|---|---|---|
| **PII in AI pipelines** | Unmasked data flowing into Vector Stores / RAG systems | Regulatory liability (GDPR, CCPA), reputational damage |
| **Optional documentation** | Schema contracts as "nice to have" | Schema drift, silent data quality failures |
| **Manual PII masking** | Analysts must remember to add `SHA256(email)` | Human error, inconsistent enforcement |
| **Runtime governance** | Compliance checks happen after materialization | Data already written to sinks before violation detected |
| **Python ecosystem overhead** | venv management, cold starts, dependency hell | Slow CI pipelines, fragile local environments |

### 2.2 Core Insight

> In the RAG era, a single pipeline error can mean PII being indexed into a vector store and served to an LLM. "Optional" governance is no longer a tradeoff — it is a liability.

---

## 3. Goals & Non-Goals

### Goals
- Enforce governance policies at **compile time** before any data is written.
- Provide a `dbt`-like developer experience (YAML schemas, SQL models, layered architecture) with governance built in.
- Be a **zero-dependency binary** — install via `cargo install`, no runtime interpreter required.
- Support both development (permissive) and production (strict) modes.
- Auto-generate schema contracts and source definitions from existing data.

### Non-Goals
- Replace a full data orchestrator (Airflow, Dagster) — Verity is focused on the transformation layer.
- Provide a hosted SaaS platform (this is an open-source CLI tool).
- Support streaming data (batch SQL-based transformations only, for now).

---

## 4. Target Users

| Persona | Description | Primary Pain |
|---|---|---|
| **Data Engineer** | Builds and maintains transformation pipelines | Ensuring PII never reaches downstream sinks unmasked |
| **Data Platform Lead** | Owns the data stack architecture | Enforcing team-wide governance standards without manual code review |
| **ML Engineer** | Prepares feature stores and training datasets | Producing versioned, compliance-certified datasets for model training |
| **Data Governance Officer** | Ensures regulatory compliance (GDPR, CCPA) | Auditing data lineage and PII handling across pipelines |
| **Security Engineer** | Reviews data infrastructure for vulnerabilities | Shift-Left: catching PII exposure before deployment |

---

## 5. Core Features & Requirements

### 5.1 Zero-Trust Compilation (FR-01)

**Description:** Verity refuses to compile a pipeline if governance rules are violated.

| Rule | Behavior |
|---|---|
| Unversioned model | Hard compile error |
| Missing schema contract | Auto-generates `schema.yml`, flags for review |
| PII column without policy | Error in `strict` mode, warning in `dev` mode |
| Multiple models with `latest: true` | Hard compile error |

**Priority:** P0 — Core differentiator.

---

### 5.2 Automatic PII Masking via PolicyRewriter (FR-02)

**Description:** Columns tagged with a governance policy in `schema.yml` are automatically wrapped in SQL expressions at compile time. The analyst's SQL is never modified.

**Supported Policies:**

| Policy | SQL Transformation |
|---|---|
| `hash` | `SHA256(CAST(<col> AS VARCHAR)) AS <col>` |
| `redact` | `'REDACTED' AS <col>` |
| `mask_email` | `j****@domain.com` pattern |

**Implementation:** The `PolicyRewriter` generates a `verity_governance_cte` wrapper around the original SQL. The compiled SQL in `target/run/` contains governance logic; `target/compiled/` contains the original business logic untouched.

**Priority:** P0.

---

### 5.3 DAG-Based Parallel Execution (FR-03)

**Description:** Verity resolves the model dependency graph (DAG) and executes independent layers concurrently via `tokio`.

- Models within the same dependency layer run in parallel.
- `ref()` Jinja macro resolves inter-model dependencies.
- Execution is blocked per layer; downstream models only run after upstream models succeed.

**Priority:** P0.

---

### 5.4 Source Auto-Generation (FR-04)

**Description:** `verity generate` (or `verity sources generate`) scans the `data/` directory and auto-creates/merges `models/sources.yaml`.

**Key Behaviors:**
- **Smart Merge:** Existing entries are never modified; only new files are added.
- **Deterministic Naming:** `data/finance/transactions.csv` → `finance_transactions`.
- **Governance Injection:** New sources pre-tagged with `--owner` and `--pii` flags.

**CLI Options:**

| Flag | Description |
|---|---|
| `--data-dir <path>` | Directory to scan (default: `data/`) |
| `--owner <name>` | Default owner for new discovered sources |
| `--pii` | Mark new sources as PII by default |
| `--prune` | Remove sources that no longer exist on disk |

**Priority:** P1.

---

### 5.5 Data Quality Tests (FR-05)

**Description:** Schema YAML files declare tests that run post-materialization.

**Built-in Tests:**

| Test | Description |
|---|---|
| `unique` | Asserts column values are unique |
| `not_null` | Asserts column has no NULL values |
| `row_count_anomaly` | Detects row count deviations above a configurable threshold |

**Configuration:**
```yaml
quality:
  rules:
    - check: unique
      severity: error
    - check: row_count_anomaly
      threshold: 0.08
      severity: warning
```

**Priority:** P1.

---

### 5.6 Auto-Schema Propagation (FR-06)

**Description:** Undocumented columns are detected at runtime and automatically added to the corresponding `schema.yml`. This ensures schema contracts evolve with the data without manual intervention.

**Priority:** P1.

---

### 5.7 Data Catalog Generation (FR-07)

**Description:** `verity docs` generates an HTML and JSON data catalog from the manifest, suitable for internal documentation portals.

**Output:**
- `target/catalog.html` — Human-readable catalog.
- `target/catalog.json` — Machine-readable lineage & metadata.

**Priority:** P2.

---

### 5.8 Multi-Environment Support (FR-08)

**Description:** Governance strictness is configurable per environment.

```yaml
governance:
  environments:
    dev:
      strict_mode: false   # Warn only
    uat:
      strict_mode: true    # Block compilation
    prod:
      strict_mode: true    # Block compilation
```

**Priority:** P0.

---

### 5.9 Ad-hoc SQL Query Interface (FR-09)

**Description:** `verity query "<SQL>"` executes arbitrary SQL against the registered sources/models for interactive data exploration.

**Priority:** P2.

---

### 5.10 Static Data Lineage Analysis (FR-10)

**Description:** `verity lineage` performs a pre-flight compliance check by walking the project DAG to detect data flow vulnerabilities without executing any SQL or reading any data.

**Detection Capabilities:**
- **Unprotected PII Flows:** Detects if a column tagged with a PII policy upstream flows into a downstream model where the policy is dropped or missing.
- **Security Downgrades:** Detects if a model with a high security level (e.g., `restricted`) feeds into a model with a lower security level (e.g., `public`) without proper masking.

**Output Formats:**
- **Mermaid:** Visual graph highlighting critical nodes and vulnerable edges.
- **JSON:** Machine-readable report for CI/CD integration.
- **JSON-LD (Semantic):** Linked-data representation for enterprise catalogs.

**CLI Option:** `--check` will force the command to fail (exit 1) if any vulnerabilities are found, acting as a CI/CD security gate.

**Priority:** P0.

---

## 6. Architecture Requirements

### 6.1 Hexagonal Architecture

Verity enforces strict layer separation:

| Layer | Responsibility |
|---|---|
| **CLI** (`verity`) | User-facing commands: `run`, `generate`, `docs`, `query`, `clean` |
| **Application** | Pipeline orchestration, materialization, validation, catalog |
| **Domain** | Pure business logic: governance, DAG resolution, compliance (zero I/O) |
| **Ports** | Trait interfaces: `Connector`, `ManifestLoader`, `TemplateEngine`, `SchemaSource` |
| **Infrastructure** | Adapters: DataFusion connector, Jinja renderer, YAML config, atomic FS |

### 6.2 SQL Engine Support

| Engine | Storage | Best For |
|---|---|---|
| **DataFusion** | Parquet in `target/data/` | Rust-native, cloud-ready, extensible |

> DuckDB was previously explored; DataFusion is the primary target engine.

### 6.3 Atomic I/O (NFR-01)

All file writes (SQL artifacts, state files, catalog) use atomic operations to prevent corrupt intermediate states.

### 6.4 Performance (NFR-02)

- Single binary, no runtime interpreter.
- Cold start < 100ms.
- Parallel DAG execution saturates available CPU cores.

### 6.5 Correctness (NFR-03)

- `cargo clippy -- -D warnings` must pass (zero warnings).
- All domain logic covered by unit tests.
- E2E tests for `basic_rag_pipeline` and `ml_pipeline` examples.

---

## 7. Project Structure Contract

```
<project>/
├── verity_project_conf.yaml     # Project config & governance defaults
├── config/
│   ├── policies.yml             # PII detection patterns & column policies
│   └── quality.yml              # Data quality rules
├── data/
│   └── raw/                     # Source data files (CSV, Parquet)
├── models/
│   ├── sources.yaml             # Auto-generated source definitions
│   ├── staging/                 # Raw ingestion layer (views)
│   ├── intermediate/            # Business logic layer (views)
│   └── marts/                   # Business-ready datasets (tables)
└── target/
    ├── compiled/                # Jinja-resolved SQL (business logic only)
    └── run/                     # Governance-wrapped SQL (executed)
```

---

## 8. CLI Command Reference

| Command | Description |
|---|---|
| `verity run` | Execute the full pipeline |
| `verity run --select <model>` | Execute a single model |
| `verity lineage` | Output the data lineage graph (Mermaid) |
| `verity lineage --check` | Pre-flight security scan for unprotected PII flows |
| `verity generate` | Scan `data/` and generate/merge `sources.yaml` |
| `verity docs` | Generate HTML/JSON data catalog |
| `verity query "<SQL>"` | Run ad-hoc SQL |
| `verity clean` | Remove build artifacts |
| `VERITY_STRICT=true verity run` | Force strict governance mode |

---

## 9. Reference Implementations (Examples)

### 9.1 `basic_rag_pipeline`

A multi-domain pipeline (HR, Supply Chain, Compliance) demonstrating:
- CSV source registration.
- Staging → Intermediate → Marts architecture.
- PII detection (email, SSN, credit card) with automatic masking.
- Data quality tests (`unique`, `not_null`, `row_count_anomaly`).

**Purpose:** Validate that PII never reaches a vector store unmasked.

### 9.2 `ml_pipeline` (Churn Prediction)

A governance-aware ML feature pipeline demonstrating:
- Feature Store with PII-masked demographics.
- Versioned, immutable training datasets.
- Prediction drift monitoring.
- Security level management (`confidential` → `internal` downgrade via masking).

**Purpose:** Validate compliance-certified dataset production for model training.

---

## 10. Success Metrics

| Metric | Target |
|---|---|
| Compile-time PII catch rate | 100% of tagged columns masked before materialization |
| Schema drift detection | 100% of undocumented columns flagged or auto-patched |
| Build time (basic_rag_pipeline) | < 10 seconds full pipeline |
| Test coverage (unit) | > 80% on domain layer |
| CI pass rate | `fmt` + `clippy` + `test` + `audit` all green |

---

## 11. Open Questions & Future Work

| Item | Status |
|---|---|
| `--interactive` flag for `verity generate` | Planned (future) |
| Additional SQL engines (e.g. DuckDB re-integration) | Under evaluation |
| Streaming / incremental materialization | Not in scope v1 |
| Hosted data catalog portal | Not in scope v1 |
| RBAC / column-level access control | Under consideration |
| OpenLineage / Marquez integration | Under consideration |

---

*"Because compliance shouldn't be optional."*
