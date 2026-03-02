# ADR-006 — Apache DataFusion as Primary SQL Engine

**Status:** Accepted  
**Date:** 2026-03-02  
**Deciders:** Verity Core Team  
**Supersedes:** Initial DuckDB evaluation

---

## Context

Verity requires a SQL execution engine to:
- Register data sources (CSV, Parquet).
- Execute SQL models (CREATE VIEW / CREATE TABLE AS SELECT).
- Support Arrow `RecordBatch` output for schema validation.
- Integrate natively with Rust without FFI overhead.
- Be extensible for future governance-layer query plan rules.

Two engines were evaluated: **DuckDB** (via the `duckdb-rs` binding) and **Apache DataFusion**.

---

## Decision

**Apache DataFusion is the primary (and currently only) SQL engine in Verity.**

---

## Rationale

### DuckDB Evaluation & Rejection

DuckDB was initially explored. Key issues encountered:

- **FFI boundary**: `duckdb-rs` wraps a C++ library. API-breaking changes in `sqlparser` (the Rust SQL AST crate) during the DuckDB integration compounded maintenance burden — the `UniversalQuoter` refactoring required significant effort to align with `sqlparser` 0.59.0 API changes.
- **Encoding issues**: Non-UTF8 data in CSV sources produced a hard panic in the DataFusion UTF-8 validation layer that was difficult to attribute and fix (the `stg_customer_profiles` issue).
- **License constraints**: DuckDB's embedding license for commercial use requires evaluation.

### DataFusion Selection

| Criterion | DataFusion | DuckDB |
|---|---|---|
| Rust-native (no FFI) | ✅ Pure Rust | ❌ C++ FFI |
| Arrow-native I/O | ✅ First-class `RecordBatch` | ⚠️ Arrow via binding |
| Query plan extensibility | ✅ `OptimizerRule` API | ❌ Limited |
| Parquet support | ✅ Native `parquet` crate | ✅ Native |
| CSV support | ✅ Native | ✅ Native |
| License | ✅ Apache 2.0 | ⚠️ MIT (commercial embedding TBD) |

### Future: Plan-Level Governance

The `Connector` trait exposes a hook for future DataFusion-specific governance:

```rust
// ports/connector.rs
fn supports_plan_governance(&self) -> bool { false }

async fn register_governance(&self, _policies: GovernancePolicySet) {
    // No-op by default — DataFusion adapter can implement OptimizerRule
}
```

This allows the DataFusion adapter to register governance rules as **query optimizer rules**, bypassing the string-based `PolicyRewriter` entirely for better performance and correctness on complex queries.

---

## Consequences

**Positive:**
- Full Rust compilation — no C++ toolchain required in CI.
- Arrow `RecordBatch` output enables schema drift detection without additional data format conversions.
- `OptimizerRule` API in DataFusion is a clear extension point for plan-level governance.
- Apache 2.0 license is unambiguous for commercial use.

**Negative:**
- DataFusion's SQL dialect differs from DuckDB and PostgreSQL — some SQL features available in DuckDB (e.g., `LIST`, `STRUCT` types, advanced window functions) require workarounds.
- Example pipelines must be validated against DataFusion's specific SQL support each time DataFusion is upgraded.

---

## References

- `verity-core/src/infrastructure/adapters/` — DataFusion connector implementation.
- `verity-core/src/ports/connector.rs` — `supports_plan_governance` hook.
- `examples/ml_pipeline/verity_project_conf.yaml` — `engine: datafusion` configuration.
- Conversation `dd654f67` — UniversalQuoter refactoring due to `sqlparser` version conflict.
- Conversation `7ce80668` — DataFusion UTF-8 encoding issue investigation.
