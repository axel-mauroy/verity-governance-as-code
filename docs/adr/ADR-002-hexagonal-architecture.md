# ADR-002 — Hexagonal Architecture (Ports & Adapters)

**Status:** Accepted  
**Date:** 2026-03-02  
**Deciders:** Verity Core Team  

---

## Context

Verity needs to support multiple SQL engines (DataFusion was prioritized; DuckDB was explored). Governance logic — PII policy application, DAG resolution, compliance checks — must remain engine-agnostic. Tightly coupling governance logic to a specific SQL engine would make testing expensive and engine migration painful.

---

## Decision

**Verity adopts a strict Hexagonal Architecture (Ports & Adapters) across four layers:**

```
CLI (verity)
    ↓
Application Layer   — Pipeline orchestration, Materializer, Validation, Catalog
    ↓
Domain Layer        — Pure logic: governance, DAG, compliance (ZERO I/O)
    ↑↓
Ports               — Trait interfaces (Connector, ManifestLoader, TemplateEngine)
    ↑
Infrastructure      — Adapters: DataFusion, Jinja, YAML config, Atomic FS
```

---

## Rationale

### Domain Purity

The `domain` crate contains **zero I/O**. `PolicyRewriter::apply_masking` is a pure function — it takes a SQL string and a `ManifestNode`, and returns a rewritten SQL string. It has no async boundary, no file system access, and no database calls.

This makes the entire governance rewriting logic unit-testable without spinning up a SQL engine:

```rust
// domain/governance/rewriter.rs
pub fn apply_masking(sql: &str, node: &ManifestNode) -> Result<String, DomainError>
```

### Engine Abstraction via `Connector` Port

The `Connector` trait (in `ports/connector.rs`) is the single seam between the application layer and any SQL engine:

```rust
#[async_trait]
pub trait Connector: Send + Sync {
    async fn execute(&self, query: &str) -> Result<(), VerityError>;
    async fn fetch_columns(&self, table_name: &str) -> Result<Vec<ColumnSchema>, VerityError>;
    async fn register_source(&self, name: &str, path: &Path) -> Result<(), VerityError>;
    async fn materialize(&self, table_name: &str, sql: &str, materialization_type: &str) -> Result<String, VerityError>;
    fn supports_plan_governance(&self) -> bool { false }
}
```

The `supports_plan_governance()` hook allows future engines to skip the string-based `PolicyRewriter` entirely and implement governance at the query plan level.

### Testability

Because the domain layer has no I/O, all unit tests in `graph/`, `governance/`, and `compliance/` run without database fixtures.

---

## Consequences

**Positive:**
- Adding a new SQL engine requires only implementing the `Connector` trait — no changes to domain or application logic.
- Domain logic tests are fast and hermetic (no DB required).
- Governance policy changes are isolated to `domain/governance/` and never touch infrastructure code.

**Negative:**
- More boilerplate than a flat architecture (trait definitions, adapter structs).
- Developers must understand the layer boundaries and resist the temptation to add I/O directly in domain code.

---

## References

- `verity-core/src/ports/connector.rs` — The engine abstraction port.
- `verity-core/src/domain/governance/rewriter.rs` — Pure domain function, zero I/O.
- `verity-core/src/infrastructure/adapters/` — DataFusion adapter implementation.
