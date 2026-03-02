# ADR-003 — Compile-Time Governance Enforcement (Zero-Trust Compilation)

**Status:** Accepted  
**Date:** 2026-03-02  
**Deciders:** Verity Core Team  

---

## Context

Traditional data transformation frameworks (dbt, Spark jobs) apply governance as runtime checks, documentation conventions, or post-hoc data catalog annotations. In a world where unmasked PII can silently flow into a Vector Store and be served to an LLM via RAG, a violation that is caught at runtime (or worse, in a post-mortem audit) is already too late.

The team needed to decide **when** governance rules are enforced:
1. At runtime, after models have already materialized.
2. At "compile time," before any SQL is executed against a database.

---

## Decision

**Governance is enforced during the compilation phase, before any data is written to any sink.**

The pipeline flow is:

```
Parse Manifests → Resolve DAG → [COMPILE] Apply Governance → Materialize → Validate
                                      ↑
                              Fails here if policy violated
```

In `strict: true` mode (UAT / Production), any governance violation causes a hard `Err(DomainError::GovernanceViolation)` that halts the pipeline before a single row is written.

---

## Rationale

### The Shift-Left Principle Applied to Data

Security engineering has widely adopted "Shift-Left" — catching bugs earlier in the development lifecycle. Verity applies the same principle to data governance:

- **Runtime enforcement** (catch violations after materialization): too late.
- **Compile-time enforcement** (catch violations before any I/O): correct and safe.

### Strict vs. Dev Mode

Governance strictness is environment-scoped, allowing iterative development without blocking productivity:

```yaml
governance:
  environments:
    dev:
      strict_mode: false   # Violations printed to stderr, pipeline continues
    prod:
      strict_mode: true    # Hard error, pipeline halted
```

The `GovernanceGuard::audit_data` function implements this duality:

```rust
// domain/governance/guard.rs
if self.strict_mode {
    return Err(err);  // Hard failure in prod
} else {
    eprintln!("⚠️  [Governance Bypass] ...");  // Warning in dev
}
```

### Dangling Reference as a Compile Error

`GraphSolver::plan_execution` validates dependencies during DAG construction — not at runtime. A model referencing a non-existent model is a **compile error**:

```rust
// domain/graph/dag.rs
if !adj_list.contains_key(parent_node) {
    return Err(DomainError::CircularDependency(format!(
        "Dangling Reference: Model '{}' depends on '{}' which does not exist.",
        current_node, parent_node
    )));
}
```

### Single `latest: true` Invariant

A compile-time guard in the manifest loading phase (`infrastructure/compiler/discovery.rs`) prevents multiple versions of the same model from having `latest: true` simultaneously, ensuring deterministic downstream resolution.

---

## Consequences

**Positive:**
- Zero PII leakage risk in production — a policy violation is structurally impossible to ship.
- Dev productivity preserved via `strict: false` bypass mode.
- Governance violations are surfaced with precise error messages (model name, rule name, severity) before CI pipelines consume compute.

**Negative:**
- Analysts must understand upfront that schema contracts are required, not optional.
- Compiling a large project with complex policies adds latency before any model runs.

---

## References

- `verity-core/src/domain/governance/guard.rs` — `GovernanceGuard` strict/dev mode.
- `verity-core/src/domain/graph/dag.rs` — Compile-time dangling reference detection.
- `verity-core/src/infrastructure/compiler/discovery.rs` — Single `latest` invariant.
- `examples/basic_rag_pipeline/verity_project_conf.yaml` — Environment-scoped strictness config.
