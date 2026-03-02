# ADR-001 — Rust as the Implementation Language

**Status:** Accepted  
**Date:** 2026-03-02  
**Deciders:** Verity Core Team  

---

## Context

Verity needs to compile and execute data governance pipelines reliably in CI/CD environments and on developer machines. The tool must:

- Ship as a **single, self-contained binary** with no runtime dependency.
- Enforce governance rules **at compile time** — errors must be hard failures, not runtime exceptions.
- Handle **parallel DAG execution** across potentially large models without GIL limitations.
- Integrate natively with **Apache DataFusion**, which is a Rust-native library.

The main alternatives considered were Python and Go.

---

## Decision

**Verity is implemented entirely in Rust (2024 Edition, stable toolchain).**

---

## Rationale

| Criterion | Rust | Python | Go |
|---|---|---|---|
| Single binary distribution | ✅ `cargo install` | ❌ venv, pip hell | ✅ |
| Native DataFusion integration | ✅ First-class | ⚠️ PyArrow bindings | ❌ No native crate |
| Memory safety without GC pauses | ✅ Ownership model | ❌ GC | ⚠️ GC |
| True parallelism (no GIL) | ✅ `tokio` | ❌ GIL | ✅ goroutines |
| Compile-time correctness guarantees | ✅ Type system | ❌ Runtime | ⚠️ Partial |
| Cold-start performance | ✅ < 100ms | ❌ Interpreter startup | ✅ |

Rust's **ownership and type system** aligns perfectly with the "fail fast, fail loudly" philosophy: governance violations surface as compile errors (Rust `Result<T, E>`) that propagate through the call stack rather than as silent runtime failures.

Zero-cost abstractions and the `tokio` async runtime allow the parallel DAG executor to saturate CPU cores without the overhead of a thread-per-model approach.

---

## Consequences

**Positive:**
- Zero-dependency binary via `cargo install --path verity`.
- `cargo clippy -- -D warnings` enforces code quality as part of CI.
- `cargo audit` integrates supply-chain security checks natively.
- Lifetime annotations (`<'a>`) in `GraphSolver::plan_execution` and `PiiScanner::scan` enable zero-copy architectures without manual memory management.

**Negative:**
- Higher onboarding cost for contributors unfamiliar with Rust.
- Longer compile times compared to Go or Python (mitigated by incremental compilation).
- Ecosystem for YAML/SQL tooling is narrower than Python's.

---

## References

- `verity-core/src/domain/graph/dag.rs` — Zero-copy DAG solver using lifetime-annotated references.
- `verity-core/src/domain/governance/scanner.rs` — Pre-compiled regex patterns with zero-copy violation structs.
- `Cargo.toml` — Workspace definition.
