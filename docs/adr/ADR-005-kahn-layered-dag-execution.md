# ADR-005 — Kahn's Algorithm for Layered Parallel DAG Execution

**Status:** Accepted  
**Date:** 2026-03-02  
**Deciders:** Verity Core Team  

---

## Context

Verity pipelines are DAGs (Directed Acyclic Graphs) of SQL models. Models that do not depend on each other can be executed concurrently. The team needed to choose a graph traversal strategy that:

1. Correctly resolves execution order respecting all dependencies.
2. Detects cycles (which would cause infinite loops).
3. Detects dangling references (models referencing non-existent models).
4. Groups independent models into **layers** for concurrent execution.

---

## Decision

**Verity uses a layered variant of Kahn's topological sort algorithm implemented in `GraphSolver::plan_execution`.**

The output is `Vec<Vec<&str>>` — a list of layers, where each layer contains models that can be safely executed in parallel.

---

## Rationale

### Algorithm Choice: Kahn's vs. DFS

| Property | Kahn's (BFS-based) | DFS-based |
|---|---|---|
| Cycle detection | ✅ Implicit (unresolved nodes remain) | ✅ Requires additional state |
| Layer grouping for parallelism | ✅ Natural (process by in-degree wave) | ❌ Requires post-processing |
| Implementation simplicity | ✅ Simple queue-based | ⚠️ Recursive, stack overflow risk on deep graphs |
| Deterministic ordering | ✅ With stable queue ordering | ⚠️ Depends on traversal order |

Kahn's algorithm produces **natural layers**: all models with in-degree 0 form layer 0, after processing them their dependents may reach in-degree 0 forming layer 1, and so on. This directly maps to the parallel execution model.

### Zero-Copy Architecture

The solver borrows string references from the `Manifest` rather than cloning model names, avoiding heap allocations on the hot path:

```rust
pub fn plan_execution<'a>(manifest: &'a Manifest) -> Result<Vec<Vec<&'a str>>, DomainError>
```

The HashMap and VecDeque are pre-allocated with the exact capacity:

```rust
let capacity = manifest.nodes.len();
let mut in_degree: HashMap<&'a str, usize> = HashMap::with_capacity(capacity);
```

### Compile-Time Guarantees

Both failure modes are hard errors:

```rust
// Dangling reference
if !adj_list.contains_key(parent_node) {
    return Err(DomainError::CircularDependency(...));
}

// Cycle detection (nodes not resolved = cycle exists)
if total_resolved != capacity {
    return Err(DomainError::CircularDependency(...));
}
```

### Parallel Execution

The pipeline orchestrator (`application/pipeline.rs`) receives the layers and executes each layer concurrently via `tokio::join` or equivalent futures joining, while preserving sequential ordering between layers.

---

## Consequences

**Positive:**
- Independent models (e.g., three staging models with no shared deps) execute in parallel, minimizing total pipeline runtime.
- Cycles and dangling references fail fast at compile time, before any I/O.
- The layered output is directly consumable by `tokio`-based concurrent executors.

**Negative:**
- Within a layer, execution order is not guaranteed (HashMap iteration order). This is acceptable because intra-layer models have no dependencies on each other by definition.
- Very deep, narrow graphs (long linear chains) see limited parallelism benefit.

---

## References

- `verity-core/src/domain/graph/dag.rs` — Full implementation with tests.
- `verity-core/src/application/pipeline.rs` — Layer-based parallel executor.
