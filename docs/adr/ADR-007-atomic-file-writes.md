# ADR-007 — Atomic File Writes for All Critical Artifacts

**Status:** Accepted  
**Date:** 2026-03-02  
**Deciders:** Verity Core Team  

---

## Context

Verity writes several critical artifacts to disk during a pipeline run:

- Compiled SQL (`target/compiled/**/*.sql`)
- Governance-wrapped SQL (`target/run/**/*.sql`)
- The data catalog (`target/catalog.html`, `target/catalog.json`)
- Source definitions (`models/sources.yaml`)
- Pipeline state files

If a pipeline run is interrupted (process kill, disk full, system crash) mid-write, partially written files could:
- Corrupt the SQL artifact that will be read on the next run.
- Produce an incomplete catalog that is partially rendered.
- Leave `sources.yaml` in an inconsistent state, breaking downstream `ref()` resolution.

---

## Decision

**All critical file writes in Verity use atomic write semantics via `atomic_write` in `infrastructure/fs.rs`.**

The implementation:
1. Creates a `NamedTempFile` **in the same directory** as the target file.
2. Writes the full content to the temp file.
3. Atomically renames (`persist`) the temp file to the target path.

```rust
// infrastructure/fs.rs
pub fn atomic_write<P: AsRef<Path>, C: AsRef<[u8]>>(
    path: P,
    content: C,
) -> Result<(), InfrastructureError> {
    let parent = path.parent().unwrap_or_else(|| Path::new("."));
    let mut temp_file = tempfile::NamedTempFile::new_in(parent)?;
    temp_file.write_all(content.as_ref())?;
    temp_file.persist(path)?;
    Ok(())
}
```

---

## Rationale

### Why Same-Directory Temp File

The temp file is always created in **the same directory as the target** (`new_in(parent)`). This is critical: `rename` (the OS primitive underlying `persist`) is only guaranteed to be atomic when source and destination are on the **same filesystem**. Cross-device renames are non-atomic on most operating systems.

### Why Not Direct Write

A direct `File::create` + `write_all` is not atomic:
- If the process is killed after `create` but before `write_all` completes, a zero-byte or partial file exists at the target path.
- The next run reads a corrupt artifact, likely failing with a cryptic parse error.

### Idempotency

Because the final rename is atomic, any reader either sees the previous complete file or the new complete file — never a partial state.

---

## Consequences

**Positive:**
- Pipeline restarts after a crash always find valid, complete artifacts.
- CI pipelines are never broken by partial writes from a previous interrupted run.
- The `tempfile` crate handles tempfile cleanup (RAII drop) automatically if persistence fails.

**Negative:**
- Requires the target directory to exist before writing (Verity creates directories as needed).
- Adds a small I/O overhead (temp file creation + rename) compared to direct writes — acceptable given that compilation is not a hot loop.

---

## References

- `verity-core/src/infrastructure/fs.rs` — `atomic_write` implementation and unit tests.
- `tempfile` crate — `NamedTempFile` and `persist`.
