# ADR-009 — Pre-Compiled Regex with Fail-Fast Initialization for PII Scanner

**Status:** Accepted  
**Date:** 2026-03-02  
**Deciders:** Verity Core Team  

---

## Context

The `PiiScanner` must scan column values and SQL query content against a configurable set of PII detection patterns (email, SSN, credit card numbers, etc.) loaded from `config/policies.yml`.

Design decisions required:
1. **When** to compile the regex patterns (at startup vs. on each scan call).
2. **What** to do when a pattern in the config file has an invalid regex syntax.
3. **How** to structure the scan result to avoid unnecessary heap allocations.

---

## Decision

**Regex patterns are compiled exactly once during `PiiScanner::new`, and an invalid regex in the config is a hard initialization error that halts the pipeline.**

---

## Rationale

### Pre-Compilation at Initialization

Compiling a `Regex` from a string is an expensive operation (NFA/DFA construction). Compiling on every `scan()` call — which runs per column value in the pipeline — would make scanning prohibitively slow.

The `CompiledPattern` struct caches the compiled `Regex` alongside its metadata:

```rust
struct CompiledPattern {
    name: String,
    regex: Regex,
    severity: PiiSeverity,
    action: PiiAction,
}
```

`PiiScanner::new` compiles all patterns upfront with pre-allocated capacity:

```rust
let mut compiled_patterns = Vec::with_capacity(config.patterns.len());
```

### Fail-Fast on Invalid Regex

A malformed regex in `policies.yml` is a **governance configuration error** — not a warning. If a security policy cannot be compiled, the scanner cannot enforce it, and the pipeline should not start:

```rust
Err(e) => {
    return Err(DomainError::GovernanceViolation {
        _asset_name: format!("Config Regex: {}", pattern.name),
        child_level: "Invalid Syntax".to_string(),
        parent_level: e.to_string(),
    });
}
```

This is consistent with the Zero-Trust Compilation philosophy: a broken security policy is a compile error.

### Zero-Copy Violation Struct

The `scan()` result uses lifetime annotations to borrow from the input text instead of cloning:

```rust
pub struct Violation<'a> {
    pub rule_name: &'a str,         // Borrowed from CompiledPattern
    pub matched_value: &'a str,     // Borrowed from the scanned text
    pub severity: PiiSeverity,
    pub action: PiiAction,
}

pub fn scan<'a>(&'a self, text: &'a str) -> Vec<Violation<'a>>
```

`matched_value` is the raw match slice from the input string — no heap allocation for the matched substring.

### Disabled Scanner Short-Circuit

When PII detection is disabled in the config, `PiiScanner::new` returns an empty scanner immediately, and `scan()` returns an empty `Vec` without iterating any patterns:

```rust
if !config.enabled {
    return Ok(Self { patterns: vec![], enabled: false });
}
// ...
pub fn scan<'a>(&'a self, text: &'a str) -> Vec<Violation<'a>> {
    if !self.enabled { return vec![]; }
    // ...
}
```

---

## Consequences

**Positive:**
- Scanning is O(n_patterns) per text value with no regex compilation overhead.
- Invalid security configs are detected before any pipeline I/O occurs.
- Zero heap allocations for violation metadata on the scan hot path.

**Negative:**
- Pattern changes require restarting Verity (no hot-reload of `policies.yml`). This is acceptable — governance policies are not expected to change mid-run.

---

## References

- `verity-core/src/domain/governance/scanner.rs` — `PiiScanner`, `CompiledPattern`, `Violation`.
- `verity-core/src/domain/governance/guard.rs` — `GovernanceGuard` consuming scanner output.
- `examples/basic_rag_pipeline/config/policies.yml` — PII pattern configuration.
