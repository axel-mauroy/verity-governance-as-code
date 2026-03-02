# Contributing to Verity

> *"Because compliance shouldn't be optional."*  
> This guide applies the same principle to contributions.

---

## Table of Contents

1. [Code of Conduct](#1-code-of-conduct)
2. [Before You Start](#2-before-you-start)
3. [Local Environment Setup](#3-local-environment-setup)
4. [Git Workflow](#4-git-workflow)
5. [Quality Gates](#5-quality-gates)
6. [The Gauntlet: Full Local CI](#6-the-gauntlet-full-local-ci)
7. [Architecture Rules](#7-architecture-rules)
8. [Code Style](#8-code-style)
9. [Testing Requirements](#9-testing-requirements)
10. [Pull Request Process](#10-pull-request-process)
11. [CODEOWNERS & Review Policy](#11-codeowners--review-policy)
12. [Security Policy](#12-security-policy)

---

## 1. Code of Conduct

Be respectful, constructive, and professional. Contributions of all sizes are welcome — from fixing a typo in a doc to adding a new SQL engine adapter.

---

## 2. Before You Start

- Check the [open issues](https://github.com/axel-mauroy/verity-governance-as-code/issues) to avoid duplicate work.
- For significant changes (new features, architectural changes), **open a discussion or issue first**.
- Read the [Implementation Guide](docs/IMPLEMENTATION_GUIDE.md) and the relevant [ADRs](docs/adr/) before touching core modules.

---

## 3. Local Environment Setup

### Prerequisites

```bash
# Rust stable toolchain (2024 edition) + components
rustup update stable
rustup component add rustfmt clippy

# Security tools
cargo install cargo-deny cargo-audit

# Optional but recommended: faster test runner
cargo install cargo-nextest
```

### Install hooks

```bash
# Register the githooks directory
git config core.hooksPath .githooks

# Make hooks executable
chmod +x .githooks/pre-commit .githooks/full_verify.sh .githooks/deny_unsecure.sh
```

> The `pre-commit` hook runs the **Zero-Panic Guard** and the **Full Verify** script before every commit. This prevents broken code from ever entering the branch.

### Build & Run

```bash
# Build the CLI
cargo build --bin verity

# Run an example
cd examples/basic_rag_pipeline
../../target/debug/verity run
```

---

## 4. Git Workflow

```
main        ← Protected. Merges from uat only. Production releases.
  └── uat   ← Protected. Merges from develop. Strict mode enforced.
        └── develop ← Integration branch. All PRs target here.
              └── feat/my-feature  ← Your feature branch.
```

**Branch naming:**

| Type | Pattern | Example |
|---|---|---|
| Feature | `feat/short-description` | `feat/add-redshift-connector` |
| Bug fix | `fix/short-description` | `fix/dag-cycle-detection` |
| Documentation | `docs/short-description` | `docs/update-adr-005` |
| Refactor | `refactor/short-description` | `refactor/pii-scanner-perf` |

**Steps:**

```bash
git checkout develop
git pull origin develop
git checkout -b feat/my-feature
# ... code ...
git add -p                          # Review each chunk before staging
git commit -m "feat: add X"         # Conventional commits format
git push origin feat/my-feature
# Open PR → develop on GitHub
```

---

## 5. Quality Gates

Every contribution must pass **all four gates**. They run automatically in CI and locally via the pre-commit hook.

### Gate 1 — 🎨 Code Quality

```bash
# Format (must pass — zero diff allowed)
cargo fmt --all -- --check

# Lint (zero warnings tolerated)
cargo clippy --workspace --all-targets -- -D warnings
```

Fix formatting with:
```bash
cargo fmt --all
```

### Gate 2 — 🛡️ Security & License

```bash
# License and dependency policy
cargo deny check

# Known CVE audit
cargo audit
```

Dependency policy is defined in `deny.toml`. If `cargo deny` blocks a new dependency:
1. Open `deny.toml` and review the relevant section.
2. Add an explicit exception with a justification comment only if it is warranted.
3. Never add exceptions for `allow = "skip"` without a reviewer approval.

### Gate 3 — 🔒 Zero-Panic Guard

```bash
./.githooks/deny_unsecure.sh
```

This script scans all production Rust files (`verity-core/src/`, `verity/src/`) for **forbidden patterns**:

| Pattern | Required Alternative |
|---|---|
| `.unwrap()` | Use `?` with `Result<T, E>` |
| `.expect("...")` | Use `?` or a domain-specific error |
| `panic!(...)` | Return `Err(...)` |

**Exceptions:**
- Test code annotated with `#[cfg(test)]` or `#[allow(clippy::unwrap_used)]` is excluded.
- Use the `// allow-panic` inline comment sparingly and only with a justification comment.

### Gate 4 — 🧪 Tests

```bash
# Unit tests (prefer nextest for speed)
cargo nextest run --workspace
# or
cargo test --workspace
```

---

## 6. The Gauntlet: Full Local CI

Before pushing to a shared branch, run the full local CI simulation:

```bash
./.githooks/full_verify.sh
```

This script runs **all four gates sequentially** plus the E2E pipeline examples:

```
1️⃣  Quality Gate    → cargo fmt --check + cargo clippy -D warnings
2️⃣  Security Gate   → cargo deny check + cargo audit + deny_unsecure.sh
3️⃣  Logic Gate      → cargo nextest run --workspace
4️⃣  E2E Gate        → verity run (basic_rag_pipeline + ml_pipeline)
```

If "The Gauntlet" passes, your code is ready for a PR.

---

## 7. Architecture Rules

These are **non-negotiable** invariants that PRs will be rejected for violating:

### Rule 1: Domain Layer has Zero I/O

No file system access, no database calls, no async in `verity-core/src/domain/`.  
All domain functions must be **pure** (same input → same output, no side effects).

```rust
// ✅ Correct
pub fn apply_masking(sql: &str, node: &ManifestNode) -> Result<String, DomainError>

// ❌ Violation — I/O in domain
pub fn apply_masking(sql: &str, path: &Path) -> Result<String, DomainError> {
    let content = fs::read_to_string(path)?; // NOT ALLOWED
    ...
}
```

### Rule 2: Cross-Layer Injection via Ports Only

Application code must interact with infrastructure only through **trait interfaces** defined in `ports/`.  
Never import an infrastructure type directly in `application/` or `domain/`.

```rust
// ✅ Correct — depends on the trait, not the concrete type
pub async fn run_pipeline(connector: &dyn Connector, ...) { ... }

// ❌ Violation — concrete adapter in application layer
use crate::infrastructure::adapters::DataFusionConnector;
pub async fn run_pipeline(connector: &DataFusionConnector, ...) { ... }
```

### Rule 3: All File Writes Must Be Atomic

Any new file write in `infrastructure/` or `application/` must use `atomic_write`:

```rust
use crate::infrastructure::fs::atomic_write;
atomic_write(path, content)?;  // ✅

std::fs::write(path, content)?;  // ❌ Not atomic
```

### Rule 4: No Silent Governance Bypass

Code that modifies governance enforcement (policy types, guard logic, strict mode) requires explicit sign-off from a CODEOWNER. Leave a `// GOVERNANCE: <justification>` comment if modifying guard logic.

---

## 8. Code Style

### Error Handling

- Use `thiserror` for library errors (`DomainError`, `InfrastructureError`, `VerityError`).
- Use `anyhow` only in tests and binary (`verity/src/main.rs`).
- Propagate errors with `?`. Never `.unwrap()` in production code.
- Error messages should be user-actionable: explain *what* failed and *why*.

```rust
// ✅ Good error message
Err(DomainError::CircularDependency(format!(
    "Dangling Reference: Model '{}' depends on '{}' which does not exist.",
    current_node, parent_node
)))

// ❌ Unhelpful
Err(DomainError::CircularDependency("error".to_string()))
```

### Naming Conventions

| Construct | Convention | Example |
|---|---|---|
| Types / Traits | `PascalCase` | `PolicyRewriter`, `GovernanceGuard` |
| Functions / methods | `snake_case` | `apply_masking`, `plan_execution` |
| Constants | `SCREAMING_SNAKE_CASE` | `NS_VERITY`, `NS_DCAT` |
| Modules | `snake_case` | `governance`, `graph`, `compliance` |

### Performance Conventions

- Use `Vec::with_capacity(n)` when the size is known.
- Prefer `Cow<'a, str>` over `String` for read-only string data in hot paths.
- Prefer lifetime-annotated references (`&'a str`) over owned `String` in pure domain functions to avoid heap allocation.
- Pre-compile `Regex` at initialization — never inside a loop.

---

## 9. Testing Requirements

### Unit Tests

Every new module must have a `#[cfg(test)]` block. Minimum coverage expectations:

| Module | Required Coverage |
|---|---|
| `domain/` (all) | High — pure functions are trivial to test |
| `application/` | Medium — focus on error paths and edge cases |
| `infrastructure/` | Medium — test config parsing, atomic writes |

**Test structure:**

```rust
#[cfg(test)]
#[allow(clippy::unwrap_used)]  // Allowed in tests only
mod tests {
    use super::*;
    use anyhow::Result;

    #[test]
    fn test_my_feature_happy_path() -> Result<()> {
        // Arrange
        let input = ...;
        // Act
        let result = my_function(input)?;
        // Assert
        assert_eq!(result, expected);
        Ok(())
    }

    #[test]
    fn test_my_feature_error_case() {
        let result = my_function(bad_input);
        assert!(result.is_err());
    }
}
```

### Integration / E2E Tests

New example pipelines or CLI commands require an E2E test that runs `verity run` against a real example directory. Use `assert_cmd` in `verity/tests/`.

### Snapshot Tests

Use `insta` for snapshot testing of deterministic outputs (JSON-LD catalog, compiled SQL). Run `cargo insta review` to review and accept new snapshots.

---

## 10. Pull Request Process

### PR Checklist

Before opening a PR, self-review against this checklist:

- [ ] `./.githooks/full_verify.sh` passes locally.
- [ ] New code follows the [Architecture Rules](#7-architecture-rules).
- [ ] Unit tests added for all new domain logic.
- [ ] No `.unwrap()` or `.expect()` in production code (Zero-Panic Guard passes).
- [ ] Doc comment (`///`) added for all public types and functions.
- [ ] If the change affects a governance rule: unit test proves the violation is caught in strict mode.
- [ ] If a new dependency is added: justified in the PR description, and `cargo deny check` passes.
- [ ] PR description explains **what** and **why**, not just **how**.

### PR Template

```markdown
## Summary
<!-- One paragraph explaining what this PR does and why. -->

## Changes
- [ ] New feature / Bug fix / Refactor / Docs
- List key files changed

## Testing
<!-- How was this tested? Local gauntlet? New unit tests? E2E? -->

## Related Issues
Closes #...
```

### CI Pipeline

PRs trigger the GitHub Actions CI, which runs:

| Job | Depends On | What It Runs |
|---|---|---|
| `lints` | — | `cargo fmt --check` + `cargo clippy -D warnings` |
| `security` | — | `cargo deny` + `cargo audit` + Gitleaks + Zero-Panic Guard |
| `test` | `lints` | `cargo tarpaulin` (unit tests + coverage) |
| `build` | `lints` | `cargo build --release --bin verity` |
| `e2e` | `build` | `verity run` on both example pipelines |

**All jobs must be green before a PR can be merged.**

---

## 11. CODEOWNERS & Review Policy

Defined in `.github/CODEOWNERS`. Key areas requiring `@axel-mauroy` approval:

| Path | Rationale |
|---|---|
| `verity-core/src/domain/governance/` | Governance rules are security-critical |
| `verity-core/src/domain/compliance/` | Anomaly detection affects data quality guarantees |
| `verity-core/src/ports/` | Port changes affect all engine adapters |
| `verity-core/src/infrastructure/adapters/` | SQL engine integrations |
| `.github/` | CI/CD and security workflows |

---

## 12. Security Policy

### Reporting a Vulnerability

Do **not** open a public GitHub issue for security vulnerabilities. Instead:

1. Email the maintainer directly (see `Cargo.toml` authors field).
2. Include a clear description of the vulnerability and reproduction steps.
3. Allow 48 hours for an initial response before public disclosure.

### Dependency Security

- `cargo audit` runs on every CI build.
- `cargo deny check` enforces license compatibility and bans known-problematic crates.
- Gitleaks scans every PR for accidentally committed secrets.
- The Zero-Panic Guard (`deny_unsecure.sh`) prevents `.unwrap()`/`.expect()`/`panic!` from reaching production code.

---

*Thank you for contributing to Verity. Every line of code that enforces governance is a line that protects someone's data.*
