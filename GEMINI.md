# Verity — Development Configuration

## Project Context
@./.agents/rules/verity.md

## Superpowers Integration

Superpowers skills are the **primary methodology** for all development work in this repo.

**Process priority:**
1. Superpowers skills control the HOW (brainstorming → plans → TDD → subagent dev → review → finish)
2. Verity `.agents/skills/` provide domain-specific WHAT (Rust architecture, governance rules, query engine patterns)

Your project-specific personas (`.agents/skills/`) provide domain context but do NOT override Superpowers process skills.

When Superpowers and a Verity skill cover the same ground, **Superpowers takes precedence** for process. Verity skills are domain knowledge references only.

## Rust Verification

After every implementation cycle, these commands MUST pass:
```bash
cargo fmt --check
cargo clippy -- -D warnings
cargo test
```

## Rust Testing Stack (Verity-specific)

These are the Verity project conventions for testing — use them when Superpowers' TDD skill asks you to write tests:

| Tool | Purpose |
|------|---------|
| `cargo-nextest` | Fast parallel test execution (`cargo nextest run`) |
| `insta` | Snapshot testing for CLI output (`insta::assert_snapshot!`) |
| `criterion` | Benchmarking (`cargo bench`) |
| `proptest` | Property-based / fuzz testing |

**Test philosophy:**
- 70% Unit Tests, 20% Integration Tests, 10% E2E Tests
- Negative testing: always test the "Unhappy Path" (garbage input, network failures)
- Snapshot testing: lock CLI output with `insta` to catch UX regressions
- Docs-as-Code: every `rustdoc` example must compile (`cargo test --doc`)

## Task Clustering

When creating implementation plans with `writing-plans`, organize tasks using these domain clusters from the Verity engineering team:

| Cluster | Domain | Context Skill |
|---------|--------|---------------|
| **C1, C2...** | Rust Core & Architecture | `.agents/skills/rust_systems_engineer` |
| **G1, G2...** | Security & Governance | `.agents/skills/governance_architect` |
| **Q1, Q2...** | Query Engine & Arrow | `.agents/skills/query_engine_specialist` |
| **M1, M2...** | Machine Learning & Vectors | `.agents/skills/machine_learning_engineer` |
| **D1, D2...** | Developer Experience & CLI | `.agents/skills/developer_experience_specialist` |
| **P1, P2...** | Specs & Product Planning | `.agents/skills/technical_product_owner` |

## Dependency Management

All dependencies MUST be defined in the root `Cargo.toml` (`[workspace.dependencies]`). Never hardcode a version in a sub-crate `Cargo.toml`.

## Git Worktrees

Worktrees are enabled for feature isolation. Follow these rules to prevent parallel agent collisions:

1. **One worktree per feature branch** — never share a worktree between concurrent agents
2. **Sequential subagent execution** — when using `subagent-driven-development`, tasks execute one at a time (built-in to Superpowers)
3. **Parallel dispatch safety** — only use `dispatching-parallel-agents` when tasks touch **completely different files/crates** (e.g., `verity-core` vs `verity-bigquery`)
4. **Lock rule** — if two tasks might modify the same `Cargo.toml` or shared module, execute them **sequentially**, not in parallel
5. **Worktree location** — use `.worktrees/` at project root (auto-created, ensure it's in `.gitignore`)

## Real-World Testing Ground

The **data-foundation** repo (`~/Code/data-foundation`) is the live GCP project for testing Verity features in production conditions.

**Relationship:**
```
verity-governance-as-code/     ← You build Verity here (Rust source)
    └── verity-core/
    └── verity-bigquery/
    └── verity/                ← Python CLI wrapper

data-foundation/               ← You TEST Verity here (real GCP project)
    └── verity/                ← Verity project with real models & configs
        ├── verity_project_conf.yaml
        ├── config/connections.yml   (DataFusion local + BigQuery dev/prod)
        ├── models/                  (staging → intermediate → marts)
        └── data/                    (CSV/JSON test datasets)
    └── terraform/             ← GCP infra (BigQuery datasets, IAM, etc.)
    └── justfile               ← Commands: verity-run, verity-bq, verity-strict
```

**Key commands (from data-foundation/):**

| Command | What it does |
|---------|-------------|
| `just verity-run` | Run Verity pipeline locally (DataFusion) |
| `just verity-bq` | Run Verity against BigQuery dev dataset |
| `just verity-bq-prod` | Run Verity against BigQuery prod (strict mode) |
| `just verity-strict` | Run Verity with strict governance enforcement |
| `just verity-smoke-test` | Smoke test BigQuery connector auth |
| `just verity-lineage` | Check data lineage for PII leaks |

**E2E testing workflow:** After implementing a Verity feature, validate it against real data:
1. Build Verity: `cargo build` (in verity-governance-as-code/)
2. Test locally: `just verity-run` (in data-foundation/)
3. Test against BigQuery: `just verity-bq` (in data-foundation/)
4. Test strict governance: `just verity-strict` (in data-foundation/)

