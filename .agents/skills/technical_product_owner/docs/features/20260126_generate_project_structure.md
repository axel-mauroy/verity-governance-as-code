# Feature Bible - Generate Project Structure
**Date:** 2026-01-26
**Status:** READY FOR REVIEW

## 1. Analysis
The request is to "Generate the project structure". This sets the foundational architecture of `verity`.
This involves:
- **Core**: Defining modules (CLI, Config, DAG, Engine).
- **DX**: Ensuring the structure is intuitive for contributors.
- **Governance**: Where do we enforce rules?
- **ML**: Where do embedding models live?

## 2. The Simulation (The Council)

### DX Specialist
> "We need a structure that screams 'Professional Rust Tool'. Standard `src/bin` for the CLI entry point, and a library crate in `src/lib.rs` for logic. We need clear separation: `config` module for parsing YAML, `cli` for Clap commands."

### Rust Systems Engineer
> "Agreed. We need a workspace or a modular monolith. Given it's a single binary, let's stick to modules for now but keep them decoupled.
> - `src/main.rs`: minimal shim.
> - `src/lib.rs`: exports modules.
> - `src/core`: The DAG engine (tokio based).
> - `src/storage`: Arrow/Parquet integration.
> - `src/connectors`: Data sources."

### Governance Architect
> "I need a dedicated module `src/governance`. It must contain the `SecurityLevel` enums and the `GovernedAsset` trait. This module should have minimal dependencies so everyone can import it without bloat. It's the 'Law'."

### ML Engineer
> "I need `src/ml` or `src/vectors`. This is where `candle` logic resides. It needs to be feature-gated (e.g., `#[cfg(feature = "cuda")]`) so we don't compile torch/candle on standard builds if not needed."

### Feature Workflow Agent (Synthesis)
The structure will be a **Modular Monolith** pattern in a single Crate (for now), with potential upgrade to a Workspace later.
Key constraint: **Cyclic Dependencies**. `core` depends on `governance`, `connectors` depend on `core`.

## 3. The Blueprint (Feature Design Document)

### Directory Structure
```text
verity/
├── Cargo.toml          # Workspace or Package definition
├── README.md
├── .gitignore
├── src/
│   ├── main.rs         # Entry point (calls cli::run())
│   ├── lib.rs          # Module exports
│   ├── cli/            # [DX] Clap definitions, Error reporting
│   ├── config/         # [DX] Serde Structs (verity.yml)
│   ├── governance/     # [Auth] Enum SecurityLevel, Traits
│   ├── core/           # [Sys] DAG Engine, Scheduler
│   ├── connectors/     # [Sys] Source integrations (Postgres, S3)
│   ├── storage/        # [Sys] Arrow/Parquet writers
│   └── ml/             # [ML] Embedding pipelines
└── tests/              # [QA] Integration tests
```

### Module Responsibilities
- **governance**: The base dependency. Defines types.
- **config**: Depends on `governance`. Parses user input into types.
- **core**: Depends on `config`. executed the plan.
- **cli**: The outer layer. Depends on everything.

## 4. The Backlog (Tasks)

- [ ] **Task 1 (Systems - P0)**: Initialize `src/lib.rs` with empty modules (`pub mod governance;`, `pub mod core;` etc.).
- [ ] **Task 2 (Governance - P0)**: Create `src/governance/mod.rs` and define the empty `SecurityLevel` enum to lock in the architecture.
- [ ] **Task 3 (DX - P0)**: Set up `src/cli/mod.rs` with a basic Clap skeleton (`verity run`, `verity check`).
- [ ] **Task 4 (Workflow - P1)**: Create placeholder `README.md` for each module explaining its purpose (Documentation Driven Development).

---
*Signed by: Feature Workflow Agent*
