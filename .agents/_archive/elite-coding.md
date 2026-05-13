---
description: End-to-end workflow to implement features quickly while ensuring elite Rust performance and strict governance.
---

# Elite Performance Workflow

This workflow guides the agent through an optimized, multi-agent process to deliver fast, highly performant, and reliable code.

## 1. Specification & Security Audit
Read the user's feature request. Activate the `technical-product-owner` and `governance-architect` skills. Write a brief analysis defining the precise scope, identifying any potential data leaks or access control (RBAC) issues, and translating the work into specialized skill clusters (e.g., C1 - Core, G1 - Governance). Pause and wait for the user to confirm this spec.

## 2. Test-Driven Setup
Activate the `quality-assurance-engineer` skill. Draft the required integration tests, Rust unit tests, or snapshot tests (`insta`) that will guarantee the feature's reliability. Plan the necessary updates to the `mdBook` documentation.

## 3. High-Performance Implementation
Activate the appropriate technical skills (e.g., `rust-systems-engineer`, `query-engine-specialist`, `machine-learning-engineer`). Write the implementation code. You must enforce zero-copy data manipulation where possible, strictly avoid `unwrap()` or `panic!()`, and structure all errors cleanly using `miette` diagnostics as per the `developer-experience-specialist` standards.

## 4. Compilation & Verification
// turbo-all
Run the following terminal commands to guarantee code quality and elite reliability:
- `cargo fmt`
- `cargo clippy -- -D warnings`
- `cargo test`

## 5. Self-Correction & Handoff
Review the output from the compiler and the linter. If there are errors, fix them recursively. Once the test suite is entirely green, complete the task by generating a Walkthrough artifact summarizing the final architecture, the performance footprint, and the new compliant feature.
