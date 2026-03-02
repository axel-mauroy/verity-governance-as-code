# ADR-004 — PII Masking via SQL CTE Wrapper (PolicyRewriter)

**Status:** Accepted  
**Date:** 2026-03-02  
**Deciders:** Verity Core Team  

---

## Context

When a column is tagged with a governance policy (`hash`, `redact`, `mask_email`, etc.), Verity must apply that masking automatically. The key design question was **how** to inject the masking into the SQL without modifying the analyst's original business logic.

Options considered:

1. **In-place modification**: Parse the analyst's SQL AST and rewrite the relevant column expressions.
2. **CTE Wrapper**: Wrap the original SQL in a CTE and apply masking in the outer SELECT.
3. **View-layer governance**: Create a separate masking view on top of the raw materialized table.

---

## Decision

**Verity uses a CTE wrapper strategy. The original SQL is placed unchanged inside a `verity_governance_cte`, and a governance SELECT wraps it.**

```sql
-- target/run/staging/stg_users.sql  (auto-generated)
WITH verity_governance_cte AS (
    -- Original analyst SQL, completely untouched
    SELECT user_id, email, name FROM "raw_users"
)
SELECT
    user_id,
    encode(sha256(CAST(email AS VARCHAR)), 'hex') AS email,
    'REDACTED' AS name
FROM verity_governance_cte
```

The governance-applied SQL is written to `target/run/`. The original compiled SQL remains at `target/compiled/` for debugging and audit purposes.

---

## Rationale

### Option 1 Rejected: In-Place AST Rewriting

Modifying the analyst's SQL AST is fragile and complex:
- SQL dialects differ (DataFusion vs DuckDB have different AST representations).
- Column aliasing, subqueries, and CTEs make position-based rewriting error-prone.
- A bug in the rewriter could silently corrupt business logic.

The `sqlparser` crate was found to have version-incompatible API changes across minor versions (observed during the DuckDB → DataFusion migration), making AST-based rewriting a high-maintenance surface.

### Option 2 Accepted: CTE Wrapper

The CTE wrapper approach is:
- **Safe**: The original SQL is never modified. A governance bug can only affect the outer SELECT.
- **Transparent**: Two directories (`compiled/` vs `run/`) clearly separate business logic from governance logic. Analysts debug using `compiled/`; production uses `run/`.
- **Engine-agnostic**: Standard SQL CTE syntax is supported by all target engines.
- **Verifiable**: The governance layer is always a simple, readable projection over the CTE.

### Masking Strategies

```rust
// domain/governance/rewriter.rs
MaskingStrategy::Hash    => "encode(sha256(CAST({col} AS VARCHAR)), 'hex') AS {col}",
MaskingStrategy::Redact  => "'REDACTED' AS {col}",
MaskingStrategy::MaskEmail => "regexp_replace({col}, '(^.).*(@.*$)', '\\1****\\2') AS {col}",
MaskingStrategy::Nullify => "NULL AS {col}",
MaskingStrategy::Partial => "concat(left(CAST({col} AS VARCHAR), 2), '***') AS {col}",
MaskingStrategy::EntityPreserving => "concat('[PRESERVED_', length(CAST({col} AS VARCHAR)), ']') AS {col}",
```

The `Drop` policy is special: the column is simply omitted from the SELECT clause entirely.

### Edge Case: All Columns Dropped

If all columns are `Drop`-policy columns, generating `SELECT FROM cte` would be invalid SQL. A guard produces a zero-row sentinel:

```rust
"SELECT 1 AS _verity_empty FROM verity_governance_cte LIMIT 0"
```

---

## Consequences

**Positive:**
- Analyst SQL is never modified — audit trail is clean.
- Adding a new masking strategy is a single pattern addition in `rewriter.rs`.
- Engine-level governance optimizations are possible via `supports_plan_governance()` on the `Connector` trait.

**Negative:**
- The CTE adds a query planning step — minimal overhead, but not zero.
- Column lists in the schema YAML must be complete for masking to work; if a column is missing from the schema, it passes through unmasked (mitigated by auto-schema propagation).

---

## References

- `verity-core/src/domain/governance/rewriter.rs` — `PolicyRewriter::apply_masking`.
- `verity-core/src/ports/connector.rs` — `supports_plan_governance()` future hook.
- `docs/PRD.md` — FR-02 (Auto PII Masking).
