# Narrative: The First 5 Minutes of Verity

**Date**: 2026-01-27  
**Author**: Technical Product Owner  
**Objective**: Demonstrate the "Verity Difference" - Speed, Safety, and Joyful DX.

---

## Minute 1: The "Single Binary" Shock (Installation)

The user, a frustrated Analytics Engineer tired of Python dependency hell, opens their terminal. They've heard Verity is the "Rust successor to dbt."

**Action**:
```bash
curl -fsSL https://get.verity.dev | sh
```

**Result**:
- In less than 10 seconds, the binary is downloaded and moved to `/usr/local/bin`.
- No `pip install`, no `virtualenv`, no `Requirement already satisfied` loops.
- The user types `verity --version` and gets an instant response: `verity 0.1.0`.

> **UX Note**: This is the first "Aha!" moment. The speed of a compiled Rust binary vs. a heavy Python package sets the tone.

---

## Minute 2: The "Clean Slate" (Initialisation)

The user wants to start a new project.

**Action**:
```bash
verity init my_analytics
cd my_analytics
```

**Interaction**:
- Verity asks: `Which data platform are you targeting? [DuckDB, Snowflake, BigQuery]`
- User selects: `DuckDB` (for local speed).
- Verity creates the structure:
    - `verity.yaml` (Project & Compliance settings)
    - `models/` (Where the magic happens)
    - `compliance/` (Governance as Code)
    - `.verity/` (Metadata & Cache)

**Result**:
- The project is ready. The user opens VS Code and sees a familiar but cleaner structure.

---

## Minute 3-4: The "Safe by Design" Config (Setup)

The user wants to transform some raw data. They create `models/staging/stg_customers.sql`.

**Action**:
1. User writes a simple SQL: `SELECT * FROM raw.customers`.
2. User opens `verity.yaml` to define the model.

**Experience**:
- Instead of just defining the table, Verity's YAML schema *forces* a safety thought process.
- **Config Snippet**:
  ```yaml
  models:
    - name: stg_customers
      description: "Cleaned customer data"
      columns:
        - name: email
          policy: pii_masking # Verity suggests this if 'email' is in the name
  ```

> **UX Note**: The CLI/IDE extension proactively warns: *"Column 'email' looks like PII. No compliance policy defined. Defaulting to 'RESTRICTED'."* This is "Safety as a Feature."

---

## Minute 5: The "Joyful Execute" (First Run)

The user is ready to see it in action.

**Action**:
```bash
verity run
```

**Result**:
- **Visuals**: A high-performance progress bar.
- **Intelligence**: Verity doesn't just run SQL. It runs a pre-flight "Audit Scan."
- **Output**:
  ```text
  [✔] Pre-flight: 0 Governance Violations detected.
  [▶] Running stg_customers...
  [✔] Completed in 0.4s (34,000 rows/sec)
  
  SUMMARY:
  - Models: 1 Success, 0 Failed
  - Compliance: GDPR-Ready (Masking applied to 'email')
  - Performance: 15x faster than legacy tools
  ```

**Conclusion**:
The user sits back. In 5 minutes, they installed a tool, initialized a project, defined a transformation with security policies, and ran it. No errors, no friction, just pure engineering joy.

---

## Key Takeaways for Engineers:
- **Systems Engineer**: The binary must be lean and ship with embedded drivers (DuckDB/SQLite) for the "Zero Config" start.
- **DX Engineer**: The `init` flow needs to be interactive and the `run` output must be beautiful and informative.
- **Governance Architect**: The "PII suggestion" and "Pre-flight Audit" are the USP (Unique Selling Points).
