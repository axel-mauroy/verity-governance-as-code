# Feature Bible - SQL Model Execution
**Date:** 2026-01-26
**Status:** READY FOR REVIEW

## 1. Analysis
The request is to "execute SQL models written in the user's project via CLI". 
This is the "Hello World" of Verity. It requires:
- **CLI**: A command `verity run`.
- **Config**: Reading `verity.yml` to find the project root.
- **Parser**: Reading `.sql` files.
- **Engine**: Executing these queries (using DataFusion context or connecting to a DB).

**Clarification**: Since Verity is "Governance first", we cannot just execute raw SQL. We must parse it into a `GovernedAsset` first.

## 2. The Simulation (The Council)

### DX Specialist
> "User Story: I create `models/my_report.sql`. I run `verity run`. It works.
> The CLI should look like: `verity run --select my_report`.
> The output should show a progress bar and a success table."

### Governance Architect
> "STOP. You cannot just run SQL. Does the `.sql` file have a header? Where are the metadata?
> I require that every `.sql` file starts with a YAML configuration block or has a companion `.yml` file.
> Constraint: If `owner` is missing, `verity run` MUST fail."

### Rust Systems Engineer (Query Specialist)
> "For local execution, we should use `DataFusion` (Apache Arrow) as the embedded engine.
> It can read CSV/Parquet as tables.
> Flow: 
> 1. Parse `verity.yml` to find data sources.
> 2. Register sources in `SessionContext`.
> 3. Parse the SQL model.
> 4. `ctx.sql(query).await?.collect().await?`."

### Feature Workflow Agent (Synthesis)
We will implement a **Local Execution Mode** using DataFusion.
Metadata will be enforced via a **Frontmatter** in the SQL file (like markdown) or a companion YAML. Let's go with **YAML Frontmatter** inside SQL comments for DX/Co-location.

## 3. The Blueprint (FDD)

### User Experience
**File**: `models/customers_enriched.sql`
```sql
/* 
config:
  name: customers_enriched
  owner: data_team@company.com
  materialized: view
  governance:
    public: true
*/
SELECT * FROM raw_customers WHERE age > 18
```

**Command**: `verity run`

### Architecture
1. **CLI Layer**: Parses args.
2. **Project Loader**: Scans `models/*.sql`.
3. **Parser**: Extracts the `/* config ... */` block (Regex or simple parsing).
4. **Governance Check**: Validates `owner` exists.
5. **DAG Builder**: Builds dependency graph (rudimentary for v1).
6. **Execution Engine**: Uses `datafusion::prelude::SessionContext` to run the SQL using registered CSV/Parquet sources.

## 4. The Backlog (Tasks)

- [ ] **Task 1 (DX - CLI)**: Implement `verity run` command in `src/cli`.
- [ ] **Task 2 (Config)**: Define the `ModelConfig` struct in `src/config` (serde).
- [ ] **Task 3 (Core - Parser)**: Implement a parser in `src/core` that extracts the YAML frontmatter from SQL files.
- [ ] **Task 4 (Governance)**: Implement a validator that checks mandatory fields in `ModelConfig`.
- [ ] **Task 5 (Engine)**: Create a basic `DataFusion` context wrapper in `src/core` that can execute a "SELECT 1" query.

---
*Signed by: Feature Workflow Agent*
