# Welcome to your Verity Project

**Verity: Governance-by-Design Data Pipelines**

This project has been bootstrapped with a standard Verity structure.

## The Verity Mindset

Data Governance is not an afterthought; it is enforced at compile time. 
Verity refuses to compile or run your data pipeline if governance guidelines are not met.

1. **Zero-Trust Schema**: If a column exists in the database but is not documented in your `.yml` schema, Verity treats it as an anomaly. In strict mode, the build fails.
2. **Auto-Masking**: If you tag a column with a policy (e.g., `policy: hash`), Verity automatically injects the masking logic into the compiled SQL. You don't need to manually write `sha256(email)`.
3. **Security Levels**: Models are categorized by security level (`public`, `internal`, `confidential`, `restricted`). You cannot expose a `confidential` column in a `public` model without applying a masking policy.

## Directory Structure

*   `config/`: Contains global configuration like PII regex patterns (`policies.yml`) and data quality rules (`quality.yml`).
*   `data/raw/`: Place your raw data files (CSV, Parquet) here.
*   `models/`: Your SQL transformations and YAML schemas.
    *   `staging/`: 1:1 mapping of raw data, applying initial masking and renaming.
    *   `intermediate/`: Joins and business logic.
    *   `marts/`: Final, aggregated datasets ready for consumption.

## First Steps

1.  Place a sample `.csv` file in `data/raw/`
2.  Run `verity generate` to scan the data and create a `models/sources.yaml` file.
3.  Look at the example model in `models/staging/`.
4.  Run `verity run` to execute your pipeline!
5.  Check the compiled artifacts in `target/`
