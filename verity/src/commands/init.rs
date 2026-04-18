// verity/src/commands/init.rs
//
// USE CASE: Bootstrap a new Verity project with best practices, governance mindset and examples.

use anyhow::{Context, Result};
use std::fs;
use std::path::{Path, PathBuf};

pub fn execute(project_name: String, path: PathBuf) -> Result<()> {
    println!(
        "🆕 Initializing Verity project: {} in {:?}",
        project_name, path
    );

    // 1. Create directory structure
    let dirs = [
        "models/staging",
        "models/intermediate",
        "models/marts",
        "data/raw",
        "config",
    ];

    for dir in dirs {
        let full_path = path.join(dir);
        fs::create_dir_all(&full_path)
            .with_context(|| format!("Failed to create directory: {:?}", full_path))?;
    }

    let conf_content = format!(
        r#"# verity_project_conf.yaml
# Global Project Configuration
# 
# Verity enforces Governance-by-Design. This configuration file determines
# how your data pipeline will behave regarding PII masking, schema drift, and data quality.
# 
# - strict_mode: When true, the pipeline will fail if an undocumented column is detected
#   or if a governance violation occurs (like exposing a PII column in a public model).
#   Use `strict: false` in dev, and `true` in CI/CD and Production.

name: {}
version: "0.1.0"
profile: dev
target-path: target

config-paths: ["config"]
model-paths: ["models"]

defaults:
  staging:
    materialized: view
  intermediate:
    materialized: view
  marts:
    materialized: table

governance:
  # Enable global PII detection. See config/policies.yml for regex patterns.
  pii_detection:
    enabled: true
  
  # Enable data quality checks (e.g., unique, not_null, anomaly detection).
  data_quality:
    enabled: true
  
  # Threshold for Row Count Anomaly detection. (0.10 means a 10% change is flagged).
  default_anomaly_threshold: 0.10

  # Fails the build on governance violations. Set to false for local dev.
  strict: false
"#,
        project_name
    );

    fs::write(path.join("verity_project_conf.yaml"), conf_content)?;

    // 3. Create config/policies.yml template
    let policies_content = r#"# config/policies.yml
# Governance Strategy: PII Detection & Auto-Masking
#
# Verity enforces Data Governance at compile time (Zero-Trust Schema).
# This file defines two main concepts:
#
# 1. PII Patterns (Regex scanning)
#    Verity can scan sample data against these regexes.
#    If a pattern matches, Verity will enforce that the column has a policy defined in the model schema.
#    If a PII column is undocumented, the pipeline build will fail (in strict mode).
#
# 2. Column Policies (Auto-Masking Rules)
#    You can define default policies based on regex matches of column names.
#    For instance, any column name matching `(?i)email` will automatically be assigned the `hash` policy
#    if not overridden in the specific model's `.yml`.

pii_detection:
  enabled: true
  patterns:
    - name: "email"
      regex: "^[A-Za-z0-9_.+-]+@[A-Za-z0-9-]+\\.[A-Za-z0-9-.]+$"
      severity: high
      action: warn
    - name: "ssn"
      regex: "^\\d{3}-\\d{2}-\\d{4}$"
      severity: critical
      action: block
      masking_strategy: redact
  column_policies:
    - column_name_pattern: "(?i)email"
      policy: hash
    - column_name_pattern: "(?i)ssn"
      policy: redact
"#;
    fs::write(path.join("config/policies.yml"), policies_content)?;

    // 4. Create config/quality.yml template
    let quality_content = r#"# config/quality.yml
# Governance Strategy: Data Quality
#
# Define your global data quality rules here. These checks are executed *after* materialization.
# - unique: Ensures no duplicate values exist.
# - not_null: Ensures no NULL values exist.

data_quality:
  enabled: true
  rules:
    - name: global_unique
      description: "Default uniqueness check"
      severity: error
    - name: global_not_null
      description: "Default nullability check"
      severity: error
"#;
    fs::write(path.join("config/quality.yml"), quality_content)?;

    // 5. Create .gitignore
    let gitignore_content = r#"# Verity build artifacts
target/
*.parquet
*.db
*.duckdb
"#;
    fs::write(path.join(".gitignore"), gitignore_content)?;

    // 6. Create Governance Best Practices README
    let readme_content = r#"# Welcome to your Verity Project

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
"#;
    fs::write(path.join("README.md"), readme_content)?;

    // 7. Create a Sample Model
    create_sample_model(&path)?;

    println!("✅ Project {} created successfully!", project_name);
    println!("\nNext steps:");
    println!("  1. Put your source CSVs in data/raw/");
    println!("  2. Run 'verity generate' to detect and register sources");
    println!("  3. Add your SQL models in models/");
    println!("  4. Run 'verity run' to execute the pipeline");
    println!("\nRead the generated README.md to understand the Verity Mindset.");

    Ok(())
}

fn create_sample_model(path: &Path) -> Result<()> {
    // We create a dummy sources.yaml to make it work out of the box
    let sources_yaml = r#"# Auto-generated by init, normally managed by `verity generate`
sources:
  - name: dummy_source
    path: data/raw/dummy.csv
    governance:
      public: true
      pii: false
      security: internal
"#;
    fs::write(path.join("models/sources.yaml"), sources_yaml)?;

    // Create a dummy CSV so the model can read something
    let dummy_csv = "user_id,email,age\n1,alice@example.com,30\n2,bob@example.com,25\n";
    fs::write(path.join("data/raw/dummy.csv"), dummy_csv)?;

    let model_sql = "SELECT user_id, email, age FROM dummy_source";
    let model_yaml = r#"version: 1
models:
  - name: stg_dummy_users
    description: "An example model demonstrating Verity auto-masking."
    owner: data-team
    security_level: internal
    columns:
      - name: user_id
        description: "Primary key"
        tests: [unique, not_null]
      
      - name: email
        description: "User's email, automatically masked by Verity"
        policy: hash
        tests: [not_null]

      - name: age
        description: "User's age"
"#;

    fs::write(path.join("models/staging/stg_dummy_users.sql"), model_sql)?;
    fs::write(path.join("models/staging/stg_dummy_users.yml"), model_yaml)?;
    Ok(())
}
