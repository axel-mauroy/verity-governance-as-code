# Using Verity in Independent Pipelines

Verity is designed not just as a library, but as a standalone Data Contract & Transformation engine. Because Verity is written in Rust, it compiles into a single executable binary. You do not need Java, Python virtual environments, or Docker to run it.

You can bootstrap a Verity project anywhere in your organization and integrate it into any CI/CD pipeline efficiently and securely.

## Getting Started

### 1. Installation

Install the `verity` CLI tool directly via Cargo (a pre-compiled binary distribution mechanism is also possible for CI without Rust toolchains):

```bash
cargo install --git https://github.com/axel-mauroy/verity-governance-as-code
```

### 2. Bootstrapping a Project

Use the `init` command to scaffold a new project with best-practice configurations for Governance-by-Design:

```bash
verity init my_data_project --path ./my_data_project
cd my_data_project
```

This will generate the following structure:
```
my_data_project/
├── config/
│   ├── policies.yml            # PII Regex definitions
│   └── quality.yml             # Global data quality rules
├── data/
│   └── raw/                    # Place source CSV/Parquet files here
├── models/
│   ├── staging/                # Dummy pipeline included!
│   ├── intermediate/
│   └── marts/
├── verity_project_conf.yaml    # Global project settings (Engine, Auth)
└── .gitignore
```

### 3. Pipeline Execution

To run your pipeline, navigate to your project directory and run:

```bash
verity run
```

*Note: In development, pipelines generally run in **non-strict mode** (warnings only), unless explicitly configured otherwise.*

## CI/CD Integration (Strict Mode)

Verity's core philosophy is **Zero-Trust Compilation**: your pipeline should fail to build if it detects undocumented schema changes or unsecured PII flows.

### Enabling Strict Mode

Strict Mode forces Verity to block execution if Governance policies are violated. You should enable this mode in your CI/CD and Production pipelines.

Do not rely on containerization to define "production". Instead, run Verity natively to easily pass cloud configurations and credentials, and use the `VERITY_STRICT` environment variable:

```bash
VERITY_STRICT=true verity run
```

### GitHub Actions Example

Here is a simple way to run Verity natively in a GitHub Action using the Rust toolchain to compile or run the CLI:

```yaml
name: Data Pipeline CI

on:
  push:
    branches: [ "main" ]
  pull_request:
    branches: [ "main" ]

jobs:
  verity_pipeline:
    runs-on: ubuntu-latest
    steps:
      - name: Checkout Code
        uses: actions/checkout@v4
        
      - name: Install Rust
        uses: actions-rust-lang/setup-rust-toolchain@v1

      - name: Install Verity
        run: cargo install --git https://github.com/axel-mauroy/verity-governance-as-code
        
      - name: Define Credentials
        run: |
          # Use native GitHub secrets rather than mounting volumes to a container!
          # export AWS_ACCESS_KEY_ID=${{ secrets.AWS_ACCESS_KEY_ID }}
          # export AWS_SECRET_ACCESS_KEY=${{ secrets.AWS_SECRET_ACCESS_KEY }}

      - name: Run Verity Governance Check & Pipeline
        working-directory: ./my_data_project
        run: verity run
        env:
          VERITY_STRICT: "true"
```

*If `VERITY_STRICT=true` is set, the job will fail the PR if any governance policy is violated, natively protecting your data.*
