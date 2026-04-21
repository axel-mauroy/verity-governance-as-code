# Cloud Infrastructure Deployment Guide

This guide details how to seamlessly deploy Verity at scale across Cloud environments. Thanks to its **modular architecture (multi-binary)** and Hexagonal design, Verity executes natively as close to the data as possible, without requiring bloated Docker images or complex abstractions.

Crucial reminder: **Verity is an end-to-end transformation engine**. Unlike pure post-process validation tools, Verity compiles the SQL (and Jinja), orchestrates the execution DAG, and pushes the computations down to the Data Warehouse while ensuring governance policies (PII masking, Quality checks) are strictly enforced upstream.

---

## 1. ELT Orchestration (Airflow, Dagster, Prefect)

The golden rule in production: **Never download executables at runtime (`curl | bash` is an anti-pattern)**. Modern orchestrators distribute tasks across multiple ephemeral workers. A binary downloaded in Task A will likely not be available to Task B.

To tame cloud environments, Verity supports two optimal deployment strategies:

### Strategy A: The Python "Trojan Horse" (PyPI Wheel) 🏆
This is the most seamless and widely adopted method in the Modern Data Stack (inspired by tools like `Ruff` or `Polars`). Although Verity is an ultra-fast Rust binary engine, it is packaged and distributed as a Python wheel via PyPI.

**The massive advantage for Airflow/MWAA**: Simply add the Verity packages to your `requirements.txt` (on Cloud Composer, MWAA, or your Astro CLI). The orchestrator will automatically pull and install the pre-compiled Rust binaries across all its workers on startup.

To guarantee that the Core and Connectors versions remain strictly aligned (preventing the internal JSON-RPC protocol from breaking), Verity relies on Python's **Extras** packaging system.

If you manage your environment with modern tools like **uv** (highly recommended) :
```bash
uv add "verity-core[bigquery]"
```

Or via the traditional approach using `requirements.txt`:
```text
# requirements.txt
verity-core[bigquery]
```

⚠️ **Version Hell (The JSON-RPC Contract)**: If you choose not to use the Extras system and download the packages separately, **the Core and Connectors versions must be strictly identical**. An asymmetry between a Core v0.3.0 and a connector v0.2.0 will violently crash the pipeline due to JSON-RPC schema drift.

Once installed via PyPI, you can securely trigger Verity using a native `BashOperator`.

⚠️ **Airflow Gotcha (The PATH Trap)**: By default, the `BashOperator` launches a raw sub-shell that does not automatically source the worker's Python virtual environment `PATH`. To avoid the fatal `bash: verity: command not found` error, secure the execution by providing the absolute installation path (usually `~/.local/bin/verity`) or by routing through the python module.

```python
from airflow.models import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

with DAG('verity_daily_transformations', start_date=datetime(2026, 1, 1)) as dag:
    
    # Secure execution using the absolute path of the binary installed via pip/uv
    run_pipeline = BashOperator(
        task_id='run_verity_pipeline',
        bash_command='~/.local/bin/verity run',
        env={
            'VERITY_STRICT': 'true', 
            'GOOGLE_CLOUD_PROJECT': 'my-company-data-prod'
        }
    )
```

### Strategy B: Container Operators (K8s Isolation)
If you do not manage dependencies via `requirements.txt` or prefer total tool isolation, the most robust and secure approach is to execute the minimalist Verity container using a Docker or Kubernetes operator.

**Example with Apache Airflow (`KubernetesPodOperator`)**:

```python
from airflow.models import DAG
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
from datetime import datetime

with DAG('verity_daily_transformations', start_date=datetime(2026, 1, 1)) as dag:
    
    # Isolated execution: Airflow launches an ephemeral pod containing Verity
    run_pipeline = KubernetesPodOperator(
        namespace='data-processing',
        image='my-registry/verity-bq:latest',
        cmds=["verity", "run"],
        name="verity-governance",
        task_id="run_verity_pipeline",
        is_delete_operator_pod=True,
        env_vars={
            'VERITY_STRICT': 'true', 
            'GOOGLE_CLOUD_PROJECT': 'my-company-data-prod'
        }
    )
```
*Note: If you use custom Docker images for Airflow workers, you can simply inject the Verity binary during the `docker build` phase.*

---

## 2. Serverless Containers (Cloud Run, Fargate)

If you need to isolate Verity for "on-demand" execution, the containerized approach remains highly effective. Because of the multi-binary architecture, you no longer need giant Rust toolchain images. The production image weighs around **~30 MB**.

⚠️ **Important Note for Google Cloud Run**: Verity is a CLI tool (`ENTRYPOINT ["verity"]`); it does not listen on an HTTP port. You must deploy it as a **Cloud Run Job** (designed for batch processing scripts), NOT as a *Cloud Run Service* (which mandates a web server).

### Minimalist Multi-Binary Dockerfile

```dockerfile
# Use an ultra-lean Debian image
FROM debian:bookworm-slim

# TLS certs required for BigQuery / Snowflake API calls
RUN apt-get update && apt-get install -y ca-certificates curl && rm -rf /var/lib/apt/lists/*

# Copy the native pre-compiled binaries
COPY verity /usr/local/bin/verity
COPY verity-bigquery /usr/local/bin/verity-bigquery

# Security best practice: avoid running as root
RUN useradd -m verityusr
USER verityusr
WORKDIR /app

# Standalone entry
ENTRYPOINT ["verity"]
CMD ["run"]
```

---

## 3. Security & IAM Recommendations

Because Verity dynamically parses schemas and (during dynamic sampling) potentially reads highly sensitive (`High` classification) data, strict security boundaries are mandatory.

1. **Ban Static JSON Keys**: 
   Verity-BigQuery inherits the native Google Cloud SDK. It perfectly understands passwordless, injected identities. Use **Workload Identity (GCP/GKE)** or **IAM Roles for Service Accounts (AWS/EKS)**. Attach the service account directly to the worker node or the Kubernetes Pod executing Verity. The tool will authenticate automatically.
2. **Principle of Least Privilege**:
   The role bound to the `verity-bigquery` binary must have:
   - `BigQuery Data Viewer` on source schemas.
   - `BigQuery Data Editor` on destination schemas (staging, marts).
   - `BigQuery Job User` to execute DDL and DML statements.
3. **Read-Only File System**:
   Verity only requires write permissions for the target compilation directory defined in `verity_project_conf.yaml` (generally `target/`). The entire remainder of the image or VM can be aggressively locked down using read-only mode (`ReadOnlyRootFilesystem=true`).

---

## 4. Observability and Centralized Logging

In production, Verity's rich Terminal UI (TUI) transitions from an asset into a massive liability (ANSI escape characters become illegible garbage inside Datadog, CloudWatch, or GCP Cloud Logging).

To guarantee flawless observability:
- Set `NO_COLOR=1` in your environment to immediately strip all generic ANSI formatting strings.
- In enterprise environments, leverage the upcoming `verity run --log-format json` (or `RUST_LOG=json`) feature to stream every pipeline event as a structured JSON log, making it instantly queryable by your favorite observability stack.
