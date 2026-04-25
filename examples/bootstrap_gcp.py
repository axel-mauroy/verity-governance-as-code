#!/usr/bin/env -S uv run --script
# /// script
# requires-python = ">=3.9"
# dependencies = [
#     "pyyaml",
# ]
# ///
#
# examples/bootstrap_gcp.py
#
# This script automates the ingestion of local CSV sources into BigQuery via GCS.
# It is used to "bootstrap" the environment for running examples on GCP.

import os
import yaml
import subprocess
import sys
import logging

logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
logger = logging.getLogger("bootstrap_gcp")

def run_command(cmd):
    logger.info(f"Executing: {' '.join(cmd)}")
    result = subprocess.run(cmd, capture_output=True, text=True)
    if result.returncode != 0:
        logger.error(f"Command failed with exit code {result.returncode}")
        logger.error(f"Stdout: {result.stdout}")
        logger.error(f"Stderr: {result.stderr}")
        sys.exit(1)
    return result.stdout

def main():
    if len(sys.argv) < 2:
        logger.error("Usage: ./bootstrap_gcp.py <example_name>")
        logger.error("Example: ./bootstrap_gcp.py basic_rag_pipeline")
        sys.exit(1)

    example_name = sys.argv[1]
    root_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    example_dir = os.path.join(root_dir, "examples", example_name)
    sources_path = os.path.join(example_dir, "models", "sources.yaml")

    if not os.path.exists(sources_path):
        logger.error(f"Sources file not found at {sources_path}")
        sys.exit(1)

    # Configuration from environment
    project_id = os.environ.get("GOOGLE_CLOUD_PROJECT")
    dataset_id = os.environ.get("VERITY_DATASET")
    bucket = os.environ.get("VERITY_GCS_STAGING_BUCKET")

    if not project_id or not dataset_id or not bucket:
        logger.error("Missing required environment variables: GOOGLE_CLOUD_PROJECT, VERITY_DATASET, VERITY_GCS_STAGING_BUCKET")
        logger.error("Tip: Copy .env.example to .env and fill in your values.")
        sys.exit(1)

    logger.info(f"🚀 Bootstrapping example '{example_name}' for BigQuery...")
    logger.info(f"📍 Project: {project_id}")
    logger.info(f"📍 Dataset: {dataset_id}")
    logger.info(f"📍 Staging Bucket: {bucket}")

    with open(sources_path, 'r') as f:
        data = yaml.safe_load(f)

    for source in data.get('sources', []):
        name = source['name']
        local_path = os.path.join(example_dir, source['path'])
        gcs_path = f"gs://{bucket}/verity_staging/{dataset_id}/{name}.csv"
        table_id = f"{project_id}:{dataset_id}.{name}"

        logger.info(f"\n--- Ingesting source: {name} ---")
        
        # 1. Upload to GCS
        logger.info(f"📤 Uploading {local_path} to {gcs_path}...")
        run_command(["gcloud", "storage", "cp", local_path, gcs_path])

        # 2. Load into BigQuery
        logger.info(f"📥 Loading {gcs_path} into BigQuery table {table_id}...")
        run_command([
            "bq", "load",
            "--project_id", project_id,
            "--source_format=CSV",
            "--autodetect",
            "--replace",
            table_id,
            gcs_path
        ])

    logger.info("\n✅ Bootstrap complete! You can now run the pipeline with Verity.")

if __name__ == "__main__":
    main()
