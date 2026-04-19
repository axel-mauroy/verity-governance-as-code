// verity/src/commands/query.rs
//
// USE CASE: Execute a raw SQL query (ad-hoc) against registered sources.
// Follows the same init flow as `run`: load config → create engine → register sources → query.

use std::path::PathBuf;

use anyhow::Context;
use verity_core::domain::ports::ManifestLoader;
use verity_core::domain::project::Engine;
use verity_core::infrastructure::adapters::datafusion::DataFusionConnector;
use verity_core::infrastructure::compiler::discovery::GraphDiscovery;
use verity_core::infrastructure::config::project::load_project_config;
use verity_core::ports::connector::Connector;

pub async fn execute(query: String, project_dir: PathBuf) -> anyhow::Result<()> {
    // A. Load config (same pattern as run.rs)
    let config = load_project_config(&project_dir).with_context(|| {
        format!(
            "No Verity project found in {:?}. \
             Either cd into a project directory or use --project-dir <path>.",
            project_dir
        )
    })?;

    // B. Instantiate the connector via the hexagonal port
    let connector: Box<dyn Connector> = match config.engine {
        Engine::DataFusion => {
            let target_dir = project_dir.join(&config.target_path);
            Box::new(DataFusionConnector::new(&target_dir).with_context(|| {
                format!(
                    "Failed to initialize DataFusion with target dir {:?}",
                    target_dir
                )
            })?)
        }
        Engine::BigQuery => {
            Box::new(verity_core::ports::connector::ProxyConnector::new("verity-bigquery", "BigQuery")
                .await
                .context("Failed to start external BigQuery connector. Is 'verity-bigquery' installed and in PATH?")?)
        }
    };

    // C. Register sources so the query can reference them
    let manifest = GraphDiscovery
        .load(&project_dir, &config)
        .with_context(|| "Failed to compile project manifest")?;

    for source in manifest.sources.values() {
        let raw_path = std::path::Path::new(&source.path);
        let absolute_path = if raw_path.is_absolute() {
            raw_path.to_path_buf()
        } else {
            project_dir.join(raw_path)
        };

        if absolute_path.exists() {
            connector
                .register_source(&source.name, &absolute_path)
                .await?;
        }
    }

    // D. Execute through the Connector port (not raw ctx access)
    let batches = connector
        .fetch_sample(&query)
        .await
        .with_context(|| format!("Query execution failed: {}", query))?;

    // E. Display results using Arrow's built-in pretty printer
    if batches.is_empty() || batches.iter().all(|b| b.num_rows() == 0) {
        println!("(no rows returned)");
        return Ok(());
    }

    datafusion::arrow::util::pretty::print_batches(&batches)
        .with_context(|| "Failed to format query results")?;

    Ok(())
}
