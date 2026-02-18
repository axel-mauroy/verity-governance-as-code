// verity/src/commands/run.rs
//
// USE CASE: Run the data pipeline.

use std::path::PathBuf;
use std::sync::Arc;

use anyhow::Context;
use verity_core::application::run_pipeline;
use verity_core::domain::project::Engine;
use verity_core::infrastructure::adapters::datafusion::DataFusionConnector;
use verity_core::infrastructure::adapters::duckdb::DuckDBConnector;
use verity_core::infrastructure::compiler::discovery::GraphDiscovery;
use verity_core::infrastructure::compiler::jinja::JinjaRenderer;
use verity_core::infrastructure::config::project::load_project_config;
use verity_core::infrastructure::config::schema::SchemaAdapter;
use verity_core::ports::connector::Connector;

pub async fn execute(project_dir: PathBuf, select: Option<String>) -> anyhow::Result<()> {
    let start = std::time::Instant::now();

    // A. Load the Config (Infra)
    println!("⚙️  Loading configuration...");
    let config = load_project_config(&project_dir).with_context(|| {
        format!(
            "Failed to load project configuration from {:?}",
            project_dir
        )
    })?;
    println!("   Project: {} (v{})", config.name, config.version);

    // B. Instantiate the DB Adapter based on engine config
    let connector: Box<dyn Connector> = match config.engine {
        Engine::DuckDB => {
            println!("   Engine: DuckDB 🦆");
            let db_path = "verity_db.duckdb";
            Box::new(
                DuckDBConnector::new(db_path)
                    .with_context(|| format!("Failed to initialize DuckDB at {}", db_path))?,
            )
        }
        Engine::DataFusion => {
            println!("   Engine: Apache DataFusion 🏹");
            let target_dir = project_dir.join(&config.target_path);
            Box::new(DataFusionConnector::new(&target_dir).with_context(|| {
                format!(
                    "Failed to initialize DataFusion with target dir {:?}",
                    target_dir
                )
            })?)
        }
    };

    // C. Run the Pipeline (Application Layer)
    let manifest_loader = GraphDiscovery;
    let template_engine = Arc::new(JinjaRenderer::new());
    let schema_source = SchemaAdapter;

    let result = run_pipeline(
        &manifest_loader,
        template_engine,
        &schema_source,
        &project_dir,
        &config,
        connector.as_ref(),
        select,
    )
    .await;

    match result {
        Ok(run_res) => {
            if run_res.success {
                println!("\n✨ SUCCESS! Pipeline finished in {:.2?}", start.elapsed());
            } else {
                eprintln!("\n❌ FAILURE. {} models failed.", run_res.errors.len());
                std::process::exit(1);
            }
        }
        Err(e) => {
            eprintln!("\n💥 CRITICAL PIPELINE ERROR: {}", e);
            std::process::exit(1);
        }
    }

    Ok(())
}
