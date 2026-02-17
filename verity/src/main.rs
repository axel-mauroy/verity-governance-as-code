// verity/src/main.rs

use clap::{Parser, Subcommand};
use std::path::PathBuf;

// Infrastructure (Config & Adapters)
use verity_core::infrastructure::adapters::duckdb::DuckDBConnector;
use verity_core::infrastructure::config::project::load_project_config;
use verity_core::infrastructure::config::source::{
    GenerateOptions, generate_sources, save_sources,
};

// Domain (Enums for the CLI)
use verity_core::domain::governance::SecurityLevel;

// Application (Use Cases)
use std::sync::Arc;
use verity_core::application::{CatalogGenerator, clean_project, execute_query, run_pipeline};
use verity_core::infrastructure::compiler::discovery::GraphDiscovery;
use verity_core::infrastructure::compiler::jinja::JinjaRenderer;
use verity_core::infrastructure::config::schema::SchemaAdapter;

#[derive(Parser)]
#[command(name = "verity")]
#[command(about = "The Hexagonal Data Contract & Transformation Engine", long_about = None)]
#[command(version)]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// 🚀 Runs the data pipeline (Models -> SQL -> Validation)
    Run {
        /// Project directory
        #[arg(long, default_value = ".")]
        project_dir: PathBuf,

        /// Run only a specific model (ex: "stg_users")
        #[arg(long, short)]
        select: Option<String>,
    },

    /// 🧹 Cleans build artifacts (target/ folder)
    Clean {
        #[arg(long, default_value = ".")]
        project_dir: PathBuf,
    },

    /// ⚡ Executes a raw SQL query (Ad-hoc)
    Query {
        query: String,
        #[arg(long, default_value = "verity_db.duckdb")]
        db_path: String,
    },

    /// 🕵️‍♀️ Scans data directory and generates 'models/sources.yaml'
    Generate {
        /// Project directory
        #[arg(long, default_value = ".")]
        project_dir: PathBuf,

        /// Data directory relative to project (default: "data")
        #[arg(long, default_value = "data")]
        data_dir: String,

        /// Default owner for new sources
        #[arg(long)]
        owner: Option<String>,

        /// Flag all new sources as containing PII by default
        #[arg(long, default_value = "false")]
        pii: bool,

        /// Remove sources from YAML that no longer exist on disk
        #[arg(long, default_value = "false")]
        prune: bool,
    },

    /// 📚 Generates the Data Catalog (HTML/JSON)
    Docs {
        /// Project directory
        #[arg(long, default_value = ".")]
        project_dir: PathBuf,
    },
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // 1. Setup Logging (Tracing)
    // RUST_LOG=debug verity run ... pour voir les détails
    tracing_subscriber::fmt::init();

    let cli = Cli::parse();

    match cli.command {
        // --- USE CASE: RUN PIPELINE ---
        Commands::Run {
            project_dir,
            select,
        } => {
            let start = std::time::Instant::now();

            // A. Load the Config (Infra)
            println!("⚙️  Loading configuration...");
            // The '?' propagates automatically InfrastructureError -> anyhow::Error
            let config = load_project_config(&project_dir)?;
            println!("   Project: {} (v{})", config.name, config.version);

            // B. Instantiate the DB Adapter (DuckDB)
            // For MVP, the DB file is hardcoded or derived.
            // Ideally, it would come from `config.target_path` or `profiles.yml`.
            let db_path = "verity_db.duckdb";
            let connector = DuckDBConnector::new(db_path)?;

            // ...

            // C. Run the Pipeline (Application Layer)
            // Here is where dependency injection happens : we pass 'connector' and 'config'.
            let manifest_loader = GraphDiscovery;
            let template_engine = Arc::new(JinjaRenderer::new());
            let schema_source = SchemaAdapter;

            let result = run_pipeline(
                &manifest_loader,
                template_engine,
                &schema_source,
                &project_dir,
                &config,
                &connector,
                select,
            )
            .await;

            match result {
                Ok(run_res) => {
                    if run_res.success {
                        println!("\n✨ SUCCESS! Pipeline finished in {:.2?}", start.elapsed());
                    } else {
                        eprintln!("\n❌ FAILURE. {} models failed.", run_res.errors.len());
                        // Exit with error code for CI/CD
                        std::process::exit(1);
                    }
                }
                Err(e) => {
                    eprintln!("\n💥 CRITICAL PIPELINE ERROR: {}", e);
                    std::process::exit(1);
                }
            }
        }

        // --- USE CASE: CLEAN ---
        Commands::Clean { project_dir } => {
            if let Err(e) = clean_project(&project_dir) {
                eprintln!("❌ Clean failed: {}", e);
                std::process::exit(1);
            }
        }

        // --- USE CASE: AD-HOC QUERY ---
        Commands::Query { query, db_path } => {
            let connector = DuckDBConnector::new(&db_path)?;
            if let Err(e) = execute_query(&connector, &query).await {
                eprintln!("❌ Query failed: {}", e);
                std::process::exit(1);
            }
        }

        // --- USE CASE: SCAFFOLDING (GENERATE SOURCES) ---
        Commands::Generate {
            project_dir,
            data_dir,
            owner,
            pii,
            prune,
        } => {
            println!(
                "🕵️‍♀️  Scanning for sources in '{}/{}'...",
                project_dir.display(),
                data_dir
            );

            let options = GenerateOptions {
                default_owner: owner,
                default_pii: pii,
                default_security: SecurityLevel::Internal, // Default value expected
                prune,
                interactive: false,
            };

            let source_list = generate_sources(&project_dir, &data_dir, options)?;

            println!("📝 Found {} sources.", source_list.sources.len());

            save_sources(&project_dir, &source_list)?;

            println!("✨ sources.yaml updated successfully!");
        }

        // --- USE CASE: GENERATE DATA CATALOG ---
        Commands::Docs { project_dir } => {
            println!("📚 Generating Data Catalog...");

            let config = load_project_config(&project_dir)?;
            let target_dir = project_dir.join(&config.target_path);

            // Discovery
            let manifest = GraphDiscovery::discover(&project_dir, &config)?;

            // Catalog generation
            CatalogGenerator::generate(&project_dir, &target_dir, &manifest)?;

            println!(
                "✨ Documentation generated successfully in {}",
                target_dir.display()
            );
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_cli_parse_run_defaults() {
        let args = Cli::parse_from(["verity", "run"]);
        match args.command {
            Commands::Run {
                project_dir,
                select,
            } => {
                assert_eq!(project_dir.to_string_lossy(), ".");
                assert_eq!(select, None);
            }
            _ => panic!("Expected Run command"),
        }
    }

    #[test]
    fn test_cli_parse_run_select() {
        let args = Cli::parse_from([
            "verity",
            "run",
            "--select",
            "my_model",
            "--project-dir",
            "/tmp",
        ]);
        match args.command {
            Commands::Run {
                project_dir,
                select,
            } => {
                assert_eq!(project_dir.to_string_lossy(), "/tmp");
                assert_eq!(select, Some("my_model".to_string()));
            }
            _ => panic!("Expected Run command"),
        }
    }

    #[test]
    fn test_cli_parse_generate() {
        let args = Cli::parse_from(["verity", "generate", "--pii"]);
        match args.command {
            Commands::Generate { pii, owner: _, .. } => {
                assert!(pii);
            }
            _ => panic!("Expected Generate command"),
        }
    }
}
