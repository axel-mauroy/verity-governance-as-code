// verity/src/cli.rs
//
// Single source of truth for all CLI definitions (Clap structs).

use clap::{Parser, Subcommand};
use std::path::PathBuf;

#[derive(Parser)]
#[command(name = "verity")]
#[command(about = "The Hexagonal Data Contract & Transformation Engine", long_about = None)]
#[command(version)]
pub struct Cli {
    #[command(subcommand)]
    pub command: Commands,
}

#[derive(Subcommand)]
pub enum Commands {
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

    /// 🔍 Inspects a DuckDB table (schema + sample rows)
    Inspect {
        /// Path to the DuckDB database file
        #[arg(long, default_value = "verity_db.duckdb")]
        db_path: String,

        /// Table name to inspect
        #[arg(long, short)]
        table: String,

        /// Number of sample rows to display
        #[arg(long, default_value = "5")]
        limit: usize,
    },
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

    #[test]
    fn test_cli_parse_inspect() {
        let args = Cli::parse_from([
            "verity",
            "inspect",
            "--table",
            "users",
            "--limit",
            "10",
        ]);
        match args.command {
            Commands::Inspect {
                table, limit, db_path,
            } => {
                assert_eq!(table, "users");
                assert_eq!(limit, 10);
                assert_eq!(db_path, "verity_db.duckdb");
            }
            _ => panic!("Expected Inspect command"),
        }
    }
}