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
    Query { query: String },

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

    /// 🔗 Analyzes data lineage and detects unsecured PII flows
    Lineage {
        /// Project directory
        #[arg(long, default_value = ".")]
        project_dir: PathBuf,

        /// Exit with error if unsecured PII flows are detected
        #[arg(long)]
        check: bool,

        /// Output format: mermaid | json
        #[arg(long, default_value = "mermaid")]
        format: String,
    },
}

#[cfg(test)]
mod tests {
    use super::*;
    use anyhow::{Result, bail};
    use clap::Parser;

    #[test]
    fn test_cli_parse_run_defaults() -> Result<()> {
        let args = Cli::parse_from(["verity", "run"]);
        match args.command {
            Commands::Run {
                project_dir,
                select,
            } => {
                assert_eq!(project_dir.to_string_lossy(), ".");
                assert_eq!(select, None);
                Ok(())
            }
            _ => bail!("Expected Run command"),
        }
    }

    #[test]
    fn test_cli_parse_run_select() -> Result<()> {
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
                Ok(())
            }
            _ => bail!("Expected Run command"),
        }
    }

    #[test]
    fn test_cli_parse_generate() -> Result<()> {
        let args = Cli::parse_from(["verity", "generate", "--pii"]);
        match args.command {
            Commands::Generate { pii, owner: _, .. } => {
                assert!(pii);
                Ok(())
            }
            _ => bail!("Expected Generate command"),
        }
    }
}
