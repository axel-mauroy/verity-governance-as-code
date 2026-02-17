// verity/src/cli.rs

use clap::{Parser, Subcommand};

#[derive(Parser)]
#[command(name = "verity")]
#[command(version)]
#[command(about = "The Governance-First Data Transformation Tool", long_about = None)]
pub struct Cli {
    /// Project directory (defaults to current directory)
    #[arg(long, global = true, default_value = ".")]
    pub project_dir: String,

    #[command(subcommand)]
    pub command: Commands,
}

#[derive(Subcommand)]
pub enum Commands {
    /// Initialize a new Verity project
    Init {
        /// Project name
        name: String,
    },
    
    /// Run the pipeline or specific models
    Run {
        /// Select specific models to run (e.g. users_enriched)
        #[arg(long, short)]
        select: Option<String>,
    },
    
    /// Manage data sources
    Sources {
        #[command(subcommand)]
        command: SourcesCommands,
    },

    /// Clean build artifacts
    Clean,

    /// 📚 Generate data catalog documentation (HTML/JSON)
    Docs {
        /// Project directory
        #[arg(long, default_value = ".")]
        project_dir: String,
    },
}

#[derive(Subcommand)]
pub enum SourcesCommands {
    /// Automatically generate models/sources.yaml by scanning a data directory
    Generate {
        /// Data directory to scan (relative to project root)
        #[arg(long, default_value = "data")]
        data_dir: String,

        /// Default owner for new sources
        #[arg(long)]
        owner: Option<String>,

        /// Mark new sources as containing PII
        #[arg(long)]
        pii: bool,

        /// Default security level for new sources (internal, confidential, restricted)
        #[arg(long, default_value = "internal")]
        security: String,

        /// Remove sources from YAML that no longer exist on disk
        #[arg(long)]
        prune: bool,

        /// Interactive mode for manual name overrides
        #[arg(long, short)]
        interactive: bool,
    },
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_run_select() {
        let args = Cli::parse_from(&["verity", "run", "--select", "my_model"]);
        match args.command {
            Commands::Run { select } => {
                assert_eq!(select, Some("my_model".to_string()));
            },
            _ => panic!("Expected Run command"),
        }
    }

    #[test]
    fn test_parse_sources_generate_defaults() {
        let args = Cli::parse_from(&["verity", "sources", "generate"]);
        match args.command {
            Commands::Sources { command: SourcesCommands::Generate { data_dir, security, .. } } => {
                assert_eq!(data_dir, "data");
                assert_eq!(security, "internal");
            },
            _ => panic!("Expected Sources Generate command"),
        }
    }
}