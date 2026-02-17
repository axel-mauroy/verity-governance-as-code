// verity/src/main.rs
//
// Thin dispatcher — parses CLI args and routes to command handlers.

mod cli;
mod commands;

use clap::Parser;
use cli::Commands;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt::init();

    let cli = cli::Cli::parse();

    match cli.command {
        Commands::Run {
            project_dir,
            select,
        } => commands::run::execute(project_dir, select).await,

        Commands::Clean { project_dir } => commands::clean::execute(project_dir),

        Commands::Query { query, db_path } => commands::query::execute(query, db_path).await,

        Commands::Generate {
            project_dir,
            data_dir,
            owner,
            pii,
            prune,
        } => commands::generate::execute(project_dir, data_dir, owner, pii, prune),

        Commands::Docs { project_dir } => commands::docs::execute(project_dir),

        Commands::Inspect {
            db_path,
            table,
            limit,
        } => commands::inspect::execute(db_path, table, limit),
    }
}
