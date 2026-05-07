// verity/src/main.rs
//
// Thin dispatcher — parses CLI args and routes to command handlers.

mod cli;
mod commands;

use clap::Parser;
use cli::Commands;

#[cfg(unix)]
use std::os::unix::process::CommandExt;
use std::path::PathBuf;
use std::process::Command;

/// Checks if we are running in a virtual environment (via `$VIRTUAL_ENV` or `.venv`)
/// and delegates execution to the local `verity` binary if it exists and is different from us.
fn auto_proxy_to_local_venv() {
    let venv_path = std::env::var("VIRTUAL_ENV")
        .map(PathBuf::from)
        .ok()
        .or_else(|| {
            std::env::current_dir()
                .map(|cwd| cwd.join(".venv"))
                .ok()
                .filter(|v| v.is_dir())
        });

    if let Some(venv) = venv_path {
        // Construct path to the local binary
        #[cfg(unix)]
        let local_bin = venv.join("bin").join("verity");
        #[cfg(not(unix))]
        let local_bin = venv.join("Scripts").join("verity.exe");

        if !local_bin.exists() {
            return;
        }

        let Ok(current_exe) = std::env::current_exe() else {
            return;
        };
        let Ok(canonical_local) = std::fs::canonicalize(&local_bin) else {
            return;
        };
        let Ok(canonical_current) = std::fs::canonicalize(&current_exe) else {
            return;
        };

        if canonical_local != canonical_current {
            eprintln!("🔄 Auto-proxying to local Verity inside virtual environment...");

            let args: Vec<String> = std::env::args().skip(1).collect();

            #[cfg(unix)]
            {
                let err = Command::new(&canonical_local).args(&args).exec();
                eprintln!("❌ Failed to exec local verity: {}", err);
                std::process::exit(1);
            }

            #[cfg(not(unix))]
            {
                match Command::new(&canonical_local).args(&args).status() {
                    Ok(status) => {
                        std::process::exit(status.code().unwrap_or(1));
                    }
                    Err(err) => {
                        eprintln!("❌ Failed to spawn local verity: {}", err);
                        std::process::exit(1);
                    }
                }
            }
        }
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // 0. Prevent PATH shadowing by delegating to local virtualenv binary if it exists
    auto_proxy_to_local_venv();

    // 3. Observability: Structured JSON Logging
    if std::env::var("VERITY_LOG_FORMAT").unwrap_or_default() == "json" {
        tracing_subscriber::fmt().json().init();
    } else {
        tracing_subscriber::fmt::init();
    }
    let cli = cli::Cli::parse();

    match cli.command {
        Commands::Run {
            project_dir,
            select,
        } => commands::run::execute(project_dir, select).await,

        Commands::Clean { project_dir } => commands::clean::execute(project_dir),

        Commands::Query { query, project_dir } => {
            commands::query::execute(query, project_dir).await
        }

        Commands::Generate {
            project_dir,
            data_dir,
            owner,
            pii,
            prune,
        } => commands::generate::execute(project_dir, data_dir, owner, pii, prune),

        Commands::Docs { project_dir } => commands::docs::execute(project_dir),

        Commands::Lineage {
            project_dir,
            check,
            format,
        } => commands::lineage::execute(project_dir, check, format),

        Commands::Init { project_name, path } => commands::init::execute(project_name, path),
    }
}
