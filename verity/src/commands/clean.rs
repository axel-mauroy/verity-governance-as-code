// verity/src/commands/clean.rs
//
// USE CASE: Clean build artifacts.

use std::path::PathBuf;

use verity_core::application::clean_project;

pub fn execute(project_dir: PathBuf) -> anyhow::Result<()> {
    if let Err(e) = clean_project(&project_dir) {
        eprintln!("❌ Clean failed: {}", e);
        std::process::exit(1);
    }
    Ok(())
}
