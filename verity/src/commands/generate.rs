// verity/src/commands/generate.rs
//
// USE CASE: Scan data directory and generate sources.yaml.

use std::path::PathBuf;

use verity_core::domain::governance::SecurityLevel;
use verity_core::infrastructure::config::source::{GenerateOptions, generate_sources, save_sources};

pub fn execute(
    project_dir: PathBuf,
    data_dir: String,
    owner: Option<String>,
    pii: bool,
    prune: bool,
) -> anyhow::Result<()> {
    println!(
        "🕵️‍♀️  Scanning for sources in '{}/{}'...",
        project_dir.display(),
        data_dir
    );

    let options = GenerateOptions {
        default_owner: owner,
        default_pii: pii,
        default_security: SecurityLevel::Internal,
        prune,
        interactive: false,
    };

    let source_list = generate_sources(&project_dir, &data_dir, options)?;

    println!("📝 Found {} sources.", source_list.sources.len());

    save_sources(&project_dir, &source_list)?;

    println!("✨ sources.yaml updated successfully!");
    Ok(())
}
