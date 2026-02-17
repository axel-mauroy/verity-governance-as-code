// verity-core/src/application/clean.rs

use std::fs;
use std::path::Path;

use crate::infrastructure::config::project;

use crate::error::VerityError;
use crate::infrastructure::error::InfrastructureError;

pub fn clean_project(project_dir: &Path) -> Result<(), VerityError> {
    println!("🧹 Cleaning project artifacts...");

    let targets = match project::load_project_config(project_dir) {
        Ok(cfg) => cfg.clean_targets,
        Err(_) => {
            println!("⚠️  Could not load verity_project_conf.yaml, defaulting to 'target/'");
            vec!["target".to_string()]
        }
    };

    if targets.is_empty() {
        println!("✨ Nothing to clean.");
        return Ok(());
    }

    for target in targets {
        let path = project_dir.join(&target);
        if path.exists() {
            fs::remove_dir_all(&path)
                .map_err(|e| VerityError::Infrastructure(InfrastructureError::Io(e)))?;
            println!("   🗑️  Removed: {}", target);
        } else {
            println!("   Example: {} (not found, skipped)", target);
        }
    }

    println!("✨ Project cleaned successfully!");
    Ok(())
}
