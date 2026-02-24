// verity-core/src/application/clean.rs

use crate::error::VerityError;
use crate::infrastructure::config::project::load_project_config;
use crate::infrastructure::error::InfrastructureError;
use std::fs;
use std::path::Path;

pub fn clean_project(project_dir: &Path) -> Result<(), VerityError> {
    tracing::info!("🧹 Initializing Verity cleanup sequence...");

    let config = load_project_config(project_dir).map_err(VerityError::Infrastructure)?;

    let targets = if config.clean_targets.is_empty() {
        vec!["target".to_string()]
    } else {
        config.clean_targets
    };

    for target_rel_path in targets {
        let full_path = project_dir.join(&target_rel_path);

        // Zero-Trust Path Traversal Guard
        if !full_path.starts_with(project_dir) {
            return Err(VerityError::UnsafePath(target_rel_path));
        }

        if full_path.exists() {
            if full_path.is_dir() {
                fs::remove_dir_all(&full_path)
                    .map_err(|e| VerityError::Infrastructure(InfrastructureError::Io(e)))?;
            } else {
                fs::remove_file(&full_path)
                    .map_err(|e| VerityError::Infrastructure(InfrastructureError::Io(e)))?;
            }
            println!("   🗑️  Artifact removed: {}", target_rel_path);
        }
    }

    Ok(())
}
