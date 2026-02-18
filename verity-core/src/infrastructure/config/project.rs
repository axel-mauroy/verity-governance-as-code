// verity-core/src/infrastructure/config/project.rs

use anyhow::Context;
use serde::{Deserialize, de::DeserializeOwned};
use std::fs;
use std::path::{Path, PathBuf};
use tracing::{info, instrument, warn};

use crate::domain::governance::{PiiConfig, QualityConfig, QualityRule};
use crate::domain::project::configuration::ProjectConfig;
use crate::infrastructure::error::InfrastructureError;

// Removed struct definitions as they are now in domain/project/configuration.rs

// --- LOADER OPTIMISÉ ---

#[instrument(skip(project_dir))] // Log automatique de l'entrée/sortie de la fonction
pub fn load_project_config(project_dir: &Path) -> Result<ProjectConfig, InfrastructureError> {
    // 1. Découverte du fichier principal
    let config_path = find_main_config(project_dir)?;
    info!(path = ?config_path, "Loading project manifest");

    // 2. Chargement YAML Base
    let content = fs::read_to_string(&config_path)
        .with_context(|| format!("Failed to read project config at {:?}", config_path))?;
    let mut config: ProjectConfig = serde_yaml::from_str(&content)
        .with_context(|| format!("Failed to parse project config YAML at {:?}", config_path))?;

    // 3. Hydratation des Satellites (Fail-Secure)
    if let Some(config_folder) = config.config_paths.first() {
        let config_dir = project_dir.join(config_folder);
        if config_dir.exists() {
            // Ici, si une erreur survient, on propage avec '?'
            load_satellite_configs(&mut config, &config_dir)?;
        }
    }

    // 4. Override via Variables d'Environnement (Pattern 'Layering')
    // Permet de faire: VERITY_TARGET_PATH=/tmp/build verity run
    // On utilise une méthode manuelle simple ici, ou la crate `envy` pour struct complet
    apply_env_overrides(&mut config);

    Ok(config)
}

fn find_main_config(root: &Path) -> Result<PathBuf, InfrastructureError> {
    let candidates = ["verity_project_conf.yaml", "verity.yaml"];
    for filename in candidates {
        let p = root.join(filename);
        if p.exists() {
            return Ok(p);
        }
    }
    Err(InfrastructureError::ConfigNotFound(format!(
        "No configuration file found in {:?}. Checked: {:?}",
        root, candidates
    )))
}

// --- LOGIQUE GÉNÉRIQUE ---

/// Charge un fragment de configuration typé depuis un fichier.
/// T est le type de la struct Wrapper attendue dans le fichier.
fn load_fragment<T: DeserializeOwned>(path: &Path) -> Result<T, InfrastructureError> {
    let content = fs::read_to_string(path)
        .with_context(|| format!("Failed to read config fragment at {:?}", path))?;
    serde_yaml::from_str(&content)
        .with_context(|| format!("Failed to parse YAML fragment at {:?}", path))
        .map_err(Into::into)
}

fn load_satellite_configs(
    config: &mut ProjectConfig,
    config_dir: &Path,
) -> Result<(), InfrastructureError> {
    // A. Policies (PII) - Utilisation du Generic Loader
    let pol_path = config_dir.join("policies.yml");
    if pol_path.exists() {
        #[derive(Deserialize)]
        struct PoliciesWrapper {
            pii_detection: PiiConfig,
        }

        // Note le '?' ici : Si le fichier est corrompu, on ARRÊTE tout.
        let wrapper: PoliciesWrapper = load_fragment(&pol_path)?;
        config.governance.pii_detection = wrapper.pii_detection;
        info!("  🔒 Governance policies loaded");
    }

    // B. Quality Rules
    let qual_path = config_dir.join("quality.yml");
    if qual_path.exists() {
        #[derive(Deserialize)]
        struct QualityWrapper {
            rules: Option<Vec<QualityRule>>,
            data_quality: Option<QualityConfig>,
        }

        let wrapper: QualityWrapper = load_fragment(&qual_path)?;

        if let Some(dq) = wrapper.data_quality {
            config.governance.data_quality = dq;
        } else if let Some(rules) = wrapper.rules {
            config.governance.data_quality.enabled = true;
            config.governance.data_quality.rules = rules;
        }
        info!("  ✅ Quality rules loaded");
    }

    Ok(())
}

fn apply_env_overrides(config: &mut ProjectConfig) {
    // Exemple simple d'override. En prod, on utiliserait la crate 'envy' ou 'figment'.
    if let Ok(val) = std::env::var("VERITY_TARGET_PATH") {
        info!(old = ?config.target_path, new = ?val, "Overriding target path via ENV");
        config.target_path = val;
    }
    if let Ok(val) = std::env::var("VERITY_PROFILE") {
        info!(old = ?config.profile, new = ?val, "Overriding profile via ENV");
        config.profile = val;
    }
}
