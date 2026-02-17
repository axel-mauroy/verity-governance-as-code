// verity-core/src/infrastructure/config/source.rs

use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use std::fs;
use std::path::Path;
use walkdir::WalkDir; // 🟢 Nécessaire pour les structs

use crate::infrastructure::error::InfrastructureError;
// Note: On n'importe plus SourceConfig/SourceList de 'config', on les définit ICI.
use crate::domain::governance::{ResourceGovernance, SecurityLevel};

const SUPPORTED_EXTENSIONS: [&str; 2] = ["csv", "parquet"];

// --- 1. DÉFINITIONS DES STRUCTS (DTOs) ---

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct SourceConfig {
    pub name: String,
    pub path: String, // Chemin relatif (ex: "data/sales.csv")

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub owner: Option<String>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub governance: Option<ResourceGovernance>,
}

#[derive(Debug, Serialize, Deserialize, Clone, Default)]
pub struct SourceList {
    pub sources: Vec<SourceConfig>,
}

pub struct GenerateOptions {
    pub default_owner: Option<String>,
    pub default_pii: bool,
    pub default_security: SecurityLevel,
    pub prune: bool,
    pub interactive: bool,
}

// --- 2. FONCTIONS DE CHARGEMENT ---

/// Charge la liste des sources existantes depuis models/sources.yaml
pub fn load_sources(project_dir: &Path) -> Result<Vec<SourceConfig>, InfrastructureError> {
    let sources_path = project_dir.join("models/sources.yaml");

    if !sources_path.exists() {
        // Pas d'erreur, juste une liste vide. C'est normal au début d'un projet.
        return Ok(Vec::new());
    }

    let content = fs::read_to_string(&sources_path).map_err(InfrastructureError::Io)?;

    let list: SourceList =
        serde_yaml::from_str(&content).map_err(InfrastructureError::YamlError)?;

    Ok(list.sources)
}

// --- 3. FONCTIONS DE GÉNÉRATION ---

pub fn generate_sources(
    project_dir: &Path,
    data_dir_rel: &str,
    options: GenerateOptions,
) -> Result<SourceList, InfrastructureError> {
    let data_dir = project_dir.join(data_dir_rel);
    if !data_dir.exists() {
        return Err(InfrastructureError::Io(std::io::Error::new(
            std::io::ErrorKind::NotFound,
            format!("Data directory {:?} does not exist", data_dir),
        )));
    }

    // 1. Chargement de l'existant (Appel local à la fonction définie plus haut)
    let existing_sources = load_sources(project_dir).unwrap_or_default();

    let mut known_paths: HashMap<String, SourceConfig> = existing_sources
        .into_iter()
        .map(|s| (s.path.clone(), s))
        .collect();

    // 2. Scan du disque
    let mut found_files = Vec::new();
    let walker = WalkDir::new(&data_dir).follow_links(true);

    for entry in walker.into_iter().filter_map(|e| e.ok()) {
        let path = entry.path();
        if path.is_file()
            && let Some(ext) = path.extension().and_then(|s| s.to_str())
            && SUPPORTED_EXTENSIONS.contains(&ext)
        {
            found_files.push(path.to_path_buf());
        }
    }

    let mut final_sources = Vec::new();
    let mut seen_names: HashSet<String> = known_paths.values().map(|s| s.name.clone()).collect();

    // 3. Smart Merge
    for path in found_files {
        let rel_path_buf = path.strip_prefix(project_dir).map_err(|_| {
            InfrastructureError::Io(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "Path prefix error",
            ))
        })?;

        let rel_path = rel_path_buf.to_string_lossy().to_string();

        if let Some(existing) = known_paths.remove(&rel_path) {
            println!("   ✅ Kept: {}", existing.name);
            final_sources.push(existing);
        } else {
            let suggested_name = derive_stable_name(&path, &data_dir);

            let mut final_name = suggested_name.clone();
            let mut i = 1;
            while seen_names.contains(&final_name) {
                final_name = format!("{}_{}", suggested_name, i);
                i += 1;
            }
            seen_names.insert(final_name.clone());

            println!("   ✨ New: {} -> {}", rel_path, final_name);

            final_sources.push(SourceConfig {
                name: final_name,
                path: rel_path,
                owner: options.default_owner.clone(),
                governance: Some(ResourceGovernance {
                    public: true,
                    pii: options.default_pii,
                    security: options.default_security,
                }),
            });
        }
    }

    // 4. Pruning
    for (missing_path, missing_source) in known_paths {
        if options.prune {
            println!("   ✂️  Pruned: {} (File not found)", missing_path);
        } else {
            eprintln!(
                "   ⚠️  Warning: Source '{}' points to missing file: {}",
                missing_source.name, missing_path
            );
            final_sources.push(missing_source);
        }
    }

    final_sources.sort_by(|a, b| a.name.cmp(&b.name));

    Ok(SourceList {
        sources: final_sources,
    })
}

fn derive_stable_name(path: &Path, base_data_dir: &Path) -> String {
    let stem = path
        .file_stem()
        .unwrap_or_default()
        .to_string_lossy()
        .to_string();

    let parent = match path.parent() {
        Some(p) => p,
        None => return stem,
    };

    if parent == base_data_dir {
        return stem;
    }

    let domain = parent
        .file_name()
        .unwrap_or_default()
        .to_string_lossy()
        .to_string();
    format!("{}_{}", domain, stem)
}

pub fn save_sources(
    project_dir: &Path,
    source_list: &SourceList,
) -> Result<(), InfrastructureError> {
    let models_dir = project_dir.join("models");
    if !models_dir.exists() {
        fs::create_dir_all(&models_dir).map_err(InfrastructureError::Io)?;
    }

    let sources_path = models_dir.join("sources.yaml");

    let content = serde_yaml::to_string(source_list).map_err(InfrastructureError::YamlError)?;

    crate::infrastructure::fs::atomic_write(&sources_path, content)?;

    Ok(())
}
