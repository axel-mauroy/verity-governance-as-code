// src/infrastructure/config/schema.rs

use regex::Regex;
use serde::{Deserialize, Serialize};

use std::fs;

use std::path::{Path, PathBuf};
use tracing::{info, instrument, warn};

use crate::domain::error::DomainError;
use crate::domain::governance::ColumnPolicy;
use crate::domain::ports::SchemaSource;
use crate::infrastructure::error::InfrastructureError;

pub struct SchemaAdapter;

impl SchemaSource for SchemaAdapter {
    fn update_model_columns(
        &self,
        path: &Path,
        model_name: &str,
        columns: &[String],
    ) -> Result<(), DomainError> {
        update_model_columns(path, model_name, columns)
            .map(|_| ())
            .map_err(|e| DomainError::SchemaError(e.to_string()))
    }

    fn create_versioned_model(
        &self,
        path: &Path,
        model_name: &str,
        version: u32,
        columns: &[String],
        policies: &[ColumnPolicy],
        status: Option<LifecycleStatus>,
    ) -> Result<(), DomainError> {
        create_versioned_model(path, model_name, version, columns, policies, status)
            .map(|_| ())
            .map_err(|e| DomainError::SchemaError(e.to_string()))
    }
}

// =============================================================================
//  1. DATA CONTRACT
// =============================================================================

#[derive(Debug, Deserialize, Serialize, Clone, Default)]
pub struct SchemaFile {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub version: Option<String>,

    #[serde(rename = "schema", default)]
    pub models: Vec<ModelSchema>,
}

use crate::domain::project::LifecycleStatus;

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct ModelSchema {
    pub model_name: String,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,

    #[serde(default)]
    pub config: ModelConfig,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub compliance: Option<ComplianceConfig>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub columns: Option<Vec<ColumnSchema>>,
}

fn default_version() -> u32 {
    1
}

#[derive(Debug, Deserialize, Serialize, Clone, Default)]
pub struct ModelConfig {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub materialized: Option<String>,

    // --- Flags Data Mesh ---
    #[serde(default)] // false par défaut
    pub data_contract: bool,

    #[serde(default)] // false par défaut
    pub data_catalog: bool, // distinguish between “plumbing” tables and “data products” tables.

    // --- Versioning & Lifecycle ---
    // Mandatory Versioning (default = 1 for legacy support)
    #[serde(default = "default_version")]
    pub version: u32,

    #[serde(default)]
    pub status: LifecycleStatus,

    #[serde(default)]
    pub latest: bool,

    // --- Governance ---
    #[serde(default)]
    pub governance: GovernanceConfig,
}

#[derive(Debug, Deserialize, Serialize, Clone, Default)]
pub struct GovernanceConfig {
    // --- Ownership ---
    #[serde(alias = "tech_owner", skip_serializing_if = "Option::is_none")]
    pub tech_owner: Option<String>,

    #[serde(alias = "business_owner", skip_serializing_if = "Option::is_none")]
    pub business_owner: Option<String>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub security_level: Option<String>,
}

// --- Compliance Blocks ---
use crate::domain::compliance::config::ComplianceConfig;

// --- Columns ---

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct ColumnSchema {
    pub name: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tests: Option<Vec<String>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub policy: Option<String>,
}

// =============================================================================
//  2. LOGIQUE DE CRÉATION & VERSIONING (The "Brain")
// =============================================================================

#[instrument(skip(columns, column_policies))]
pub fn create_versioned_model(
    sql_path: &Path,
    base_name: &str,
    target_version: u32,
    columns: &[String],
    column_policies: &[ColumnPolicy],
    requested_status: Option<LifecycleStatus>,
) -> Result<PathBuf, InfrastructureError> {
    let yaml_path = sql_path.with_extension("yml");

    // 1. Chargement de l'existant (ou création à vide)
    let mut schema_file = if yaml_path.exists() {
        let content = fs::read_to_string(&yaml_path).map_err(InfrastructureError::Io)?;
        serde_yaml::from_str::<SchemaFile>(&content).map_err(InfrastructureError::YamlError)?
    } else {
        SchemaFile::default()
    };

    // 2. Calcul du nom canonique (Suffixing strict)
    let canonical_name = if target_version == 1 {
        base_name.to_string()
    } else {
        format!("{}_v{}", base_name, target_version)
    };

    // 3. Validation du Statut Initial
    let initial_status = requested_status.unwrap_or(LifecycleStatus::Provisioning);

    if matches!(
        initial_status,
        LifecycleStatus::Deprecated | LifecycleStatus::Erased
    ) {
        return Err(InfrastructureError::ConfigError(
            "Cannot initialize a new model as 'deprecated' or 'erased'.".into(),
        ));
    }

    // 4. Validation Cohérence Cycle de Vie (State Machine Guard)
    validate_lifecycle_state(
        &schema_file.models,
        base_name,
        target_version,
        initial_status,
    )?;

    // 5. Construction des colonnes avec Policies
    let cached_policies = CachedPolicy::compile_all(column_policies)?;
    let schema_columns: Vec<ColumnSchema> = columns
        .iter()
        .map(|col_name| {
            let policy = match_policy(col_name, &cached_policies);
            ColumnSchema {
                name: col_name.clone(),
                description: Some("Auto-detected".into()),
                tests: None,
                policy,
            }
        })
        .collect();

    // 6. Instanciation du Modèle
    let new_model = ModelSchema {
        model_name: canonical_name.clone(),
        description: Some(format!("Version {} of {}", target_version, base_name)),
        config: ModelConfig {
            materialized: Some("table".to_string()),
            version: target_version,
            status: initial_status,
            latest: true, // Nouvelle version = dernière version code
            governance: GovernanceConfig {
                tech_owner: detect_tech_owner(),
                ..Default::default()
            },
            ..Default::default()
        },
        compliance: None,
        columns: Some(schema_columns),
    };

    // 7. Mise à jour des drapeaux 'latest' des anciens modèles
    for model in &mut schema_file.models {
        if is_same_family(&model.model_name, base_name) {
            model.config.latest = false;
        }
    }

    // 8. Sauvegarde Atomique
    schema_file.models.push(new_model);
    atomic_write_yaml(&yaml_path, &schema_file)?;

    info!(
        "✅ Created {} (v{}) - Status: {:?}",
        canonical_name, target_version, initial_status
    );

    Ok(yaml_path)
}

// =============================================================================
//  3. LOGIQUE DE MISE À JOUR (The "Updater")
// =============================================================================

#[instrument(skip(new_columns))]
pub fn update_model_columns(
    schema_path: &Path,
    model_name: &str,
    new_columns: &[String],
) -> Result<bool, InfrastructureError> {
    if !schema_path.exists() {
        return Ok(false);
    }

    let content = fs::read_to_string(schema_path).map_err(InfrastructureError::Io)?;
    let mut schema_file: SchemaFile =
        serde_yaml::from_str(&content).map_err(InfrastructureError::YamlError)?;

    let mut file_changed = false;

    // On cherche le modèle spécifique (v1, v2...) dans la liste
    if let Some(model) = schema_file
        .models
        .iter_mut()
        .find(|m| m.model_name == model_name)
    {
        let columns = model.columns.get_or_insert_with(Vec::new);
        let existing_names: std::collections::HashSet<String> =
            columns.iter().map(|c| c.name.to_lowercase()).collect();

        for col_name in new_columns {
            if !existing_names.contains(&col_name.to_lowercase()) {
                columns.push(ColumnSchema {
                    name: col_name.clone(),
                    description: Some("Auto-detected by Verity".into()),
                    tests: None,
                    policy: None, // On ne force pas de policy sur un update pour ne pas écraser l'existant
                });
                file_changed = true;
            }
        }
    }

    if file_changed {
        atomic_write_yaml(schema_path, &schema_file)?;
        info!(path = ?schema_path, "Updated schema with new columns");
    }

    Ok(file_changed)
}

// =============================================================================
//  4. VALIDATEURS & HELPERS
// =============================================================================

fn validate_lifecycle_state(
    existing_models: &[ModelSchema],
    base_name: &str,
    target_version: u32,
    target_status: LifecycleStatus,
) -> Result<(), InfrastructureError> {
    let previous_models: Vec<&ModelConfig> = existing_models
        .iter()
        .filter(|m| is_same_family(&m.model_name, base_name))
        .map(|m| &m.config)
        .collect();

    // Guard: Continuité (v2 requiert v1)
    let existing_versions: Vec<u32> = previous_models.iter().map(|c| c.version).collect();
    if target_version > 1 && !existing_versions.contains(&(target_version - 1)) {
        return Err(InfrastructureError::ConfigError(format!(
            "Missing version v{}. You cannot create v{} without it.",
            target_version - 1,
            target_version
        )));
    }

    // Guard: Duplication
    if existing_versions.contains(&target_version) {
        return Err(InfrastructureError::ConfigError(format!(
            "Version {} already exists for model {}.",
            target_version, base_name
        )));
    }

    // Guard: Double Active
    if target_status == LifecycleStatus::Active {
        for prev in &previous_models {
            if prev.status == LifecycleStatus::Active {
                return Err(InfrastructureError::ConfigError(format!(
                    "Lifecycle Violation: Version {} is still 'active'. \
                    Deprecate v{} before activating v{}.",
                    prev.version, prev.version, target_version
                )));
            }
        }
    }

    Ok(())
}

fn is_same_family(full_name: &str, base_name: &str) -> bool {
    full_name == base_name || full_name.starts_with(&format!("{}_v", base_name))
}

struct CachedPolicy {
    regex: Regex,
    policy_name: String,
}

impl CachedPolicy {
    fn compile_all(policies: &[ColumnPolicy]) -> Result<Vec<Self>, InfrastructureError> {
        let mut cached = Vec::with_capacity(policies.len());
        for p in policies {
            let re = Regex::new(&p.column_name_pattern).map_err(|e| {
                InfrastructureError::ConfigError(format!("Invalid Regex in policy: {}", e))
            })?;
            cached.push(CachedPolicy {
                regex: re,
                policy_name: p.policy.clone(),
            });
        }
        Ok(cached)
    }
}

fn match_policy(column_name: &str, policies: &[CachedPolicy]) -> Option<String> {
    for p in policies {
        if p.regex.is_match(column_name) {
            return Some(p.policy_name.clone());
        }
    }
    None
}

/// Écriture sûre (Atomic Write)
fn atomic_write_yaml<T: Serialize>(path: &Path, data: &T) -> Result<(), InfrastructureError> {
    let content = serde_yaml::to_string(data).map_err(InfrastructureError::YamlError)?;
    crate::infrastructure::fs::atomic_write(path, content)?;
    Ok(())
}

/// Detection of Technical Owner via Git
fn detect_tech_owner() -> Option<String> {
    std::process::Command::new("git")
        .args(["config", "user.name"])
        .output()
        .ok()
        .and_then(|output| {
            if output.status.success() {
                String::from_utf8(output.stdout)
                    .ok()
                    .map(|s| s.trim().to_string())
                    .filter(|s| !s.is_empty())
            } else {
                None
            }
        })
}

// =============================================================================
//  5. TESTS UNITAIRES (The "Safety Net")
// =============================================================================

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;
    use std::fs;
    use tempfile::tempdir;

    fn mock_policies() -> Vec<ColumnPolicy> {
        vec![ColumnPolicy {
            column_name_pattern: "email".to_string(),
            policy: "hash".to_string(),
        }]
    }

    fn manual_update_status(path: &Path, model_name: &str, new_status: LifecycleStatus) {
        let content = fs::read_to_string(path).unwrap();
        let mut schema: SchemaFile = serde_yaml::from_str(&content).unwrap();
        if let Some(model) = schema
            .models
            .iter_mut()
            .find(|m| m.model_name == model_name)
        {
            model.config.status = new_status;
        }
        let new_content = serde_yaml::to_string(&schema).unwrap();
        fs::write(path, new_content).unwrap();
    }

    #[test]
    fn test_lifecycle_happy_path_provisioning() {
        let dir = tempdir().unwrap();
        let sql_path = dir.path().join("stg_users.sql");
        let base_name = "stg_users";
        let cols = vec!["id".to_string(), "email".to_string()];

        // 1. Création V1 (Active)
        create_versioned_model(
            &sql_path,
            base_name,
            1,
            &cols,
            &mock_policies(),
            Some(LifecycleStatus::Active),
        )
        .unwrap();

        // 2. Création V2 (Provisioning) -> OK
        create_versioned_model(
            &sql_path,
            base_name,
            2,
            &cols,
            &mock_policies(),
            Some(LifecycleStatus::Provisioning),
        )
        .unwrap();

        let content = fs::read_to_string(sql_path.with_extension("yml")).unwrap();
        let schema: SchemaFile = serde_yaml::from_str(&content).unwrap();

        assert_eq!(schema.models.len(), 2);
        assert_eq!(schema.models[0].config.status, LifecycleStatus::Active);
        assert_eq!(
            schema.models[1].config.status,
            LifecycleStatus::Provisioning
        );
        assert!(schema.models[1].config.latest);
        assert!(!schema.models[0].config.latest);
    }

    #[test]
    fn test_lifecycle_block_double_active() {
        let dir = tempdir().unwrap();
        let sql_path = dir.path().join("stg_sales.sql");
        let base_name = "stg_sales";
        let cols = vec!["amount".to_string()];

        create_versioned_model(
            &sql_path,
            base_name,
            1,
            &cols,
            &mock_policies(),
            Some(LifecycleStatus::Active),
        )
        .unwrap();

        let result = create_versioned_model(
            &sql_path,
            base_name,
            2,
            &cols,
            &mock_policies(),
            Some(LifecycleStatus::Active),
        );

        assert!(result.is_err());
        match result.unwrap_err() {
            InfrastructureError::ConfigError(msg) => {
                assert!(msg.contains("Version 1 is still 'active'"))
            }
            _ => panic!("Expected ConfigError"),
        }
    }

    #[test]
    fn test_sequence_continuity_guard() {
        let dir = tempdir().unwrap();
        let sql_path = dir.path().join("stg_gl.sql");

        create_versioned_model(&sql_path, "stg_gl", 1, &[], &mock_policies(), None).unwrap();

        // Skip v2 -> Error
        let result = create_versioned_model(&sql_path, "stg_gl", 3, &[], &mock_policies(), None);
        assert!(result.is_err());
    }

    #[test]
    fn test_transition_active_to_deprecated_allows_new_active() {
        let dir = tempdir().unwrap();
        let sql_path = dir.path().join("stg_churn.sql");
        let yaml_path = sql_path.with_extension("yml");

        create_versioned_model(
            &sql_path,
            "stg_churn",
            1,
            &[],
            &mock_policies(),
            Some(LifecycleStatus::Active),
        )
        .unwrap();
        manual_update_status(&yaml_path, "stg_churn", LifecycleStatus::Deprecated);

        let result = create_versioned_model(
            &sql_path,
            "stg_churn",
            2,
            &[],
            &mock_policies(),
            Some(LifecycleStatus::Active),
        );
        assert!(result.is_ok());
    }

    #[test]
    fn test_prevent_duplicate_version() {
        let dir = tempdir().unwrap();
        let sql_path = dir.path().join("dup.sql");
        create_versioned_model(&sql_path, "dup", 1, &[], &mock_policies(), None).unwrap();
        let result = create_versioned_model(&sql_path, "dup", 1, &[], &mock_policies(), None);
        assert!(result.is_err());
    }
}
