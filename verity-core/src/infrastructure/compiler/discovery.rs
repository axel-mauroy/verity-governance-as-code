// verity-core/src/infrastructure/compiler/discovery.rs

use crate::domain::project::ProjectConfig;
use crate::domain::project::manifest::{
    ColumnInfo, Manifest, ManifestNode, MaterializationType, NodeConfig, ResourceType,
    SourceDefinition,
};
use crate::infrastructure::config::{self, ModelSchema, SchemaFile};
use crate::infrastructure::error::InfrastructureError;

use regex::Regex;
use std::collections::{HashMap, HashSet};
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::OnceLock;
use walkdir::WalkDir;

fn re_ref() -> &'static Regex {
    static RE: OnceLock<Regex> = OnceLock::new();
    RE.get_or_init(|| {
        Regex::new(r#"ref\s*\(\s*['"]([^'"]+)['"]\s*\)"#).unwrap_or_else(|_| {
            // This should never happen as the regex is hardcoded
            // and we avoid unsafe methods to satisfy Clippy and the security guard.
            Regex::new("$^").unwrap_or_else(|_| unreachable!())
        })
    })
}

// Imports for Trait Implementation
use crate::domain::error::DomainError;
use crate::domain::ports::ManifestLoader;

pub struct GraphDiscovery;

impl ManifestLoader for GraphDiscovery {
    fn load(&self, root: &Path, config: &ProjectConfig) -> Result<Manifest, DomainError> {
        Self::discover(root, config).map_err(|e| DomainError::ManifestError(e.to_string()))
    }
}

impl GraphDiscovery {
    pub fn discover(
        project_dir: &Path,
        config: &ProjectConfig,
    ) -> Result<Manifest, InfrastructureError> {
        let models_dir = project_dir.join("models");

        println!("📝 Loading YAML schemas...");
        let schema_map = Self::load_all_schemas(&models_dir)?;

        let mut nodes = HashMap::new();
        println!("🕵️‍♀️  Scanning SQL models in: {:?}", models_dir);

        if !models_dir.exists() {
            return Ok(Manifest {
                project_name: config.name.clone(),
                nodes: HashMap::new(),
                sources: HashMap::new(),
            });
        }

        let walker = WalkDir::new(&models_dir).follow_links(true);

        for entry in walker.into_iter().filter_map(|e| e.ok()) {
            let path = entry.path();

            if path.extension().is_some_and(|ext| ext == "sql")
                && let Ok(node) = Self::parse_sql_file(path, project_dir, &schema_map, config)
            {
                nodes.insert(node.name.clone(), node);
            }
        }

        let loaded_sources_vec = config::load_sources(project_dir).map_err(|e| {
            InfrastructureError::ConfigNotFound(format!("Failed to load sources: {}", e))
        })?;

        let mut sources_map = HashMap::new();
        for source in loaded_sources_vec {
            sources_map.insert(
                source.name.clone(),
                SourceDefinition {
                    name: source.name,
                    path: source.path,
                    owner: source.owner,
                },
            );
        }

        Ok(Manifest {
            project_name: config.name.clone(),
            nodes,
            sources: sources_map,
        })
    }

    /// Load schemas from YAML files.
    /// Priority: 1) Per-model YAML (<model>.yml next to <model>.sql)
    ///           2) Centralized schema.yml files
    fn load_all_schemas(
        root_dir: &Path,
    ) -> Result<HashMap<String, (ModelSchema, PathBuf)>, InfrastructureError> {
        let mut map = HashMap::new();
        let walker = WalkDir::new(root_dir).follow_links(true);

        // First pass: find all SQL files and check for accompanying .yml
        for entry in walker.into_iter().filter_map(|e| e.ok()) {
            let path = entry.path();

            // Check for per-model YAML (e.g., stg_users.yml next to stg_users.sql)
            if path.extension().is_some_and(|ext| ext == "sql") {
                let model_name = path
                    .file_stem()
                    .unwrap_or_default()
                    .to_string_lossy()
                    .to_string();
                let yaml_path = path.with_extension("yml");

                if yaml_path.exists()
                    && let Ok(content) = fs::read_to_string(&yaml_path)
                {
                    // Per-model YAML can be either SchemaFile or direct ModelSchema
                    if let Ok(parsed) = serde_yaml::from_str::<SchemaFile>(&content) {
                        // CORRECTION: parsed.models est un Vec direct, pas une Option
                        for model in parsed.models {
                            // CORRECTION: model.name -> model.model_name
                            // On vérifie si le nom du modèle correspond au fichier ou si c'est une version
                            if model.model_name == model_name
                                || model.model_name.starts_with(&format!("{}_v", model_name))
                            {
                                map.insert(model.model_name.clone(), (model, yaml_path.clone()));
                            }
                        }
                    } else if let Ok(model) = serde_yaml::from_str::<ModelSchema>(&content) {
                        map.insert(model.model_name.clone(), (model, yaml_path.clone()));
                    }
                }
            }
        }

        Ok(map)
    }

    fn parse_sql_file(
        path: &Path,
        project_root: &Path,
        schema_map: &HashMap<String, (ModelSchema, PathBuf)>,
        project_config: &ProjectConfig,
    ) -> Result<ManifestNode, InfrastructureError> {
        let raw_sql = fs::read_to_string(path).map_err(InfrastructureError::Io)?;

        let name = path
            .file_stem()
            .ok_or_else(|| {
                InfrastructureError::Io(std::io::Error::new(
                    std::io::ErrorKind::InvalidInput,
                    "Invalid filename",
                ))
            })?
            .to_string_lossy()
            .to_string();

        let rel_path = path
            .strip_prefix(project_root)
            .unwrap_or(path)
            .to_path_buf();

        let mut refs = HashSet::new();
        for cap in re_ref().captures_iter(&raw_sql) {
            refs.insert(cap[1].to_string());
        }

        // --- LOGIQUE DE CASCADE DE CONFIGURATION ---

        // 1. Determine the "Layer" (parent folder : staging, marts...)
        let models_path = project_root.join("models");
        let relative_to_models = path.strip_prefix(&models_path).unwrap_or(path);
        let layer = relative_to_models
            .components()
            .next()
            .map(|c| c.as_os_str().to_string_lossy().to_string())
            .unwrap_or_default();

        // 2. Extraire les configs (Specifique > Layer > Defaut)
        let schema_entry = schema_map.get(&name);
        let schema_def = schema_entry.map(|(s, _)| s);
        let schema_path = schema_entry.map(|(_, p)| p.clone());

        // CORRECTION MAJEURE : s.config est maintenant une Struct, pas une Option.
        // On accède directement aux champs.
        let specific_mat = schema_def.and_then(|s| s.config.materialized.clone());

        // 3. Security Level (Override : Schema > Project)
        let specific_security = schema_def.and_then(|s| s.config.governance.security_level.clone());

        let specific_prot = specific_security
            .as_ref()
            .map(|lvl| lvl.to_lowercase() != "public" && lvl.to_lowercase() != "unclassified");

        let security_level_resolved = specific_security
            .or_else(|| project_config.governance.default_security_level.clone())
            .unwrap_or_else(|| "internal".to_string());

        let security_level: crate::domain::governance::SecurityLevel =
            serde_json::from_value(serde_json::Value::String(security_level_resolved))
                .unwrap_or_default();

        // 4. Owners (Override : Schema > Project)
        let specific_tech_owner = schema_def.and_then(|s| s.config.governance.tech_owner.clone());
        let specific_business_owner =
            schema_def.and_then(|s| s.config.governance.business_owner.clone());

        // Fallback Layer defaults
        let layer_mat = project_config
            .defaults
            .get(&layer)
            .and_then(|c| c.materialized.clone());
        let layer_prot = project_config
            .defaults
            .get(&layer)
            .and_then(|c| c.protected);

        // 3. Final resolution (String)
        let mat_string = specific_mat
            .or(layer_mat)
            .unwrap_or_else(|| "view".to_string());
        let protected = specific_prot.or(layer_prot).unwrap_or(false);

        // 4. Conversion String -> Enum (Domain)
        let materialized = match mat_string.to_lowercase().as_str() {
            "table" => Some(MaterializationType::Table),
            "view" => Some(MaterializationType::View),
            "ephemeral" => Some(MaterializationType::Ephemeral),
            "incremental" => Some(MaterializationType::Incremental),
            _ => Some(MaterializationType::View), // Fallback safe
        };

        // 5. Mapping of Columns
        let columns = if let Some(schema_def) = schema_def {
            if let Some(cols) = &schema_def.columns {
                cols.iter()
                    .map(|c| {
                        let mut policy = c.policy.clone();

                        // Fuzzy Policy Injection
                        // Check if policy is missing and matches a fuzzy rule
                        if policy.is_none() {
                            for rule in &project_config.governance.pii_detection.column_policies {
                                // Compile regex on the fly (performance trade-off for simplicity)
                                if let Ok(re) = Regex::new(&rule.column_name_pattern) {
                                    if re.is_match(&c.name) {
                                        // TODO: Environment check if added to ColumnPolicy
                                        policy = Some(rule.policy.clone());
                                        break; // Apply first matching rule
                                    }
                                }
                            }
                        }

                        ColumnInfo {
                            name: c.name.clone(),
                            tests: c.tests.clone().unwrap_or_default(),
                            policy,
                        }
                    })
                    .collect()
            } else {
                vec![]
            }
        } else {
            vec![]
        };

        let compliance = schema_def.and_then(|s| s.compliance.clone());

        Ok(ManifestNode {
            name,
            resource_type: ResourceType::Model,
            path: rel_path,
            schema_path,
            raw_sql,
            refs: refs.into_iter().collect(),
            config: NodeConfig {
                materialized,
                schema: None,
                // Priorité au Tech Owner, sinon Business Owner
                tech_owner: specific_tech_owner.or(specific_business_owner),
                business_owner: None, // Tu pourras mapper ça plus tard si NodeConfig évolue
                protected,
            },
            columns,
            security_level,
            compliance,
        })
    }
}
