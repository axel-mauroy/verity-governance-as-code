// verity-core/src/domain/project/manifest.rs

use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::path::PathBuf;

/// The Manifest represents the complete and resolved state of the Verity project.
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct Manifest {
    pub project_name: String,

    /// All nodes of the graph (Models, Tests...)
    pub nodes: HashMap<String, ManifestNode>,

    /// External sources definitions
    #[serde(default)]
    pub sources: HashMap<String, SourceDefinition>,
}

/// Represents an external data source definition in the Domain.
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct SourceDefinition {
    pub name: String,
    pub path: String,
    pub owner: Option<String>,
    // Si tu as besoin de gouvernance ici, utilise les types du domaine (ex: SecurityLevel)
}

/// A unique node in the execution graph.
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct ManifestNode {
    pub name: String,
    pub resource_type: ResourceType,
    pub path: PathBuf,
    pub schema_path: Option<PathBuf>, // 🟢 Path to the YAML file defining this model
    pub raw_sql: String,
    pub refs: Vec<String>,
    pub config: NodeConfig,

    #[serde(default)]
    pub columns: Vec<ColumnInfo>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub compliance: Option<crate::domain::compliance::config::ComplianceConfig>,
}

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum ResourceType {
    Model,
    Source,
    Analysis,
    Test,
}

/// Configuration specific to a node.
#[derive(Debug, Serialize, Deserialize, Clone, Default)]
pub struct NodeConfig {
    pub materialized: Option<MaterializationType>,
    pub schema: Option<String>,
    pub tech_owner: Option<String>,
    pub business_owner: Option<String>,
    #[serde(default)]
    pub protected: bool,
}

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum MaterializationType {
    View,
    Table,
    Ephemeral,
    Incremental,
}

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Default, Copy)]
#[serde(rename_all = "snake_case")]
pub enum LifecycleStatus {
    #[default]
    Provisioning, // Pre-prod (Blue/Green)
    Active,     // Production
    Deprecated, // Soft-delete
    Erased,
}

impl LifecycleStatus {
    pub fn can_transition_to(&self, next: &LifecycleStatus) -> bool {
        match (self, next) {
            // Self -> Self is always allowed (idempotency)
            (s, n) if s == n => true,

            // Forward transitions
            (Self::Provisioning, Self::Active) => true,
            (Self::Active, Self::Deprecated) => true,
            (Self::Deprecated, Self::Erased) => true,

            // Allowed Rollbacks (e.g. strict governance might allow Active -> Provisioning for hotfixes?)
            // For now, let's keep it strict: NO rollbacks.
            _ => false,
        }
    }
}

#[derive(Debug, Serialize, Deserialize, Clone, Default)]
pub struct ColumnInfo {
    pub name: String,

    #[serde(default)]
    pub tests: Vec<String>,

    #[serde(default)]
    pub policy: Option<String>,
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;

    #[test]
    fn test_manifest_deserialization() {
        let json_data = r#"
        {
            "project_name": "demo_project",
            "nodes": {
                "model_a": {
                    "name": "model_a",
                    "resource_type": "model",
                    "path": "models/m_a.sql",
                    "raw_sql": "SELECT 1",
                    "refs": [],
                    "config": {
                        "materialized": "table",
                        "protected": true
                    },
                    "columns": [
                        { "name": "id", "tests": ["unique", "not_null"] }
                    ]
                }
            }
        }
        "#;

        let manifest: Manifest = serde_json::from_str(json_data).expect("Should deserialize");

        assert_eq!(manifest.project_name, "demo_project");
        let node = manifest.nodes.get("model_a").expect("Node should exist");
        assert_eq!(node.resource_type, ResourceType::Model);
        assert_eq!(node.config.materialized, Some(MaterializationType::Table));
        assert!(node.config.protected);
        assert_eq!(node.columns.len(), 1);
        assert_eq!(node.columns[0].name, "id");
    }

    #[test]
    fn test_default_values() {
        let json_data = r#"
        {
            "project_name": "defaults",
            "nodes": {}
        }
        "#;
        let manifest: Manifest = serde_json::from_str(json_data).unwrap();
        assert!(manifest.sources.is_empty());
    }
}
