// src/domain/project/configuration.rs

use crate::domain::governance::configuration::GovernanceConfig;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// SQL engine to use for pipeline execution.
#[derive(Debug, Deserialize, Serialize, Clone, Default, PartialEq)]
#[serde(rename_all = "lowercase")]
pub enum Engine {
    #[default]
    DuckDB,
    DataFusion,
}

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct ProjectConfig {
    pub name: String,
    pub version: String,
    #[serde(default = "default_profile")]
    pub profile: String,

    #[serde(default)]
    pub engine: Engine,

    #[serde(rename = "config-paths", default)]
    pub config_paths: Vec<String>,

    #[serde(rename = "model-paths", default)]
    pub model_paths: Vec<String>,

    #[serde(rename = "target-path", default = "default_target_path")]
    pub target_path: String,

    #[serde(rename = "clean-targets", default = "default_clean_targets")]
    pub clean_targets: Vec<String>,

    #[serde(default)]
    pub governance: GovernanceConfig,

    #[serde(default)]
    pub defaults: HashMap<String, LayerConfig>,
}

#[derive(Debug, Deserialize, Serialize, Clone, Default)]
pub struct LayerConfig {
    pub materialized: Option<String>,
    pub schema: Option<String>,
    pub protected: Option<bool>,
}

fn default_clean_targets() -> Vec<String> {
    vec!["target".to_string()]
}
fn default_target_path() -> String {
    "target".to_string()
}
fn default_profile() -> String {
    "dev".to_string()
}
