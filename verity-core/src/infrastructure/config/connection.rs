use crate::domain::project::ProjectConfig;
use anyhow::{Context, Result};
use serde::Deserialize;
use std::collections::HashMap;
use std::fs;
use std::path::Path;

#[derive(Debug, Deserialize, Clone)]
pub struct ConnectionProfile {
    pub target: String,
    pub outputs: HashMap<String, ConnectionOutput>,
}

#[derive(Debug, Deserialize, Clone)]
pub struct ConnectionOutput {
    #[serde(rename = "type")]
    pub output_type: String,
    pub database: Option<String>,
    pub account: Option<String>,
    pub user: Option<String>,
    pub schema: Option<String>,
    pub threads: Option<usize>,
}

pub fn load_connections(
    project_dir: &Path,
    config: &ProjectConfig,
) -> Result<HashMap<String, ConnectionProfile>> {
    let config_subpath = config
        .config_paths
        .first()
        .map(|s: &String| s.as_str())
        .unwrap_or("config");
    let config_dir = project_dir.join(config_subpath);

    // Support yml/yaml
    let paths = [
        config_dir.join("connections.yml"),
        config_dir.join("connections.yaml"),
    ];
    let connections_path = paths.iter().find(|p| p.exists()).ok_or_else(|| {
        anyhow::anyhow!(
            "Could not find connections.yml or connections.yaml in {:?}",
            config_dir
        )
    })?;

    let content = fs::read_to_string(connections_path)?;
    let connections: HashMap<String, ConnectionProfile> =
        serde_yaml::from_str(&content).context("Failed to parse connections file")?;

    Ok(connections)
}
