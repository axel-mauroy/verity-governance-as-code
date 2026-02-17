// verity-core/src/domain/governance/pii.rs

use serde::{Deserialize, Serialize};

#[derive(Debug, Deserialize, Serialize, Clone, Default)]
pub struct PiiConfig {
    #[serde(default)]
    pub enabled: bool,
    #[serde(default)]
    pub patterns: Vec<PiiPattern>,
    #[serde(default)]
    pub column_policies: Vec<ColumnPolicy>,
}

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct PiiPattern {
    pub name: String,
    pub regex: String,
    pub severity: PiiSeverity,
    pub action: PiiAction,
    pub masking_strategy: Option<String>,
}

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct ColumnPolicy {
    pub column_name_pattern: String,
    pub policy: String,
}

// --- NOUVEAUX ENUMS ---

#[derive(Debug, Deserialize, Serialize, Clone, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum PiiAction {
    Block,
    Warn,
    Mask,
    Ignore,
}

#[derive(Debug, Deserialize, Serialize, Clone, PartialEq, Eq, PartialOrd, Ord)]
#[serde(rename_all = "snake_case")]
pub enum PiiSeverity {
    Low,
    Medium,
    High,
    Critical,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_pii_severity_ordering() {
        assert!(PiiSeverity::Low < PiiSeverity::Critical);
        assert!(PiiSeverity::High > PiiSeverity::Medium);
    }

    #[test]
    fn test_pii_config_defaults() {
        let config = PiiConfig::default();
        assert!(!config.enabled);
        assert!(config.patterns.is_empty());
    }
}
