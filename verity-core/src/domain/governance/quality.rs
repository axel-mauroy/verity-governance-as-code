// verity-core/src/domain/governance/quality.rs

use serde::{Deserialize, Serialize};

#[derive(Debug, Deserialize, Serialize, Clone, Default)]
pub struct QualityConfig {
    #[serde(default)]
    pub enabled: bool,
    #[serde(default)]
    pub rules: Vec<QualityRule>,
}

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct QualityRule {
    pub name: String,
    pub description: Option<String>,
    pub severity: String,

    // Targets
    pub column: Option<String>,
    pub columns: Option<Vec<String>>,

    // Validation criteria
    pub min_value: Option<i64>,
    pub max_value: Option<i64>,
    pub pattern: Option<String>,
    pub expected_dimensions: Option<usize>,
    pub check_normalization: Option<bool>,
    pub reference_table: Option<String>,
    pub values: Option<Vec<String>>,
}
