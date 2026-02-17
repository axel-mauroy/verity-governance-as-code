use serde::{Deserialize, Serialize};
use std::collections::HashMap;

#[derive(Debug, Deserialize, Serialize, Clone, Default)]
pub struct ComplianceConfig {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub pre_flight: Option<Vec<ComplianceCheck>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub post_flight: Option<Vec<ComplianceCheck>>,
}

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct ComplianceCheck {
    pub check: String,
    #[serde(default = "default_severity")]
    pub severity: String,
    #[serde(flatten)]
    pub params: HashMap<String, serde_yaml::Value>,
}

fn default_severity() -> String {
    "error".to_string()
}
