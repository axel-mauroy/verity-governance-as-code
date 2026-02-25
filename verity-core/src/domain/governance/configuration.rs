use crate::domain::governance::{PiiConfig, QualityConfig};
use serde::{Deserialize, Serialize};

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct GovernanceConfig {
    #[serde(default)]
    pub pii_detection: PiiConfig,

    #[serde(default)]
    pub data_quality: QualityConfig,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub default_security_level: Option<String>,

    #[serde(default = "default_true")]
    pub strict: bool,

    #[serde(default = "default_anomaly_threshold")]
    pub default_anomaly_threshold: f64,
}

fn default_true() -> bool {
    true
}

fn default_anomaly_threshold() -> f64 {
    0.1
}

impl Default for GovernanceConfig {
    fn default() -> Self {
        Self {
            pii_detection: PiiConfig::default(),
            data_quality: QualityConfig::default(),
            default_security_level: None,
            strict: true,
            default_anomaly_threshold: 0.1,
        }
    }
}
