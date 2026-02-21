// verity-core/src/domain/governance/pii.rs

use crate::domain::governance::masking::MaskingStrategy;
use serde::{Deserialize, Deserializer, Serialize, Serializer, de};
use std::str::FromStr;

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
    pub masking_strategy: Option<MaskingStrategy>,
}

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct ColumnPolicy {
    pub column_name_pattern: String,
    pub policy: PolicyType,
}

impl MaskingStrategy {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Hash => "hash",
            Self::Redact => "redact",
            Self::Nullify => "nullify",
            Self::Partial => "partial",
            Self::MaskEmail => "mask_email",
            Self::EntityPreserving => "entity_preserving",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PolicyType {
    Masking(MaskingStrategy),
    Encryption,
    Drop,
}

impl PolicyType {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Masking(s) => s.as_str(),
            Self::Encryption => "encryption",
            Self::Drop => "drop",
        }
    }
}

impl<'de> Deserialize<'de> for PolicyType {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;
        match s.as_str() {
            "encryption" => Ok(PolicyType::Encryption),
            "drop" => Ok(PolicyType::Drop),
            // On délègue au FromStr de MaskingStrategy pour éviter la duplication
            other => {
                if let Ok(strategy) = MaskingStrategy::from_str(other) {
                    Ok(PolicyType::Masking(strategy))
                } else {
                    Err(de::Error::custom(format!(
                        "Invalid policy type: '{}'. Expected one of: hash, redact, nullify, partial, mask_email, entity_preserving, encryption, drop.",
                        other
                    )))
                }
            }
        }
    }
}

impl Serialize for PolicyType {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(self.as_str())
    }
}

use std::fmt;

impl fmt::Display for PolicyType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.as_str())
    }
}

#[derive(Debug, Deserialize, Serialize, Clone, Copy, PartialEq, Eq, Default)]
#[serde(rename_all = "snake_case")]
pub enum PiiAction {
    Block,
    #[default]
    Warn,
    Mask,
    Ignore,
}

#[derive(Debug, Deserialize, Serialize, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Default)]
#[serde(rename_all = "snake_case")]
pub enum PiiSeverity {
    Low,
    #[default]
    Medium,
    High,
    Critical,
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;
    use anyhow::Result;

    #[test]
    fn test_pii_severity_ordering() {
        assert!(PiiSeverity::Low < PiiSeverity::Critical);
        assert!(PiiSeverity::High > PiiSeverity::Medium);
    }

    #[test]
    fn test_strict_deserialization() -> Result<()> {
        // Test Masking
        let yaml = "column_name_pattern: '*email*'\npolicy: hash";
        let policy: ColumnPolicy = serde_yaml::from_str(yaml)?;
        assert_eq!(policy.policy, PolicyType::Masking(MaskingStrategy::Hash));

        // Test Entity Preserving (Celui qui faisait échouer tes tests !)
        let yaml_ep = "column_name_pattern: '*name*'\npolicy: entity_preserving";
        let policy_ep: ColumnPolicy = serde_yaml::from_str(yaml_ep)?;
        assert_eq!(
            policy_ep.policy,
            PolicyType::Masking(MaskingStrategy::EntityPreserving)
        );

        // Test Encryption
        let yaml_unit = "column_name_pattern: '*id*'\npolicy: encryption";
        let policy_unit: ColumnPolicy = serde_yaml::from_str(yaml_unit)?;
        assert_eq!(policy_unit.policy, PolicyType::Encryption);

        Ok(())
    }
}
