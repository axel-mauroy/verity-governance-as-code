// verity-core/src/domain/governance/masking.rs

use serde::{Deserialize, Serialize};
use std::str::FromStr;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MaskingStrategy {
    Hash,
    Redact,
    Nullify,
    Partial,
    MaskEmail,
    EntityPreserving,
}

impl FromStr for MaskingStrategy {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "hash" => Ok(Self::Hash),
            "redact" => Ok(Self::Redact),
            "nullify" => Ok(Self::Nullify),
            "partial" => Ok(Self::Partial),
            "mask_email" => Ok(Self::MaskEmail),
            "entity_preserving" => Ok(Self::EntityPreserving),
            _ => Err(format!("Unknown masking strategy: {}", s)),
        }
    }
}

impl std::fmt::Display for MaskingStrategy {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let s = match self {
            Self::Hash => "hash",
            Self::Redact => "redact",
            Self::Nullify => "nullify",
            Self::Partial => "partial",
            Self::MaskEmail => "mask_email",
            Self::EntityPreserving => "entity_preserving",
        };
        write!(f, "{}", s)
    }
}
