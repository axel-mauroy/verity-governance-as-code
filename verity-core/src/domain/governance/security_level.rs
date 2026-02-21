// verity-core/src/domain/governance/security_level.rs

use serde::{Deserialize, Serialize};
use std::fmt;

// Rust implicitly assigns ascending integer discriminators (0, 1, 2, 3), which makes
// SecurityLevel::Public < SecurityLevel::Restricted native and free at runtime.
// This is optimal for our Lineage engine: we can use mathematical comparison operators (>=) to check ACL propagations.

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum SecurityLevel {
    Public, // 0
    #[default]
    Internal, // 1
    Confidential, // 2
    Restricted, // 3
}

impl SecurityLevel {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Public => "public",
            Self::Internal => "internal",
            Self::Confidential => "confidential",
            Self::Restricted => "restricted",
        }
    }
}

impl fmt::Display for SecurityLevel {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.as_str())
    }
}

impl std::str::FromStr for SecurityLevel {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "public" => Ok(Self::Public),
            "internal" => Ok(Self::Internal),
            "confidential" => Ok(Self::Confidential),
            "restricted" => Ok(Self::Restricted),
            _ => Err(format!("Unknown security level: {}", s)),
        }
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;

    #[test]
    fn test_security_level_ordering() {
        assert!(SecurityLevel::Public < SecurityLevel::Internal);
        assert!(SecurityLevel::Internal < SecurityLevel::Confidential);
        assert!(SecurityLevel::Confidential < SecurityLevel::Restricted);
    }

    #[test]
    fn test_security_level_default() {
        assert_eq!(SecurityLevel::default(), SecurityLevel::Internal);
    }

    #[test]
    fn test_display_and_parsing_consistency() -> anyhow::Result<()> {
        let level = SecurityLevel::Confidential;
        assert_eq!(level.to_string(), "confidential");

        use std::str::FromStr;
        assert_eq!(
            SecurityLevel::from_str("public").map_err(|e| anyhow::anyhow!(e))?,
            SecurityLevel::Public
        );
        assert_eq!(
            SecurityLevel::from_str("INTERNAL").map_err(|e| anyhow::anyhow!(e))?,
            SecurityLevel::Internal
        );
        assert!(SecurityLevel::from_str("invalid").is_err());

        Ok(())
    }
}
