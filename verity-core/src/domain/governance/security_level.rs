// verity-core/src/domain/governance/security_level.rs

use serde::{Deserialize, Serialize};
use std::fmt;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum SecurityLevel {
    Public, // 0
    #[default]
    Internal, // 1
    Confidential, // 2
    Restricted, // 3
}

impl fmt::Display for SecurityLevel {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{:?}", self)
    }
}

#[cfg(test)]
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
}
