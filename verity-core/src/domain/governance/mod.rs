// verity-core/src/domain/governance/mod.rs

pub mod governance_rule;
pub mod guard;
pub mod pii;
pub mod quality;
pub mod rewriter;
pub mod scanner;
pub mod security_level;

// Re-exports
pub use guard::GovernanceGuard;
pub use pii::{ColumnPolicy, PiiAction, PiiConfig, PiiPattern, PiiSeverity};
pub mod configuration;
pub use configuration::GovernanceConfig;
// pub use self::governance::{GovernanceConfig as LegacyGovernanceConfig, PiiConfig, QualityConfig}; // Removed after migration
pub use governance_rule::GovernancePolicySet;
pub use quality::{QualityConfig, QualityRule};
pub use rewriter::PolicyRewriter;
pub use scanner::PiiScanner;
pub use security_level::SecurityLevel;

// Structure ResourceGovernance
use serde::{Deserialize, Serialize};

// This struct is used by models and sources (individual level)
#[derive(Debug, Deserialize, Serialize, Clone, Default)]
pub struct ResourceGovernance {
    #[serde(default)]
    pub public: bool,
    #[serde(default)]
    pub pii: bool,

    // Here, we use the real Domain Type, not a String
    // Serde will handle the conversion "internal" -> SecurityLevel::Internal
    #[serde(default)]
    pub security: SecurityLevel,
}
