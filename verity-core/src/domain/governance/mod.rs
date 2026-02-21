// verity-core/src/domain/governance/mod.rs

pub mod configuration;
pub mod governance_rule;
pub mod guard;
pub mod linter;
pub mod masking;
pub mod pii;
pub mod quality;
pub mod rewriter;
pub mod scanner;
pub mod security_level;
pub mod semantic;

// Re-exports
pub use configuration::GovernanceConfig;
pub use governance_rule::GovernancePolicySet;
pub use guard::GovernanceGuard;
pub use linter::GovernanceLinter;
pub use masking::MaskingStrategy;
pub use pii::{ColumnPolicy, PiiAction, PiiConfig, PiiPattern, PiiSeverity, PolicyType};
pub use quality::{QualityConfig, QualityRule};
pub use rewriter::PolicyRewriter;
pub use scanner::PiiScanner;
pub use security_level::SecurityLevel;

use serde::{Deserialize, Serialize};

#[derive(Debug, Deserialize, Serialize, Clone, Default)]
pub struct ResourceGovernance {
    #[serde(default)]
    pub public: bool,
    #[serde(default)]
    pub pii: bool,
    #[serde(default)]
    pub security: SecurityLevel,
}
