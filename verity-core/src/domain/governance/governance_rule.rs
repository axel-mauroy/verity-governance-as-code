// verity-core/src/domain/governance/governance_rule.rs
use super::masking::MaskingStrategy; // On importe la stratégie centralisée
use std::collections::HashMap;
use std::str::FromStr;

#[derive(Debug, Clone, Default)]
pub struct GovernancePolicySet {
    pub column_policies: HashMap<String, MaskingStrategy>,
    pub salt: Option<String>,
}

impl GovernancePolicySet {
    pub fn from_pairs(pairs: Vec<(String, String)>) -> Self {
        let mut policies = HashMap::new();
        for (col, strategy_str) in pairs {
            if let Ok(strategy) = MaskingStrategy::from_str(&strategy_str) {
                policies.insert(col.to_lowercase(), strategy);
            }
        }
        Self {
            column_policies: policies,
            salt: None,
        }
    }
}
