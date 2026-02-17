// verity-core/src/domain/governance/governance_rule.rs
//
// Engine-agnostic domain types for column-level governance policies.
// These types are consumed by engine-specific adapters (e.g. DataFusion's OptimizerRule).

use std::collections::HashMap;

/// A set of column-level masking policies for a given query/model.
#[derive(Debug, Clone)]
pub struct GovernancePolicySet {
    /// Maps column name (lowercase) → masking policy
    pub column_policies: HashMap<String, MaskingPolicy>,
    /// Optional salt for hash-based masking (prevents dictionary attacks on PII)
    pub salt: Option<String>,
}

impl GovernancePolicySet {
    pub fn new() -> Self {
        Self {
            column_policies: HashMap::new(),
            salt: None,
        }
    }

    pub fn is_empty(&self) -> bool {
        self.column_policies.is_empty()
    }

    /// Build a policy set from a list of (column_name, policy_string) pairs.
    pub fn from_pairs(pairs: Vec<(String, String)>) -> Self {
        let mut policies = HashMap::new();
        for (col, policy_str) in pairs {
            if let Ok(policy) = policy_str.parse() {
                policies.insert(col.to_lowercase(), policy);
            }
        }
        Self {
            column_policies: policies,
            salt: None,
        }
    }
}

impl Default for GovernancePolicySet {
    fn default() -> Self {
        Self::new()
    }
}

/// The types of masking that can be applied to a column.
#[derive(Debug, Clone, PartialEq)]
pub enum MaskingPolicy {
    /// SHA256(CAST(col AS VARCHAR))
    Hash,
    /// Replace with 'REDACTED'
    Redact,
    /// Partial email masking: j****@domain.com
    MaskEmail,
    /// Generic PII masking (defaults to SHA256 hash)
    PiiMasking,
}

use std::str::FromStr;

impl FromStr for MaskingPolicy {
    type Err = ();

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "hash" => Ok(Self::Hash),
            "redact" => Ok(Self::Redact),
            "mask_email" => Ok(Self::MaskEmail),
            "pii_masking" => Ok(Self::PiiMasking),
            _ => Err(()),
        }
    }
}

    #[cfg(test)]
    mod tests {
        use super::*;
        use std::str::FromStr;
    
        #[test]
        fn test_masking_policy_from_str() {
            assert_eq!(MaskingPolicy::from_str("hash"), Ok(MaskingPolicy::Hash));
            assert_eq!(
                MaskingPolicy::from_str("redact"),
                Ok(MaskingPolicy::Redact)
            );
            assert_eq!(
                MaskingPolicy::from_str("mask_email"),
                Ok(MaskingPolicy::MaskEmail)
            );
            assert_eq!(
                MaskingPolicy::from_str("pii_masking"),
                Ok(MaskingPolicy::PiiMasking)
            );
            assert_eq!(MaskingPolicy::from_str("unknown"), Err(()));
        }
    
        #[test]
        fn test_policy_set_from_pairs() {
            let pairs = vec![
                ("email".to_string(), "hash".to_string()),
                ("ssn".to_string(), "redact".to_string()),
                ("unknown_col".to_string(), "nonexistent".to_string()),
            ];
            let set = GovernancePolicySet::from_pairs(pairs);
            assert_eq!(set.column_policies.len(), 2);
            assert_eq!(set.column_policies.get("email"), Some(&MaskingPolicy::Hash));
            assert_eq!(set.column_policies.get("ssn"), Some(&MaskingPolicy::Redact));
        }

    #[test]
    fn test_policy_set_empty() {
        let set = GovernancePolicySet::new();
        assert!(set.is_empty());
    }
}
