// verity-core/src/domain/governance/scanner.rs

use crate::domain::error::DomainError;
use crate::domain::governance::pii::{PiiAction, PiiConfig, PiiSeverity};
use regex::Regex;

/// A violation detected in data.
#[derive(Debug, Clone, PartialEq)]
pub struct Violation {
    pub rule_name: String,
    pub severity: PiiSeverity,
    pub action: PiiAction,
    pub matched_value: String, // The part of the text that triggered the alert
}

/// Optimized version of a pattern for runtime.
/// The Regex is compiled only once at initialization.
struct CompiledPattern {
    name: String,
    regex: Regex,
    severity: PiiSeverity,
    action: PiiAction,
}

pub struct PiiScanner {
    patterns: Vec<CompiledPattern>,
    enabled: bool,
}

impl PiiScanner {
    /// Initializes the scanner by compiling all Regex from the config file.
    pub fn new(config: &PiiConfig) -> Result<Self, DomainError> {
        if !config.enabled {
            return Ok(Self {
                patterns: vec![],
                enabled: false,
            });
        }

        let mut compiled_patterns = Vec::new();

        for pattern in &config.patterns {
            match Regex::new(&pattern.regex) {
                Ok(regex) => {
                    compiled_patterns.push(CompiledPattern {
                        name: pattern.name.clone(),
                        regex,
                        severity: pattern.severity.clone(),
                        action: pattern.action.clone(),
                    });
                }
                Err(e) => {
                    // Dans le Domain, on ne veut pas dépendre de 'serde_yaml'.
                    // On peut soit loguer un warning (si on avait un logger injecté),
                    // soit renvoyer une erreur bloquante. Ici, choisissons l'erreur bloquante
                    // car une regex PII malformée est un risque de sécurité (silence = fail).
                    return Err(DomainError::GovernanceViolation {
                        _asset_name: format!("Config Regex: {}", pattern.name),
                        child_level: "Invalid Syntax".to_string(),
                        parent_level: e.to_string(),
                    });
                }
            }
        }

        Ok(Self {
            patterns: compiled_patterns,
            enabled: true,
        })
    }

    /// Scans a string and returns the list of violations found.
    pub fn scan(&self, text: &str) -> Vec<Violation> {
        if !self.enabled {
            return vec![];
        }

        let mut violations = Vec::new();

        for pattern in &self.patterns {
            // find() returns the first occurrence, which is enough to flag a line
            if let Some(mat) = pattern.regex.find(text) {
                violations.push(Violation {
                    rule_name: pattern.name.clone(),
                    severity: pattern.severity.clone(),
                    action: pattern.action.clone(),
                    matched_value: mat.as_str().to_string(),
                });
            }
        }

        violations
    }
}

// --- UNIT TESTS ---
#[cfg(test)]
#[allow(clippy::expect_used)]
mod tests {
    use super::*;
    use crate::domain::governance::pii::PiiPattern;
    use anyhow::Result;

    #[test]
    fn test_pii_scanning_flow() -> Result<()> {
        // 1. Setup config with a pattern
        let config = PiiConfig {
            enabled: true,
            column_policies: vec![],
            patterns: vec![PiiPattern {
                name: "Email".to_string(),
                regex: r"(?i)[a-z0-9._%+-]+@[a-z0-9.-]+\.[a-z]{2,}".to_string(),
                severity: PiiSeverity::High,
                action: PiiAction::Block,
                masking_strategy: None,
            }],
        };

        // 2. Build Scanner
        let scanner = PiiScanner::new(&config)?;

        // 3. Test Positive
        let violations = scanner.scan("Contactez-moi sur ceo@verity.ai pour discuter.");
        assert_eq!(violations.len(), 1);
        assert_eq!(violations[0].matched_value, "ceo@verity.ai");
        assert_eq!(violations[0].action, PiiAction::Block);

        // 4. Test Negative
        let safe_text = "Bonjour tout le monde";
        let violations = scanner.scan(safe_text);
        assert!(violations.is_empty());
        Ok(())
    }

    #[test]
    fn test_invalid_regex_fails() {
        let config = PiiConfig {
            enabled: true,
            column_policies: vec![],
            patterns: vec![PiiPattern {
                name: "Bad Regex".to_string(),
                regex: r"[unclosed-bracket".to_string(), // Regex invalide
                severity: PiiSeverity::Low,
                action: PiiAction::Warn,
                masking_strategy: None,
            }],
        };

        let result = PiiScanner::new(&config);
        assert!(result.is_err(), "Scanner should fail on invalid regex");
    }
}
