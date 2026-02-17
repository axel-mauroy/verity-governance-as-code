// domain/governance/guard.rs

use crate::domain::error::DomainError;
use crate::domain::governance::SecurityLevel;
use crate::domain::governance::pii::{PiiAction, PiiSeverity};
use crate::domain::governance::scanner::{PiiScanner, Violation};

pub struct GovernanceGuard {
    scanner: PiiScanner,
    strict_mode: bool,
}

impl GovernanceGuard {
    pub fn new(scanner: PiiScanner, strict_mode: bool) -> Self {
        Self {
            scanner,
            strict_mode,
        }
    }

    pub fn audit_data(&self, content: &str, context: &str) -> Result<(), DomainError> {
        let violations = self.scanner.scan(content);

        if violations.is_empty() {
            return Ok(());
        }

        for v in violations {
            match v.action {
                PiiAction::Block => {
                    let err = DomainError::GovernanceViolation {
                        _asset_name: context.to_string(),
                        child_level: format!("{:?}", v.severity),
                        parent_level: format!("Rule: {}", v.rule_name),
                    };

                    if self.strict_mode {
                        return Err(err);
                    } else {
                        eprintln!(
                            "⚠️  [Governance Bypass] Blocking Violation on '{}': {} (Strict Mode: OFF)",
                            context, v.rule_name
                        );
                    }
                }
                PiiAction::Warn => {
                    eprintln!(
                        "⚠️  [Governance] Warning on '{}': Detected {} ({})",
                        context, v.rule_name, v.matched_value
                    );
                }
                PiiAction::Mask => {
                    // Correction ici : suppression de la ligne en doublon qui causait l'erreur
                    eprintln!(
                        "⚠️  [Governance] Unmasked Data on '{}': Should be masked per rule {}",
                        context, v.rule_name
                    );
                }
                PiiAction::Ignore => {}
            }
        }

        Ok(())
    }

    pub fn validate_security_boundary(
        &self,
        current_level: &SecurityLevel,
        violations: &[Violation],
    ) -> Result<(), DomainError> {
        let is_open_environment = matches!(
            current_level,
            SecurityLevel::Public | SecurityLevel::Internal
        );

        if is_open_environment {
            for v in violations {
                if matches!(v.severity, PiiSeverity::High | PiiSeverity::Critical) {
                    return Err(DomainError::GovernanceViolation {
                        _asset_name: "Security Boundary Check".into(),
                        child_level: format!("{:?}", v.severity),
                        parent_level: format!("{:?} Environment", current_level),
                    });
                }
            }
        }
        Ok(())
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;
    use crate::domain::governance::pii::{PiiAction, PiiConfig, PiiPattern, PiiSeverity};

    fn create_test_scanner(action: PiiAction) -> PiiScanner {
        let config = PiiConfig {
            enabled: true,
            column_policies: vec![],
            patterns: vec![PiiPattern {
                name: "TestRule".to_string(),
                regex: "SECRET".to_string(),
                severity: PiiSeverity::High,
                action,
                masking_strategy: None,
            }],
        };
        PiiScanner::new(&config).unwrap()
    }

    #[test]
    fn test_audit_data_block() {
        let scanner = create_test_scanner(PiiAction::Block);
        let guard = GovernanceGuard::new(scanner, true);

        let result = guard.audit_data("This contains a SECRET value", "test_context");
        assert!(result.is_err());
        match result {
            Err(DomainError::GovernanceViolation { _asset_name, .. }) => {
                assert_eq!(_asset_name, "test_context");
            }
            _ => panic!("Expected GovernanceViolation"),
        }
    }

    #[test]
    fn test_audit_data_warn() {
        let scanner = create_test_scanner(PiiAction::Warn);
        let guard = GovernanceGuard::new(scanner, true);

        // Should print warning to stderr but return Ok
        let result = guard.audit_data("This contains a SECRET value", "test_context");
        assert!(result.is_ok());
    }

    #[test]
    fn test_security_boundary_violation() {
        let scanner = create_test_scanner(PiiAction::Warn); // Action doesn't matter for this test directly, 
        // but we construct violations manually below usually.
        // However, validate_security_boundary takes violations slice.
        let guard = GovernanceGuard::new(scanner, true);

        let violations = vec![Violation {
            rule_name: "CriticalRule".into(),
            severity: PiiSeverity::Critical,
            action: PiiAction::Warn,
            matched_value: "val".into(),
        }];

        let result = guard.validate_security_boundary(&SecurityLevel::Public, &violations);
        assert!(result.is_err());
    }

    #[test]
    fn test_security_boundary_compliant() {
        let scanner = create_test_scanner(PiiAction::Warn);
        let guard = GovernanceGuard::new(scanner, true);

        let violations = vec![Violation {
            rule_name: "LowRule".into(),
            severity: PiiSeverity::Low, // Low severity is allowed in public?
            // Implementation says: if matches!(High | Critical) -> Err
            action: PiiAction::Warn,
            matched_value: "val".into(),
        }];

        let result = guard.validate_security_boundary(&SecurityLevel::Public, &violations);
        assert!(result.is_ok());
    }
    #[test]
    fn test_audit_data_block_bypass() {
        let scanner = create_test_scanner(PiiAction::Block);
        // strict_mode = false
        let guard = GovernanceGuard::new(scanner, false);

        let result = guard.audit_data("This contains a SECRET value", "test_context");
        // Should be Ok because strict_mode is false, even if Action is Block
        assert!(result.is_ok());
    }
}
