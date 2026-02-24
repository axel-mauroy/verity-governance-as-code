// verity-core/src/domain/error.rs

use miette::Diagnostic;
use thiserror::Error;

#[derive(Error, Debug, Diagnostic)]
pub enum DomainError {
    #[error("Governance violation: ({_asset_name})")]
    #[diagnostic(
        code(verity::domain::governance),
        help("Security level '{child_level}' cannot depend on '{parent_level}' without masking.")
    )]
    #[allow(unused_assignments)]
    GovernanceViolation {
        _asset_name: String,
        child_level: String,
        parent_level: String,
    },

    #[error("Circular dependency detected involving: {0}")]
    #[diagnostic(code(verity::domain::cycle), help("Check your {{ ref() }} macros."))]
    CircularDependency(String),

    #[error("Model '{0}' not found in manifest")]
    #[diagnostic(code(verity::domain::model_not_found))]
    ModelNotFound(String),

    #[error("Compliance Check Failed: {0}")]
    #[diagnostic(code(verity::domain::compliance))]
    ComplianceError(String),

    #[error("Manifest loading Error: {0}")]
    #[diagnostic(code(verity::domain::manifest))]
    ManifestError(String),

    #[error("Schema Error: {0}")]
    #[diagnostic(code(verity::domain::schema))]
    SchemaError(String),
}
