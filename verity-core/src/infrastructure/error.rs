// verity-core/src/infrastructure/error.rs

use miette::Diagnostic;
use thiserror::Error;

#[derive(Error, Debug, Diagnostic)]
pub enum DatabaseError {
    #[error("DuckDB Engine Error: {0}")]
    #[diagnostic(
        code(verity::infra::database::duckdb),
        help("An error occurred inside the SQL engine.")
    )]
    DuckDB(#[from] duckdb::Error),
}

#[derive(Error, Debug, Diagnostic)]
pub enum InfrastructureError {
    // --- DATABASE (Abstracted) ---
    #[error(transparent)]
    #[diagnostic(transparent)]
    Database(#[from] DatabaseError),

    // --- FILESYSTEM (IO) ---
    #[error("File System Error: {0}")]
    #[diagnostic(
        code(verity::infra::io),
        help("Check file permissions or path validity.")
    )]
    Io(#[from] std::io::Error),

    // --- CONFIG / YAML ---
    #[error("YAML Parsing Error: {0}")]
    #[diagnostic(
        code(verity::infra::yaml),
        help("Check your YAML syntax (indentation, types).")
    )]
    YamlError(#[from] serde_yaml::Error),

    #[error("Configuration Error: {0}")]
    ConfigError(String),

    #[error("Project configuration not found at '{0}'")]
    #[diagnostic(code(verity::infra::config_missing))]
    ConfigNotFound(String),

    // --- TEMPLATING ---
    #[error("Template Rendering Error: {0}")]
    #[diagnostic(
        code(verity::infra::template),
        help("Check your Jinja syntax ({{ ... }}) inside the SQL file.")
    )]
    TemplateError(#[from] minijinja::Error),
}

// Manual implementation for shortcuts (e.g. `?` operator on duckdb calls)
impl From<duckdb::Error> for InfrastructureError {
    fn from(err: duckdb::Error) -> Self {
        InfrastructureError::Database(DatabaseError::DuckDB(err))
    }
}
