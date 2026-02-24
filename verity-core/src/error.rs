// verity-core/src/error.rs

use crate::domain::error::DomainError;
use crate::infrastructure::error::InfrastructureError;
use thiserror::Error;

#[derive(Error, Debug)]
pub enum VerityError {
    // --- ERREURS DU DOMAINE (Règles métier, Cycles, Governance) ---
    #[error(transparent)]
    Domain(#[from] DomainError),

    // --- ERREURS D'INFRASTRUCTURE (IO, Parsing) ---
    #[error(transparent)]
    Infrastructure(#[from] InfrastructureError),

    // --- ERREURS GÉNÉRIQUES / APPLICATIVES ---
    #[error("Internal Error: {0}")]
    InternalError(String),

    #[error("Unsafe path traversal detected: {0}")]
    UnsafePath(String),
}

// Manual implementation to avoid duplicate enum variant but keep ergonomics
impl From<std::io::Error> for VerityError {
    fn from(err: std::io::Error) -> Self {
        VerityError::Infrastructure(InfrastructureError::Io(err))
    }
}
