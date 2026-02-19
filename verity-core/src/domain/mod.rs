pub mod compiler;
pub mod compliance;
pub mod error;
pub mod governance;
pub mod graph;
pub mod ports;
pub mod project;

// Re-exports pratiques pour simplifier les imports ailleurs
pub use error::DomainError;
