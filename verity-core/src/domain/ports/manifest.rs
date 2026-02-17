use crate::domain::error::DomainError;
use crate::domain::project::Manifest;
use crate::domain::project::configuration::ProjectConfig;
use std::path::Path;

pub trait ManifestLoader: Send + Sync {
    fn load(&self, root: &Path, config: &ProjectConfig) -> Result<Manifest, DomainError>;
}
