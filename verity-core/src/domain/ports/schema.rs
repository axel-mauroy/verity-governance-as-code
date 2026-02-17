use crate::domain::error::DomainError;
use crate::domain::governance::ColumnPolicy;
use crate::domain::project::LifecycleStatus;
use std::path::Path;

pub trait SchemaSource: Send + Sync {
    fn update_model_columns(
        &self,
        path: &Path,
        model_name: &str,
        columns: &[String],
    ) -> Result<(), DomainError>;

    fn create_versioned_model(
        &self,
        path: &Path,
        model_name: &str,
        version: u32,
        columns: &[String],
        policies: &[ColumnPolicy],
        status: Option<LifecycleStatus>,
    ) -> Result<(), DomainError>;
}
