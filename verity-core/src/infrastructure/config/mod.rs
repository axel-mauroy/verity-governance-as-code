pub mod connection;
pub mod governance;
pub mod project;
pub mod schema;
pub mod source;

pub use crate::domain::project::ProjectConfig;
pub use connection::{ConnectionProfile, load_connections};
pub use governance::{GovernanceConfig, PiiConfig, QualityConfig};
pub use schema::{ColumnSchema, ModelSchema, SchemaFile};
pub use source::{SourceConfig, SourceList, load_sources};
