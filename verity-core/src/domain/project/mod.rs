// verity-core/src/domain/project/mod.rs

pub mod configuration;
pub mod manifest;
pub use configuration::{Engine, ProjectConfig};

pub use manifest::{
    ColumnInfo, LifecycleStatus, Manifest, ManifestNode, MaterializationType, NodeConfig,
    ResourceType, SourceDefinition,
};
