// verity-core/src/application/mod.rs

pub mod catalog;
pub mod clean;
pub mod engine;
pub mod materialization;
pub mod ports;

pub mod pipeline;
pub mod validation;

// --- RE-EXPORTS (FACADE PATTERN) ---
// Cela permet au CLI de faire :
// `use verity_core::application::{run_pipeline, clean_project, CatalogGenerator};`
// sans avoir à connaître la structure interne des fichiers.

pub use catalog::CatalogGenerator;
pub use clean::clean_project;
pub use engine::execute_query;
pub use materialization::Materializer;
pub use pipeline::run_pipeline;
pub use validation::run_tests;
