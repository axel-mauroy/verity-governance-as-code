// verity-core/src/lib.rs

// 1. Mandatory documentation for production code
#![allow(missing_docs)] // On autorise le manque de doc pour le moment

// 2. Memory safety
#![deny(unsafe_code)]
// 3. Robustness
#![warn(clippy::unwrap_used)]
#![warn(clippy::expect_used)]
// 4. Performance
#![warn(clippy::perf)]

// --- MODULES HEXAGONAUX ---

// 1. Ports (Interfaces / Traits)
// Définit les contrats (Connector, Logger...)
pub mod ports;

// 2. Domain (Cœur du métier)
// Règles de gouvernance, Graphe DAG, Manifeste...
// Ne dépend de RIEN d'autre (ni infra, ni app).
pub mod domain;

// 3. Infrastructure (Adapters)
// Implémentation technique (DuckDB, Config Files, Jinja, Discovery)
// Dépend du Domain et des Ports.
pub mod infrastructure;

// 4. Application (Use Cases)
// Orchestration (Pipeline, Clean, Materialization)
// Dépend du Domain, de l'Infra et des Ports.
pub mod application;

// --- GESTION DES ERREURS GLOBALE ---
pub mod error;

// --- RE-EXPORTS (FACADE) ---
// Permet d'importer l'erreur principale facilement : use verity_core::VerityError;
pub use error::VerityError;
