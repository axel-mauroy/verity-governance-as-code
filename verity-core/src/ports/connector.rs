// verity-core/src/ports/connector.rs

// This file defines what your application needs, without knowing how it's done.
// Analogy: It's the electrical outlet in the wall. It defines the shape (2 holes) and the voltage (220V), but
// it doesn't know if the electricity comes from nuclear, wind, or coal.

use crate::error::VerityError;
use async_trait::async_trait;

// Struct simple pour décrire une colonne (indépendant de la DB)
#[derive(Debug, Clone)]
pub struct ColumnSchema {
    pub name: String,
    pub data_type: String,
    pub is_nullable: bool,
}

#[async_trait]
pub trait Connector: Send + Sync {
    /// Execute a SQL statement (DDL or DML, no result expected).
    async fn execute(&self, query: &str) -> Result<(), VerityError>;

    /// Fetch the column schema of a table/view.
    async fn fetch_columns(&self, table_name: &str) -> Result<Vec<ColumnSchema>, VerityError>;

    /// Register a data source (e.g. CSV file) as a named table/view.
    async fn register_source(&self, name: &str, path: &str) -> Result<(), VerityError>;

    /// Materialize a SQL query as a table or view.
    async fn materialize(
        &self,
        table_name: &str,
        sql: &str,
        materialization_type: &str,
    ) -> Result<String, VerityError>;

    /// Execute a query and return a single scalar u64 value.
    async fn query_scalar(&self, query: &str) -> Result<u64, VerityError>;

    /// Return the engine name (for logging purposes).
    fn engine_name(&self) -> &str;

    /// Whether this engine handles governance at the plan level (e.g. DataFusion optimizer rules).
    /// When true, the pipeline skips the SQL-string-based PolicyRewriter
    /// and relies on the engine's built-in governance rules instead.
    fn supports_plan_governance(&self) -> bool {
        false
    }

    /// Register governance masking policies at the engine level.
    /// Default is no-op (engines like DuckDB use string-based PolicyRewriter instead).
    async fn register_governance(
        &self,
        _policies: crate::domain::governance::GovernancePolicySet,
    ) {
        // No-op by default
    }
}
