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
    // 🟢 Changer Result<()> par Result<(), VerityError>
    async fn execute(&self, query: &str) -> Result<(), VerityError>;

    // 🟢 Idem ici
    async fn fetch_columns(&self, table_name: &str) -> Result<Vec<ColumnSchema>, VerityError>;

    // 🟢 Abstraction de l'enregistrement de source (ex: read_csv_auto)
    async fn register_source(&self, name: &str, path: &str) -> Result<(), VerityError>;
}
