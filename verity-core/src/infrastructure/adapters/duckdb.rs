// verity-core/src/infrastructure/adapters/duckdb.rs

use async_trait::async_trait;
use duckdb::{Config, Connection};
use std::sync::{Arc, Mutex};

// Imports Hexagonaux
use crate::error::VerityError;
use crate::infrastructure::error::{DatabaseError, InfrastructureError};
use crate::ports::connector::{ColumnSchema, Connector};

pub struct DuckDBConnector {
    conn: Arc<Mutex<Connection>>,
}

impl DuckDBConnector {
    pub fn new(db_path: &str) -> Result<Self, InfrastructureError> {
        let config = Config::default();

        // API DUCKDB : open_with_flags ne prend plus AccessMode directement
        // Si tu veux lecture/écriture, c'est le défaut.
        let conn = if db_path == ":memory:" {
            Connection::open_in_memory_with_flags(config)?
        } else {
            Connection::open_with_flags(db_path, config)?
        };

        Ok(Self {
            conn: Arc::new(Mutex::new(conn)),
        })
    }
}

#[async_trait]
impl Connector for DuckDBConnector {
    async fn execute(&self, query: &str) -> Result<(), VerityError> {
        let conn = self.conn.lock().map_err(|_| {
            VerityError::Infrastructure(InfrastructureError::Io(std::io::Error::other(
                "DuckDB Mutex Poisoned",
            )))
        })?;
        conn.execute(query, []).map(|_rows| ()).map_err(|e| {
            VerityError::Infrastructure(InfrastructureError::Database(DatabaseError::DuckDB(e)))
        })
    }

    async fn fetch_columns(&self, table_name: &str) -> Result<Vec<ColumnSchema>, VerityError> {
        let conn = self.conn.lock().map_err(|_| {
            VerityError::Infrastructure(InfrastructureError::Io(std::io::Error::other(
                "DuckDB Mutex Poisoned",
            )))
        })?;

        let mut stmt = conn
            .prepare(&format!("PRAGMA table_info('{}')", table_name))
            .map_err(|e| {
                VerityError::Infrastructure(InfrastructureError::Database(DatabaseError::DuckDB(e)))
            })?;

        let rows = stmt
            .query_map([], |row| {
                Ok(ColumnSchema {
                    name: row.get("name")?,
                    data_type: row.get("type")?,
                    is_nullable: !row.get::<_, bool>("notnull")?,
                })
            })
            .map_err(|e| {
                VerityError::Infrastructure(InfrastructureError::Database(DatabaseError::DuckDB(e)))
            })?;

        let mut columns = Vec::new();
        for row in rows {
            columns.push(row.map_err(|e| {
                VerityError::Infrastructure(InfrastructureError::Database(DatabaseError::DuckDB(e)))
            })?);
        }

        Ok(columns)
    }

    async fn register_source(&self, name: &str, path: &str) -> Result<(), VerityError> {
        let query = format!(
            "CREATE OR REPLACE VIEW \"{}\" AS SELECT * FROM read_csv_auto('{}')",
            name, path
        );
        self.execute(&query).await
    }

    async fn materialize(
        &self,
        table_name: &str,
        sql: &str,
        materialization_type: &str,
    ) -> Result<String, VerityError> {
        let query = match materialization_type {
            "view" => format!("CREATE OR REPLACE VIEW \"{}\" AS {}", table_name, sql),
            "table" => format!("CREATE OR REPLACE TABLE \"{}\" AS {}", table_name, sql),
            _ => return Err(VerityError::InternalError("Unknown Strategy".into())),
        };

        self.execute(&query).await?;
        Ok(materialization_type.to_string())
    }

    async fn query_scalar(&self, query: &str) -> Result<u64, VerityError> {
        let conn = self.conn.lock().map_err(|_| {
            VerityError::Infrastructure(InfrastructureError::Io(std::io::Error::other(
                "DuckDB Mutex Poisoned",
            )))
        })?;
        let mut stmt = conn.prepare(query).map_err(|e| {
            VerityError::Infrastructure(InfrastructureError::Database(DatabaseError::DuckDB(e)))
        })?;

        let mut rows = stmt.query([]).map_err(|e| {
            VerityError::Infrastructure(InfrastructureError::Database(DatabaseError::DuckDB(e)))
        })?;

        let row = rows
            .next()
            .map_err(|e| {
                VerityError::Infrastructure(InfrastructureError::Database(DatabaseError::DuckDB(e)))
            })?
            .ok_or_else(|| VerityError::InternalError("No scalar value returned".into()))?;

        let value: u64 = row.get(0).map_err(|e| {
            VerityError::Infrastructure(InfrastructureError::Database(DatabaseError::DuckDB(e)))
        })?;

        Ok(value)
    }

    fn engine_name(&self) -> &str {
        "duckdb"
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;
    use anyhow::Result;

    #[tokio::test]
    async fn test_duckdb_flow() -> Result<()> {
        let connector = DuckDBConnector::new(":memory:")?;

        // 1. Create table
        connector
            .execute("CREATE TABLE users (id INTEGER, name VARCHAR)")
            .await?;

        // 2. Fetch columns
        let columns = connector.fetch_columns("users").await?;
        assert_eq!(columns.len(), 2);

        let name_col = columns
            .iter()
            .find(|c| c.name == "name")
            .ok_or_else(|| anyhow::anyhow!("Column 'name' not found"))?;
        assert_eq!(name_col.data_type, "VARCHAR");

        let id_col = columns
            .iter()
            .find(|c| c.name == "id")
            .ok_or_else(|| anyhow::anyhow!("Column 'id' not found"))?;
        assert_eq!(id_col.data_type, "INTEGER");
        Ok(())
    }

    #[tokio::test]
    async fn test_duckdb_error() -> Result<()> {
        let connector = DuckDBConnector::new(":memory:")?;
        // Invalid SQL
        let result = connector.execute("SELECT * FROM non_existent_table").await;
        assert!(result.is_err());
        Ok(())
    }
}
