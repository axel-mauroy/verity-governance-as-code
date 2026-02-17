// verity-core/src/infrastructure/adapters/datafusion.rs

use async_trait::async_trait;
use datafusion::prelude::*;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tokio::sync::Mutex;

// Hexagonal Imports
use crate::error::VerityError;
use crate::infrastructure::error::{DatabaseError, InfrastructureError};
use crate::ports::connector::{ColumnSchema, Connector};

pub struct DataFusionConnector {
    ctx: Arc<Mutex<SessionContext>>,
    target_dir: PathBuf,
}

impl DataFusionConnector {
    pub fn new(target_dir: &Path) -> Result<Self, InfrastructureError> {
        let ctx = SessionContext::new();
        Ok(Self {
            ctx: Arc::new(Mutex::new(ctx)),
            target_dir: target_dir.to_path_buf(),
        })
    }
}

#[async_trait]
impl Connector for DataFusionConnector {
    async fn execute(&self, query: &str) -> Result<(), VerityError> {
        let ctx = self.ctx.lock().await;
        let df = ctx.sql(query).await.map_err(|e| {
            VerityError::Infrastructure(InfrastructureError::Database(DatabaseError::DataFusion(e)))
        })?;
        // Collect to trigger execution
        df.collect().await.map_err(|e| {
            VerityError::Infrastructure(InfrastructureError::Database(DatabaseError::DataFusion(e)))
        })?;
        Ok(())
    }

    async fn fetch_columns(&self, table_name: &str) -> Result<Vec<ColumnSchema>, VerityError> {
        let ctx = self.ctx.lock().await;
        let df = ctx.table(table_name).await.map_err(|e| {
            VerityError::Infrastructure(InfrastructureError::Database(DatabaseError::DataFusion(e)))
        })?;

        let schema = df.schema();
        let columns = schema
            .fields()
            .iter()
            .map(|field| ColumnSchema {
                name: field.name().clone(),
                data_type: format!("{:?}", field.data_type()),
                is_nullable: field.is_nullable(),
            })
            .collect();

        Ok(columns)
    }

    async fn register_source(&self, name: &str, path: &str) -> Result<(), VerityError> {
        let ctx = self.ctx.lock().await;
        ctx.register_csv(name, path, CsvReadOptions::default())
            .await
            .map_err(|e| {
                VerityError::Infrastructure(InfrastructureError::Database(
                    DatabaseError::DataFusion(e),
                ))
            })?;
        Ok(())
    }

    async fn materialize(
        &self,
        table_name: &str,
        sql: &str,
        materialization_type: &str,
    ) -> Result<String, VerityError> {
        let ctx = self.ctx.lock().await;

        match materialization_type {
            "view" => {
                // DataFusion supports CREATE OR REPLACE VIEW natively
                let ddl = format!("CREATE OR REPLACE VIEW \"{}\" AS {}", table_name, sql);
                let df = ctx.sql(&ddl).await.map_err(|e| {
                    VerityError::Infrastructure(InfrastructureError::Database(
                        DatabaseError::DataFusion(e),
                    ))
                })?;
                df.collect().await.map_err(|e| {
                    VerityError::Infrastructure(InfrastructureError::Database(
                        DatabaseError::DataFusion(e),
                    ))
                })?;
            }
            "table" => {
                // DataFusion: execute the SQL query, then write results to Parquet
                // and re-register as a table for downstream models
                let df = ctx.sql(sql).await.map_err(|e| {
                    VerityError::Infrastructure(InfrastructureError::Database(
                        DatabaseError::DataFusion(e),
                    ))
                })?;

                // Write to Parquet file in the target directory
                let parquet_dir = self.target_dir.join("data");
                if !parquet_dir.exists() {
                    std::fs::create_dir_all(&parquet_dir)?;
                }
                let parquet_path = parquet_dir.join(format!("{}.parquet", table_name));

                df.write_parquet(
                    parquet_path
                        .to_str()
                        .ok_or_else(|| VerityError::InternalError("Invalid parquet path".into()))?,
                    datafusion::dataframe::DataFrameWriteOptions::new(),
                    None,
                )
                .await
                .map_err(|e| {
                    VerityError::Infrastructure(InfrastructureError::Database(
                        DatabaseError::DataFusion(e),
                    ))
                })?;

                // Re-register the Parquet file as a table for downstream queries
                ctx.register_parquet(
                    table_name,
                    parquet_path
                        .to_str()
                        .ok_or_else(|| VerityError::InternalError("Invalid parquet path".into()))?,
                    ParquetReadOptions::default(),
                )
                .await
                .map_err(|e| {
                    VerityError::Infrastructure(InfrastructureError::Database(
                        DatabaseError::DataFusion(e),
                    ))
                })?;
            }
            _ => return Err(VerityError::InternalError("Unknown Strategy".into())),
        }

        Ok(materialization_type.to_string())
    }

    async fn query_scalar(&self, query: &str) -> Result<u64, VerityError> {
        let ctx = self.ctx.lock().await;
        let df = ctx.sql(query).await.map_err(|e| {
            VerityError::Infrastructure(InfrastructureError::Database(DatabaseError::DataFusion(e)))
        })?;

        let batches = df.collect().await.map_err(|e| {
            VerityError::Infrastructure(InfrastructureError::Database(DatabaseError::DataFusion(e)))
        })?;

        // Extract the first value from the first column of the first batch
        let batch = batches
            .first()
            .ok_or_else(|| VerityError::InternalError("No result returned".into()))?;

        if batch.num_rows() == 0 {
            return Err(VerityError::InternalError(
                "No scalar value returned".into(),
            ));
        }

        let col = batch.column(0);

        // Try to extract as various integer types that DataFusion might return
        use datafusion::arrow::array::{Int64Array, UInt64Array};
        if let Some(arr) = col.as_any().downcast_ref::<Int64Array>() {
            Ok(arr.value(0) as u64)
        } else if let Some(arr) = col.as_any().downcast_ref::<UInt64Array>() {
            Ok(arr.value(0))
        } else {
            // Fallback: try to cast to string and parse
            let str_val = format!("{:?}", col);
            Err(VerityError::InternalError(format!(
                "Could not extract scalar value from column type: {}",
                str_val
            )))
        }
    }

    fn engine_name(&self) -> &str {
        "datafusion"
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_datafusion_execution_and_schema() {
        let tmp = tempfile::tempdir().expect("Failed to create temp dir");
        let connector =
            DataFusionConnector::new(tmp.path()).expect("Failed to create DataFusion connector");

        // Create a table via SQL
        connector
            .execute("CREATE TABLE test_users (id INT, name VARCHAR, age INT) AS VALUES (1, 'Alice', 30), (2, 'Bob', 25)")
            .await
            .expect("Failed to create table");

        // Verify Schema
        let columns = connector
            .fetch_columns("test_users")
            .await
            .expect("Failed to fetch columns");

        assert_eq!(columns.len(), 3);

        let name_col = columns
            .iter()
            .find(|c| c.name == "name")
            .expect("Column 'name' not found");
        assert_eq!(name_col.data_type, "Utf8View");

        let id_col = columns
            .iter()
            .find(|c| c.name == "id")
            .expect("Column 'id' not found");
        assert_eq!(id_col.data_type, "Int32");
    }

    #[tokio::test]
    async fn test_datafusion_query_scalar() {
        let tmp = tempfile::tempdir().expect("Failed to create temp dir");
        let connector =
            DataFusionConnector::new(tmp.path()).expect("Failed to create DataFusion connector");

        connector
            .execute("CREATE TABLE counts (id INT) AS VALUES (1), (2), (3)")
            .await
            .expect("Failed to create table");

        let count = connector
            .query_scalar("SELECT count(*) FROM counts")
            .await
            .expect("Failed to query scalar");

        assert_eq!(count, 3);
    }

    #[tokio::test]
    async fn test_datafusion_error() {
        let tmp = tempfile::tempdir().expect("Failed to create temp dir");
        let connector =
            DataFusionConnector::new(tmp.path()).expect("Failed to create DataFusion connector");

        let result = connector.execute("SELECT * FROM non_existent_table").await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_datafusion_engine_name() {
        let tmp = tempfile::tempdir().expect("Failed to create temp dir");
        let connector =
            DataFusionConnector::new(tmp.path()).expect("Failed to create DataFusion connector");
        assert_eq!(connector.engine_name(), "datafusion");
    }
}
