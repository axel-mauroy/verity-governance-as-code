// verity-core/src/infrastructure/adapters/datafusion.rs

use async_trait::async_trait;
use datafusion::prelude::*;
use std::path::{Path, PathBuf};
use std::sync::Arc;
// use tokio::sync::Mutex;

// Hexagonal Imports
use crate::domain::governance::governance_rule::GovernancePolicySet;
use crate::error::VerityError;
use crate::infrastructure::adapters::governance_optimizer::GovernanceRule;
use crate::infrastructure::error::{DatabaseError, InfrastructureError};
use crate::ports::connector::{ColumnSchema, Connector};

use datafusion::arrow::array::Array;
use datafusion::arrow::datatypes::DataType;
use datafusion::logical_expr::{Volatility, create_udf};

pub struct DataFusionConnector {
    ctx: Arc<SessionContext>,
    target_dir: PathBuf,
}

impl DataFusionConnector {
    pub fn new(target_dir: &Path) -> Result<Self, InfrastructureError> {
        let ctx = SessionContext::new();
        Self::register_error_udf(&ctx);
        Ok(Self {
            ctx: Arc::new(ctx),
            target_dir: target_dir.to_path_buf(),
        })
    }

    /// Register a custom UDF 'error(msg)' that panics the execution plan.
    /// This is used by data quality tests to abort the pipeline.
    fn register_error_udf(ctx: &SessionContext) {
        use datafusion::logical_expr::ColumnarValue;
        use datafusion::scalar::ScalarValue;

        let error_func = Arc::new(|args: &[ColumnarValue]| {
            let arg = &args[0];

            let msg = match arg {
                ColumnarValue::Array(arr) => arr
                    .as_any()
                    .downcast_ref::<datafusion::arrow::array::StringArray>()
                    .map(|a| a.value(0).to_string())
                    .unwrap_or_else(|| "Unknown Data Quality Error".to_string()),
                ColumnarValue::Scalar(scalar) => match scalar {
                    ScalarValue::Utf8(Some(s)) => s.clone(),
                    ScalarValue::LargeUtf8(Some(s)) => s.clone(),
                    ScalarValue::Utf8View(Some(s)) => s.clone(),
                    _ => "Unknown Data Quality Error".to_string(),
                },
            };

            Err(datafusion::error::DataFusionError::Execution(msg))
        });

        let udf = create_udf(
            "error",
            vec![DataType::Utf8],
            DataType::Int64,
            Volatility::Immutable,
            error_func,
        );

        ctx.register_udf(udf);
    }

    /// Register governance masking rules as a DataFusion optimizer rule.
    /// Once registered, every SQL query through this session will have
    /// matching columns automatically rewritten at the logical plan level.
    pub async fn register_governance_rules(&self, policy_set: GovernancePolicySet) {
        if policy_set.column_policies.is_empty() {
            return;
        }
        self.ctx
            .add_analyzer_rule(Arc::new(GovernanceRule::new(policy_set)));
        println!("    🛡️  Governance rules registered at plan level (DataFusion optimizer)");
    }
}

#[async_trait]
impl Connector for DataFusionConnector {
    async fn execute(&self, query: &str) -> Result<(), VerityError> {
        let df = self.ctx.sql(query).await.map_err(|e| {
            VerityError::Infrastructure(InfrastructureError::Database(DatabaseError::DataFusion(e)))
        })?;
        // Collect to trigger execution
        df.collect().await.map_err(|e| {
            VerityError::Infrastructure(InfrastructureError::Database(DatabaseError::DataFusion(e)))
        })?;
        Ok(())
    }

    async fn fetch_sample(
        &self,
        query: &str,
    ) -> Result<Vec<datafusion::arrow::record_batch::RecordBatch>, VerityError> {
        let df = self.ctx.sql(query).await.map_err(|e| {
            VerityError::Infrastructure(InfrastructureError::Database(DatabaseError::DataFusion(e)))
        })?;
        df.collect().await.map_err(|e| {
            VerityError::Infrastructure(InfrastructureError::Database(DatabaseError::DataFusion(e)))
        })
    }

    async fn fetch_columns(&self, table_name: &str) -> Result<Vec<ColumnSchema>, VerityError> {
        let df = self.ctx.table(table_name).await.map_err(|e| {
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

    async fn register_source(&self, name: &str, path: &std::path::Path) -> Result<(), VerityError> {
        let path_str = path.to_str().ok_or_else(|| {
            VerityError::InternalError(format!("Invalid path for source {}: {:?}", name, path))
        })?;
        self.ctx
            .register_csv(name, path_str, CsvReadOptions::default())
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
        match materialization_type {
            "view" => {
                // CORRECTION: Use DataFrame API to register view programmatically
                // instead of raw SQL DDL, to avoid double-quoting issues with UniversalQuoter
                let df = self.ctx.sql(sql).await.map_err(|e| {
                    VerityError::Infrastructure(InfrastructureError::Database(
                        DatabaseError::DataFusion(e),
                    ))
                })?;

                self.ctx
                    .register_table(table_name, df.into_view())
                    .map_err(|e| {
                        VerityError::Infrastructure(InfrastructureError::Database(
                            DatabaseError::DataFusion(e),
                        ))
                    })?;
            }
            "table" => {
                // DataFusion: execute the SQL query, then write results to Parquet
                // and re-register as a table for downstream models
                let df = self.ctx.sql(sql).await.map_err(|e| {
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
                self.ctx
                    .register_parquet(
                        table_name,
                        parquet_path.to_str().ok_or_else(|| {
                            VerityError::InternalError("Invalid parquet path".into())
                        })?,
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
        let df = self.ctx.sql(query).await.map_err(|e| {
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
        // CORRECTION: Add Int32 support which is common for small counts
        use datafusion::arrow::array::{Int32Array, Int64Array, UInt64Array};

        if let Some(arr) = col.as_any().downcast_ref::<Int64Array>() {
            Ok(arr.value(0) as u64)
        } else if let Some(arr) = col.as_any().downcast_ref::<Int32Array>() {
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

    async fn fetch_column_averages(
        &self,
        table_name: &str,
        columns: &[&str],
    ) -> Result<std::collections::HashMap<String, f64>, VerityError> {
        if columns.is_empty() {
            return Ok(std::collections::HashMap::new());
        }

        // Single query: SELECT AVG("c1") AS c1, AVG("c2") AS c2, ... FROM "table"
        let select_parts: Vec<String> = columns
            .iter()
            .map(|c| format!("AVG(\"{}\") AS \"{}\"", c, c))
            .collect();
        let query = format!("SELECT {} FROM \"{}\"", select_parts.join(", "), table_name);

        let df = self.ctx.sql(&query).await.map_err(|e| {
            VerityError::Infrastructure(InfrastructureError::Database(DatabaseError::DataFusion(e)))
        })?;
        let batches = df.collect().await.map_err(|e| {
            VerityError::Infrastructure(InfrastructureError::Database(DatabaseError::DataFusion(e)))
        })?;

        let mut result = std::collections::HashMap::new();

        if let Some(batch) = batches.first()
            && batch.num_rows() > 0
        {
            use datafusion::arrow::array::{Float32Array, Float64Array};

            for (i, &col_name) in columns.iter().enumerate() {
                let col = batch.column(i);
                let val = if let Some(arr) = col.as_any().downcast_ref::<Float64Array>() {
                    if arr.is_valid(0) {
                        Some(arr.value(0))
                    } else {
                        None
                    }
                } else if let Some(arr) = col.as_any().downcast_ref::<Float32Array>() {
                    if arr.is_valid(0) {
                        Some(arr.value(0) as f64)
                    } else {
                        None
                    }
                } else {
                    None
                };
                if let Some(v) = val {
                    result.insert(col_name.to_string(), v);
                }
            }
        }
        Ok(result)
    }

    fn supports_plan_governance(&self) -> bool {
        true
    }

    async fn register_governance(&self, policies: crate::domain::governance::GovernancePolicySet) {
        self.register_governance_rules(policies).await;
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;
    use anyhow::Result;

    #[tokio::test]
    async fn test_datafusion_execution_and_schema() -> Result<()> {
        let tmp = tempfile::tempdir()?;
        let connector = DataFusionConnector::new(tmp.path())?;

        // Create a table via SQL
        connector
            .execute("CREATE TABLE test_users (id INT, name VARCHAR, age INT) AS VALUES (1, 'Alice', 30), (2, 'Bob', 25)")
            .await?;

        // Verify Schema
        let columns = connector.fetch_columns("test_users").await?;

        assert_eq!(columns.len(), 3);

        let name_col = columns
            .iter()
            .find(|c| c.name == "name")
            .ok_or_else(|| anyhow::anyhow!("Column 'name' not found"))?;
        assert_eq!(name_col.data_type, "Utf8View");

        let id_col = columns
            .iter()
            .find(|c| c.name == "id")
            .ok_or_else(|| anyhow::anyhow!("Column 'id' not found"))?;
        assert_eq!(id_col.data_type, "Int32");
        Ok(())
    }

    #[tokio::test]
    async fn test_datafusion_query_scalar() -> Result<()> {
        let tmp = tempfile::tempdir()?;
        let connector = DataFusionConnector::new(tmp.path())?;

        connector
            .execute("CREATE TABLE counts (id INT) AS VALUES (1), (2), (3)")
            .await?;

        let count = connector
            .query_scalar("SELECT count(*) FROM counts")
            .await?;

        assert_eq!(count, 3);
        Ok(())
    }

    #[tokio::test]
    async fn test_datafusion_error() -> Result<()> {
        let tmp = tempfile::tempdir()?;
        let connector = DataFusionConnector::new(tmp.path())?;

        let result = connector.execute("SELECT * FROM non_existent_table").await;
        assert!(result.is_err());
        Ok(())
    }

    #[tokio::test]
    async fn test_datafusion_engine_name() -> Result<()> {
        let tmp = tempfile::tempdir()?;
        let connector = DataFusionConnector::new(tmp.path())?;
        assert_eq!(connector.engine_name(), "datafusion");
        Ok(())
    }
}
