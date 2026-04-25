// verity-bigquery/src/connector.rs

use async_trait::async_trait;
use verity_core::error::VerityError;
use verity_core::ports::connector::{ColumnSchema, Connector};

use crate::client::BqEngine;
use crate::config::BqConfig;
use crate::utils::cell_value;

pub struct BigQueryConnector {
    engine: BqEngine,
    project_id: String,
    dataset_id: String,
}

impl BigQueryConnector {
    pub fn new(config: BqConfig) -> Self {
        Self {
            engine: BqEngine::new(config.client, config.project_id.clone()),
            project_id: config.project_id,
            dataset_id: config.dataset_id,
        }
    }
}

#[async_trait]
impl Connector for BigQueryConnector {
    fn engine_name(&self) -> &str {
        "BigQuery"
    }

    async fn execute(&self, query: &str) -> Result<(), VerityError> {
        self.engine.run_query(query).await?;
        Ok(())
    }

    async fn fetch_columns(&self, table_name: &str) -> Result<Vec<ColumnSchema>, VerityError> {
        // Prevent SQL Injection
        if !table_name.chars().all(|c| c.is_alphanumeric() || c == '_') {
            tracing::error!(
                "💥 Security Warning: Invalid table name characters detected: {}",
                table_name
            );
            return Err(VerityError::InternalError(
                "Invalid table name. Only alphanumeric and underscores allowed.".into(),
            ));
        }

        let query = format!(
            "SELECT column_name, data_type, is_nullable FROM `{}.{}.INFORMATION_SCHEMA.COLUMNS` WHERE table_name = '{}'",
            self.project_id, self.dataset_id, table_name
        );

        let rows_opt = self.engine.run_query(&query).await?;

        let mut columns = Vec::new();
        if let Some(rows) = rows_opt {
            for row in rows {
                let name = cell_value(&row, 0).ok_or_else(|| {
                    VerityError::InternalError("Missing column_name in schema".into())
                })?;
                let data_type = cell_value(&row, 1).ok_or_else(|| {
                    VerityError::InternalError("Missing data_type in schema".into())
                })?;
                let is_nullable_str = cell_value(&row, 2).unwrap_or_else(|| "NO".to_string());

                columns.push(ColumnSchema {
                    name,
                    data_type,
                    is_nullable: is_nullable_str.to_uppercase() == "YES",
                });
            }
        }
        Ok(columns)
    }

    async fn register_source(
        &self,
        _name: &str,
        _path: &std::path::Path,
    ) -> Result<(), VerityError> {
        Err(VerityError::InternalError(
            "register_source not supported on BigQuery connector binary.".into(),
        ))
    }

    async fn materialize(
        &self,
        table_name: &str,
        sql: &str,
        materialization_type: &str,
    ) -> Result<String, VerityError> {
        tracing::info!(
            "🚀 Materializing '{}' as '{}'",
            table_name,
            materialization_type
        );
        let full_name = format!("`{}.{}.{}`", self.project_id, self.dataset_id, table_name);
        let ddl = match materialization_type {
            "view" => format!("CREATE OR REPLACE VIEW {full_name} AS {sql}"),
            "table" => format!("CREATE OR REPLACE TABLE {full_name} AS {sql}"),
            _ => {
                tracing::error!("💥 Invalid materialization type: {}", materialization_type);
                return Err(VerityError::InternalError(
                    "Invalid materialization type".into(),
                ));
            }
        };

        tracing::info!("Executing DDL: {}", ddl);
        self.execute(&ddl).await?;

        tracing::info!("✅ Successfully materialized {}", full_name);
        Ok(full_name)
    }

    async fn query_scalar(&self, query: &str) -> Result<u64, VerityError> {
        let rows_opt = self.engine.run_query(query).await?;
        let rows = rows_opt.ok_or_else(|| VerityError::InternalError("No rows returned".into()))?;

        let first_row = rows
            .first()
            .ok_or_else(|| VerityError::InternalError("No rows returned".into()))?;

        let cell_str = cell_value(first_row, 0)
            .ok_or_else(|| VerityError::InternalError("Scalar result is NULL".into()))?;

        let val: u64 = cell_str.parse().map_err(|e| {
            VerityError::InternalError(format!(
                "Failed to parse u64 from '{}'. Ensure your query uses CAST(x AS INT64): {}",
                cell_str, e
            ))
        })?;

        Ok(val)
    }
}
