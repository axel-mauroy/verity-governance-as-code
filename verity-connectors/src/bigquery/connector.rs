// verity-connectors/src/bigquery/connector.rs

use async_trait::async_trait;
use gcp_bigquery_client::Client;
use gcp_bigquery_client::model::query_request::QueryRequest;
use std::path::Path;
use tracing::info;
use verity_core::error::VerityError;
use verity_core::ports::connector::{ColumnSchema, Connector};

pub struct BigQueryConnector {
    client: Client,
    project_id: String,
    dataset_id: String,
}

impl BigQueryConnector {
    /// Create a new BigQuery connector using a service account key file.
    pub async fn from_service_account_key(
        sa_key_path: &str,
        project_id: &str,
        dataset_id: &str,
    ) -> Result<Self, VerityError> {
        let client = Client::from_service_account_key_file(sa_key_path)
            .await
            .map_err(|e| VerityError::InternalError(format!("GCP Auth Error: {e}")))?;

        Ok(Self {
            client,
            project_id: project_id.to_string(),
            dataset_id: dataset_id.to_string(),
        })
    }

    /// Helper: extract a string value from a TableCell at the given index.
    fn cell_value(row: &gcp_bigquery_client::model::table_row::TableRow, index: usize) -> String {
        row.columns
            .as_ref()
            .and_then(|cols| cols.get(index))
            .and_then(|cell| cell.value.as_ref())
            .map(|v| match v {
                serde_json::Value::String(s) => s.clone(),
                other => other.to_string(),
            })
            .unwrap_or_default()
    }
}

#[async_trait]
impl Connector for BigQueryConnector {
    fn engine_name(&self) -> &str {
        "BigQuery"
    }

    async fn execute(&self, query: &str) -> Result<(), VerityError> {
        info!("Executing BigQuery statement");
        let request = QueryRequest::new(query);
        self.client
            .job()
            .query(&self.project_id, request)
            .await
            .map_err(|e| VerityError::InternalError(format!("BigQuery Execution Error: {e}")))?;

        Ok(())
    }

    async fn fetch_columns(&self, table_name: &str) -> Result<Vec<ColumnSchema>, VerityError> {
        let query = format!(
            "SELECT column_name, data_type, is_nullable \
             FROM `{}.{}.INFORMATION_SCHEMA.COLUMNS` \
             WHERE table_name = '{}'",
            self.project_id, self.dataset_id, table_name
        );

        let request = QueryRequest::new(&query);
        let response = self
            .client
            .job()
            .query(&self.project_id, request)
            .await
            .map_err(|e| VerityError::InternalError(format!("BigQuery Schema Error: {e}")))?;

        let mut columns = Vec::new();
        if let Some(rows) = response.rows.as_ref() {
            for row in rows {
                let name = Self::cell_value(row, 0);
                let data_type = Self::cell_value(row, 1);
                let is_nullable_str = Self::cell_value(row, 2);

                columns.push(ColumnSchema {
                    name,
                    data_type,
                    is_nullable: is_nullable_str == "YES",
                });
            }
        }

        Ok(columns)
    }

    async fn register_source(&self, _name: &str, _path: &Path) -> Result<(), VerityError> {
        Err(VerityError::InternalError(
            "register_source (local file → BQ) not yet implemented. Use GCS as intermediary."
                .into(),
        ))
    }

    async fn materialize(
        &self,
        table_name: &str,
        sql: &str,
        materialization_type: &str,
    ) -> Result<String, VerityError> {
        let full_name = format!("`{}.{}.{}`", self.project_id, self.dataset_id, table_name);

        let ddl = match materialization_type {
            "view" => format!("CREATE OR REPLACE VIEW {full_name} AS {sql}"),
            "table" => format!("CREATE OR REPLACE TABLE {full_name} AS {sql}"),
            other => {
                return Err(VerityError::InternalError(format!(
                    "Unknown materialization type: {other}"
                )));
            }
        };

        self.execute(&ddl).await?;
        Ok(full_name)
    }

    async fn query_scalar(&self, query: &str) -> Result<u64, VerityError> {
        let request = QueryRequest::new(query);
        let response = self
            .client
            .job()
            .query(&self.project_id, request)
            .await
            .map_err(|e| VerityError::InternalError(e.to_string()))?;

        let rows = response
            .rows
            .as_ref()
            .ok_or_else(|| VerityError::InternalError("Query returned no rows".into()))?;

        let first_row = rows
            .first()
            .ok_or_else(|| VerityError::InternalError("Query returned no rows".into()))?;

        let val_str = Self::cell_value(first_row, 0);
        let val: u64 = val_str
            .parse()
            .map_err(|e| VerityError::InternalError(format!("Failed to parse scalar: {e}")))?;

        Ok(val)
    }
}
