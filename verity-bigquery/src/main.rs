// verity-bigquery/src/main.rs

use async_trait::async_trait;
use gcp_bigquery_client::Client;
use gcp_bigquery_client::model::query_request::QueryRequest;
use std::sync::Arc;
use verity_core::error::VerityError;
use verity_core::ports::connector::ConnectorRunner;
use verity_core::ports::connector::{ColumnSchema, Connector};

pub struct BigQueryConnector {
    client: Client,
    project_id: String,
    dataset_id: String,
}

impl BigQueryConnector {
    pub async fn from_env() -> Result<Self, VerityError> {
        let project_id = std::env::var("GOOGLE_CLOUD_PROJECT").map_err(|_| {
            VerityError::InternalError("GOOGLE_CLOUD_PROJECT env var missing".into())
        })?;
        let dataset_id = std::env::var("VERITY_DATASET")
            .map_err(|_| VerityError::InternalError("VERITY_DATASET env var missing".into()))?;

        let client = if let Ok(sa_path) = std::env::var("GOOGLE_APPLICATION_CREDENTIALS") {
            Client::from_service_account_key_file(&sa_path)
                .await
                .map_err(|e| VerityError::InternalError(format!("GCP Auth Error: {e}")))?
        } else {
            return Err(VerityError::InternalError(
                "No auth configured. Set GOOGLE_APPLICATION_CREDENTIALS.".into(),
            ));
        };

        Ok(Self {
            client,
            project_id,
            dataset_id,
        })
    }

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
            "SELECT column_name, data_type, is_nullable FROM `{}.{}.INFORMATION_SCHEMA.COLUMNS` WHERE table_name = '{}'",
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
                columns.push(ColumnSchema {
                    name: Self::cell_value(row, 0),
                    data_type: Self::cell_value(row, 1),
                    is_nullable: Self::cell_value(row, 2) == "YES",
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
        let full_name = format!("`{}.{}.{}`", self.project_id, self.dataset_id, table_name);
        let ddl = match materialization_type {
            "view" => format!("CREATE OR REPLACE VIEW {full_name} AS {sql}"),
            "table" => format!("CREATE OR REPLACE TABLE {full_name} AS {sql}"),
            _ => {
                return Err(VerityError::InternalError(
                    "Invalid materialization type".into(),
                ));
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
            .ok_or_else(|| VerityError::InternalError("No rows".into()))?;
        let first_row = rows
            .first()
            .ok_or_else(|| VerityError::InternalError("No rows".into()))?;
        let val: u64 = Self::cell_value(first_row, 0)
            .parse()
            .map_err(|e| VerityError::InternalError(format!("Parse error: {e}")))?;
        Ok(val)
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // ENFORCE DISCIPLINE: All logs must go to stderr to protect the JSON-RPC stdout channel.
    tracing_subscriber::fmt()
        .with_writer(std::io::stderr)
        .init();

    let connector = BigQueryConnector::from_env().await?;
    ConnectorRunner::run(Arc::new(connector)).await?;
    Ok(())
}
