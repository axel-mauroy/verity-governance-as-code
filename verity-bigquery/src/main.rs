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
        tracing::info!("Initializing BigQueryConnector from environment variables...");
        let project_id = std::env::var("GOOGLE_CLOUD_PROJECT").map_err(|_| {
            let err = "GOOGLE_CLOUD_PROJECT env var missing";
            tracing::error!("💥 {}", err);
            VerityError::InternalError(err.into())
        })?;
        let dataset_id = std::env::var("VERITY_DATASET").map_err(|_| {
            let err = "VERITY_DATASET env var missing";
            tracing::error!("💥 {}", err);
            VerityError::InternalError(err.into())
        })?;

        tracing::info!("Initializing Application Default Credentials (ADC)...");
        let client = Client::from_application_default_credentials()
            .await
            .map_err(|e| {
                tracing::error!("💥 GCP Auth Error (ADC): {:?}", e);
                VerityError::InternalError(format!("GCP Auth Error (ADC): {e}"))
            })?;

        tracing::info!(
            "✅ BigQuery client initialized successfully for project '{}', dataset '{}'",
            project_id,
            dataset_id
        );
        Ok(Self {
            client,
            project_id,
            dataset_id,
        })
    }

    fn cell_value(
        row: &gcp_bigquery_client::model::table_row::TableRow,
        index: usize,
    ) -> Option<String> {
        let val = row.columns.as_ref()?.get(index)?.value.as_ref()?;
        match val {
            serde_json::Value::Null => None,
            serde_json::Value::String(s) => Some(s.clone()),
            other => Some(other.to_string()),
        }
    }

    async fn wait_for_job(&self, job_id: &str, location: Option<&str>) -> Result<(), VerityError> {
        let mut is_complete = false;
        while !is_complete {
            tracing::info!("⏳ Job {} is still running, waiting 2 seconds...", job_id);
            tokio::time::sleep(std::time::Duration::from_secs(2)).await;

            let poll_res = self
                .client
                .job()
                .get_job(&self.project_id, job_id, location)
                .await
                .map_err(|e| {
                    VerityError::InternalError(format!("Failed to poll job status: {}", e))
                })?;

            if let Some(ref status) = poll_res.status {
                if let Some(first_err) = status.errors.as_ref().and_then(|e| e.first()) {
                    let msg = first_err
                        .message
                        .as_deref()
                        .unwrap_or("Unknown BigQuery error during polling");
                    tracing::error!("💥 BigQuery Error during polling: {}", msg);
                    return Err(VerityError::InternalError(format!(
                        "BigQuery job failed: {}",
                        msg
                    )));
                }
                is_complete = status.state.as_deref() == Some("DONE");
            } else {
                break;
            }
        }
        Ok(())
    }

    async fn run_query(
        &self,
        query: &str,
    ) -> Result<Option<Vec<gcp_bigquery_client::model::table_row::TableRow>>, VerityError> {
        tracing::info!("🚀 Executing BigQuery SQL:\n{}", query);
        let request = QueryRequest::new(query);
        let res = self
            .client
            .job()
            .query(&self.project_id, request)
            .await
            .map_err(|e| {
                tracing::error!(
                    "💥 BIGQUERY CONNECTOR CRASHED during query submission:\n{:?}",
                    e
                );
                VerityError::InternalError(format!("BigQuery Execution Error: {}", e))
            })?;

        let job_id = res
            .job_reference
            .as_ref()
            .and_then(|j| j.job_id.clone())
            .unwrap_or_else(|| "UNKNOWN".to_string());
        let location = res.job_reference.as_ref().and_then(|j| j.location.clone());

        // 1. Initial Check for SQL/Execution Errors
        if let Some(first_err) = res.errors.as_ref().and_then(|e| e.first()) {
            let msg = first_err
                .message
                .as_deref()
                .unwrap_or("Unknown BigQuery error");
            tracing::error!("💥 BigQuery Error: {}", msg);
            return Err(VerityError::InternalError(format!(
                "BigQuery job failed: {}",
                msg
            )));
        }

        // 2. Poll if necessary
        if !res.job_complete.unwrap_or(true) {
            self.wait_for_job(&job_id, location.as_deref()).await?;

            // Fetch rows if job had to be polled
            let mut options = gcp_bigquery_client::model::get_query_results_parameters::GetQueryResultsParameters::default();
            if let Some(loc) = &location {
                options.location = Some(loc.clone());
            }

            let query_results = self
                .client
                .job()
                .get_query_results(&self.project_id, &job_id, options)
                .await
                .map_err(|e| {
                    VerityError::InternalError(format!(
                        "Failed to fetch query results after polling: {}",
                        e
                    ))
                })?;

            tracing::info!("✅ Job BQ terminé: {}", job_id);
            Ok(query_results.rows)
        } else {
            tracing::info!("✅ Job BQ terminé: {}", job_id);
            Ok(res.rows)
        }
    }
}

#[async_trait]
impl Connector for BigQueryConnector {
    fn engine_name(&self) -> &str {
        "BigQuery"
    }

    async fn execute(&self, query: &str) -> Result<(), VerityError> {
        self.run_query(query).await?;
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

        let rows_opt = self.run_query(&query).await?;

        let mut columns = Vec::new();
        if let Some(rows) = rows_opt {
            for row in rows {
                let name = Self::cell_value(&row, 0).ok_or_else(|| {
                    VerityError::InternalError("Missing column_name in schema".into())
                })?;
                let data_type = Self::cell_value(&row, 1).ok_or_else(|| {
                    VerityError::InternalError("Missing data_type in schema".into())
                })?;
                let is_nullable_str = Self::cell_value(&row, 2).unwrap_or_else(|| "NO".to_string());

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
        match self.execute(&ddl).await {
            Ok(_) => {
                tracing::info!("✅ Successfully materialized {}", full_name);
                Ok(full_name)
            }
            Err(e) => {
                tracing::error!(
                    "💥 BIGQUERY CONNECTOR CRASHED during materialize of {}",
                    table_name
                );
                Err(e)
            }
        }
    }

    async fn query_scalar(&self, query: &str) -> Result<u64, VerityError> {
        let rows_opt = self.run_query(query).await?;
        let rows = rows_opt.ok_or_else(|| VerityError::InternalError("No rows returned".into()))?;

        let first_row = rows
            .first()
            .ok_or_else(|| VerityError::InternalError("No rows returned".into()))?;

        let cell_str = Self::cell_value(first_row, 0)
            .ok_or_else(|| VerityError::InternalError("Scalar result is NULL".into()))?;

        // Parse strictement en u64. Si la requête SQL ne renvoie pas un entier pur,
        // on échoue explicitement. C'est le rôle de SQL de forcer le type via CAST(x AS INT64).
        let val: u64 = cell_str.parse().map_err(|e| {
            VerityError::InternalError(format!(
                "Failed to parse u64 from '{}'. Ensure your query uses CAST(x AS INT64): {}",
                cell_str, e
            ))
        })?;

        Ok(val)
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // ENFORCE DISCIPLINE: All logs must go to stderr to protect the JSON-RPC stdout channel.
    if std::env::var("VERITY_LOG_FORMAT").unwrap_or_default() == "json" {
        tracing_subscriber::fmt()
            .json()
            .with_writer(std::io::stderr)
            .init();
    } else {
        tracing_subscriber::fmt()
            .with_writer(std::io::stderr)
            .init();
    }

    let connector = BigQueryConnector::from_env().await?;
    ConnectorRunner::run(Arc::new(connector)).await?;
    Ok(())
}
