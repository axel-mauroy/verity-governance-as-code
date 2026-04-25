// verity-bigquery/src/client.rs

use gcp_bigquery_client::Client;
use gcp_bigquery_client::model::query_request::QueryRequest;
use gcp_bigquery_client::model::table_row::TableRow;
use verity_core::error::VerityError;

pub struct BqEngine {
    client: Client,
    project_id: String,
}

impl BqEngine {
    pub fn new(client: Client, project_id: String) -> Self {
        Self { client, project_id }
    }

    pub async fn wait_for_job(
        &self,
        job_id: &str,
        location: Option<&str>,
    ) -> Result<(), VerityError> {
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

    pub async fn run_query(&self, query: &str) -> Result<Option<Vec<TableRow>>, VerityError> {
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
