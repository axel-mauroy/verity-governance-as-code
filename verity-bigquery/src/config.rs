// verity-bigquery/src/config.rs

use gcp_bigquery_client::Client;
use verity_core::error::VerityError;

pub struct BqConfig {
    pub client: Client,
    pub project_id: String,
    pub dataset_id: String,
}

impl BqConfig {
    pub async fn from_env() -> Result<Self, VerityError> {
        tracing::info!("Initializing BigQuery configuration from environment variables...");

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
}
