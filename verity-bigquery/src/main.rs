// verity-bigquery/src/main.rs

use std::sync::Arc;
use verity_bigquery::config::BqConfig;
use verity_bigquery::connector::BigQueryConnector;
use verity_core::ports::connector::ConnectorRunner;

fn setup_logging() {
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
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // 1. Initialisation des logs
    setup_logging();

    // 2. Chargement de la config et de l'Auth GCP
    let config = BqConfig::from_env().await?;

    // 3. Initialisation du connecteur
    let connector = BigQueryConnector::new(config);

    // 4. Lancement
    ConnectorRunner::run(Arc::new(connector)).await?;

    Ok(())
}
