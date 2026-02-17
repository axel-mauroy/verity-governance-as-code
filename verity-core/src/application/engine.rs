// verity-core/src/application/engine.rs

use std::time::Instant;
use tracing::{debug, error, instrument};

// Imports Hexagonaux corrects
use crate::error::VerityError;
use crate::ports::connector::Connector;

/// Exécute une requête SQL brute avec instrumentation (Logs + Timing).
/// Ce wrapper permet de surveiller la performance de toutes les requêtes ad-hoc.
#[instrument(skip(connector), fields(query.len = query.len()))]
pub async fn execute_query(connector: &dyn Connector, query: &str) -> Result<(), VerityError> {
    let start = Instant::now();
    debug!("⚡ Executing Query: {}", query);

    // Exécution déléguée au Port
    let result = connector.execute(query).await;

    let duration = start.elapsed();

    match result {
        Ok(_) => {
            debug!("✅ Query finished in {:.2?}", duration);
            Ok(())
        }
        Err(e) => {
            // On log l'erreur ici pour avoir le contexte de temps,
            // même si elle sera remontée plus haut.
            error!("❌ Query failed after {:.2?}: {}", duration, e);

            // On propage l'erreur d'origine si possible, ou on wrap
            Err(e)
        }
    }
}
