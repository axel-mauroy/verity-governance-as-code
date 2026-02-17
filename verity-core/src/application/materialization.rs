// verity-core/src/application/materialization.rs

use crate::domain::project::manifest::{MaterializationType, NodeConfig};
use crate::error::VerityError;
use crate::ports::connector::Connector;

pub struct Materializer;

impl Materializer {
    /// Génère et exécute le DDL (CREATE TABLE/VIEW) pour un modèle donné.
    pub async fn materialize(
        connector: &dyn Connector,
        model_name: &str,
        executed_sql: &str,
        config: &NodeConfig,
    ) -> Result<String, VerityError> {
        // 1. Résolution du Type (Domaine Enum)
        // Par défaut, si non spécifié, c'est une Vue.
        let strategy = config
            .materialized
            .as_ref()
            .unwrap_or(&MaterializationType::View);

        let is_protected = config.protected;

        // 2. Gestion Spéciale : Ephemeral
        // Un modèle éphémère ne crée aucun objet en base de données.
        if matches!(strategy, MaterializationType::Ephemeral) {
            // On ne fait rien, on retourne juste l'info.
            return Ok("ephemeral".to_string());
        }

        // 3. Construction de la requête DDL
        let ddl_query = match (strategy, is_protected) {
            // --- TABLE ---
            (MaterializationType::Table, true) => {
                // Protected : On ne crée que si ça n'existe pas.
                // On ne touche pas aux données existantes.
                format!(
                    "CREATE TABLE IF NOT EXISTS {} AS {}",
                    model_name, executed_sql
                )
            }
            (MaterializationType::Table, false) => {
                // Standard : Full Refresh (On remplace tout).
                // DuckDB supporte CREATE OR REPLACE TABLE.
                format!("CREATE OR REPLACE TABLE {} AS {}", model_name, executed_sql)
            }

            // --- VIEW ---
            (MaterializationType::View, true) => {
                format!(
                    "CREATE VIEW IF NOT EXISTS {} AS {}",
                    model_name, executed_sql
                )
            }
            (MaterializationType::View, false) => {
                format!("CREATE OR REPLACE VIEW {} AS {}", model_name, executed_sql)
            }

            // --- INCREMENTAL (Future) ---
            (MaterializationType::Incremental, _) => {
                // Pour le MVP, on fallback sur une table replace
                eprintln!(
                    "⚠️ Incremental not yet implemented for '{}', falling back to Table Replace.",
                    model_name
                );
                format!("CREATE OR REPLACE TABLE {} AS {}", model_name, executed_sql)
            }

            // Cas impossible théoriquement grâce au match Ephemeral plus haut
            (MaterializationType::Ephemeral, _) => String::new(),
        };

        // 4. Exécution via le Port
        // On n'a plus besoin des DROP manuels car CREATE OR REPLACE gère ça proprement
        // (sauf si on change de type View <-> Table, mais DuckDB est permissif).
        if !ddl_query.is_empty() {
            connector.execute(&ddl_query).await.map_err(|e| {
                VerityError::InternalError(format!(
                    "Model '{}' failed.\n    🛑 DB Error: {}\n    📄 Query: {}",
                    model_name, e, ddl_query
                ))
            })?;
        }

        // Retourne le nom de la stratégie pour les logs (ex: "table", "view")
        Ok(format!("{:?}", strategy).to_lowercase())
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;
    use crate::ports::connector::ColumnSchema;
    use async_trait::async_trait;
    use std::sync::{Arc, Mutex};

    // --- MOCK CONNECTOR ---
    #[derive(Clone)]
    struct MockConnector {
        pub executed_queries: Arc<Mutex<Vec<String>>>,
    }

    impl MockConnector {
        fn new() -> Self {
            Self {
                executed_queries: Arc::new(Mutex::new(Vec::new())),
            }
        }
    }

    #[async_trait]
    impl Connector for MockConnector {
        async fn execute(&self, query: &str) -> Result<(), VerityError> {
            self.executed_queries
                .lock()
                .unwrap()
                .push(query.to_string());
            Ok(())
        }
        async fn fetch_columns(&self, _table_name: &str) -> Result<Vec<ColumnSchema>, VerityError> {
            Ok(vec![])
        }
        async fn register_source(&self, _name: &str, _path: &str) -> Result<(), VerityError> {
            Ok(())
        }
    }

    #[tokio::test]
    async fn test_materialize_view_default() {
        let connector = MockConnector::new();
        let config = NodeConfig::default(); // default is None -> View

        let result = Materializer::materialize(&connector, "my_model", "SELECT 1", &config)
            .await
            .unwrap();

        assert_eq!(result, "view");
        let queries = connector.executed_queries.lock().unwrap();
        assert_eq!(queries.len(), 1);
        assert_eq!(queries[0], "CREATE OR REPLACE VIEW my_model AS SELECT 1");
    }

    #[tokio::test]
    async fn test_materialize_table_standard() {
        let connector = MockConnector::new();
        let config = NodeConfig {
            materialized: Some(MaterializationType::Table),
            protected: false,
            ..Default::default()
        };

        let result =
            Materializer::materialize(&connector, "my_table", "SELECT * FROM src", &config)
                .await
                .unwrap();

        assert_eq!(result, "table");
        let queries = connector.executed_queries.lock().unwrap();
        assert_eq!(
            queries[0],
            "CREATE OR REPLACE TABLE my_table AS SELECT * FROM src"
        );
    }

    #[tokio::test]
    async fn test_materialize_table_protected() {
        let connector = MockConnector::new();
        let config = NodeConfig {
            materialized: Some(MaterializationType::Table),
            protected: true,
            ..Default::default()
        };

        let _ = Materializer::materialize(&connector, "prot_table", "SELECT 1", &config).await;

        let queries = connector.executed_queries.lock().unwrap();
        assert_eq!(
            queries[0],
            "CREATE TABLE IF NOT EXISTS prot_table AS SELECT 1"
        );
    }

    #[tokio::test]
    async fn test_materialize_ephemeral() {
        let connector = MockConnector::new();
        let config = NodeConfig {
            materialized: Some(MaterializationType::Ephemeral),
            ..Default::default()
        };

        let result = Materializer::materialize(&connector, "eph", "SELECT 1", &config)
            .await
            .unwrap();

        assert_eq!(result, "ephemeral");
        let queries = connector.executed_queries.lock().unwrap();
        assert!(
            queries.is_empty(),
            "Ephemeral models should not execute DDL"
        );
    }
}
