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
            return Ok("ephemeral".to_string());
        }

        // 3. Déterminer le type de matérialisation
        let mat_type = match (strategy, is_protected) {
            (MaterializationType::Table, _) => "table",
            (MaterializationType::View, _) => "view",
            (MaterializationType::Incremental, _) => {
                eprintln!(
                    "⚠️ Incremental not yet implemented for '{}', falling back to Table Replace.",
                    model_name
                );
                "table"
            }
            (MaterializationType::Ephemeral, _) => unreachable!(),
        };

        // 4. Construire le SQL en tenant compte du mode protected
        let final_sql = if is_protected {
            // Protected mode: use IF NOT EXISTS semantics
            match mat_type {
                "table" => format!(
                    "CREATE TABLE IF NOT EXISTS {} AS {}",
                    model_name, executed_sql
                ),
                "view" => format!(
                    "CREATE VIEW IF NOT EXISTS {} AS {}",
                    model_name, executed_sql
                ),
                _ => unreachable!(),
            }
        } else {
            // Standard: delegate fully to the connector
            return connector
                .materialize(model_name, executed_sql, mat_type)
                .await
                .map_err(|e| {
                    VerityError::InternalError(format!(
                        "Model '{}' failed.\n    🛑 DB Error: {}",
                        model_name, e
                    ))
                });
        };

        // 5. Execute protected DDL directly
        connector.execute(&final_sql).await.map_err(|e| {
            VerityError::InternalError(format!(
                "Model '{}' failed.\n    🛑 DB Error: {}\n    📄 Query: {}",
                model_name, e, final_sql
            ))
        })?;

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
        async fn materialize(
            &self,
            _table_name: &str,
            _sql: &str,
            materialization_type: &str,
        ) -> Result<String, VerityError> {
            Ok(materialization_type.to_string())
        }
        async fn query_scalar(&self, _query: &str) -> Result<u64, VerityError> {
            Ok(0)
        }
        fn engine_name(&self) -> &str {
            "mock"
        }
    }

    #[tokio::test]
    async fn test_materialize_view_default() {
        let connector = MockConnector::new();
        let config = NodeConfig::default(); // default is None -> View

        let result = Materializer::materialize(&connector, "my_model", "SELECT 1", &config)
            .await
            .unwrap();

        // Non-protected mode delegates to connector.materialize() which returns the type
        assert_eq!(result, "view");
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

        // Non-protected mode delegates to connector.materialize() which returns the type
        assert_eq!(result, "table");
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
