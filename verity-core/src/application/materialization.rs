// verity-core/src/application/materialization.rs

use crate::domain::project::manifest::{MaterializationType, NodeConfig};
use crate::error::VerityError;
use crate::ports::connector::Connector;

pub struct Materializer;

impl Materializer {
    pub async fn materialize(
        connector: &dyn Connector,
        model_name: &str,
        executed_sql: &str,
        config: &NodeConfig,
    ) -> Result<String, VerityError> {
        let strategy = config
            .materialized
            .as_ref()
            .unwrap_or(&MaterializationType::View);

        let is_protected = config.protected;

        if matches!(strategy, MaterializationType::Ephemeral) {
            return Ok("ephemeral".to_string());
        }

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

        let final_sql = if is_protected {
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
    use anyhow::Result;
    use async_trait::async_trait;
    use std::sync::{Arc, Mutex};

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
                .map_err(|_| VerityError::InternalError("Mutex poisoned".into()))?
                .push(query.to_string());
            Ok(())
        }
        async fn fetch_columns(&self, _table_name: &str) -> Result<Vec<ColumnSchema>, VerityError> {
            Ok(vec![])
        }
        async fn register_source(
            &self,
            _name: &str,
            _path: &std::path::Path,
        ) -> Result<(), VerityError> {
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
    async fn test_materialize_view_default() -> Result<()> {
        let connector = MockConnector::new();
        let config = NodeConfig::default(); // default is None -> View

        let result = Materializer::materialize(&connector, "my_model", "SELECT 1", &config).await?;

        assert_eq!(result, "view");
        Ok(())
    }

    #[tokio::test]
    async fn test_materialize_table_standard() -> Result<()> {
        let connector = MockConnector::new();
        let config = NodeConfig {
            materialized: Some(MaterializationType::Table),
            protected: false,
            ..Default::default()
        };

        let result =
            Materializer::materialize(&connector, "my_table", "SELECT * FROM src", &config).await?;

        assert_eq!(result, "table");
        Ok(())
    }

    #[tokio::test]
    async fn test_materialize_table_protected() -> Result<()> {
        let connector = MockConnector::new();
        let config = NodeConfig {
            materialized: Some(MaterializationType::Table),
            protected: true,
            ..Default::default()
        };

        let _ = Materializer::materialize(&connector, "prot_table", "SELECT 1", &config).await;

        let queries = connector
            .executed_queries
            .lock()
            .map_err(|_| anyhow::anyhow!("Mutex poisoned"))?;
        assert_eq!(
            queries[0],
            "CREATE TABLE IF NOT EXISTS prot_table AS SELECT 1"
        );
        Ok(())
    }

    #[tokio::test]
    async fn test_materialize_ephemeral() -> Result<()> {
        let connector = MockConnector::new();
        let config = NodeConfig {
            materialized: Some(MaterializationType::Ephemeral),
            ..Default::default()
        };

        let result = Materializer::materialize(&connector, "eph", "SELECT 1", &config).await?;

        assert_eq!(result, "ephemeral");
        let queries = connector
            .executed_queries
            .lock()
            .map_err(|_| anyhow::anyhow!("Mutex poisoned"))?;
        assert!(
            queries.is_empty(),
            "Ephemeral models should not execute DDL"
        );
        Ok(())
    }
}
