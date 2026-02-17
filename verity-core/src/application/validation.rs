// verity-core/src/application/validation.rs

use std::collections::HashSet;

// Imports Hexagonaux
use crate::domain::project::manifest::ManifestNode;
use crate::error::VerityError;
use crate::ports::connector::Connector;

pub async fn run_tests(
    node: &ManifestNode,
    connector: &dyn Connector,
) -> Result<HashSet<String>, VerityError> {
    // 1. Validation du Contrat de Structure
    let undocumented_columns = validate_structure_strict(node, connector).await?;

    if node.columns.is_empty() {
        return Ok(undocumented_columns);
    }

    println!("    🧪 Running data tests for {}", node.name);

    for col in &node.columns {
        for test in &col.tests {
            let result = match test.as_str() {
                "not_null" => check_not_null(&node.name, &col.name, connector).await,
                "unique" => check_unique(&node.name, &col.name, connector).await,
                _ => {
                    println!("      ⚠️ Unknown test type '{}' (skipping)", test);
                    Ok(())
                }
            };

            match result {
                Ok(_) => println!("      ✅ PASS: {} on {}.{}", test, node.name, col.name),
                Err(e) => {
                    return Err(e);
                }
            }
        }
    }

    Ok(undocumented_columns)
}

async fn validate_structure_strict(
    node: &ManifestNode,
    connector: &dyn Connector,
) -> Result<HashSet<String>, VerityError> {
    let actual_columns_raw = connector
        .fetch_columns(&node.name)
        .await
        .map_err(|e| VerityError::InternalError(format!("Could not fetch schema: {}", e)))?;

    let actual_columns: HashSet<String> = actual_columns_raw
        .into_iter()
        .map(|c| c.name.to_lowercase())
        .collect();

    let expected_columns: HashSet<String> =
        node.columns.iter().map(|c| c.name.to_lowercase()).collect();

    let undocumented_columns: HashSet<String> = actual_columns
        .difference(&expected_columns)
        .cloned()
        .collect();

    if !undocumented_columns.is_empty() {
        // Au lieu de crash, on prévient le pipeline qu'il y a du travail de sync à faire
        println!(
            "    ⚠️  [Governance] Undocumented columns detected in {}: {:?}",
            node.name, undocumented_columns
        );
    }

    Ok(undocumented_columns)
}

// --- SQL ASSERTIONS ---

async fn check_not_null(
    table: &str,
    column: &str,
    connector: &dyn Connector,
) -> Result<(), VerityError> {
    let sql = format!(
        "SELECT CASE WHEN COUNT(*) > 0 THEN error('ASSERTION FAILED: Found NULL values in {}.{}') ELSE 0 END FROM {} WHERE {} IS NULL",
        table, column, table, column
    );
    connector.execute(&sql).await
}

async fn check_unique(
    table: &str,
    column: &str,
    connector: &dyn Connector,
) -> Result<(), VerityError> {
    let sql = format!(
        "SELECT CASE WHEN count(*) > 0 THEN error('ASSERTION FAILED: Found DUPLICATES in {}.{}') ELSE 0 END 
         FROM (SELECT {} FROM {} GROUP BY {} HAVING count(*) > 1)",
        table, column, column, table, column
    );
    connector.execute(&sql).await
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;
    use crate::domain::project::{ColumnInfo, NodeConfig, ResourceType};
    use crate::ports::connector::ColumnSchema;
    use async_trait::async_trait;
    use std::path::PathBuf;
    use std::sync::{Arc, Mutex};

    // --- MOCK CONNECTOR ---
    #[derive(Clone)]
    struct MockConnector {
        pub executed_queries: Arc<Mutex<Vec<String>>>,
        pub columns_return: Vec<ColumnSchema>,
    }

    #[allow(dead_code)]
    impl MockConnector {
        fn new() -> Self {
            Self {
                executed_queries: Arc::new(Mutex::new(Vec::new())),
                columns_return: vec![],
            }
        }
        fn with_columns(cols: Vec<ColumnSchema>) -> Self {
            Self {
                executed_queries: Arc::new(Mutex::new(Vec::new())),
                columns_return: cols,
            }
        }
    }

    #[async_trait]
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
            Ok(self.columns_return.clone())
        }
        async fn register_source(&self, _name: &str, _path: &str) -> Result<(), VerityError> {
            Ok(())
        }
    }

    fn create_manifest_node(name: &str, columns: Vec<ColumnInfo>) -> ManifestNode {
        ManifestNode {
            name: name.to_string(),
            resource_type: ResourceType::Model,
            path: PathBuf::from("test"),
            schema_path: None,
            raw_sql: "".into(),
            refs: vec![],
            config: NodeConfig::default(),
            columns,
            compliance: None, // Added compliance field
        }
    }

    #[tokio::test]
    async fn test_run_tests_no_drift() {
        let node = ManifestNode {
            name: "test_model".into(),
            resource_type: ResourceType::Model,
            path: PathBuf::from("models/test.sql"),
            schema_path: None,
            raw_sql: "SELECT 1".into(),
            refs: vec![],
            config: Default::default(),
            columns: vec![],
            compliance: None,
        };
    }

    #[tokio::test]
    async fn test_run_tests_not_null_unique() {
        let node = create_manifest_node(
            "users",
            vec![ColumnInfo {
                name: "id".into(),
                tests: vec!["not_null".into(), "unique".into()],
                policy: None,
            }],
        );

        let connector_passing_struct = MockConnector::with_columns(vec![ColumnSchema {
            name: "id".into(),
            data_type: "INT".into(),
            is_nullable: false,
        }]);

        let result = run_tests(&node, &connector_passing_struct).await;
        assert!(result.is_ok());
        let undocumented = result.unwrap();
        assert!(undocumented.is_empty());

        let queries = connector_passing_struct.executed_queries.lock().unwrap();
        assert_eq!(queries.len(), 2);
    }

    #[tokio::test]
    async fn test_validate_structure_undocumented() {
        let node = create_manifest_node(
            "users",
            vec![ColumnInfo {
                name: "id".into(),
                tests: vec![],
                policy: None,
            }],
        );

        let connector_extra = MockConnector::with_columns(vec![
            ColumnSchema {
                name: "id".into(),
                data_type: "INT".into(),
                is_nullable: false,
            },
            ColumnSchema {
                name: "email".into(),
                data_type: "TEXT".into(),
                is_nullable: false,
            },
        ]);

        let result = run_tests(&node, &connector_extra).await;
        assert!(result.is_ok());
        let undocumented = result.unwrap();
        assert_eq!(undocumented.len(), 1);
        assert!(undocumented.contains("email"));
    }
}
