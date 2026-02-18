// verity-core/src/domain/governance/rewriter.rs

use crate::domain::error::DomainError;
use crate::domain::project::ManifestNode;

pub struct PolicyRewriter;

impl PolicyRewriter {
    /// Take the raw SQL and apply the masks defined in the Node (Manifest).
    /// This function is PURE : it does not depend on any database.
    pub fn apply_masking(sql: &str, node: &ManifestNode) -> Result<String, DomainError> {
        // If no column is defined (no contract), we don't touch anything
        if node.columns.is_empty() {
            return Ok(sql.to_string());
        }

        // Check if there is at least one active policy
        let has_policies = node.columns.iter().any(|c| c.policy.is_some());

        if !has_policies {
            return Ok(sql.to_string());
        }

        // Note: In a strict hexagonal architecture, we would avoid println! here
        // or pass through a Logger trait. For now, we leave it for debugging.
        // println!("    🛡️  Applying Governance Masking Layer...");

        // 1. Build the list of columns for the final SELECT
        let mut select_clause = Vec::new();

        for col in &node.columns {
            let column_expr = match col.policy.as_deref() {
                // 🔒 HASH (SHA256)
                // Note : We assume a standard SQL syntax (DuckDB/Postgres).
                // If we wanted to support multiple dialects, we would need a "SqlDialectAdapter".
                Some("hash") => format!("SHA256(CAST({} AS VARCHAR)) AS {}", col.name, col.name),

                // 🔒 REDACT (Total replacement)
                Some("redact") => format!("'REDACTED' AS {}", col.name),

                // 🔒 EMAIL MASK (Partiel) -> j***@domain.com
                Some("mask_email") => format!(
                    "regexp_replace({}, '(^.).*(@.*$)', '\\1****\\2') AS {}",
                    col.name, col.name
                ),

                // 🔒 PII MASKING (Generic) -> Hash by default
                Some("pii_masking") => {
                    format!("SHA256(CAST({} AS VARCHAR)) AS {}", col.name, col.name)
                }

                // No policy or unknown -> Keep the column as is
                _ => col.name.clone(),
            };
            select_clause.push(column_expr);
        }

        let final_columns = select_clause.join(",\n    ");

        // 2. Wrap the original SQL in a CTE
        // This is where the magic happens: we isolate the user logic to apply security on top.
        let wrapped_sql = format!(
            "WITH verity_governance_cte AS (\n{}\n)\nSELECT \n    {}\nFROM verity_governance_cte",
            sql, final_columns
        );

        Ok(wrapped_sql)
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;
    use crate::domain::project::{ColumnInfo, ManifestNode, NodeConfig, ResourceType};
    use anyhow::Result;
    use std::path::PathBuf;

    fn create_mock_node(columns: Vec<ColumnInfo>) -> ManifestNode {
        ManifestNode {
            name: "test_model".to_string(),
            resource_type: ResourceType::Model,
            path: PathBuf::from("test.sql"),
            schema_path: None,
            raw_sql: "SELECT * FROM raw_data".to_string(),
            refs: vec![],
            config: NodeConfig::default(),
            columns,
            security_level: Default::default(),
            compliance: None,
        }
    }

    #[test]
    fn test_apply_masking_no_policy() -> Result<()> {
        let node = create_mock_node(vec![
            ColumnInfo {
                name: "id".to_string(),
                tests: vec![],
                policy: None,
            },
            ColumnInfo {
                name: "email".to_string(),
                tests: vec![],
                policy: None,
            },
        ]);

        let sql = "SELECT * FROM raw_table";
        let result = PolicyRewriter::apply_masking(sql, &node)?;

        // No policy => should return original SQL
        assert_eq!(result, sql);
        Ok(())
    }

    #[test]
    fn test_apply_masking_hash() -> Result<()> {
        let node = create_mock_node(vec![ColumnInfo {
            name: "user_id".to_string(),
            tests: vec![],
            policy: Some("hash".to_string()),
        }]);

        let sql = "SELECT * FROM raw_table";
        let result = PolicyRewriter::apply_masking(sql, &node)?;

        assert!(result.contains("WITH verity_governance_cte AS"));
        assert!(result.contains("SHA256(CAST(user_id AS VARCHAR)) AS user_id"));
        Ok(())
    }

    #[test]
    fn test_apply_masking_redact() -> Result<()> {
        let node = create_mock_node(vec![ColumnInfo {
            name: "ssn".to_string(),
            tests: vec![],
            policy: Some("redact".to_string()),
        }]);

        let sql = "SELECT * FROM raw_table";
        let result = PolicyRewriter::apply_masking(sql, &node)?;

        assert!(result.contains("'REDACTED' AS ssn"));
        Ok(())
    }

    #[test]
    fn test_apply_masking_email() -> Result<()> {
        let node = create_mock_node(vec![ColumnInfo {
            name: "email".to_string(),
            tests: vec![],
            policy: Some("mask_email".to_string()),
        }]);

        let sql = "SELECT * FROM raw_table";
        let result = PolicyRewriter::apply_masking(sql, &node)?;

        assert!(result.contains("regexp_replace(email, '(^.).*(@.*$)', '\\1****\\2') AS email"));
        Ok(())
    }

    #[test]
    fn test_apply_masking_mixed() -> Result<()> {
        let node = create_mock_node(vec![
            ColumnInfo {
                name: "id".to_string(),
                tests: vec![],
                policy: None,
            },
            ColumnInfo {
                name: "email".to_string(),
                tests: vec![],
                policy: Some("mask_email".to_string()),
            },
            ColumnInfo {
                name: "salary".to_string(),
                tests: vec![],
                policy: Some("redact".to_string()),
            },
        ]);

        let sql = "SELECT * FROM raw_table";
        let result = PolicyRewriter::apply_masking(sql, &node)?;

        assert!(result.contains("id"));
        assert!(result.contains("regexp_replace(email, '(^.).*(@.*$)', '\\1****\\2') AS email"));
        assert!(result.contains("'REDACTED' AS salary"));
        Ok(())
    }
}
