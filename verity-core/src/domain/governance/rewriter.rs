// verity-core/src/domain/governance/rewriter.rs

use crate::domain::error::DomainError;
use crate::domain::governance::{MaskingStrategy, PolicyType};
use crate::domain::project::ManifestNode; // Import propre

pub struct PolicyRewriter;

impl PolicyRewriter {
    /// Take the raw SQL and apply the masks defined in the Node (Manifest).
    /// This function is PURE : it does not depend on any database.
    pub fn apply_masking(sql: &str, node: &ManifestNode) -> Result<String, DomainError> {
        if node.columns.is_empty() {
            return Ok(sql.to_string());
        }

        let has_policies = node.columns.iter().any(|c| c.policy.is_some());
        if !has_policies {
            return Ok(sql.to_string());
        }

        let mut select_clause = Vec::new();

        for col in &node.columns {
            let column_expr = match &col.policy {
                // 🔒 MASKING
                Some(PolicyType::Masking(strategy)) => match strategy {
                    MaskingStrategy::Hash => {
                        format!(
                            "encode(sha256(CAST({} AS VARCHAR)), 'hex') AS {}",
                            col.name, col.name
                        )
                    }
                    MaskingStrategy::Redact => {
                        format!("'REDACTED' AS {}", col.name)
                    }
                    MaskingStrategy::MaskEmail => {
                        format!(
                            "regexp_replace({}, '(^.).*(@.*$)', '\\1****\\2') AS {}",
                            col.name, col.name
                        )
                    }
                    MaskingStrategy::Nullify => {
                        format!("NULL AS {}", col.name)
                    }
                    MaskingStrategy::Partial => {
                        format!(
                            "concat(left(CAST({} AS VARCHAR), 2), '***') AS {}",
                            col.name, col.name
                        )
                    }
                    MaskingStrategy::EntityPreserving => {
                        format!(
                            "concat('[PRESERVED_', length(CAST({} AS VARCHAR)), ']') AS {}",
                            col.name, col.name
                        )
                    }
                },
                // 🔒 ENCRYPTION
                Some(PolicyType::Encryption) => {
                    // Placeholder pour l'encryption, on fallback sur du Hash robuste pour l'instant
                    format!(
                        "encode(sha256(CAST({} AS VARCHAR)), 'hex') AS {}",
                        col.name, col.name
                    )
                }
                // 🔒 DROP
                Some(PolicyType::Drop) => continue, // La colonne n'est pas ajoutée au SELECT

                // ✅ PAS DE POLITIQUE -> On garde la colonne telle quelle
                None => col.name.clone(),
            };
            select_clause.push(column_expr);
        }

        // Si toutes les colonnes ont été "Drop", on évite de générer un "SELECT FROM" invalide
        if select_clause.is_empty() {
            return Ok(format!(
                "WITH verity_governance_cte AS (\n{}\n)\nSELECT 1 AS _verity_empty FROM verity_governance_cte LIMIT 0",
                sql
            ));
        }

        let final_columns = select_clause.join(",\n    ");

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
    use crate::domain::governance::{MaskingStrategy, PolicyType};
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
            policy: Some(PolicyType::Masking(MaskingStrategy::Hash)),
        }]);

        let sql = "SELECT * FROM raw_table";
        let result = PolicyRewriter::apply_masking(sql, &node)?;

        assert!(result.contains("WITH verity_governance_cte AS"));
        assert!(result.contains("encode(sha256(CAST(user_id AS VARCHAR)), 'hex') AS user_id"));
        Ok(())
    }

    #[test]
    fn test_apply_masking_email() -> Result<()> {
        let node = create_mock_node(vec![ColumnInfo {
            name: "email".to_string(),
            tests: vec![],
            policy: Some(PolicyType::Masking(MaskingStrategy::MaskEmail)),
        }]);

        let sql = "SELECT * FROM raw_table";
        let result = PolicyRewriter::apply_masking(sql, &node)?;

        assert!(result.contains("regexp_replace(email, '(^.).*(@.*$)', '\\1****\\2') AS email"));
        Ok(())
    }

    #[test]
    fn test_apply_masking_redact() -> Result<()> {
        let node = create_mock_node(vec![ColumnInfo {
            name: "ssn".to_string(),
            tests: vec![],
            policy: Some(PolicyType::Masking(MaskingStrategy::Redact)),
        }]);

        let sql = "SELECT * FROM raw_table";
        let result = PolicyRewriter::apply_masking(sql, &node)?;

        assert!(result.contains("'REDACTED' AS ssn"));
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
                policy: Some(PolicyType::Masking(MaskingStrategy::MaskEmail)),
            },
            ColumnInfo {
                name: "salary".to_string(),
                tests: vec![],
                policy: Some(PolicyType::Masking(MaskingStrategy::Redact)),
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
