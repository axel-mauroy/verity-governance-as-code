// verity-core/src/application/lineage.rs
//
// Static Data Lineage Analyzer — Pre-flight compliance check.
// Walks the DAG and detects unsecured PII flows BEFORE execution.

use serde::{Deserialize, Serialize};
use std::collections::HashMap;

use crate::domain::project::manifest::Manifest;

// ── Report Structures ────────────────────────────────────────────────

#[derive(Debug, Serialize, Deserialize)]
pub struct LineageReport {
    pub project_name: String,
    pub nodes: Vec<LineageNode>,
    pub edges: Vec<LineageEdge>,
    pub violations: Vec<LineageViolation>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct LineageNode {
    pub name: String,
    /// Columns with a PII policy attached
    pub pii_columns: Vec<String>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct LineageEdge {
    pub from: String,
    pub to: String,
    /// PII columns that flow through this edge
    pub pii_columns: Vec<PiiFlow>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct PiiFlow {
    pub column: String,
    pub upstream_policy: String,
    pub downstream_policy: Option<String>,
    pub secured: bool,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct LineageViolation {
    pub column: String,
    pub upstream_node: String,
    pub upstream_policy: String,
    pub downstream_node: String,
    pub message: String,
}

impl LineageReport {
    pub fn has_violations(&self) -> bool {
        !self.violations.is_empty()
    }

    pub fn to_json(&self) -> Result<String, serde_json::Error> {
        serde_json::to_string_pretty(self)
    }

    pub fn to_mermaid(&self) -> String {
        let mut lines = Vec::new();
        lines.push("graph LR".to_string());

        // Node styles
        for node in &self.nodes {
            if node.pii_columns.is_empty() {
                lines.push(format!("    {}[{}]", node.name, node.name));
            } else {
                let pii_list = node.pii_columns.join(", ");
                lines.push(format!(
                    "    {}[\"🔒 {} (PII: {})\"]",
                    node.name, node.name, pii_list
                ));
            }
        }

        // Edges
        for edge in &self.edges {
            if edge.pii_columns.is_empty() {
                lines.push(format!("    {} --> {}", edge.from, edge.to));
            } else {
                for flow in &edge.pii_columns {
                    if flow.secured {
                        lines.push(format!(
                            "    {} -->|\"🛡️ {} ({})\"| {}",
                            edge.from,
                            flow.column,
                            flow.downstream_policy.as_deref().unwrap_or("?"),
                            edge.to
                        ));
                    } else {
                        lines.push(format!(
                            "    {} -.->|\"⚠️ {} (UNPROTECTED)\"| {}",
                            edge.from, flow.column, edge.to
                        ));
                    }
                }
            }
        }

        // Style violations in red
        let violation_nodes: Vec<&str> = self
            .violations
            .iter()
            .map(|v| v.downstream_node.as_str())
            .collect();
        for vn in &violation_nodes {
            lines.push(format!("    style {} fill:#ff6b6b,stroke:#c0392b", vn));
        }

        lines.join("\n")
    }
}

// ── Analyzer ─────────────────────────────────────────────────────────

pub struct LineageAnalyzer;

impl LineageAnalyzer {
    /// Analyze the manifest and produce a lineage report.
    pub fn analyze(manifest: &Manifest) -> LineageReport {
        let mut nodes = Vec::new();
        let mut edges = Vec::new();
        let mut violations = Vec::new();

        // Build a lookup: node_name -> columns with policies
        let pii_map: HashMap<&str, Vec<(&str, &str)>> = manifest
            .nodes
            .iter()
            .map(|(name, node)| {
                let pii_cols: Vec<(&str, &str)> = node
                    .columns
                    .iter()
                    .filter_map(|c| c.policy.as_ref().map(|p| (c.name.as_str(), p.as_str())))
                    .collect();
                (name.as_str(), pii_cols)
            })
            .collect();

        // Build nodes
        for (name, node) in &manifest.nodes {
            let pii_columns: Vec<String> = node
                .columns
                .iter()
                .filter(|c| c.policy.is_some())
                .map(|c| c.name.clone())
                .collect();

            nodes.push(LineageNode {
                name: name.clone(),
                pii_columns,
            });
        }

        // Sort nodes for deterministic output
        nodes.sort_by(|a, b| a.name.cmp(&b.name));

        // Build edges and detect violations
        for (name, node) in &manifest.nodes {
            for ref_name in &node.refs {
                if !manifest.nodes.contains_key(ref_name) {
                    continue;
                }

                let mut pii_flows = Vec::new();

                // Check if upstream has PII columns
                if let Some(upstream_pii) = pii_map.get(ref_name.as_str()) {
                    for (col_name, upstream_policy) in upstream_pii {
                        // Does the downstream node have this column?
                        if let Some(downstream_col) =
                            node.columns.iter().find(|c| c.name == *col_name)
                        {
                            let secured = downstream_col.policy.is_some();
                            let flow = PiiFlow {
                                column: col_name.to_string(),
                                upstream_policy: upstream_policy.to_string(),
                                downstream_policy: downstream_col.policy.clone(),
                                secured,
                            };

                            if !secured {
                                violations.push(LineageViolation {
                                    column: col_name.to_string(),
                                    upstream_node: ref_name.clone(),
                                    upstream_policy: upstream_policy.to_string(),
                                    downstream_node: name.clone(),
                                    message: format!(
                                        "PII column '{}' flows from '{}' (policy: {}) to '{}' WITHOUT a policy.",
                                        col_name, ref_name, upstream_policy, name
                                    ),
                                });
                            }

                            pii_flows.push(flow);
                        }
                    }
                }

                edges.push(LineageEdge {
                    from: ref_name.clone(),
                    to: name.clone(),
                    pii_columns: pii_flows,
                });
            }
        }

        // Sort for deterministic output
        edges.sort_by(|a, b| (&a.from, &a.to).cmp(&(&b.from, &b.to)));
        violations
            .sort_by(|a, b| (&a.downstream_node, &a.column).cmp(&(&b.downstream_node, &b.column)));

        LineageReport {
            project_name: manifest.project_name.clone(),
            nodes,
            edges,
            violations,
        }
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;
    use crate::domain::project::manifest::ManifestNode;
    use crate::domain::project::{ColumnInfo, NodeConfig, ResourceType};
    use std::path::PathBuf;

    fn mock_node(name: &str, refs: Vec<&str>, columns: Vec<ColumnInfo>) -> ManifestNode {
        ManifestNode {
            name: name.to_string(),
            resource_type: ResourceType::Model,
            path: PathBuf::from("test.sql"),
            schema_path: None,
            raw_sql: String::new(),
            refs: refs.into_iter().map(String::from).collect(),
            config: NodeConfig::default(),
            columns,
            compliance: None,
        }
    }

    #[test]
    fn test_no_violations_when_no_pii() {
        let mut nodes = HashMap::new();
        nodes.insert("a".into(), mock_node("a", vec![], vec![]));
        nodes.insert("b".into(), mock_node("b", vec!["a"], vec![]));

        let manifest = Manifest {
            project_name: "test".into(),
            nodes,
            sources: HashMap::new(),
        };

        let report = LineageAnalyzer::analyze(&manifest);
        assert!(!report.has_violations());
        assert_eq!(report.edges.len(), 1);
    }

    #[test]
    fn test_violation_detected_when_pii_unprotected() {
        let mut nodes = HashMap::new();
        nodes.insert(
            "stg_users".into(),
            mock_node(
                "stg_users",
                vec![],
                vec![ColumnInfo {
                    name: "email".into(),
                    tests: vec![],
                    policy: Some("pii_masking".into()),
                }],
            ),
        );
        nodes.insert(
            "int_users".into(),
            mock_node(
                "int_users",
                vec!["stg_users"],
                vec![ColumnInfo {
                    name: "email".into(),
                    tests: vec![],
                    policy: None, // ← NO POLICY = VIOLATION
                }],
            ),
        );

        let manifest = Manifest {
            project_name: "test".into(),
            nodes,
            sources: HashMap::new(),
        };

        let report = LineageAnalyzer::analyze(&manifest);
        assert!(report.has_violations());
        assert_eq!(report.violations.len(), 1);
        assert_eq!(report.violations[0].column, "email");
    }

    #[test]
    fn test_no_violation_when_pii_secured() {
        let mut nodes = HashMap::new();
        nodes.insert(
            "stg_users".into(),
            mock_node(
                "stg_users",
                vec![],
                vec![ColumnInfo {
                    name: "email".into(),
                    tests: vec![],
                    policy: Some("pii_masking".into()),
                }],
            ),
        );
        nodes.insert(
            "int_users".into(),
            mock_node(
                "int_users",
                vec!["stg_users"],
                vec![ColumnInfo {
                    name: "email".into(),
                    tests: vec![],
                    policy: Some("pii_hashing".into()),
                }],
            ),
        );

        let manifest = Manifest {
            project_name: "test".into(),
            nodes,
            sources: HashMap::new(),
        };

        let report = LineageAnalyzer::analyze(&manifest);
        assert!(!report.has_violations());
    }

    #[test]
    fn test_mermaid_output_contains_nodes() {
        let mut nodes = HashMap::new();
        nodes.insert("a".into(), mock_node("a", vec![], vec![]));
        nodes.insert("b".into(), mock_node("b", vec!["a"], vec![]));

        let manifest = Manifest {
            project_name: "test".into(),
            nodes,
            sources: HashMap::new(),
        };

        let report = LineageAnalyzer::analyze(&manifest);
        let mermaid = report.to_mermaid();
        assert!(mermaid.contains("graph LR"));
        assert!(mermaid.contains("a[a]"));
        assert!(mermaid.contains("b[b]"));
    }
}
