use crate::domain::project::{Manifest, ResourceType};
use serde::Serialize;
use std::collections::HashMap;

/// Represents the entire graph in a semantic format (JSON-LD).
/// This structure is designed to be easily ingested by LLMs or Knowledge Graphs.
#[derive(Debug, Serialize)]
pub struct SemanticGraph {
    #[serde(rename = "@context")]
    pub context: HashMap<String, String>,

    #[serde(rename = "@graph")]
    pub graph: Vec<JsonLdNode>,
}

/// A single node in the semantic graph (Model, Source, or Column).
#[derive(Debug, Serialize)]
pub struct JsonLdNode {
    #[serde(rename = "@id")]
    pub id: String,

    #[serde(rename = "@type")]
    pub type_: String,

    #[serde(rename = "rdfs:label")]
    pub label: String,

    #[serde(rename = "verity:resourceType")]
    pub resource_type: String,

    #[serde(rename = "prov:wasDerivedFrom", skip_serializing_if = "Vec::is_empty")]
    pub was_derived_from: Vec<String>,

    #[serde(rename = "verity:securityLevel")]
    pub security_level: String,

    #[serde(rename = "verity:columns", skip_serializing_if = "Vec::is_empty")]
    pub columns: Vec<JsonLdColumn>,
}

#[derive(Debug, Serialize)]
pub struct JsonLdColumn {
    #[serde(rename = "@type")]
    pub type_: String,
    pub name: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub policy: Option<String>,
}

impl SemanticGraph {
    pub fn new() -> Self {
        let mut context = HashMap::new();
        context.insert("verity".to_string(), "https://verity.ai/ns/".to_string());
        context.insert("dcat".to_string(), "http://www.w3.org/ns/dcat#".to_string());
        context.insert("prov".to_string(), "http://www.w3.org/ns/prov#".to_string());
        context.insert("rdfs".to_string(), "http://www.w3.org/2000/01/rdf-schema#".to_string());

        Self {
            context,
            graph: Vec::new(),
        }
    }

    pub fn from_manifest(manifest: &Manifest) -> Self {
        let mut semantic_graph = Self::new();

        for (name, node) in &manifest.nodes {
            if node.resource_type == ResourceType::Test {
                continue;
            }

            let id = format!("verity:{}", name);
            let type_ = match node.resource_type {
                ResourceType::Model => "dcat:Dataset",
                ResourceType::Source => "dcat:Distribution",
                ResourceType::Analysis => "verity:Analysis",
                _ => "verity:Resource",
            }
            .to_string();

            let derived_from = node
                .refs
                .iter()
                .map(|ref_name| format!("verity:{}", ref_name))
                .collect();

            let columns = node
                .columns
                .iter()
                .map(|c| JsonLdColumn {
                    type_: "verity:Column".to_string(),
                    name: c.name.clone(),
                    policy: c.policy.clone(),
                })
                .collect();

            let ld_node = JsonLdNode {
                id,
                type_,
                label: name.clone(),
                resource_type: format!("{:?}", node.resource_type),
                was_derived_from: derived_from,
                security_level: format!("{:?}", node.security_level),
                columns,
            };

            semantic_graph.graph.push(ld_node);
        }

        semantic_graph
    }

    pub fn to_json_string(&self) -> serde_json::Result<String> {
        serde_json::to_string_pretty(self)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::domain::project::{ColumnInfo, NodeConfig};
    use anyhow::Result;

    #[test]
    fn test_semantic_graph_generation() -> Result<()> {
        let mut nodes = HashMap::new();

        let model_a = ManifestNode {
            name: "model_a".to_string(),
            resource_type: ResourceType::Model,
            path: "models/a.sql".into(),
            schema_path: None,
            raw_sql: "SELECT 1".to_string(),
            refs: vec![],
            config: NodeConfig::default(),
            columns: vec![ColumnInfo {
                name: "id".to_string(),
                tests: vec![],
                policy: None,
            }],
            security_level: Default::default(),
            compliance: None,
        };

        let model_b = ManifestNode {
            name: "model_b".to_string(),
            resource_type: ResourceType::Model,
            path: "models/b.sql".into(),
            schema_path: None,
            raw_sql: "SELECT * FROM model_a".to_string(),
            refs: vec!["model_a".to_string()],
            config: NodeConfig::default(),
            columns: vec![ColumnInfo {
                name: "id".to_string(),
                tests: vec![],
                policy: Some("hash".to_string()),
            }],
            security_level: Default::default(),
            compliance: None,
        };

        nodes.insert("model_a".to_string(), model_a);
        nodes.insert("model_b".to_string(), model_b);

        let manifest = Manifest {
            project_name: "test_project".to_string(),
            nodes,
            sources: HashMap::new(),
        };

        let semantic_graph = SemanticGraph::from_manifest(&manifest);
        let json_output = semantic_graph.to_json_string()?;

        println!("{}", json_output);

        assert!(json_output.contains("@context"));
        assert!(json_output.contains("verity:model_a"));
        assert!(json_output.contains("verity:model_b"));
        assert!(json_output.contains("prov:wasDerivedFrom"));
        assert!(
            json_output.contains("verity:model_a"),
            "model_b should derive from model_a"
        );
        assert!(json_output.contains("dcat:Dataset"));

        Ok(())
    }
}
