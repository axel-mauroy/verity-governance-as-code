use crate::domain::project::{Manifest, ManifestNode, ResourceType};
use serde::Serialize;
use std::collections::BTreeMap;

// Utilisation de constantes pour les namespaces fixes (évite les allocations String)
const NS_VERITY: &str = "https://verity.ai/ns/";
const NS_DCAT: &str = "http://www.w3.org/ns/dcat#";
const NS_PROV: &str = "http://www.w3.org/ns/prov#";
const NS_RDFS: &str = "http://www.w3.org/2000/01/rdf-schema#";

#[derive(Debug, Serialize)]
pub struct SemanticGraph {
    #[serde(rename = "@context")]
    pub context: BTreeMap<&'static str, &'static str>,

    #[serde(rename = "@graph")]
    pub graph: Vec<JsonLdNode>,
}

#[derive(Debug, Serialize)]
pub struct JsonLdNode {
    #[serde(rename = "@id")]
    pub id: String,
    #[serde(rename = "@type")]
    pub type_: &'static str,
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
    pub type_: &'static str,
    pub name: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub policy: Option<String>,
}

impl Default for SemanticGraph {
    fn default() -> Self {
        Self::new()
    }
}

impl SemanticGraph {
    pub fn new() -> Self {
        let mut context = BTreeMap::new();
        context.insert("verity", NS_VERITY);
        context.insert("dcat", NS_DCAT);
        context.insert("prov", NS_PROV);
        context.insert("rdfs", NS_RDFS);

        Self {
            context,
            graph: Vec::new(),
        }
    }

    pub fn from_manifest(manifest: &Manifest) -> Self {
        let mut semantic_graph = Self::new();

        // On pré-alloue pour éviter les reallocs sur de gros graphes
        let mut sorted_nodes: Vec<_> = manifest
            .nodes
            .iter()
            .filter(|(_, n)| n.resource_type != ResourceType::Test)
            .collect();

        // Tri déterministe
        sorted_nodes.sort_unstable_by_key(|(name, _)| *name);

        semantic_graph.graph = sorted_nodes
            .into_iter()
            .map(|(name, node)| JsonLdNode::from_node(name, node))
            .collect();

        semantic_graph
    }

    pub fn to_json_string(&self) -> serde_json::Result<String> {
        serde_json::to_string_pretty(self)
    }
}

impl JsonLdNode {
    fn from_node(name: &str, node: &ManifestNode) -> Self {
        let type_ = match node.resource_type {
            ResourceType::Model => "dcat:Dataset",
            ResourceType::Source => "dcat:Distribution",
            ResourceType::Analysis => "verity:Analysis",
            _ => "verity:Resource",
        };

        let mut was_derived_from: Vec<String> =
            node.refs.iter().map(|r| format!("verity:{}", r)).collect();
        was_derived_from.sort_unstable(); // Plus rapide que sort() si on se moque de l'ordre relatif des égaux

        let columns = node
            .columns
            .iter()
            .map(|c| JsonLdColumn {
                type_: "verity:Column",
                name: c.name.clone(),
                policy: c.policy.clone(),
            })
            .collect();

        Self {
            id: format!("verity:{}", name),
            type_,
            label: name.to_string(),
            resource_type: format!("{:?}", node.resource_type),
            was_derived_from,
            security_level: format!("{:?}", node.security_level),
            columns,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::domain::project::{ColumnInfo, ManifestNode, NodeConfig};
    use anyhow::Result;
    use std::collections::HashMap;

    #[test]
    fn test_semantic_graph_generation() -> Result<()> {
        let mut nodes = HashMap::new();

        let model_a = ManifestNode {
            name: "model_a".to_string(),
            resource_type: ResourceType::Model,
            path: "models/a.sql".into(),
            raw_sql: "SELECT 1".to_string(),
            config: NodeConfig::default(),
            columns: vec![ColumnInfo {
                name: "id".to_string(),
                tests: vec![],
                policy: None,
            }],
            ..Default::default()
        };

        let model_b = ManifestNode {
            name: "model_b".to_string(),
            resource_type: ResourceType::Model,
            path: "models/b.sql".into(),
            raw_sql: "SELECT * FROM model_a".to_string(),
            refs: vec!["model_a".to_string()],
            config: NodeConfig::default(),
            columns: vec![ColumnInfo {
                name: "id".to_string(),
                tests: vec![],
                policy: Some("hash".to_string()),
            }],
            ..Default::default()
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
