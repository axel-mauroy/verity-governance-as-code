// verity-core/src/domain/governance/semantic.rs

use crate::domain::project::{Manifest, ManifestNode, ResourceType};
use serde::Serialize;
use std::borrow::Cow;
use std::collections::BTreeMap;

// Utilisation de constantes pour les namespaces fixes (évite les allocations String)
const NS_VERITY: &str = "https://verity.ai/ns/";
const NS_DCAT: &str = "http://www.w3.org/ns/dcat#";
const NS_PROV: &str = "http://www.w3.org/ns/prov#";
const NS_RDFS: &str = "http://www.w3.org/2000/01/rdf-schema#";

#[derive(Debug, Serialize)]
pub struct SemanticGraph<'a> {
    #[serde(rename = "@context")]
    pub context: BTreeMap<&'static str, &'static str>,

    #[serde(rename = "@graph")]
    pub graph: Vec<JsonLdNode<'a>>,
}

#[derive(Debug, Serialize)]
pub struct JsonLdNode<'a> {
    #[serde(rename = "@id")]
    pub id: String,
    #[serde(rename = "@type")]
    pub type_: &'static str,
    #[serde(rename = "rdfs:label")]
    pub label: Cow<'a, str>,
    #[serde(rename = "verity:resourceType")]
    pub resource_type: &'static str,
    #[serde(rename = "prov:wasDerivedFrom", skip_serializing_if = "Vec::is_empty")]
    pub was_derived_from: Vec<String>,
    #[serde(rename = "verity:securityLevel")]
    pub security_level: &'static str,
    #[serde(rename = "verity:columns", skip_serializing_if = "Vec::is_empty")]
    pub columns: Vec<JsonLdColumn<'a>>,
}

#[derive(Debug, Serialize)]
pub struct JsonLdColumn<'a> {
    #[serde(rename = "@type")]
    pub type_: &'static str,
    pub name: Cow<'a, str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub policy: Option<Cow<'a, str>>,
}

impl<'a> Default for SemanticGraph<'a> {
    fn default() -> Self {
        Self::new()
    }
}

impl<'a> SemanticGraph<'a> {
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

    pub fn from_manifest(manifest: &'a Manifest) -> Self {
        let mut sorted_nodes: Vec<_> = manifest
            .nodes
            .iter()
            .filter(|(_, n)| n.resource_type != ResourceType::Test)
            .collect();

        // Tri déterministe
        sorted_nodes.sort_unstable_by_key(|(name, _)| *name);

        // Optimisation : On pré-alloue le vecteur final du graphe
        let mut graph = Vec::with_capacity(sorted_nodes.len());

        graph.extend(
            sorted_nodes
                .into_iter()
                .map(|(name, node)| JsonLdNode::from_node(name, node)),
        );

        let mut context = BTreeMap::new();
        context.insert("verity", NS_VERITY);
        context.insert("dcat", NS_DCAT);
        context.insert("prov", NS_PROV);
        context.insert("rdfs", NS_RDFS);

        Self { context, graph }
    }

    pub fn to_json_string(&self) -> serde_json::Result<String> {
        serde_json::to_string_pretty(self)
    }
}

impl<'a> JsonLdNode<'a> {
    fn from_node(name: &'a str, node: &'a ManifestNode) -> Self {
        let type_ = match node.resource_type {
            ResourceType::Model => "dcat:Dataset",
            ResourceType::Source => "dcat:Distribution",
            ResourceType::Analysis => "verity:Analysis",
            _ => "verity:Resource",
        };

        let mut was_derived_from: Vec<String> = node
            .refs
            .iter()
            .map(|r| {
                let mut s = String::with_capacity(7 + r.len());
                s.push_str("verity:");
                s.push_str(r);
                s
            })
            .collect();
        was_derived_from.sort_unstable(); // Crucial pour l'idempotence du JSON produit

        let columns = node
            .columns
            .iter()
            .map(|c| JsonLdColumn {
                type_: "verity:Column",
                name: Cow::Borrowed(&c.name),
                policy: c
                    .policy
                    .as_ref()
                    .map(|p: &crate::domain::governance::PolicyType| Cow::Borrowed(p.as_str())),
            })
            .collect();

        let mut id = String::with_capacity(7 + name.len());
        id.push_str("verity:");
        id.push_str(name);

        Self {
            id,
            type_,
            label: Cow::Borrowed(name),
            resource_type: node.resource_type.as_str(),
            was_derived_from,
            security_level: node.security_level.as_str(),
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
                policy: Some(crate::domain::governance::PolicyType::Masking(
                    crate::domain::governance::MaskingStrategy::Hash,
                )),
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
