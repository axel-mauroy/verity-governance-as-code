// verity-core/src/domain/graph/dag.rs

use crate::domain::error::DomainError;
use crate::domain::project::Manifest;
use std::collections::{HashMap, VecDeque};

pub struct GraphSolver;

impl GraphSolver {
    /// Calculates the execution order of models (Topological Sort with Layers).
    /// Uses Zero-Copy architecture: returns references to the Manifest strings.
    pub fn plan_execution<'a>(manifest: &'a Manifest) -> Result<Vec<Vec<&'a str>>, DomainError> {
        // Pré-allocation pour éviter les redimensionnements dynamiques
        let capacity = manifest.nodes.len();
        let mut in_degree: HashMap<&'a str, usize> = HashMap::with_capacity(capacity);
        let mut adj_list: HashMap<&'a str, Vec<&'a str>> = HashMap::with_capacity(capacity);

        // 1. Initialization: Prepare all known nodes using references
        for node_name in manifest.nodes.keys() {
            in_degree.insert(node_name.as_str(), 0);
            adj_list.insert(node_name.as_str(), Vec::new());
        }

        // 2. Graph Construction with Strict Validation (Zero-Trust)
        for (node_name, node) in &manifest.nodes {
            let current_node = node_name.as_str();

            for dep_name in &node.refs {
                let parent_node = dep_name.as_str();

                // Faille de compilation bloquante si la référence n'existe pas
                if !adj_list.contains_key(parent_node) {
                    return Err(DomainError::CircularDependency(format!(
                        "Dangling Reference: Model '{}' depends on '{}' which does not exist.",
                        current_node, parent_node
                    )));
                    // Note: Il serait préférable de créer un `DomainError::DanglingReference`
                    // dans ton enum d'erreur pour être plus sémantique.
                }

                // Inversion de dépendance : Le parent pointe vers l'enfant
                if let Some(list) = adj_list.get_mut(parent_node) {
                    list.push(current_node);
                }
                if let Some(degree) = in_degree.get_mut(current_node) {
                    *degree += 1;
                }
            }
        }

        // 3. Kahn's Algorithm (Layered)
        let mut layers: Vec<Vec<&'a str>> = Vec::new();
        let mut queue: VecDeque<&'a str> = VecDeque::new();

        // Initial layer: nodes with 0 dependencies
        for (name, &degree) in &in_degree {
            if degree == 0 {
                queue.push_back(name);
            }
        }

        let mut total_resolved = 0;

        while !queue.is_empty() {
            let layer_size = queue.len();
            let mut current_layer = Vec::with_capacity(layer_size);

            for _ in 0..layer_size {
                if let Some(current) = queue.pop_front() {
                    current_layer.push(current);
                    total_resolved += 1;

                    if let Some(neighbors) = adj_list.get(current) {
                        for neighbor in neighbors {
                            if let Some(degree) = in_degree.get_mut(neighbor) {
                                *degree -= 1;
                                if *degree == 0 {
                                    queue.push_back(neighbor);
                                }
                            }
                        }
                    }
                }
            }
            layers.push(current_layer);
        }

        // 4. Cycle Detection
        if total_resolved != capacity {
            return Err(DomainError::CircularDependency(format!(
                "Graph contains a cycle. Resolved {}/{} nodes. Check your dependencies.",
                total_resolved, capacity
            )));
        }

        Ok(layers)
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;
    use crate::domain::project::{Manifest, ManifestNode, NodeConfig, ResourceType};
    use anyhow::Result;
    use std::collections::HashMap;

    fn create_mock_node(name: &str, refs: Vec<&str>) -> ManifestNode {
        ManifestNode {
            name: name.to_string(),
            resource_type: ResourceType::Model,
            path: std::path::PathBuf::from("mock.sql"),
            schema_path: None,
            raw_sql: "".to_string(),
            refs: refs.iter().map(|s| s.to_string()).collect(),
            config: NodeConfig::default(),
            columns: vec![],
            security_level: Default::default(),
            compliance: None,
        }
    }

    #[test]
    fn test_dag_linear() -> Result<()> {
        let mut nodes = HashMap::new();
        nodes.insert("model_a".to_string(), create_mock_node("model_a", vec![]));
        nodes.insert(
            "model_b".to_string(),
            create_mock_node("model_b", vec!["model_a"]),
        );
        nodes.insert(
            "model_c".to_string(),
            create_mock_node("model_c", vec!["model_b"]),
        );

        let manifest = Manifest {
            project_name: "test".into(),
            nodes,
            sources: HashMap::new(),
        };

        let plan = GraphSolver::plan_execution(&manifest)?;
        assert_eq!(plan.len(), 3);
        assert!(plan[0].contains(&"model_a"));
        assert!(plan[1].contains(&"model_b"));
        assert!(plan[2].contains(&"model_c"));
        Ok(())
    }

    #[test]
    fn test_dag_cycle_error() {
        let mut nodes = HashMap::new();
        nodes.insert(
            "model_a".to_string(),
            create_mock_node("model_a", vec!["model_b"]),
        );
        nodes.insert(
            "model_b".to_string(),
            create_mock_node("model_b", vec!["model_a"]),
        );

        let manifest = Manifest {
            project_name: "test".into(),
            nodes,
            sources: HashMap::new(),
        };
        let result = GraphSolver::plan_execution(&manifest);
        assert!(matches!(result, Err(DomainError::CircularDependency(_))));
    }

    #[test]
    fn test_dangling_reference_fails() {
        let mut nodes = HashMap::new();
        // model_a depends on non_existent_model
        nodes.insert(
            "model_a".to_string(),
            create_mock_node("model_a", vec!["non_existent_model"]),
        );

        let manifest = Manifest {
            project_name: "test".into(),
            nodes,
            sources: HashMap::new(),
        };
        let result = GraphSolver::plan_execution(&manifest);
        assert!(
            result.is_err(),
            "A missing dependency should trigger a hard compilation error."
        );
    }
}
