// verity-core/src/domain/graph/dag.rs

use crate::domain::error::DomainError;
use crate::domain::project::Manifest;
use std::collections::{HashMap, VecDeque};

pub struct GraphSolver;

impl GraphSolver {
    /// Calculates the execution order of models (Topological Sort with Layers).
    /// Returns a list of layers, where each layer contains nodes that can be executed in parallel.
    /// Layer N depends only on layers 0..N-1.
    pub fn plan_execution(manifest: &Manifest) -> Result<Vec<Vec<String>>, DomainError> {
        let mut in_degree: HashMap<String, usize> = HashMap::new();
        let mut adj_list: HashMap<String, Vec<String>> = HashMap::new();

        // 1. Initialization: Prepare all known nodes
        for node_name in manifest.nodes.keys() {
            in_degree.insert(node_name.clone(), 0);
            adj_list.insert(node_name.clone(), Vec::new());
        }

        // 2. Graph Construction (Dependency Inversion)
        for (node_name, node) in &manifest.nodes {
            for dep_name in &node.refs {
                if manifest.nodes.contains_key(dep_name) {
                    adj_list
                        .entry(dep_name.clone())
                        .or_default()
                        .push(node_name.clone());

                    *in_degree.entry(node_name.clone()).or_insert(0) += 1;
                }
            }
        }

        // 3. Kahn's Algorithm (Layered)
        let mut layers: Vec<Vec<String>> = Vec::new();
        let mut queue: VecDeque<String> = VecDeque::new();

        // Initial layer: nodes with 0 dependencies
        for (name, &degree) in &in_degree {
            if degree == 0 {
                queue.push_back(name.clone());
            }
        }

        let mut total_resolved = 0;

        while !queue.is_empty() {
            let mut current_layer = Vec::new();
            // Capture the current size of queue - these are all nodes in the *current* layer
            // that are ready to run.
            let layer_size = queue.len();

            for _ in 0..layer_size {
                if let Some(current) = queue.pop_front() {
                    current_layer.push(current.clone());
                    total_resolved += 1;

                    if let Some(neighbors) = adj_list.get(&current) {
                        for neighbor in neighbors {
                            if let Some(degree) = in_degree.get_mut(neighbor) {
                                *degree -= 1;
                                if *degree == 0 {
                                    queue.push_back(neighbor.clone());
                                }
                            }
                        }
                    }
                }
            }
            layers.push(current_layer);
        }

        // 4. Cycle Detection
        if total_resolved != manifest.nodes.len() {
            return Err(DomainError::CircularDependency(format!(
                "Graph contains a cycle. Resolved {}/{} nodes.",
                total_resolved,
                manifest.nodes.len()
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
        // A -> B -> C (C depends on B, B depends on A)
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
        // A (no deps) -> Layer 0
        // B (deps on A) -> Layer 1
        // C (deps on B) -> Layer 2
        assert_eq!(plan.len(), 3);
        assert!(plan[0].contains(&"model_a".to_string()));
        assert!(plan[1].contains(&"model_b".to_string()));
        assert!(plan[2].contains(&"model_c".to_string()));
        Ok(())
    }

    #[test]
    fn test_dag_cycle_error() {
        // A -> B -> A (Cycle)
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
}
