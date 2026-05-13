# Data Catalog API Export Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Implement automatic, deterministic W3C DCAT/PROV-O compliant JSON-LD semantic data catalog export in `verity docs` for external API consumption.

**Architecture:** Enrich `CatalogGenerator::generate` to instantiate `SemanticGraph::from_manifest`, serialize it to JSON-LD, and persist it to `target/semantic_catalog.jsonld` alongside existing UI artifacts.

**Tech Stack:** Rust, serde_json, W3C DCAT/PROV-O vocabularies.

---

### Task 1 (C1): W3C Semantic Graph Export & Testing

**Files:**
- Modify: `verity-core/src/application/catalog.rs`

- [ ] **Step 1: Write the failing/verifying test**

Add a unit test module at the bottom of `verity-core/src/application/catalog.rs`:

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use crate::domain::project::{Manifest, ManifestNode, NodeConfig, ResourceType};
    use std::collections::HashMap;

    #[test]
    fn test_catalog_generator_all_artifacts() {
        let temp_dir = tempfile::tempdir().unwrap();
        let target_dir = temp_dir.path().join("target");
        std::fs::create_dir_all(&target_dir).unwrap();

        let mut nodes = HashMap::new();
        nodes.insert(
            "stg_users".to_string(),
            ManifestNode {
                name: "stg_users".to_string(),
                resource_type: ResourceType::Model,
                path: "models/stg_users.sql".into(),
                raw_sql: "SELECT 1".to_string(),
                config: NodeConfig::default(),
                columns: vec![],
                ..Default::default()
            },
        );

        let manifest = Manifest {
            project_name: "test_project".to_string(),
            nodes,
            sources: HashMap::new(),
        };

        CatalogGenerator::generate(temp_dir.path(), &target_dir, &manifest).unwrap();

        assert!(target_dir.join("catalog.json").exists());
        assert!(target_dir.join("index.html").exists());
        assert!(target_dir.join("semantic_catalog.jsonld").exists());

        let jsonld_content = std::fs::read_to_string(target_dir.join("semantic_catalog.jsonld")).unwrap();
        assert!(jsonld_content.contains("@context"));
        assert!(jsonld_content.contains("verity:stg_users"));
        assert!(jsonld_content.contains("dcat:Dataset"));
    }
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test test_catalog_generator_all_artifacts -- --nocapture`
Expected: FAIL due to `semantic_catalog.jsonld` not found.

- [ ] **Step 3: Write minimal implementation**

Modify `verity-core/src/application/catalog.rs` inside `CatalogGenerator::generate` around line 195, right before generating HTML:

```rust
        // 5. Generate the W3C Semantic Graph (JSON-LD)
        let semantic_graph = crate::domain::governance::semantic::SemanticGraph::from_manifest(manifest);
        let jsonld_path = target_dir.join("semantic_catalog.jsonld");
        let jsonld_content = semantic_graph.to_json_string()
            .context("Failed to serialize semantic graph to JSON-LD")
            .map_err(|e| VerityError::InternalError(e.to_string()))?;
        crate::infrastructure::fs::atomic_write(&jsonld_path, &jsonld_content)
            .with_context(|| format!("Failed to write semantic_catalog.jsonld to {:?}", jsonld_path))
            .map_err(|e| VerityError::InternalError(e.to_string()))?;
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cargo test test_catalog_generator_all_artifacts -- --nocapture`
Expected: PASS

- [ ] **Step 5: Run full workspace test suite**

Run: `cargo test`
Expected: PASS across all crates

- [ ] **Step 6: Commit**

```bash
git add verity-core/src/application/catalog.rs
git commit -m "feat(core): generate W3C DCAT/PROV-O semantic_catalog.jsonld in CatalogGenerator for API export"
```
