# Design Specification — Data Catalog API Export (W3C DCAT / PROV-O)

**Date:** 2026-05-14  
**Author:** Antigravity / Superpowers  
**Status:** Approved  

---

## 1. Objective & Context

Verity acts as a "Compliance-as-Code" data transformation and governance engine. The `verity docs` command generates documentation for data pipelines. 
Currently, it produces a standalone HTML portal (`index.html`) and a `catalog.json` file structured specifically for UI rendering.

The objective of this feature is to make the data catalog **consumable by external APIs and systems** (e.g., enterprise data catalogs like Collibra/Alation, OpenLineage explorers, GCP/Airflow orchestrators). 

In accordance with the architectural decision (Approach 2), Verity will adopt a **systematic "All-in-one" generation** strategy.

---

## 2. Architecture & Execution Flow

```
                       ┌─────────────────────────┐
                       │   verity docs (CLI)     │
                       └────────────┬────────────┘
                                    │
                                    ▼
                       ┌─────────────────────────┐
                       │ GraphDiscovery (Rust)   │
                       └────────────┬────────────┘
                                    │
                  ┌─────────────────┴─────────────────┐
                  │                                   │
                  ▼                                   ▼
      ┌───────────────────────┐           ┌───────────────────────┐
      │  CatalogArtifact (UI) │           │  SemanticGraph (W3C)  │
      └───────────┬───────────┘           └───────────┬───────────┘
                  │                                   │
         ┌────────┴────────┐                          │
         ▼                 ▼                          ▼
   catalog.json       index.html            semantic_catalog.jsonld
```

Upon each invocation of `verity docs`, the `CatalogGenerator` application layer will orchestrate two distinct serialization pipelines:
1. The existing UI pipeline (`CatalogArtifact` → `catalog.json` / `index.html`).
2. The Semantic API pipeline (`SemanticGraph` → `semantic_catalog.jsonld`).

---

## 3. Implementation Details (`verity-core`)

### 3.1. Domain Layer (`verity-core/src/domain/governance/semantic.rs`)
The `SemanticGraph` struct already implements the conversion of the manifest into a W3C semantic graph compliant with established vocabularies:
- `dcat:Dataset` (Datasets / Models)
- `dcat:Distribution` (Data Sources)
- `prov:wasDerivedFrom` (Data Lineage / Dependencies)

### 3.2. Application Layer (`verity-core/src/application/catalog.rs`)
Within `CatalogGenerator::generate`:
- Semantic graph instantiation:
  ```rust
  let semantic_graph = crate::domain::governance::semantic::SemanticGraph::from_manifest(manifest);
  ```
- Deterministic serialization into formatted JSON-LD (`to_json_string`).
- Disk persistence via `atomic_write` to `target_dir.join("semantic_catalog.jsonld")`.

---

## 4. Error Handling & Idempotency

- **Atomicity:** The use of `atomic_write` ensures that no incomplete or corrupted files are exposed to API consumers.
- **Idempotency:** The deterministic sorting of keys and lineage references (already implemented in `SemanticGraph`) ensures that two executions on the same manifest produce a bit-perfect, identical `.jsonld` file.
- **Error Propagation:** Any JSON serialization or disk write failure will return a clear `VerityError::InternalError` captured and formatted by `miette`.

---

## 5. Verification Plan (TDD)

### 5.1. Unit & Integration Tests
- Verification within `verity-core` that the `CatalogGenerator::generate` method successfully produces the `semantic_catalog.jsonld` file in the target directory.
- Verification that the JSON-LD content correctly includes `@context` and W3C attributes.

### 5.2. E2E Validation in `data-foundation`
- Execution of the `just verity-docs` command from your GCP test project (`data-foundation`).
- Verification of the presence and syntactic validity of `target/semantic_catalog.jsonld`.
