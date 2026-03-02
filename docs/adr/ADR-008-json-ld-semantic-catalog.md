# ADR-008 — JSON-LD Semantic Graph for the Data Catalog

**Status:** Accepted  
**Date:** 2026-03-02  
**Deciders:** Verity Core Team  

---

## Context

`verity docs` generates a data catalog from the pipeline manifest. The catalog must:
- Be machine-readable for integration with external tools (data portals, lineage explorers).
- Represent data lineage (which model derives from which source/model).
- Encode governance metadata (security level, column policies).
- Be standards-compliant to interoperate with the broader data ecosystem.

Options considered:
1. **Custom JSON schema** — simple, no standard compliance.
2. **JSON-LD with established vocabularies** — standards-compliant, interoperable.
3. **OpenLineage format** — lineage-focused, less governance-aware.

---

## Decision

**The Verity catalog is serialized as a JSON-LD semantic graph using established vocabularies: DCAT, PROV-O, RDFS, and a custom `verity:` namespace.**

```json
{
  "@context": {
    "verity": "https://verity.ai/ns/",
    "dcat":   "http://www.w3.org/ns/dcat#",
    "prov":   "http://www.w3.org/ns/prov#",
    "rdfs":   "http://www.w3.org/2000/01/rdf-schema#"
  },
  "@graph": [
    {
      "@id":                   "verity:stg_users",
      "@type":                 "dcat:Dataset",
      "rdfs:label":            "stg_users",
      "verity:securityLevel":  "confidential",
      "prov:wasDerivedFrom":   ["verity:raw_users"],
      "verity:columns": [
        { "@type": "verity:Column", "name": "email", "policy": "masking:hash" }
      ]
    }
  ]
}
```

---

## Rationale

### DCAT for Dataset Typing

The W3C Data Catalog Vocabulary (DCAT) provides `dcat:Dataset` and `dcat:Distribution` — exact semantic matches for Verity's `Model` and `Source` resource types:

```rust
// domain/governance/semantic.rs
let type_ = match node.resource_type {
    ResourceType::Model  => "dcat:Dataset",
    ResourceType::Source => "dcat:Distribution",
    ResourceType::Analysis => "verity:Analysis",
    _ => "verity:Resource",
};
```

### PROV-O for Lineage

`prov:wasDerivedFrom` is the W3C PROV-O standard predicate for data lineage. Deriving lineage from the `refs` field in each `ManifestNode` maps directly:

```rust
let was_derived_from: Vec<String> = node.refs
    .iter()
    .map(|r| format!("verity:{}", r))
    .collect();
```

### Deterministic Output

The graph is sorted by model name (`sort_unstable_by_key`) and lineage refs are sorted before serialization. This ensures **idempotent** JSON output — two identical manifests always produce byte-identical catalogs, enabling diff-based change detection in CI.

### Zero-Copy in Hot Path

`Cow<'a, str>` is used for column names and labels, borrowing string slices from the manifest instead of cloning:

```rust
label: Cow::Borrowed(name),
name:  Cow::Borrowed(&c.name),
```

---

## Consequences

**Positive:**
- DCAT/PROV-O compliance enables integration with W3C-compatible data portals and triple stores.
- Deterministic output enables catalog diffs as part of PR reviews.
- The `verity:` namespace is extensible for future governance predicates.

**Negative:**
- JSON-LD is more verbose than a custom JSON schema — larger file size for large projects.
- Consumers unfamiliar with JSON-LD may find the `@context`/`@graph` structure surprising.

---

## References

- `verity-core/src/domain/governance/semantic.rs` — `SemanticGraph`, `JsonLdNode`, `JsonLdColumn`.
- `verity-core/src/application/catalog.rs` — Catalog generation orchestration.
- [W3C DCAT](https://www.w3.org/TR/vocab-dcat/) — Dataset catalog vocabulary.
- [W3C PROV-O](https://www.w3.org/TR/prov-o/) — Provenance ontology.
