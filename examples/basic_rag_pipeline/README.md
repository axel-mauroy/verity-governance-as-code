# Basic RAG Pipeline Example

This example demonstrates the **first 5 minutes** of using Verity to build a production-ready RAG (Retrieval-Augmented Generation) pipeline.

## What This Example Shows

1. **Installation**: How to get started with Verity (single binary)
2. **Initialization**: Project structure created by `verity init`
3. **Configuration**: Safe-by-design YAML with compliance policies
4. **First Run**: Execute transformations with pre-flight governance checks

## Project Structure
basic_rag_pipeline/
├── verity.yaml              # Main configuration
├── models/                  # Data transformations
│   ├── staging/
│   │   ├── stg_documents.sql
│   │   └── stg_embeddings.sql
│   ├── intermediate/
│   │   └── int_chunked_docs.sql
│   └── marts/
│       └── rag_knowledge_base.sql
├── compliance/              # Governance as Code
│   └── policies.yaml
├── data/                    # Sample raw data
│   ├── raw_documents.csv
│   └── raw_embeddings.parquet
└── .verity/                 # Metadata & cache
    └── manifest.json
```

## Quick Start

```bash
# Navigate to this example
cd examples/basic_rag_pipeline

# Run the pipeline
verity run

# Run specific models
verity run --select stg_documents
```

## What Makes This Different from dbt?

- ✅ **Vector-native**: Embeddings are first-class citizens
- ✅ **Compliance-first**: PII detection and masking built-in
- ✅ **Performance**: 15x faster than Python-based tools
- ✅ **Safety**: Pre-flight governance checks prevent data leaks

## Next Steps

- Explore `verity.yaml` to see how models are configured
- Check `compliance/policies.yaml` for governance rules
- Modify `models/staging/stg_documents.sql` to add your own logic
