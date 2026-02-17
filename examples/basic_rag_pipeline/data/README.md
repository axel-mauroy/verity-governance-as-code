# Sample Data README

This directory contains sample raw data for the basic RAG pipeline example.

## Files

### raw_documents.csv
Sample documents with the following schema:
- `document_id`: Unique identifier
- `content`: Text content of the document
- `source_url`: Original URL
- `author_email`: Email of the author (PII - will be masked in non-prod)
- `created_at`: Creation timestamp
- `updated_at`: Last update timestamp

### raw_embeddings.parquet (TODO)
Vector embeddings for the documents:
- `document_id`: Foreign key to documents
- `embedding`: 768-dimensional vector (array of floats)
- `model_version`: Embedding model identifier
- `embedding_timestamp`: When the embedding was generated

### access_control.csv
Permissions mapping for testing governance features:
- `document_id`: Foreign key to documents
- `user_id`: Unique user identifier
- `access_level`: `read`, `write`, or `admin`
- `department`: Department associated with the access
- `is_internal`: Boolean flag for internal-only documents

### audit_logs.json
Activity logs for data access and compliance auditing:
- `timestamp`: ISO-8601 timestamp of the event
- `user_id`: Who accessed the data
- `action`: Type of action performed (e.g., `READ`, `EXPORT`, `DELETE`)
- `resource_id`: Which document or resource was targeted
- `status`: Success or failure of the operation

## Loading Data

The data is automatically loaded by Verity when you run:
```bash
verity run
```

## Generating Embeddings

In a real scenario, embeddings would be generated using:
- OpenAI's `text-embedding-3-small` model
- Local models like `sentence-transformers`
- Custom embedding models

For this example, you can generate mock embeddings or use pre-computed ones.
