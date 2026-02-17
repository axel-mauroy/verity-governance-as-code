-- models/marts/rag/rag_context.sql
-- The table used by the semantic search engine.

SELECT 
    document_id,
    content,
    embedding_vector,
    author_customer_id,
    author_segment,
    created_at
FROM {{ ref('int_doc_embeddings') }}
