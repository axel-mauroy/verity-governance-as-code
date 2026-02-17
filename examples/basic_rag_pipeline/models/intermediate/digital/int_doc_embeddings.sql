-- models/intermediate/digital/int_doc_embeddings.sql
-- Joins 50k documents with 50k embeddings and 100k customers

SELECT 
    d.document_id,
    d.content,
    d.author_email,
    e.embedding_vector,
    c.customer_id as author_customer_id,
    c.segment as author_segment,
    d.created_at
FROM {{ ref('stg_documents') }} d
JOIN {{ ref('stg_embeddings') }} e 
    ON d.document_id = e.document_id
LEFT JOIN {{ ref('stg_profiles') }} c 
    ON d.author_email = c.email
