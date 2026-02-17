-- models/staging/digital/stg_embeddings.sql
SELECT 
    embedding_id,
    document_id,
    embedding_vector,
    model_name,
    created_at::TIMESTAMP as created_at
FROM {{ source('digital', 'embeddings') }}
