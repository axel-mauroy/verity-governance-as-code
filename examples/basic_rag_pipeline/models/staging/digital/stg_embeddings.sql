-- models/staging/digital/stg_embeddings.sql
SELECT 
    embedding_id,
    document_id,
    embedding_vector,
    model_name,
    CAST(created_at AS TIMESTAMP) as created_at
FROM {{ source('digital', 'embeddings') }}
