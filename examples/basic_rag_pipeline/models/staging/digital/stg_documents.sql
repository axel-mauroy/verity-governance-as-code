-- models/staging/digital/stg_documents.sql
SELECT 
    document_id,
    content,
    source_url,
    author_email,
    CAST(created_at AS TIMESTAMP) as created_at,
    CAST(updated_at AS TIMESTAMP) as updated_at,
    length(content) as content_length,
    CURRENT_TIMESTAMP() as processed_at
FROM {{ source('digital', 'documents') }}
