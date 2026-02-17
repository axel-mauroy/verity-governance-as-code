-- models/staging/digital/stg_documents.sql
SELECT 
    document_id,
    content,
    source_url,
    author_email,
    created_at::TIMESTAMP as created_at,
    updated_at::TIMESTAMP as updated_at,
    length(content) as content_length,
    now() as processed_at
FROM {{ source('digital', 'documents') }}
