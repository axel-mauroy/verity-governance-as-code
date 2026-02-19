WITH documents AS (
    SELECT * FROM {{ ref('stg_documents') }}
),

embeddings AS (
    SELECT * FROM {{ ref('stg_embeddings') }}
),

joined AS (
    SELECT
        d.document_id,
        d.content,
        d.source_url,
        d.author_email,
        e.embedding_vector,
        e.model_name,
        d.created_at,
        d.processed_at
    FROM documents d
    LEFT JOIN embeddings e 
      ON d.document_id = e.document_id
),

final AS (
    SELECT
        *,
        -- Dummy PII Flagging for demonstration
        CASE 
            WHEN content LIKE '%@%' OR content LIKE '%06%' THEN TRUE
            ELSE FALSE
        END AS contains_pii,
        CASE
            WHEN author_email LIKE '%@%' THEN TRUE
            ELSE FALSE
        END AS email_pii
    FROM joined
)

SELECT * FROM final
