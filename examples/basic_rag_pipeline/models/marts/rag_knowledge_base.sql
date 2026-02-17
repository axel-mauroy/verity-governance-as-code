/* 
config:
  name: rag_knowledge_base
  owner: ai_team@verity.dev
  materialized: table
  governance:
    public: true
    pii: false
*/

WITH enriched AS (
    SELECT * FROM {{ ref('int_documents_enriched') }}
),

final AS (
    SELECT
        document_id,
        content,
        embedding_vector,
        model_name,
        created_at
    FROM enriched
    WHERE contains_pii = FALSE -- Compliance filter
)

SELECT * FROM final
