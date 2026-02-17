-- models/staging/stg_model_metadata.sql
/* 
config:
  name: stg_model_metadata
  materialized: view
  header: true
  governance:
    security_level: internal
*/

SELECT 
    model_id,
    version,
    created_by,
    algorithm,
    hyperparameters,
    created_at::TIMESTAMP as created_at
FROM {{ source('raw', 'model_metadata') }}
