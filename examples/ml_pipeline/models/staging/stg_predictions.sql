-- models/staging/stg_predictions.sql
/* 
config:
  name: stg_predictions
  materialized: view
  header: true
  governance:
    security_level: internal
*/

SELECT 
    prediction_id,
    model_id,
    user_id,
    arrow_cast(churn_probability, 'Float64') as churn_probability,
    CAST(prediction_date AS DATE) as prediction_date
FROM {{ source('raw', 'predictions') }}
