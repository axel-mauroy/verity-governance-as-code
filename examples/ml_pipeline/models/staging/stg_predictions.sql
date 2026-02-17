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
    CAST(churn_probability AS DOUBLE) as churn_probability,
    prediction_date::DATE as prediction_date
FROM {{ source('raw', 'predictions') }}
