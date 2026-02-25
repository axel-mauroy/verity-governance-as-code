-- models/staging/stg_predictions.sql

SELECT 
    prediction_id,
    model_id,
    user_id,
    CAST(churn_probability AS DOUBLE) as churn_probability,
    CAST(prediction_date AS DATE) as prediction_date
FROM {{ source('raw', 'predictions') }}
