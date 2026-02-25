-- models/monitoring/mon_prediction_drift.sql

SELECT 
    prediction_date,
    model_id,
    COUNT(*) as prediction_count,
    AVG(churn_probability) as avg_churn_prob,
    MIN(churn_probability) as min_churn_prob,
    MAX(churn_probability) as max_churn_prob
FROM {{ ref('stg_predictions') }}
GROUP BY 1, 2
