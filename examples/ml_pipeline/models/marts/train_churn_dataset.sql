-- models/marts/train_churn_dataset.sql

WITH features AS (
    SELECT 
        u.user_id,
        u.region,
        u.subscription_tier,
        u.signup_date,
        e.total_sessions,
        e.avg_session_duration,
        e.api_usage_count
    FROM {{ ref('feat_masked_user_demographics') }} u
    JOIN {{ ref('feat_user_engagement') }} e ON u.user_id = e.user_id
),

latest_prediction AS (
    SELECT
        user_id,
        churn_probability,
        prediction_date,
        ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY prediction_date DESC) AS rn
    FROM {{ ref('stg_predictions') }}
),

targets AS (
    SELECT 
        user_id,
        CASE WHEN churn_probability > 0.7 THEN 1 ELSE 0 END AS target_churn
    FROM latest_prediction
    WHERE rn = 1
)

SELECT 
    f.*,
    t.target_churn
FROM features f
INNER JOIN targets t ON f.user_id = t.user_id
