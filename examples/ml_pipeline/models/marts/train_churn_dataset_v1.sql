-- models/marts/train_churn_dataset_v1.sql
/* 
config:
  name: train_churn_dataset_v1
  materialized: table
  governance:
    security_level: internal
    owner: data_science
    versioned: true
*/

WITH features AS (
    SELECT 
        u.user_id,
        u.region,
        u.subscription_tier,
        u.signup_date,
        e.total_sessions,
        e.avg_session_duration,
        e.api_usage_count
    FROM {{ ref('feat_user_demographics_masked') }} u
    JOIN {{ ref('feat_user_engagement') }} e ON u.user_id = e.user_id
),

targets AS (
    SELECT 
        user_id,
        CASE WHEN churn_probability > 0.7 THEN 1 ELSE 0 END as target_churn
    FROM {{ ref('stg_predictions') }}
    -- Use latest prediction
    QUALIFY ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY prediction_date DESC) = 1
)

SELECT 
    f.*,
    t.target_churn
FROM features f
INNER JOIN targets t ON f.user_id = t.user_id
