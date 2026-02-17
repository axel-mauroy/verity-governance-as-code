-- models/features/feat_user_engagement.sql
/* 
config:
  name: feat_user_engagement
  materialized: table
  governance:
    security_level: internal
*/

SELECT 
    user_id,
    COUNT(activity_id) as total_sessions,
    AVG(duration_sec) as avg_session_duration,
    MAX(activity_at) as last_active_at,
    COUNT(CASE WHEN activity_type = 'api_call' THEN 1 END) as api_usage_count
FROM {{ ref('stg_activity') }}
GROUP BY user_id
