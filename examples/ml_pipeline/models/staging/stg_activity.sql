-- models/staging/stg_activity.sql

SELECT 
    activity_id,
    user_id,
    activity_type,
    CAST(timestamp AS TIMESTAMP) as activity_at,
    CAST(duration_sec AS INTEGER) as duration_sec
FROM {{ source('raw', 'user_activity') }}
