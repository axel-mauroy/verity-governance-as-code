-- models/staging/stg_users.sql
/* 
config:
  name: stg_users
  materialized: view
  header: true
  governance:
    security_level: confidential
*/

SELECT 
    user_id,
    email, -- PII
    name,  -- PII
    CAST(signup_date AS DATE) as signup_date,
    region,
    subscription_tier
FROM {{ source('raw', 'users') }}
