-- models/features/feat_user_demographics_masked.sql
/* 
config:
  name: feat_user_demographics_masked
  materialized: view
  governance:
    security_level: internal
*/

SELECT 
    user_id,
    -- PII is masked to allow downgrade to Internal
    md5(email) as email_hash,
    'REDACTED' as name_masked,
    region,
    subscription_tier,
    signup_date
FROM {{ ref('stg_users') }}
