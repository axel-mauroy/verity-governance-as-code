-- models/staging/customer/stg_profiles.sql
SELECT 
    customer_id,
    email,
    first_name,
    last_name,
    segment,
    CAST(last_login AS TIMESTAMP) as last_login,
    CAST(signup_date AS DATE) as signup_date,
    account_status
FROM {{ source('customer', 'profiles') }}
