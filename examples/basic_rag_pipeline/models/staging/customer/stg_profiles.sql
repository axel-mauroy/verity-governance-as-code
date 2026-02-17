-- models/staging/customer/stg_profiles.sql
SELECT 
    customer_id,
    email,
    first_name,
    last_name,
    segment,
    last_login::TIMESTAMP as last_login,
    signup_date::DATE as signup_date,
    account_status
FROM {{ source('customer', 'profiles') }}
