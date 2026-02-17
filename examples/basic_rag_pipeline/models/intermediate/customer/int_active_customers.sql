-- models/intermediate/customer/int_active_customers.sql
SELECT * 
FROM {{ ref('stg_profiles') }}
WHERE account_status = 'ACTIVE'
