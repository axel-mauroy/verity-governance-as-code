WITH source AS (
    SELECT * FROM {{ source('customer', 'profiles') }}
),
Renamed AS (
    SELECT
        customer_id,
        email,
        first_name,
        last_name,
        segment,
        last_login,
        signup_date,
        account_status,
        account_end_date
    FROM source
)
SELECT * FROM Renamed
