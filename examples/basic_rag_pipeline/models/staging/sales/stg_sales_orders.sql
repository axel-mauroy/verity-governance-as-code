WITH source AS (
    SELECT * FROM {{ source('sales', 'orders') }}
),
Renamed AS (
    SELECT
        order_id,
        customer_id,
        order_date,
        total_amount,
        status
    FROM source
)
SELECT * FROM Renamed
