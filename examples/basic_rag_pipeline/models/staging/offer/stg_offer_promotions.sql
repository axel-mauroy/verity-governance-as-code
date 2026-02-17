WITH source AS (
    SELECT * FROM {{ source('offer', 'promotions') }}
),
Renamed AS (
    SELECT
        promotion_id,
        name AS promotion_name,
        discount_pct,
        start_date,
        end_date
    FROM source
)
SELECT * FROM Renamed
