WITH source AS (
    SELECT * FROM {{ source('supply', 'inventory') }}
)
SELECT
    product_id,
    warehouse_id,
    stock_level,
    reorder_point,
    last_updated
FROM source
