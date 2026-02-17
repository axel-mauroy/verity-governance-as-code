WITH source AS (
    SELECT * FROM {{ source('finance', 'transactions') }}
),
Renamed AS (
    SELECT
        transaction_id,
        account_id,
        amount_vat_excluded AS amount,
        currency,
        transaction_type,
        transaction_timestamp AS transaction_date
    FROM source
)
SELECT * FROM Renamed
