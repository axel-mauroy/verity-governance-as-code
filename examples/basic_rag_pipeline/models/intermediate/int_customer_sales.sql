/* 
config:
  name: int_customer_sales
  owner: business_ops@verity.dev
  materialized: view
*/

WITH customers AS (
    SELECT * FROM {{ ref('stg_customer_profiles') }}
),

orders AS (
    SELECT * FROM {{ ref('stg_sales_orders') }}
),

joined AS (
    SELECT
        c.customer_id,
        c.first_name,
        c.last_name,
        c.email,
        o.order_id,
        o.total_amount,
        o.order_date,
        o.status
    FROM customers c
    LEFT JOIN orders o ON c.customer_id = o.customer_id
)

SELECT * FROM joined
