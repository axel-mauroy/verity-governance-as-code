/* 
config:
  name: business_intelligence
  owner: management@verity.dev
  materialized: table
*/

WITH customer_sales AS (
    SELECT 
        customer_id,
        COUNT(order_id) as total_orders,
        SUM(total_amount) as lifetime_value
    FROM {{ ref('int_customer_sales') }}
    GROUP BY 1
),

finance AS (
    SELECT 
        SUM(amount) as total_finance_volume
    FROM {{ ref('stg_finance_transactions') }}
),

promotions AS (
    SELECT 
        COUNT(*) as total_active_promotions
    FROM {{ ref('stg_offer_promotions') }}
),

hr_metrics AS (
    SELECT 
        COUNT(*) as total_employees,
        AVG(salary_band) as avg_salary_band
    FROM {{ ref('stg_employees') }}
),

supply_metrics AS (
    SELECT 
        COUNT(DISTINCT product_id) as total_products,
        SUM(stock_level) as total_stock
    FROM {{ ref('stg_supply_inventory') }}
)

SELECT 
    cs.*,
    f.total_finance_volume,
    p.total_active_promotions,
    h.total_employees,
    h.avg_salary_band,
    s.total_products,
    s.total_stock
FROM customer_sales cs
CROSS JOIN finance f
CROSS JOIN promotions p
CROSS JOIN hr_metrics h
CROSS JOIN supply_metrics s
