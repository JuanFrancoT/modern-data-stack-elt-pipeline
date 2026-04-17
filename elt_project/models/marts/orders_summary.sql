SELECT
    order_date,
    COUNT(*) AS total_orders
FROM {{ ref('stg_orders') }}
GROUP BY order_date
ORDER BY order_date