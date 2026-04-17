{{ config(
    materialized='incremental',
    unique_key='order_id',
    schema='marts'
) }}

SELECT
    order_id,
    customer_id,
    order_status,

    order_purchase_timestamp,
    order_delivered_customer_date,

    DATEDIFF('day', order_purchase_timestamp, order_delivered_customer_date) AS delivery_days,

    CASE 
        WHEN order_delivered_customer_date IS NULL THEN 'not_delivered'
        ELSE 'delivered'
    END AS delivery_status

FROM {{ ref('stg_orders') }}