{{ config(
    materialized='incremental',
    unique_key='order_id',
    schema='staging'
) }}

SELECT
    order_id,
    customer_id,
    LOWER(TRIM(order_status)) AS order_status,

    order_purchase_timestamp,
    order_approved_at,
    order_delivered_carrier_date,
    order_delivered_customer_date,
    order_estimated_delivery_date,

    DATE(order_purchase_timestamp) AS order_date

FROM {{ source('raw', 'orders') }}

WHERE order_id IS NOT NULL

{% if is_incremental() %}
    AND order_purchase_timestamp > (
        SELECT MAX(order_purchase_timestamp) FROM {{ this }}
    )
{% endif %}