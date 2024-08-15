-- models/intermediate/int_orders_by_state.sql
SELECT
    customer_state,
    COUNT(DISTINCT order_id) AS total_orders
FROM
    {{ ref('stg_orders') }} 
GROUP BY
    customer_state
