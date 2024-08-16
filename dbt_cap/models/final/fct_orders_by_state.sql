-- models/final/fct_orders_by_state.sql
SELECT
    customer_state,
    total_orders,
    RANK() OVER (ORDER BY total_orders DESC) AS order_rank
FROM
    {{ ref('int_orders_by_state') }}
order by 3
