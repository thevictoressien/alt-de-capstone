-- models/intermediate/int_orders_by_state.sql
select
    c.customer_state,
    COUNT(DISTINCT order_id) AS total_orders
from
    {{ ref('stg_orders') }} o
join 
    {{source('raw', 'customers_raw')}} c
    on o.customer_id = c.customer_id
where o.order_status = 'delivered' and o.order_delivered_customer_date is not null
group by
    1
