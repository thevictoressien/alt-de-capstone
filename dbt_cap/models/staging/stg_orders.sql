-- models/staging/stg_orders.sql
select
  o.*, c.customer_city,c.customer_state
from
    {{ source('raw', 'orders_raw') }} o
join {{ source('raw', 'customers_raw') }}  c on o.customer_id = c.customer_id