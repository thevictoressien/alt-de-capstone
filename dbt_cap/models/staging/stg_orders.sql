
select
  order_id, 
  customer_id, 
  order_status,
  order_purchase_timestamp, 
  order_delivered_customer_date
from
    {{ source('raw', 'orders_raw') }} 


