
select
  AVG(delivery_time_days) AS avg_delivery_time
from {{ref('int_avg_delivery_time')}}

