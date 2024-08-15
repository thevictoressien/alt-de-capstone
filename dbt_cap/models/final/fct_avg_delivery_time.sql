-- models/final/fct_avg_delivery_time.sql

WITH delivery_stats AS (
  SELECT
    order_id,
    delivery_time_days,
    avg_delivery_time_days,
    COUNT(*) OVER() AS total_orders,
    SUM(CASE WHEN delivery_time_days > avg_delivery_time_days THEN 1 ELSE 0 END) OVER() AS orders_above_avg,
    SUM(CASE WHEN delivery_time_days <= avg_delivery_time_days THEN 1 ELSE 0 END) OVER() AS orders_below_avg
  FROM {{ ref('int_avg_delivery_time') }}
)
SELECT
  AVG(avg_delivery_time_days) AS avg_delivery_time,
  MAX(orders_above_avg) AS orders_above_avg,
  MAX(orders_below_avg) AS orders_below_avg,
  MAX(orders_above_avg) / MAX(total_orders) * 100 AS percent_above_avg,
  MAX(orders_below_avg) / MAX(total_orders) * 100 AS percent_below_avg
FROM delivery_stats

