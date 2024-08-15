-- models/intermediate/int_sales_by_category.sql
with sales as (
select o.order_id, oi.price, oi.product_id, p.product_category_name_english as product_category_name
from {{ ref('stg_orders') }} o
join {{ source('raw', 'orderitems_raw') }} oi
    on o.order_id = oi.order_id
join {{ ref('stg_products') }} p
    on oi.product_id = p.product_id
)
select product_category_name, sum(price) as total_sales
from sales
group by product_category_name
