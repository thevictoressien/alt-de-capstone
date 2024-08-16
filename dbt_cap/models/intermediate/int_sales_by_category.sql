
with sales as (
    select 
        o.order_id,
        o.order_status,
        o.order_delivered_customer_date,
        oi.price, 
        oi.product_id, 
        pt.product_category_name_english as product_category_name
    from {{ ref('stg_orders') }} o
    join {{ source('raw', 'orderitems_raw') }} oi
        on o.order_id = oi.order_id
    join {{ ref('stg_products') }} p
        on p.product_id = oi.product_id
    join {{ source('raw', 'productcategorynametranslation_raw') }} pt
        on pt.product_category_name = p.product_category_name
)
select 
    product_category_name, 
    sum(price) as total_sales
from sales
where order_status = 'delivered' and order_delivered_customer_date is not null
group by product_category_name
