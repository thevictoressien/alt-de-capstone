-- models/final/fct_sales_by_category.sql
SELECT
    product_category_name,
    total_sales,
    RANK() OVER (ORDER BY total_sales DESC) AS sales_rank
FROM
    {{ ref('int_sales_by_category') }}
order by 3 
