
SELECT
    product_id, product_category_name
FROM
    {{ source('raw', 'products_raw') }} 