-- models/staging/stg_products.sql
SELECT
    p.*, pt.product_category_name_english
FROM
    {{ source('raw', 'products_raw') }} p
LEFT JOIN {{ source('raw', 'productcategorynametranslation_raw') }} pt
   ON p.product_category_name = pt.product_category_name
