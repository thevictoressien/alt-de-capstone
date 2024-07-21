CREATE SCHEMA IF NOT EXISTS ECOMM;

CREATE TABLE IF NOT EXISTS ECOMM.orders (
	order_id UUID NOT NULL, 
	customer_id UUID, 
	order_status VARCHAR, 
	order_purchase_timestamp TIMESTAMP, 
	order_approved_at TIMESTAMP, 
	order_delivered_carrier_date TIMESTAMP, 
	order_delivered_customer_date TIMESTAMP, 
	order_estimated_delivery_date TIMESTAMP
);
COPY ECOMM.orders (order_id, 
					customer_id, 
					order_status, 
					order_purchase_timestamp, 
					order_approved_at, 
					order_delivered_carrier_date, 
					order_delivered_customer_date, 
					order_estimated_delivery_date)
FROM '/data/olist_orders_dataset.csv' DELIMITER ',' CSV HEADER;


CREATE TABLE IF NOT EXISTS ECOMM.sellers (
	seller_id UUID NOT NULL, 
	seller_zip_code_prefix VARCHAR, 
	seller_city VARCHAR, 
	seller_state VARCHAR
);
COPY ECOMM.sellers (
	seller_id,
	seller_zip_code_prefix,
	seller_city,
	seller_state
) 
FROM '/data/olist_sellers_dataset.csv' DELIMITER ',' CSV HEADER;


CREATE TABLE IF NOT EXISTS ECOMM.customers (
	customer_id UUID NOT NULL, 
	customer_unique_id UUID, 
	customer_zip_code_prefix VARCHAR, 
	customer_city VARCHAR, 
	customer_state VARCHAR
);
COPY ECOMM.customers(
	customer_id, 
	customer_unique_id, 
	customer_zip_code_prefix, 
	customer_city, 
	customer_state
)
FROM '/data/olist_customers_dataset.csv' DELIMITER ',' CSV HEADER;


CREATE TABLE IF NOT EXISTS ECOMM.geolocation (
	geolocation_zip_code_prefix VARCHAR, 
	geolocation_lat FLOAT, 
	geolocation_lng FLOAT, 
	geolocation_city VARCHAR, 
	geolocation_state VARCHAR
);
COPY ECOMM.geolocation (
	geolocation_zip_code_prefix,
	geolocation_lat,
	geolocation_lng,
	geolocation_city,
	geolocation_state
)
FROM '/data/olist_geolocation_dataset.csv' DELIMITER ',' CSV HEADER;


CREATE TABLE IF NOT EXISTS ECOMM.products (
	product_id UUID NOT NULL, 
	product_category_name VARCHAR, 
	product_name_length INT, 
	product_description_length INT, 
	product_photos_qty INT, 
	product_weight_g INT, 
	product_length_cm INT, 
	product_height_cm INT, 
	product_width_cm INT
);
COPY ECOMM.products (
	product_id,
	product_category_name,
	product_name_length,
	product_description_length,
	product_photos_qty,
	product_weight_g,
	product_length_cm,
	product_height_cm,
	product_width_cm
)
FROM '/data/olist_products_dataset.csv' DELIMITER ',' CSV HEADER;


CREATE TABLE  IF NOT EXISTS ECOMM.ProductCategoryNameTranslation (
	product_category_name VARCHAR, 
	product_category_name_english VARCHAR
);
COPY ECOMM.ProductCategoryNameTranslation (
	product_category_name,
	product_category_name_english
)
FROM '/data/product_category_name_translation.csv' DELIMITER ',' CSV HEADER;


CREATE TABLE IF NOT EXISTS ECOMM.OrderReviews (
	review_id UUID NOT NULL, 
	order_id UUID, 
	review_score INT, 
	review_comment_title VARCHAR, 
	review_comment_message VARCHAR, 
	review_creation_date TIMESTAMP, 
	review_answer_timestamp TIMESTAMP
);
COPY ECOMM.OrderReviews (
	review_id,
	order_id,
	review_score,
	review_comment_title,
	review_comment_message,
	review_creation_date,
	review_answer_timestamp
)
FROM '/data/olist_order_reviews_dataset.csv' DELIMITER ',' CSV HEADER;


CREATE TABLE IF NOT EXISTS ECOMM.OrderItems (
	order_id UUID, 
	order_item_id INT, 
	product_id UUID, 
	seller_id UUID, 
	shipping_limit_date TIMESTAMP, 
	price FLOAT, 
	freight_value FLOAT
);
COPY ECOMM.OrderItems (
	order_id,
	order_item_id,
	product_id,
	seller_id,
	shipping_limit_date,
	price,
	freight_value
)
FROM '/data/olist_order_items_dataset.csv' DELIMITER ',' CSV HEADER;


CREATE TABLE ECOMM.OrderPayments (
	order_id UUID, 
	payment_sequential INT, 
	payment_type VARCHAR, 
	payment_installments INT, 
	payment_value FLOAT
);
COPY ECOMM.OrderPayments (
	order_id,
	payment_sequential,
	payment_type,
	payment_installments,
	payment_value
)
FROM '/data/olist_order_payments_dataset.csv' DELIMITER ',' CSV HEADER;