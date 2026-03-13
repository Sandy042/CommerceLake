CREATE OR REFRESH MATERIALIZED VIEW main.dev_gold.gold_revenue_trends 
AS
SELECT 
geolocation_state,
TO_DATE(DATE_FORMAT(order_purchase_timestamp,'yyyy-MM'),'yyyy-MM') AS month_year,
ROUND(SUM(price+freight_value),2) AS total_revenue,
COUNT(o.order_id) AS total_orders,
ROUND(avg(price+freight_value),2) AS average_order_value
FROM main.dev_silver.silver_orders o
JOIN main.dev_silver.silver_orderlist ol
ON o.order_id=ol.order_id
JOIN main.dev_silver.silver_customers c   
ON o.customer_id=c.customer_id
JOIN main.dev_silver.silver_geolocation g 
ON g.geolocation_zip_code_prefix=c.customer_zip_code_prefix
WHERE o.order_status='delivered'
GROUP BY geolocation_state,to_date(date_format(order_purchase_timestamp,'yyyy-MM'),'yyyy-MM');



CREATE OR REFRESH MATERIALIZED VIEW main.dev_gold.gold_delivery_sla
AS
SELECT 
  pc.product_category_name_english,
  geolocation_state,
  ROUND(AVG(DATE_DIFF(order_delivered_customer_date, order_purchase_timestamp)), 0) AS average_actual_delivery_days,
  ROUND(AVG(DATE_DIFF(order_estimated_delivery_date, order_purchase_timestamp)), 0) AS average_expected_delivery_days,
  100.0 * AVG(CASE WHEN DATE_DIFF(order_delivered_customer_date, order_purchase_timestamp) <= DATE_DIFF(order_estimated_delivery_date, order_purchase_timestamp) THEN 1 ELSE 0 END) AS on_time_delivery_rate,
  SUM(CASE WHEN DATE_DIFF(order_delivered_customer_date, order_purchase_timestamp) > DATE_DIFF(order_estimated_delivery_date, order_purchase_timestamp) THEN 1 ELSE 0 END) AS total_late_deliveries
FROM main.dev_silver.silver_orders o  
JOIN MAIN.dev_silver.silver_orderlist ol
ON o.order_id = ol.order_id
JOIN main.dev_silver.silver_products prod
ON ol.product_id = prod.product_id
JOIN main.dev_silver.silver_productcategorytranslation pc
ON prod.product_category_name = pc.product_category_name
JOIN main.dev_silver.silver_customers c
ON o.customer_id = c.customer_id
JOIN main.dev_silver.silver_geolocation g
ON g.geolocation_zip_code_prefix = c.customer_zip_code_prefix
WHERE o.order_status = 'delivered'
GROUP BY geolocation_state, product_category_name_english;


CREATE OR REFRESH MATERIALIZED VIEW main.dev_gold.gold_product_sales
AS
WITH aggregation_query AS
(SELECT
 product_category_name_english,
 to_date(date_format(order_purchase_timestamp,'yyyy-MM'),'yyyy-MM') AS year_month,
 COUNT(o.order_id) AS total_orders,
 ROUND(SUM(price+freight_value),2) AS total_revenue
FROM main.dev_silver.silver_orders o  
JOIN MAIN.dev_silver.silver_orderlist ol
ON o.order_id = ol.order_id
JOIN main.dev_silver.silver_products prod
ON ol.product_id = prod.product_id
JOIN main.dev_silver.silver_productcategorytranslation pc
ON prod.product_category_name = pc.product_category_name
WHERE o.order_status = 'delivered'
GROUP BY product_category_name_english,to_date(date_format(order_purchase_timestamp,'yyyy-MM'),'yyyy-MM'))
SELECT 
a.*,
ROUND(AVG
(total_revenue)
OVER (PARTITION BY product_category_name_english 
      ORDER BY year_month 
      ROWS BETWEEN 2 PRECEDING AND CURRENT ROW),2) 
      AS rolling_revenue_3m_window
FROM aggregation_query a;

CREATE OR REFRESH MATERIALIZED VIEW main.dev_gold.gold_review_analysis 
AS
WITH aggregate_query 
AS(
SELECT product_category_name_english,
ROUND(avg(review_score),0) AS avg_review_score,
SUM (CASE WHEN ordrev.review_score  >=4 THEN 1 ELSE 0 END) AS postivie_reviews,
SUM (CASE WHEN ordrev.review_score  <=2 THEN 1 ELSE 0 END) AS negative_reviews,
COUNT(*) AS total_review_count
FROM main.dev_silver.silver_orderreview ordrev
JOIN main.dev_silver.silver_orderlist ol
on ordrev.order_id=ol.order_id
JOIN main.dev_silver.silver_products prod
on ol.product_id=prod.product_id
JOIN main.dev_silver.silver_productcategorytranslation prdcat
ON prod.product_category_name=prdcat.product_category_name
GROUP BY product_category_name_english
)
SELECT 
a.*,
ROUND(100*(postivie_reviews/total_review_count),2) AS postive_percentage,
ROUND(100*(negative_reviews/total_review_count),2) AS negative_percentage
FROM aggregate_query a
ORDER BY total_review_count;





