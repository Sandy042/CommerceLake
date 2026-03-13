"""
=============================================================
Silver Ingestion Framework
=============================================================
Project      : CommerceLake ETL Pipeline
Layer        : Silver (Cleansed & Conformed)
Framework    : Lakeflow Spark Declarative Pipelines (DLT)
Language     : Python
Author       : Sandesh M S
Date         : 09-03-2026
=============================================================

Description:
    This script implements the Silver layer transformation
    pipeline for the CommerceLake project using Databricks
    Lakeflow (Delta Live Tables).

    It performs the following operations:
    - Type casting: converts STRING columns to appropriate
      data types (INT, DOUBLE, TIMESTAMP)
    - Null handling: drops fully null rows and rows with
      null primary keys
    - Deduplication: uses APPLY CHANGES INTO (SCD Type 1)
      to ensure one record per unique key
    - Streaming: processes new Bronze records incrementally

Pipeline Flow:
    Bronze Tables (dev_bronze)
        ↓
    DLT Views (type casting + null handling)
        ↓
    Silver Streaming Tables (dev_silver)

Dependencies:
    - All 9 Bronze tables must exist before pipeline runs
    - Pipeline is triggered via ComerceLake_ETL_Job workflow
=============================================================
"""

import json
import dlt
from pyspark.sql.functions import *

# ============================================
# Silver Pipeline Configuration
# ============================================
# silver_configs is a data-driven configuration list that drives
# the entire silver pipeline dynamically.
#
# Instead of writing repetitive code for each of the 9 domains,
# a single apply_changes loop iterates over this config list,
# making the pipeline reusable and easy to maintain.
#
# Config Keys:
#   viewname    → Name of the DLT view that performs type casting
#                 and null handling before apply_changes
#   target      → Silver streaming table name (output)
#   source      → Bronze Delta table name (input)
#   keys        → Primary key column(s) for deduplication
#   sequence_by → Column used to determine the latest record
#                 (handles out-of-order CDC records)
#
# To add a new domain: simply add a new entry to this list!
# ============================================

silver_configs = [{"viewname": "products_transformed", "target": "silver_products", "source": "main.dev_bronze.bronze_products", "keys": ["product_id"], "sequence_by": "ingestion_date"},
{"viewname": "orders_transformed","target": "silver_orders",   "source": "main.dev_bronze.bronze_orders", "keys": ["order_id"], "sequence_by": "ingestion_date"},
{"viewname": "review_transformed","target": "silver_orderreview",   "source": "main.dev_bronze.bronze_orderreview", "keys": ["review_id"], "sequence_by": "ingestion_date"},
{"viewname": "customer_transformed","target": "silver_customers",   "source": "main.dev_bronze.bronze_customers", "keys": ["customer_id"], "sequence_by": "ingestion_date"},
{"viewname": "geolocation_transformed","target": "silver_geolocation",   "source": "main.dev_bronze.bronze_geolocation", "keys": ["geolocation_zip_code_prefix"], "sequence_by": "ingestion_date"},
{"viewname": "orderlist_transformed","target": "silver_orderlist",   "source": "main.dev_bronze.bronze_orderlist", "keys": ["order_id","order_item_id"], "sequence_by": "ingestion_date"},
{"viewname": "seller_transformed","target": "silver_seller",   "source": "main.dev_bronze.bronze_seller", "keys": ["seller_id"], "sequence_by": "ingestion_date"},
{"viewname": "payments_transformed","target": "silver_payments",   "source": "main.dev_bronze.bronze_payments", "keys": ["order_id","payment_sequential"], "sequence_by": "ingestion_date"},
{"viewname": "productcategorytranslation_transformed","target": "silver_productcategorytranslation",   "source": "main.dev_bronze.bronze_productcategorytranslation", "keys": ["product_category_name"], "sequence_by": "ingestion_date"}
]

@dlt.view(name="products_transformed")
def products_transformed():
    """
    DLT View: products_transformed
    --------------------------------
    Source  : main.dev_bronze.bronze_products
    Target  : consumed by silver_products via apply_changes

    Transformations:
        Type Casting:
        - product_name_lenght        : STRING → INT
        - product_description_lenght : STRING → INT
        - product_photos_qty         : STRING → INT
        - product_weight_g           : STRING → DOUBLE
        - product_length_cm          : STRING → DOUBLE
        - product_height_cm          : STRING → DOUBLE
        - product_width_cm           : STRING → DOUBLE

        Null Handling:
        - Drop rows where ALL columns are null
        - Drop rows where product_id is null (primary key)

    Note:
        All Bronze columns are STRING type since Auto Loader
        ingests raw CSV data without type inference.
        Type casting happens here in Silver to ensure
        data quality before apply_changes.
    """
    return spark.readStream.table("main.dev_bronze.bronze_products")\
        .withColumn("product_name_lenght", col('product_name_lenght').cast("int"))\
		.withColumn("product_description_lenght",col('product_description_lenght').cast("int"))\
		.withColumn("product_photos_qty", col('product_photos_qty').cast("int"))\
		.withColumn("product_weight_g",col('product_weight_g').cast("double"))\
		.withColumn("product_length_cm",col('product_length_cm').cast("double"))\
		.withColumn("product_height_cm",col('product_height_cm').cast("double"))\
		.withColumn("product_width_cm",col('product_width_cm').cast("double"))\
		.dropna('all')\
		.dropna(subset=['product_id'])

@dlt.view(name="orderlist_transformed")
def orderlist_transformed():
    return spark.readStream.table("main.dev_bronze.bronze_orderlist")\
		.withColumn("order_item_id", col('order_item_id').cast("int"))\
		.withColumn("shipping_limit_date",to_timestamp(date_format(col('shipping_limit_date'), 'dd-MM-yyyy HH:mm'),'dd-MM-yyyy HH:mm'))\
		.withColumn("price", col('price').cast("double"))\
		.withColumn("freight_value",col('freight_value').cast("double"))\
		.dropna('all')\
		.dropna(subset=['order_id','product_id'])

@dlt.view(name="geolocation_transformed")
def geolocation_transformed():
    return spark.readStream.table("main.dev_bronze.bronze_geolocation")\
		.withColumn("geolocation_zip_code_prefix", col('geolocation_zip_code_prefix').cast("int"))\
		.dropna('all')\
		.dropna(subset=['geolocation_zip_code_prefix'])
  
@dlt.view(name="payments_transformed")
def payments_transformed():
    return spark.readStream.table("main.dev_bronze.bronze_payments")\
		.withColumn("payment_sequential", col('payment_sequential').cast("int"))\
		.withColumn("payment_installments", col('payment_installments').cast("int"))\
		.withColumn("payment_value",col('payment_value').cast("double"))\
		.dropna('all')\
		.dropna(subset=['order_id'])

@dlt.view(name="review_transformed")
def review_transformed():
    return spark.readStream.table("main.dev_bronze.bronze_orderreview")\
		.withColumn("review_score", col('review_score').cast("int"))\
		.dropna('all')\
		.dropna(subset=['review_id','order_id'])
  
@dlt.view(name="productcategorytranslation_transformed")
def productcategorytranslation_transformed():
    return spark.readStream.table("main.dev_bronze.bronze_productcategorytranslation")\
		.dropna('all')\

@dlt.view(name="orders_transformed")
def orders_transformed():
    return spark.readStream.table("main.dev_bronze.bronze_orders")\
		.withColumn("order_purchase_timestamp", to_timestamp(date_format(col('order_purchase_timestamp'), 'dd-MM-yyyy HH:mm'), 'dd-MM-yyyy HH:mm'))\
		.withColumn("order_approved_at", to_timestamp(date_format(col('order_approved_at'), 'dd-MM-yyyy HH:mm'), 'dd-MM-yyyy HH:mm'))\
		.withColumn("order_delivered_carrier_date", to_timestamp(date_format(col('order_delivered_carrier_date'), 'dd-MM-yyyy HH:mm'), 'dd-MM-yyyy HH:mm'))\
		.withColumn("order_delivered_customer_date", to_timestamp(date_format(col('order_delivered_customer_date'), 'dd-MM-yyyy HH:mm'), 'dd-MM-yyyy HH:mm'))\
		.withColumn("order_estimated_delivery_date", to_timestamp(date_format(col('order_estimated_delivery_date'), 'dd-MM-yyyy HH:mm'), 'dd-MM-yyyy HH:mm'))\
		.dropna('all')\
		.dropna(subset=['order_id','customer_id'])

@dlt.view(name="seller_transformed")
def seller_transformed():
    return spark.readStream.table("main.dev_bronze.bronze_seller")\
		.withColumn("seller_zip_code_prefix",col('seller_zip_code_prefix').cast("int"))\
		.dropna('all')\
		.dropna(subset=['seller_id'])


@dlt.view(name="customer_transformed")
def customer_transformed():
    return spark.readStream.table("main.dev_bronze.bronze_customers")\
		.withColumn("customer_zip_code_prefix",col('customer_zip_code_prefix').cast("int"))\
		.dropna('all')\
		.dropna(subset=['customer_id'])



# ============================================
# Dynamic Silver Table Creation Loop
# ============================================
# Iterates over silver_configs list and dynamically creates
# Silver Streaming Tables with SCD Type 1 logic.
#
# For each config entry:
#   1. create_streaming_table → declares the target Silver
#      streaming table managed by the DLT pipeline
#   2. apply_changes → applies SCD Type 1 upsert logic:
#      - INSERT new records
#      - UPDATE existing records (latest version wins)
#      - Handles out-of-order records via sequence_by
#
# This pattern (Data-Driven Design) eliminates repetitive
# code — adding a new domain only requires a new entry
# in silver_configs, no code changes needed!
# ============================================
               
for config in silver_configs:
    dlt.create_streaming_table(config["target"])
    
    dlt.apply_changes(
        target=config["target"],
        source=config["viewname"],
        keys=config["keys"],
        sequence_by=col(config["sequence_by"]),
        stored_as_scd_type=1
    )
