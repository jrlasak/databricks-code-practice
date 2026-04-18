from variables import *
import dlt
from datetime import datetime, timezone
from pyspark.sql import functions as F
from variables import *


"""
✅ Bronze Layer: Raw Data Ingestion
===================================

Goal:
    Ingest raw source data as streams, add metadata for lineage, and enforce critical 
    schema rules. This layer is the foundation for all downstream processing and should 
    guarantee that only structurally valid records enter the pipeline.

Source Path:
    /Volumes/apparel_store/00_landing/streaming/

Table Name Prefix:
    01_bronze.

Bronze Layer Tips:
    - Focus on schema enforcement and metadata. This is your first line of defense 
      against bad data.
    - Use the variables from `variables.py` for paths and table names to keep your 
      code maintainable.
    - For a more interactive experience, run the `data_generator.py` script while 
      testing your pipeline. This will continuously generate new data, allowing you 
      to observe how each layer processes incoming records in real time. You can stop 
      and restart the generator as needed to see immediate effects in your tables and views.
"""

#######################################################################################
# -----------------------------------------
# 01 BRONZE: Raw Layer
# -----------------------------------------
#######################################################################################

"""
Task 1: Create `01_bronze.bronze_sales` Table
==============================================

Business Logic:
    Ingest raw sales transactions as a stream, enforce schema by casting columns to 
    correct types, and add metadata (`ingest_timestamp`, `source_file_path`) for 
    auditability. Fail the pipeline if any record is missing a `transaction_id`, 
    ensuring only valid sales events are processed downstream.

Requirements:
    [ ] Read as a stream from the `sales` directory - use the `RAW_SALES_PATH` variable.
    
    [ ] Add `comment`: "Raw sales data".
        Business logic: Comments help future maintainers understand the table's purpose. 
        Use clear, business-focused descriptions.
    
    [ ] Add `ingest_timestamp` and `source_file_path` columns.
        Business logic: These columns provide lineage and traceability, which are 
        critical for audits and debugging data issues.
    
    [ ] Cast all columns to their specified types (e.g., `transaction_id` to `int`, 
        `event_time` to `timestamp`, `unit_price` to `double`). 
        Use SynteticDataGenerator.md for information about available columns.
        Business logic: Enforcing types ensures consistency for analytics and prevents 
        silent errors. Think about what each type means for business logic 
        (e.g., `unit_price` as double for currency calculations).
    
    [ ] Set Failure Condition: The pipeline must fail if `transaction_id` is NULL.
        Business logic: A missing transaction ID means the sale cannot be tracked or 
        joined to other data. This is a critical business rule.

Tip:
    If stuck, see the solution below.
"""
### ---------------------
### Write Your Code Here ###


### ---------------------
### Solution Is Below 
# @dlt.table(name=BRONZE_SALES, comment = "Raw sales data")
# @dlt.expect_or_fail("valid_transaction_id", "transaction_id is not null")
# def bronze_sales():
#     return (
#         spark.readStream.format("delta").load(RAW_SALES_PATH)
#         .withColumn("ingest_timestamp", F.lit(datetime.now(timezone.utc)))
#         .withColumn("source_file_path", F.col("_metadata.file_path"))
#         .withColumn("transaction_id", F.col("transaction_id").cast("int"))
#         .withColumn("store_id", F.col("store_id").cast("int"))
#         .withColumn("event_time", F.col("event_time").cast("timestamp"))
#         .withColumn("customer_id", F.col("customer_id").cast("int"))
#         .withColumn("product_id", F.col("product_id").cast("int"))
#         .withColumn("quantity", F.col("quantity").cast("int"))
#         .withColumn("unit_price", F.col("unit_price").cast("double"))
#         .withColumn("total_amount", F.col("total_amount").cast("double"))
#         .withColumn("payment_method", F.col("payment_method").cast("string"))
#         .withColumn("discount_applied", F.col("discount_applied").cast("double"))
#         .withColumn("tax_amount", F.col("tax_amount").cast("double"))
#     )

##########################################################################################
##########################################################################################
##########################################################################################

"""
Task 2: Create `01_bronze.bronze_customers` Table
==================================================

Business Logic:
    Ingest raw customer records as a stream, enforce schema and add lineage metadata. 
    Fail the pipeline if any record is missing a `customer_id`, ensuring referential 
    integrity for all downstream customer analytics.

Requirements:
    [ ] Read as a stream from the `customers` directory - use the `RAW_CUSTOMERS_PATH` variable.
    
    [ ] Add `comment`: "Raw customers data from landing zone".
        Business logic: Use comments to clarify the source and business role of the table.
    
    [ ] Add `ingest_timestamp` and `source_file_path` columns.
        Business logic: Track when and where customer data was ingested for compliance 
        and troubleshooting.
    
    [ ] Cast all columns to their specified types.
        Use SynteticDataGenerator.md for information about available columns.
        Business logic: Proper types help with downstream joins and analytics. For example, 
        casting `age` to integer allows for age-based segmentation.
    
    [ ] Set Failure Condition: The pipeline must fail if `customer_id` is NULL.
        Business logic: Customer ID is the primary key for all customer analytics. 
        Missing IDs break referential integrity.

Tips:
    - Run the pipeline after this step to validate customer data ingestion and catch 
      schema or data quality issues early.
    - If stuck, see the solution below.
"""
### ---------------------
### Write Your Code Here ###


### ---------------------
### Solution Is Below 
# @dlt.table(name=BRONZE_CUSTOMERS, comment="Raw customers data from landing zone")
# @dlt.expect_or_fail("valid_customer_id", "customer_id is not null")
# def bronze_customers():
#     return (
#         spark.readStream.format("delta").load(RAW_CUSTOMERS_PATH)
#         .withColumn("ingest_timestamp", F.lit(datetime.now(timezone.utc)))
#         .withColumn("source_file_path", F.col("_metadata.file_path"))
#         .withColumn("customer_id", F.col("customer_id").cast("int"))
#         .withColumn("name", F.col("name").cast("string"))
#         .withColumn("email", F.col("email").cast("string"))
#         .withColumn("address", F.col("address").cast("string"))
#         .withColumn("join_date", F.col("join_date").cast("timestamp"))
#         .withColumn("loyalty_points", F.col("loyalty_points").cast("int"))
#         .withColumn("phone_number", F.col("phone_number").cast("string"))
#         .withColumn("age", F.col("age").cast("int"))
#         .withColumn("gender", F.col("gender").cast("string"))
#         .withColumn("last_update_time", F.col("last_update_time").cast("timestamp"))
#     )

##########################################################################################
##########################################################################################
##########################################################################################
"""
Task 3: Create `01_bronze.bronze_products` Table
=================================================

Business Logic:
    Ingest raw product records as a stream, enforce schema and add lineage metadata. 
    Fail the pipeline if any record is missing a `product_id`, ensuring all products 
    are uniquely identifiable for inventory and sales analytics.

Requirements:
    [ ] Read as a stream from the `items` directory - use the `RAW_PRODUCTS_PATH` variable.
    
    [ ] Add `comment`: "Raw products data".
        Business logic: Comments clarify the business role and source of the product data.
    
    [ ] Add `ingest_timestamp` and `source_file_path` columns.
        Business logic: These columns help track product data lineage and support troubleshooting.
    
    [ ] Cast all columns to their specified types.
        Use SynteticDataGenerator.md for information about available columns.
        Business logic: Type enforcement is key for analytics and reporting. For example, 
        casting `price` to double ensures correct calculations.
    
    [ ] Set Failure Condition: The pipeline must fail if `product_id` is NULL.
        Business logic: Product ID is required for all product-level analytics and joins.

Tips:
    - Run the pipeline after this step to check for product data issues and validate 
      schema enforcement.
    - If stuck, see the solution below.
"""
### ---------------------
### Write Your Code Here ###


### ---------------------
### Solution Is Below 
# @dlt.table(name=BRONZE_PRODUCTS, comment="Raw products data")
# @dlt.expect_or_fail("valid_product_id", "product_id is not null")
# def bronze_products():
#     return (
#         spark.readStream.format("delta").load(RAW_PRODUCTS_PATH)
#         .withColumn("ingest_timestamp", F.lit(datetime.now(timezone.utc)))
#         .withColumn("source_file_path", F.col("_metadata.file_path"))
#         .withColumn("product_id", F.col("product_id").cast("int"))
#         .withColumn("name", F.col("name").cast("string"))
#         .withColumn("category", F.col("category").cast("string"))
#         .withColumn("brand", F.col("brand").cast("string"))
#         .withColumn("price", F.col("price").cast("double"))
#         .withColumn("stock_quantity", F.col("stock_quantity").cast("int"))
#         .withColumn("size", F.col("size").cast("string"))
#         .withColumn("color", F.col("color").cast("string"))
#         .withColumn("description", F.col("description").cast("string"))
#         .withColumn("last_update_time", F.col("last_update_time").cast("timestamp"))
#     )

##########################################################################################
##########################################################################################
##########################################################################################

"""
Task 4: Create `01_bronze.bronze_stores` Table
===============================================

Business Logic:
    Ingest raw store records as a stream, enforce schema and add lineage metadata. 
    Fail the pipeline if any record is missing a `store_id`, ensuring all stores are 
    uniquely tracked for location-based analytics.

Requirements:
    [ ] Read as a stream from the `stores` directory - use the `RAW_STORES_PATH` variable.
    
    [ ] Add `comment`: "Raw stores data".
        Business logic: Comments help clarify the business context and source of the store data.
    
    [ ] Add `ingest_timestamp` and `source_file_path` columns.
        Business logic: Track store data lineage for audits and troubleshooting.
    
    [ ] Cast all columns to their specified types.
        Use SynteticDataGenerator.md for information about available columns.
        Business logic: Type enforcement ensures store data can be joined and analyzed reliably.
    
    [ ] Set Failure Condition: The pipeline must fail if `store_id` is NULL.
        Business logic: Store ID is the key for all store-level analytics and reporting.

Tips:
    - Run the pipeline after this step to confirm store data is ingested correctly and 
      matches business expectations.
    - If stuck, see the solution below.
"""
### ---------------------
### Write Your Code Here ###


### ---------------------
### Solution Is Below 
# @dlt.table(name=BRONZE_STORES, comment="Raw stores data")
# @dlt.expect_or_fail("valid_store_id", "store_id is not null")
# def bronze_stores():
#     return (
#         spark.readStream.format("delta").load(RAW_STORES_PATH)
#         .withColumn("ingest_timestamp", F.lit(datetime.now(timezone.utc)))
#         .withColumn("source_file_path", F.col("_metadata.file_path"))
#         .withColumn("store_id", F.col("store_id").cast("int"))
#         .withColumn("name", F.col("name").cast("string"))
#         .withColumn("address", F.col("address").cast("string"))
#         .withColumn("manager", F.col("manager").cast("string"))
#         .withColumn("open_date", F.col("open_date").cast("timestamp"))
#         .withColumn("status", F.col("status").cast("string"))
#         .withColumn("phone_number", F.col("phone_number").cast("string"))
#         .withColumn("last_update_time", F.col("last_update_time").cast("timestamp"))
#     )