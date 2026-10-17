from pyspark import pipelines as dp
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
    - Where a task asks for a `comment`, use the exact text it gives: a short,
      business-facing description. On a published table it shows in Catalog Explorer,
      so the next engineer knows what the table holds without reading the code.
    - Pipeline code uses `from pyspark import pipelines as dp`: `@dp.table` for streaming
      tables, `@dp.materialized_view` for batch aggregates, `@dp.temporary_view` for
      pipeline-only views and `@dp.expect*` for expectations. (The older `import dlt`
      module still works, but Databricks recommends `pyspark.pipelines`.)
    - To watch increments, keep `data_generator.py` running and start another pipeline
      update: each update processes only the data that landed since the previous one.
      Stop the generator when you are done.
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
    auditability. Fail the update if any record is missing a `transaction_id`.

Requirements:
    [ ] Read the landing folder as a Delta stream (the generator writes each folder as a
        Delta table, not as JSON or CSV files) - use the `RAW_SALES_PATH` variable.

    [ ] Add `comment`: "Raw sales data".

    [ ] Add `ingest_timestamp` (the time the micro-batch is processed,
        `current_timestamp()`) and `source_file_path` (`_metadata.file_path`) columns.
        Business logic: When a bad row shows up in silver, `source_file_path` tells you
        which landing file to open and `ingest_timestamp` tells you which update loaded it.

    [ ] Cast all columns to their specified types (e.g., `transaction_id` to `int`,
        `event_time` to `timestamp`, `unit_price` to `double`).
        The target type of every column is in data_contract.md > Bronze.
        Business logic: Every later layer joins and filters on these types
        (e.g., `unit_price` as double so price arithmetic works here; in production,
        money columns are usually `DECIMAL(10,2)` because double cannot store cents exactly).
    
    [ ] Set Failure Condition (`valid_transaction_id`): The pipeline must fail if `transaction_id` is NULL.
        Business logic: A missing transaction ID means the sale cannot be tracked or 
        joined to other data.

Tip: if stuck, see the solution below.
"""
### ---------------------
### Write Your Code Here ###


### ---------------------
### Solution Is Below 
# @dp.table(name=BRONZE_SALES, comment = "Raw sales data")
# @dp.expect_or_fail("valid_transaction_id", "transaction_id is not null")
# def bronze_sales():
#     return (
#         spark.readStream.format("delta").load(RAW_SALES_PATH)
#         .withColumn("ingest_timestamp", F.current_timestamp())
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
    Fail the update if any record is missing a `customer_id`.

Requirements:
    [ ] Read the landing folder as a Delta stream, as in Task 1 - use the `RAW_CUSTOMERS_PATH` variable.
    
    [ ] Add `comment`: "Raw customers data from landing zone".
    
    [ ] Add `ingest_timestamp` and `source_file_path` columns.
        Business logic: Same lineage columns as Task 1.
    
    [ ] Cast all columns to their specified types.
        The target type of every column is in data_contract.md > Bronze.
        Business logic: These types are the contract every later layer joins and filters
        on: int `customer_id` and `age`, timestamp `join_date` and `last_update_time`.
    
    [ ] Set Failure Condition (`valid_customer_id`): The pipeline must fail if `customer_id` is NULL.
        Business logic: The SCD2 key in silver and every customer join in gold depend on this ID.

Tips:
    - Run the pipeline after this step to validate customer data ingestion and catch 
      schema or data quality issues early.
    - If stuck, see the solution below.
"""
### ---------------------
### Write Your Code Here ###


### ---------------------
### Solution Is Below 
# @dp.table(name=BRONZE_CUSTOMERS, comment="Raw customers data from landing zone")
# @dp.expect_or_fail("valid_customer_id", "customer_id is not null")
# def bronze_customers():
#     return (
#         spark.readStream.format("delta").load(RAW_CUSTOMERS_PATH)
#         .withColumn("ingest_timestamp", F.current_timestamp())
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
    Fail the update if any record is missing a `product_id`.

Requirements:
    [ ] Read the landing folder as a Delta stream, as in Task 1 - use the `RAW_PRODUCTS_PATH` variable.
    
    [ ] Add `comment`: "Raw products data".
    
    [ ] Add `ingest_timestamp` and `source_file_path` columns.
        Business logic: Same lineage columns as Task 1.
    
    [ ] Cast all columns to their specified types.
        The target type of every column is in data_contract.md > Bronze.
        Business logic: These types are the contract later layers filter on: `price` as
        double for the 0-1,000 price rule in Task 6 (DECIMAL in production, see Task 1),
        `stock_quantity` as int for the stock rule.
    
    [ ] Set Failure Condition (`valid_product_id`): The pipeline must fail if `product_id` is NULL.
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
# @dp.table(name=BRONZE_PRODUCTS, comment="Raw products data")
# @dp.expect_or_fail("valid_product_id", "product_id is not null")
# def bronze_products():
#     return (
#         spark.readStream.format("delta").load(RAW_PRODUCTS_PATH)
#         .withColumn("ingest_timestamp", F.current_timestamp())
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
    Fail the update if any record is missing a `store_id`.

Requirements:
    [ ] Read the landing folder as a Delta stream, as in Task 1 - use the `RAW_STORES_PATH` variable.
    
    [ ] Add `comment`: "Raw stores data".
    
    [ ] Add `ingest_timestamp` and `source_file_path` columns.
        Business logic: Same lineage columns as Task 1.
    
    [ ] Cast all columns to their specified types.
        The target type of every column is in data_contract.md > Bronze.
        Business logic: As in Task 2: int `store_id`, timestamp `open_date` and
        `last_update_time`.
    
    [ ] Set Failure Condition (`valid_store_id`): The pipeline must fail if `store_id` is NULL.
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
# @dp.table(name=BRONZE_STORES, comment="Raw stores data")
# @dp.expect_or_fail("valid_store_id", "store_id is not null")
# def bronze_stores():
#     return (
#         spark.readStream.format("delta").load(RAW_STORES_PATH)
#         .withColumn("ingest_timestamp", F.current_timestamp())
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

##########################################################################################
# Done with this file? Run a pipeline update, do the Bronze checks in
# ProjectPlan.md > Check your work, then open `02A_silver.py`.
##########################################################################################