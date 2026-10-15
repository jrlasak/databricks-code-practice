from pyspark import pipelines as dp
from pyspark.sql import functions as F
from variables import *

"""
✅ Silver Layer: Cleansed & Conformed Data
==========================================

Goal:
    Apply incremental data quality rules, cleanse and conform records, and prepare 
    for historical tracking and analytics. This layer improves data reliability and 
    prepares dimensions and facts for business use.

Table Name Prefix:
    02_silver.

Silver Layer Tips:
    - Apply expectations to catch and handle dirty data. Use views for intermediate
      cleansing and tables for historical tracking.
    - Declare the four views in this file with `@dp.temporary_view`: they exist only
      inside the pipeline run and are not published to Unity Catalog.
    - Think about how CDC (Change Data Capture) and SCD2 (Slowly Changing Dimension
      Type 2) work in practice.
    - To watch increments, keep `data_generator.py` running and start another pipeline
      update: each update processes only the data that landed since the previous one.
      Stop the generator when you are done.
"""
# -----------------------------------------
# 02A SILVER: Cleaned Streams (Intermediate Views)
# -----------------------------------------


"""
Task 5: Create the `customers_cleaned_stream` View
===================================================

Business Logic:
    Cleanse customer data by enforcing valid IDs, and flag invalid emails, outlier ages,
    negative loyalty points, and invalid gender values. Only drop records with missing
    IDs. This ensures only usable customer records are tracked, while allowing for
    business review of questionable but not fatal data issues.

Requirements:
    [ ] Read the bronze customers stream with `spark.readStream.table` - use the
        `BRONZE_CUSTOMERS` variable (it holds the schema-qualified name
        `01_bronze.bronze_customers`; a bare `bronze_customers` resolves to the
        pipeline's default schema `02_silver` and is not found).

    [ ] Add `comment`: "QC for customers stream".
        Business logic: Documenting the purpose of this view helps future users 
        understand its role in the pipeline.
    
    [ ] Apply Expectations:
        - Drop (`valid_customer_id`) if `customer_id` is NULL.
          Business logic: No customer ID means the record can't be used for analytics 
          or marketing.
        
        - Warn (`valid_email`) if `email` is not NULL and does not match the pattern `%@%.%`.
          Business logic: Invalid emails impact communication and customer engagement.
        
        - Warn (`realistic_age`) if `age` is not between 18 and 100.
          Business logic: Age outliers may indicate data entry errors or special cases.
        
        - Warn (`realistic_loyalty_points`) if `loyalty_points` are less than 0.
          Business logic: Negative loyalty points are not possible in a real program.
        
        - Warn (`valid_gender`) if `gender` is not one of ('Male', 'Female', 'Other').
          Business logic: A 3-value allow-list catches legacy till codes ('M', 'F') and
          blanks that would otherwise split gender reports into extra groups.

Tip:
    - If stuck, see the solution below.
"""
### ---------------------
### Write Your Code Here ###


### ---------------------
### Solution Is Below 
# @dp.temporary_view(name=CUSTOMERS_CLEANED_STREAM, comment="QC for customers stream")
# @dp.expect_or_drop("valid_customer_id", "customer_id IS NOT NULL")
# @dp.expect("valid_email", "email IS NULL OR email LIKE '%@%.%'")
# @dp.expect("realistic_age", "age >= 18 AND age <= 100")
# @dp.expect("realistic_loyalty_points", "loyalty_points >= 0")
# @dp.expect("valid_gender", "gender IN ('Male', 'Female', 'Other')")
# def customers_cleaned_stream():
#     return spark.readStream.table(BRONZE_CUSTOMERS)

##########################################################################################
##########################################################################################
##########################################################################################
"""
Task 6: Create the `products_cleaned_stream` View
==================================================

Business Logic:
    Cleanse product data by enforcing valid IDs and realistic prices, and warn on 
    negative stock, invalid categories, or missing brands. Only drop records with 
    missing IDs or out-of-range prices, ensuring inventory and sales analytics are 
    based on trustworthy product records.

Requirements:
    [ ] Read the bronze products stream - use the `BRONZE_PRODUCTS` variable (the
        schema-qualified name `01_bronze.bronze_products`).
    
    [ ] Add `comment`: "QC for products stream".
        Business logic: Comments clarify the business role of the cleaned product view.
    
    [ ] Apply Expectations:
        - Drop (`valid_product_id`) if `product_id` is NULL.
          Business logic: Product ID is required for all product-level analytics.
        
        - Keep only rows where 0 < `price` < 1000 (`valid_product_price`, drop the rest).
          Business logic: Nothing in this catalog costs over $1,000; a higher price is
          almost always cents typed as dollars. Dropping a dimension row has a cost: a
          new product dropped here never reaches `silver_products`, so
          `gold_product_performance` shows its name as NULL until the source sends a
          corrected record, and the `denormalized_sales_facts` rows already joined keep
          a NULL `product_name` for good (Task 17). That is why only the price, which
          would corrupt revenue, is a drop rule.

        - Warn (`non_negative_stock`) if `stock_quantity` is less than 0.
          Business logic: Negative stock is not possible in a real inventory system.
        
        - Warn (`valid_category`) if `category` is not one of ('Casual Wear', 'Formal Wear', 'Sportswear', 
          'Accessories', 'Footwear').
          Business logic: Ensures products are categorized for reporting and merchandising.
        
        - Warn (`valid_brand`) if `brand` is NULL.
          Business logic: Brand is important for marketing and product analysis.

Tips:
    - Run the pipeline after this step to check it succeeds; the cleaned and flagged
      counts show once `02B_silver.py` reads this view.
    - If stuck, see the solution below.
"""
### ---------------------
### Write Your Code Here ###


### ---------------------
### Solution Is Below 
# @dp.temporary_view(name=PRODUCTS_CLEANED_STREAM, comment="QC for products stream")
# @dp.expect_or_drop("valid_product_id", "product_id IS NOT NULL")
# @dp.expect_or_drop("valid_product_price", "price > 0 AND price < 1000")
# @dp.expect("non_negative_stock", "stock_quantity >= 0")
# @dp.expect("valid_category", "category IN ('Casual Wear', 'Formal Wear', 'Sportswear', 'Accessories', 'Footwear')")
# @dp.expect("valid_brand", "brand IS NOT NULL")
# def products_cleaned_stream():
#     return spark.readStream.table(BRONZE_PRODUCTS)

##########################################################################################
##########################################################################################
##########################################################################################
"""
Task 7: Create the `stores_cleaned_stream` View
================================================

Business Logic:
    Cleanse store data by enforcing valid IDs, and replace missing manager names with
    'Unknown' for reporting consistency. Flag vacant manager positions and invalid
    status, but only drop records with missing IDs. This ensures store-level
    analytics are reliable.

Requirements:
    [ ] Read the bronze stores stream - use the `BRONZE_STORES` variable (the
        schema-qualified name `01_bronze.bronze_stores`).

    [ ] Add `comment`: "QC for stores stream".
        Business logic: Comments clarify the business context of the cleaned store view.

    [ ] Apply Transformation: Replace NULL values in the `manager` column with the string 'Unknown'.
        Business logic: Missing manager names can be handled gracefully for reporting.
        The fill also turns a vacancy into a tracked change: `ignore_null_updates` in
        Task 11 never sees a NULL here, so a vacancy opens a new SCD2 version on purpose
        (a vacant position is a business event).

    [ ] Apply Expectations (post-transformation):
        - Drop (`valid_store_id`) if `store_id` is NULL.
          Business logic: Store ID is required for all store-level analytics.

        - Warn (`manager_assigned`) if `manager` is 'Unknown' (the value you just filled in for a NULL).
          Business logic: The expectation sees the view's output, after the fill, so it
          counts vacant positions by the default value; a `manager IS NOT NULL` check
          here could never fail. A vacant manager position is an accountability gap the
          operations team must see in the data quality metrics.
        
        - Warn (`valid_status`) if `status` is not one of ('Open', 'Under Renovation').
          Business logic: Store status affects operational reporting and analysis.

Tips:
    - Run the pipeline after this step to check it succeeds; the store data quality
      counts show once `02B_silver.py` reads this view.
    - If stuck, see the solution below.
"""
### ---------------------
### Write Your Code Here ###


### ---------------------
### Solution Is Below 
# @dp.temporary_view(name=STORES_CLEANED_STREAM, comment="QC for stores stream")
# @dp.expect_or_drop("valid_store_id", "store_id IS NOT NULL")
# @dp.expect("manager_assigned", "manager <> 'Unknown'")
# @dp.expect("valid_status", "status IN ('Open', 'Under Renovation')")
# def stores_cleaned_stream():
#     return spark.readStream.table(BRONZE_STORES).withColumn("manager", F.coalesce(F.col("manager"), F.lit("Unknown")))


##########################################################################################
##########################################################################################
##########################################################################################
"""
Task 8: Create the `sales_cleaned_stream` View
===============================================

Business Logic:
    Cleanse sales data by flagging invalid payment methods and negative discounts (the
    sales and returns tables drop the negative discounts in Tasks 12-13), and add a
    derived date column for time-based analytics. Declare a watermark so later
    stateful operators on this stream have a lateness bound.

Requirements:
    [ ] Read the bronze sales stream - use the `BRONZE_SALES` variable (the
        schema-qualified name `01_bronze.bronze_sales`).

    [ ] Add `comment`: "QC for sales stream (fact)".
        Business logic: Comments clarify the business role of the cleaned sales view.

    [ ] Apply Transformation: Add a `date` column by casting `event_time` to a date type.
        Business logic: Date columns are essential for time-based reporting and analysis.
        Gold carries it through as `sale_date` (Tasks 17-18).

    [ ] Apply Watermarking: Set a 10-minute watermark on the `event_time` column.
        Business logic: A watermark bounds how late an event may be for STATEFUL
        streaming operators (windowed aggregations, dropDuplicates, stream-stream joins).
        Everything downstream here is stateless or a materialized view, so in this
        pipeline it changes no output; it is declared so that a streaming aggregation
        added on this view later has a lateness bound. The generator delays events by up
        to 60 s (`LATENCY_MAX_S`), so 10 minutes leaves headroom.
    
    [ ] Apply Expectations:
        - Warn (`valid_payment_method`) if `payment_method` is not one of ('Cash', 'Credit Card', 'Debit Card', 
          'Mobile Pay', 'Gift Card').
          Business logic: Invalid payment methods may indicate data entry errors or fraud.
        
        - Warn (`realistic_discount`) if `discount_applied` is less than 0.
          Business logic: Negative discounts are not possible in real sales.

Tips:
    - Run the pipeline after this step to check it succeeds; the cleaned and flagged
      counts show once `02C_silver.py` reads this view.
    - If stuck, see the solution below.
"""
### ---------------------
### Write Your Code Here ###


### ---------------------
### Solution Is Below 
# @dp.temporary_view(name=SALES_CLEANED_STREAM, comment="QC for sales stream (fact)")
# @dp.expect("valid_payment_method", "payment_method IN ('Cash', 'Credit Card', 'Debit Card', 'Mobile Pay', 'Gift Card')")
# @dp.expect("realistic_discount", "discount_applied >= 0")
# def sales_cleaned_stream():
#     return (
#         spark.readStream.table(BRONZE_SALES)
#         .withColumn("date", F.col("event_time").cast("date"))
#         .withWatermark("event_time", "10 minutes")
#         # Data delay is controlled by LATENCY_MAX_S in data generator
#     )

##########################################################################################
# Done with this file? Run a pipeline update (it should succeed; the views are checked
# once 02B and 02C read them), then open `02B_silver.py`.
##########################################################################################