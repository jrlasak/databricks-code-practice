from pyspark import pipelines as dp
from pyspark.sql import functions as F
from variables import *

"""
✅ Silver Layer: Sales and Returns Facts
========================================

Goal:
    Split the cleaned sales view from 02A into sales and returns fact tables; gold
    reads the sales one.
"""

# -----------------------------------------
# 02C SILVER: Sales and Returns Fact Tables
# -----------------------------------------

"""
Task 12: Create the `02_silver.silver_sales_transactions` Table
================================================================

Business Logic:
    Filter sales records to include only positive quantities (actual sales), and drop 
    records with invalid discounts or missing foreign keys.

Requirements:
    [ ] Read `sales_cleaned_stream` as a stream - use the `SALES_CLEANED_STREAM` variable -
        and add `comment`: "Stream sales table (positive quantity)".

    [ ] Filter to include only records where `quantity > 0`.
        Business logic: Negative or zero quantities are not valid sales.
    
    [ ] Apply Expectations:
        - Drop (`valid_discount`) if `discount_applied` is less than 0.
          Business logic: This is the sign error Task 8 warned about; here it is
          dropped, so no fact table carries it.
        
        - Drop (`valid_store_id`) if `store_id` is NULL.
          Business logic: Store ID is required for all store-level analytics.
        
        - Drop (`valid_customer_id`) if `customer_id` is NULL.
          Business logic: Customer ID is required for all customer-level analytics.
        
        - Drop (`valid_product_id`) if `product_id` is NULL.
          Business logic: Product ID is required for all product-level analytics.

Tip: if stuck, see the solution below.
"""
### ---------------------
### Write Your Code Here ###


### ---------------------
### Solution Is Below 
# @dp.table(name=SILVER_SALES_TRANSACTIONS, comment="Stream sales table (positive quantity)")
# @dp.expect_or_drop("valid_discount", "discount_applied >= 0")
# @dp.expect_or_drop("valid_store_id", "store_id IS NOT NULL")
# @dp.expect_or_drop("valid_customer_id", "customer_id IS NOT NULL")
# @dp.expect_or_drop("valid_product_id", "product_id IS NOT NULL")
# def silver_sales_transactions():
#     return (
#         spark.readStream.table(SALES_CLEANED_STREAM)
#         .filter(F.col("quantity") > 0)
#     )



##########################################################################################
##########################################################################################
##########################################################################################
"""
Task 13: Create the `02_silver.silver_returns_transactions` Table
==================================================================

Business Logic:
    Filter sales records to include only negative quantities (returns), transform to 
    absolute values for reporting, and drop records with invalid discounts or missing
    foreign keys. Returns are kept apart so finance can report sales and refunds
    separately; the gold tables in this lab report sales after discounts but before returns.

Requirements:
    [ ] Read `sales_cleaned_stream` as a stream - use the `SALES_CLEANED_STREAM` variable -
        and add `comment`: "Stream returns table (negative quantity)".
    
    [ ] Filter to include only records where `quantity < 0`.
        Business logic: Only negative quantities should be considered returns.
    
    [ ] Apply Transformations:
        - Create `returned_quantity` column using the absolute value of `quantity`.
          Business logic: A returned quantity reads as a positive count of units back.

        - Create `returned_amount` column using the absolute value of `total_amount`.
          Business logic: Refund totals sum as positive amounts.
          `tax_amount` is left as the source sends it (negative on a return); refunded
          tax is its negation.
        
        - Drop the original `quantity` and `total_amount` columns.
          Business logic: Removes ambiguity in reporting.
    
    [ ] Apply the same four "expect or drop" quality rules, with the same names, as the sales transactions table.
        Business logic: A return with a wrong-sign discount or a missing key would corrupt
        refund totals just as it would sales.

Tips:
    - Run the pipeline after this step to validate returns processing and expectations.
    - If stuck, see the solution below.
"""
### ---------------------
### Write Your Code Here ###


### ---------------------
### Solution Is Below 
# @dp.table(name=SILVER_RETURNS_TRANSACTIONS, comment="Stream returns table (negative quantity)")
# @dp.expect_or_drop("valid_discount", "discount_applied >= 0")
# @dp.expect_or_drop("valid_store_id", "store_id IS NOT NULL")
# @dp.expect_or_drop("valid_customer_id", "customer_id IS NOT NULL")
# @dp.expect_or_drop("valid_product_id", "product_id IS NOT NULL")
# def silver_returns_transactions():
#     return (
#         spark.readStream.table(SALES_CLEANED_STREAM)
#         .filter(F.col("quantity") < 0)
#         .withColumn("returned_quantity", F.abs(F.col("quantity")))
#         .withColumn("returned_amount", F.abs(F.col("total_amount")))
#         .drop("quantity", "total_amount")
#     )

##########################################################################################
# Done with this file? Run a pipeline update, do the Silver facts checks and the sales
# part of the Silver data quality checks in ProjectPlan.md > Check your work, then open
# `02D_silver.py`.
##########################################################################################