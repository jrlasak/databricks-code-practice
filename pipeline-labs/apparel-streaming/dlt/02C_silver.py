from variables import *
import dlt
from datetime import datetime, timezone
from pyspark.sql import functions as F
from variables import *

# -----------------------------------------
# 02C SILVER: Current State Views for Dimension Lookup
# (using __END_AT IS NULL for current, since create_auto_cdc_flow does not use _dlt_current)
# -----------------------------------------

"""
Task 12: Create the `02_silver.silver_sales_transactions` Table
================================================================

Business Logic:
    Filter sales records to include only positive quantities (actual sales), and drop 
    records with invalid discounts or missing foreign keys. This ensures only valid 
    sales transactions are available for analytics.

Requirements:
    [ ] Read from the `live.sales_cleaned_stream`.
    
    [ ] Filter to include only records where `quantity > 0`.
        Business logic: Negative or zero quantities are not valid sales.
    
    [ ] Apply Expectations:
        - Drop if `discount_applied` is less than 0.
          Business logic: Negative discounts are not possible in real sales.
        
        - Drop if `store_id` is NULL.
          Business logic: Store ID is required for all store-level analytics.
        
        - Drop if `customer_id` is NULL.
          Business logic: Customer ID is required for all customer-level analytics.
        
        - Drop if `product_id` is NULL.
          Business logic: Product ID is required for all product-level analytics.

Tip:
    - If stuck, see the solution below.
"""
### ---------------------
### Write Your Code Here ###


### ---------------------
### Solution Is Below 
# @dlt.table(name=SILVER_SALES_TRANSACTIONS, comment="Stream sales table (positive quantity)")
# @dlt.expect_or_drop("valid_discount", "discount_applied >= 0")
# @dlt.expect_or_drop("valid_store_id", "store_id IS NOT NULL")
# @dlt.expect_or_drop("valid_customer_id", "customer_id IS NOT NULL")
# @dlt.expect_or_drop("valid_product_id", "product_id IS NOT NULL")
# def silver_sales_transactions():
#     return (
#         dlt.read_stream(f"live.{SALES_CLEANED_STREAM}")
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
    foreign keys. This ensures returns are tracked separately and accurately.

Requirements:
    [ ] Read from the `live.sales_cleaned_stream`.
    
    [ ] Filter to include only records where `quantity < 0`.
        Business logic: Only negative quantities should be considered returns.
    
    [ ] Apply Transformations:
        - Create `returned_quantity` column using the absolute value of `quantity`.
          Business logic: Absolute value makes reporting and analysis easier.
        
        - Create `returned_amount` column using the absolute value of `total_amount`.
          Business logic: Absolute value makes reporting and analysis easier.
        
        - Drop the original `quantity` and `total_amount` columns.
          Business logic: Removes ambiguity in reporting.
    
    [ ] Apply the same four "expect or drop" quality rules as the sales transactions table.
        Business logic: Ensures returns data is as clean as sales data.

Tips:
    - Run the pipeline after this step to validate returns processing and expectations.
    - If stuck, see the solution below.
"""
### ---------------------
### Write Your Code Here ###


### ---------------------
### Solution Is Below 
# @dlt.table(name=SILVER_RETURNS_TRANSACTIONS, comment="Stream returns table (negative quantity)")
# @dlt.expect_or_drop("valid_discount", "discount_applied >= 0")
# @dlt.expect_or_drop("valid_store_id", "store_id IS NOT NULL")
# @dlt.expect_or_drop("valid_customer_id", "customer_id IS NOT NULL")
# @dlt.expect_or_drop("valid_product_id", "product_id IS NOT NULL")
# def silver_returns_transactions():
#     return (
#         dlt.read_stream(f"live.{SALES_CLEANED_STREAM}")
#         .filter(F.col("quantity") < 0)
#         .withColumn("returned_quantity", F.abs(F.col("quantity")))
#         .withColumn("returned_amount", F.abs(F.col("total_amount")))
#         .drop("quantity", "total_amount")
#     )