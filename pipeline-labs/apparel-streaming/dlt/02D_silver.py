from pyspark import pipelines as dp
from pyspark.sql import functions as F
from variables import *

"""
✅ Silver Layer: Current Dimension Views
========================================

Goal:
    Expose the current version of each SCD2 dimension from 02B as a lookup for the
    gold joins. Declare each with `@dp.temporary_view(name=...)`, as in 02A.
"""

# -----------------------------------------
# 02D SILVER: Create "Current" Dimension Views
# (current version of a key = the row where __END_AT IS NULL)
# -----------------------------------------

"""
Task 14: Create `silver_customers_current` View
================================================

Business Logic:
    Filter SCD2 customer table for current records (no end date), providing the latest 
    snapshot for analytics and lookups.

Requirements:
    [ ] Read `02_silver.silver_customers` as a batch source (`spark.read.table`) - use
        the `SILVER_CUSTOMERS` variable.

    [ ] Add `comment`: "Current customers (SCD2)".

    [ ] Filter for records where the `__END_AT` column is NULL.
        Business logic: Only records without an end date are considered current.

Tip: if stuck, see the solution below.
"""
### ---------------------
### Write Your Code Here ###


### ---------------------
### Solution Is Below 
# @dp.temporary_view(
#     name=SILVER_CUSTOMERS_CURRENT,
#     comment="Current customers (SCD2)"
# )
# def silver_customers_current():
#     return spark.read.table(SILVER_CUSTOMERS).filter(F.col("__END_AT").isNull())


##########################################################################################
##########################################################################################
##########################################################################################
"""
Task 15: Create `silver_products_current` View
===============================================

Business Logic:
    Filter SCD2 product table for current records, ensuring analytics use the latest 
    product attributes.

Requirements:
    [ ] Read `02_silver.silver_products` as a batch source (`spark.read.table`) - use
        the `SILVER_PRODUCTS` variable.

    [ ] Add `comment`: "Current products (SCD2)".

    [ ] Filter for records where the `__END_AT` column is NULL.
        Business logic: Only records without an end date are considered current.

Tips:
    - Run the pipeline after this step to check it succeeds; gold reads this view
      from Task 17 on.
    - If stuck, see the solution below.
"""
### ---------------------
### Write Your Code Here ###


### ---------------------
### Solution Is Below 
# @dp.temporary_view(
#     name=SILVER_PRODUCTS_CURRENT,
#     comment="Current products (SCD2)"
# )
# def silver_products_current():
#     return spark.read.table(SILVER_PRODUCTS).filter(F.col("__END_AT").isNull())


##########################################################################################
##########################################################################################
##########################################################################################
"""
Task 16: Create `silver_stores_current` View
=============================================

Business Logic:
    Filter SCD2 store table for current records, ensuring analytics use the latest 
    store attributes.

Requirements:
    [ ] Read `02_silver.silver_stores` as a batch source (`spark.read.table`) - use
        the `SILVER_STORES` variable.

    [ ] Add `comment`: "Current stores (SCD2)".

    [ ] Filter for records where the `__END_AT` column is NULL.
        Business logic: Only records without an end date are considered current.

Tips:
    - Run the pipeline after this step to check it succeeds; gold reads this view
      from Task 17 on.
    - If stuck, see the solution below.
"""
### ---------------------
### Write Your Code Here ###


### ---------------------
### Solution Is Below 
# @dp.temporary_view(
#     name=SILVER_STORES_CURRENT,
#     comment="Current stores (SCD2)"
# )
# def silver_stores_current():
#     return spark.read.table(SILVER_STORES).filter(F.col("__END_AT").isNull())

##########################################################################################
# Done with this file? Run a pipeline update, do the Silver current views checks in
# ProjectPlan.md > Check your work, then open `03_gold.py`.
##########################################################################################
