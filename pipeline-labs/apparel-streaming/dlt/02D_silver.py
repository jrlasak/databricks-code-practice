from variables import *
import dlt
from datetime import datetime, timezone
from pyspark.sql import functions as F
from variables import *

# -----------------------------------------
# 02D SILVER: Create "Current" Dimension Views
# -----------------------------------------

"""
Task 14: Create `silver_customers_current` View
================================================

Business Logic:
    Filter SCD2 customer table for current records (no end date), providing the latest 
    snapshot for analytics and lookups.

Requirements:
    [ ] Read from `02_silver.silver_customers`.
    
    [ ] Filter for records where the `__END_AT` column is NULL.
        Business logic: Only records without an end date are considered current.

Tip:
    - If stuck, see the solution below.
"""
### ---------------------
### Write Your Code Here ###


### ---------------------
### Solution Is Below 
# @dlt.view(
#     name=SILVER_CUSTOMERS_CURRENT,
#     comment="Current customers (SCD2)"
# )
# def silver_customers_current():
#     return dlt.read(SILVER_CUSTOMERS).filter(F.col("__END_AT").isNull())


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
    [ ] Read from `02_silver.silver_products`.
    
    [ ] Filter for records where the `__END_AT` column is NULL.
        Business logic: Only records without an end date are considered current.

Tips:
    - Run the pipeline after this step to validate current product view logic.
    - If stuck, see the solution below.
"""
### ---------------------
### Write Your Code Here ###


### ---------------------
### Solution Is Below 
# @dlt.view(
#     name=SILVER_PRODUCTS_CURRENT,
#     comment="Current products (SCD2)"
# )
# def silver_products_current():
#     return dlt.read(SILVER_PRODUCTS).filter(F.col("__END_AT").isNull())


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
    [ ] Read from `02_silver.silver_stores`.
    
    [ ] Filter for records where the `__END_AT` column is NULL.
        Business logic: Only records without an end date are considered current.

Tips:
    - Run the pipeline after this step to validate current store view logic.
    - If stuck, see the solution below.
"""
### ---------------------
### Write Your Code Here ###


### ---------------------
### Solution Is Below 
# @dlt.view(
#     name=SILVER_STORES_CURRENT,
#     comment="Current stores (SCD2)"
# )
# def silver_stores_current():
#     return dlt.read(SILVER_STORES).filter(F.col("__END_AT").isNull())
