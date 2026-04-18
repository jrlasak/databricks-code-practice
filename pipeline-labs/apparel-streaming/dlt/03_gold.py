from variables import *
import dlt
from datetime import datetime, timezone
from pyspark.sql import functions as F
from variables import *

# -----------------------------------------
# 03 GOLD: Denormalized Analytics and Aggregates
# -----------------------------------------

"""
✅ Gold Layer: Business-Ready Analytics
========================================

Goal:
    Create denormalized and aggregated tables for direct use by analysts and BI tools. 
    This layer delivers business value by joining, enriching, and summarizing data for 
    reporting and decision-making.

Table Name Prefix:
    03_gold.

Gold Layer Tips:
    - Focus on joins and aggregations. This is where you create business value from your data.
    - Think about how analysts will use these tables for reporting and BI.
    - For a more interactive experience, run the `data_generator.py` script while 
      testing your pipeline. This will continuously generate new data, allowing you 
      to observe how each layer processes incoming records in real time. You can stop 
      and restart the generator as needed to see immediate effects in your tables and views.
"""

"""
Task 17: Create `03_gold.denormalized_sales_facts` Streaming Table
===================================================================

Business Logic:
    Join sales transactions with current dimension tables to enrich each sale with 
    customer, product, and store attributes. Use left joins to ensure all sales are 
    included, even if some dimension data is missing. Alias columns for clarity and 
    usability in BI tools.

Requirements:
    [ ] Read as a stream from `LIVE.02_silver.silver_sales_transactions`.
    
    [ ] Read `LIVE.silver_customers_current`, `LIVE.silver_products_current`, and 
        `LIVE.silver_stores_current` as static/lookup tables.
        Business logic: Lookup tables provide the latest dimension attributes for each sale.
    
    [ ] Join Logic: `left` join the sales stream with stores, then customers, then products 
        on their respective IDs.
        Business logic: Left joins ensure all sales are included, even if some dimension 
        data is missing.
    
    [ ] Select & Alias Columns: Construct the final schema with aliased columns like 
        `customer_name`, `product_name`, and `store_name`.
        Business logic: Aliased columns make reporting and analysis easier for business users.

Tip:
    - If stuck, see the solution below.
"""
### ---------------------
### Write Your Code Here ###


### ---------------------
### Solution Is Below 

# @dlt.table(
#     name=GOLD_DENORMALIZED_SALES_FACTS,
#     comment="Fully denormalized sales fact table for analytic use. A streaming table."
# )
# def denormalized_sales_facts():
#     sales = dlt.read_stream(f"LIVE.{SILVER_SALES_TRANSACTIONS}")
#     customers = dlt.read(f"LIVE.{SILVER_CUSTOMERS_CURRENT}")
#     products = dlt.read(f"LIVE.{SILVER_PRODUCTS_CURRENT}")
#     stores = dlt.read(f"LIVE.{SILVER_STORES_CURRENT}")
    
#     df = (
#         sales
#         .join(stores, "store_id", "left")
#         .join(customers, "customer_id", "left")
#         .join(products, "product_id", "left")
#         .select(
#             sales.transaction_id,
#             sales.event_time,
#             sales.customer_id,
#             customers.name.alias("customer_name"),
#             sales.product_id,
#             products.name.alias("product_name"),
#             products.category.alias("product_category"),
#             sales.store_id,
#             stores.name.alias("store_name"),
#             sales.quantity,
#             sales.unit_price,
#             sales.total_amount,
#             sales.payment_method,
#             sales.discount_applied,
#             sales.tax_amount,
#         )
#     )
#     return df


##########################################################################################
##########################################################################################
##########################################################################################
"""
Task 18: Create `03_gold.gold_daily_sales_by_store` Aggregate Table
====================================================================

Business Logic:
    Aggregate denormalized sales facts by store and day, calculating total revenue, 
    transaction count, items sold, and unique customers. This enables daily performance 
    tracking for each store.

Requirements:
    [ ] Read from `LIVE.03_gold.denormalized_sales_facts`.
    
    [ ] Group by:
        - A `1 day` window on `event_time`.
          Business logic: Daily windows align with business reporting cycles.
        
        - `store_id`
          Business logic: Store-level grouping enables location-based analysis.
        
        - `store_name`
          Business logic: Store names make reports more readable.
    
    [ ] Calculate Aggregations:
        - `total_revenue`: `round(sum(total_amount), 2)`
          Business logic: Total revenue is a primary business KPI.
        
        - `total_transactions`: `count(transaction_id)`
          Business logic: Transaction count helps track store activity.
        
        - `total_items_sold`: `sum(quantity)`
          Business logic: Items sold is important for inventory and sales analysis.
        
        - `unique_customers`: `countDistinct(customer_id)`
          Business logic: Unique customers help measure store reach and loyalty.
    
    [ ] Final Select: Choose the grouping columns and aggregated metrics, casting the 
        window start time to a `sale_date`.
        Business logic: Sale date is essential for time-based reporting.

Tips:
    - Run the pipeline after this step to validate aggregations and reporting logic.
    - If stuck, see the solution below.
"""
### ---------------------
### Write Your Code Here ###


### ---------------------
### Solution Is Below 
# @dlt.table(
#     name=GOLD_DAILY_SALES_BY_STORE,
#     comment="Daily store sales summary. A batch table."
# )
# def gold_daily_sales_by_store():
#     df = dlt.read(f"LIVE.{GOLD_DENORMALIZED_SALES_FACTS}")
#     agg = (
#         df.groupBy(
#                 F.window("event_time", "1 day").alias("sale_window"),
#                 "store_id",
#                 "store_name"
#             )
#             .agg(
#                 F.round(F.sum("total_amount"), 2).alias("total_revenue"),
#                 F.count("transaction_id").alias("total_transactions"),
#                 F.sum("quantity").alias("total_items_sold"),
#                 F.countDistinct("customer_id").alias("unique_customers")
#             )
#             .select(
#                 F.col("sale_window.start").cast("date").alias("sale_date"),
#                 "store_id",
#                 "store_name",
#                 "total_revenue",
#                 "total_transactions",
#                 "total_items_sold",
#                 "unique_customers"
#             )
#     )
#     return agg



##########################################################################################
##########################################################################################
##########################################################################################
"""
Task 19: Create `03_gold.gold_product_performance` Aggregate Table
===================================================================

Business Logic:
    Product performance metrics drive merchandising, inventory, and marketing decisions.

Requirements:
    [ ] Read from `LIVE.03_gold.denormalized_sales_facts`.
    
    [ ] Group by: `product_id`, `product_name`, `product_category`.
        Business logic: Grouping by product attributes enables detailed analysis.
    
    [ ] Calculate Aggregations:
        - `total_revenue`: `round(sum(total_amount), 2)`
          Business logic: Revenue by product is key for profitability analysis.
        
        - `total_quantity_sold`: `round(sum(quantity), 2)`
          Business logic: Quantity sold helps with inventory planning.
        
        - `total_orders`: `count(transaction_id)`
          Business logic: Order count is useful for demand forecasting.

Tips:
    - Run the pipeline after this step to validate product performance metrics and aggregations.
    - If stuck, see the solution below.
"""

### ---------------------
### Write Your Code Here ###


### ---------------------
### Solution Is Below 
# @dlt.table(
#     name=GOLD_PRODUCT_PERFORMANCE,
#     comment="Product sales performance metrics. A batch table."
# )
# def gold_product_performance():
#     df = dlt.read(f"LIVE.{GOLD_DENORMALIZED_SALES_FACTS}")
#     agg = (
#         df.groupBy("product_id", "product_name", "product_category")
#           .agg(
#               F.round(F.sum("total_amount"),2).alias("total_revenue"),
#               F.round(F.sum("quantity"), 2).alias("total_quantity_sold"),
#               F.count("transaction_id").alias("total_orders")
#           )
#     )
#     return agg


##########################################################################################
##########################################################################################
##########################################################################################
"""
Task 20: Create `03_gold.gold_customer_lifetime_value` Aggregate Table
=======================================================================

Business Logic:
    Customer lifetime value is a key metric for marketing and retention strategies.

Requirements:
    [ ] Read from `LIVE.03_gold.denormalized_sales_facts`.
    
    [ ] Group by: `customer_id`, `customer_name`.
        Business logic: Grouping by customer enables personalized analytics.
    
    [ ] Calculate Aggregations:
        - `total_spend`: `round(sum(total_amount), 2)`
          Business logic: Total spend is the foundation of CLV.
        
        - `total_orders`: `countDistinct(transaction_id)`
          Business logic: Order count shows engagement.
        
        - `first_purchase_date`: `min(event_time)` cast to date.
          Business logic: First purchase date helps track customer lifecycle.
        
        - `last_purchase_date`: `max(event_time)` cast to date.
          Business logic: Last purchase date helps track retention.
        
        - `avg_order_value`: `round(avg(total_amount), 2)`
          Business logic: Average order value is a key marketing metric.

Tips:
    - Run the pipeline after this task to validate CLV calculations and aggregations.
    - If stuck, see the solution below.
"""

### ---------------------
### Write Your Code Here ###


### ---------------------
### Solution Is Below 
# @dlt.table(
#     name=GOLD_CUSTOMER_LIFETIME_VALUE,
#     comment="Customer lifetime value metrics. A batch table."
# )
# def gold_customer_lifetime_value():
#     df = dlt.read(f"LIVE.{GOLD_DENORMALIZED_SALES_FACTS}")
#     agg = (
#         df.groupBy("customer_id", "customer_name")
#           .agg(
#               F.round(F.sum("total_amount"),2).alias("total_spend"),
#               F.countDistinct("transaction_id").alias("total_orders"),
#               F.min("event_time").cast("date").alias("first_purchase_date"),
#               F.max("event_time").cast("date").alias("last_purchase_date"),
#               F.round(F.avg("total_amount"),2).alias("avg_order_value")
#           )
#     )
#     return agg
