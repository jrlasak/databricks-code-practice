from pyspark import pipelines as dp
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

Table Name Prefix:
    03_gold.

Gold Layer Tips:
    - Every gold table answers one question from the README: daily sales per store
      (Task 18), which items sell (Task 19) and customer spend (Task 20).
    - To watch increments, keep `data_generator.py` running and start another pipeline
      update: each update processes only the data that landed since the previous one.
      Stop the generator when you are done.
"""

"""
Task 17: Create `03_gold.denormalized_sales_facts` Streaming Table
===================================================================

Business Logic:
    Join each sale to the current customer, product and store, and alias the columns
    for BI tools.

Requirements:
    [ ] Read `02_silver.silver_sales_transactions` as a stream - use the
        `SILVER_SALES_TRANSACTIONS` variable. Add `comment`: "Sales joined to the current
        customer, product and store".

    [ ] Read the three current views (`SILVER_CUSTOMERS_CURRENT`, `SILVER_PRODUCTS_CURRENT`,
        `SILVER_STORES_CURRENT`) as batch lookups (`spark.read.table`).
        Business logic: Lookup tables provide the latest dimension attributes for each sale.
        In a stream-static join each micro-batch of sales is joined to the dimension
        snapshot as of that moment. A sale whose customer has not reached silver yet is
        written with a NULL customer_name and is never revisited, and the names on a fact
        row stay as they were at join time even if the dimension changes later. That is
        why the joins are left joins, why you may see a few NULL names right after a
        fresh start, and why Tasks 18-20 group by ID and take the current name from the
        current views.

    [ ] Join Logic: `left` join the sales stream with stores, then customers, then products
        on their respective IDs.

    [ ] Select & Alias Columns: select exactly the columns listed in
        data_contract.md > denormalized_sales_facts, in that order (including the `date`
        from Task 8 carried as `sale_date`, which Task 18 groups by).
        Business logic: Three dimensions all have a `name` column; aliasing them
        (`customer_name`, `product_name`, `store_name`) keeps them apart.

Tip: if stuck, see the solution below.
"""
### ---------------------
### Write Your Code Here ###


### ---------------------
### Solution Is Below 

# @dp.table(
#     name=GOLD_DENORMALIZED_SALES_FACTS,
#     comment="Sales joined to the current customer, product and store"
# )
# def denormalized_sales_facts():
#     sales = spark.readStream.table(SILVER_SALES_TRANSACTIONS)
#     customers = spark.read.table(SILVER_CUSTOMERS_CURRENT)
#     products = spark.read.table(SILVER_PRODUCTS_CURRENT)
#     stores = spark.read.table(SILVER_STORES_CURRENT)
    
#     df = (
#         sales
#         .join(stores, "store_id", "left")
#         .join(customers, "customer_id", "left")
#         .join(products, "product_id", "left")
#         .select(
#             sales.transaction_id,
#             sales.event_time,
#             sales["date"].alias("sale_date"),
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
Task 18: Create the `03_gold.gold_daily_sales_by_store` Materialized View
==========================================================================

Business Logic:
    Aggregate denormalized sales facts by store and day, calculating total revenue, 
    transaction count, items sold, and unique customers.

Requirements:
    [ ] Read the denormalized sales facts (`GOLD_DENORMALIZED_SALES_FACTS`) as a batch
        source (`spark.read.table`) and declare the table with `@dp.materialized_view`,
        which makes it recomputed on each update.
        Add `comment`: "Daily store sales summary".
        Business logic: A distinct count over all history cannot be maintained by a plain
        streaming aggregation; a materialized view recomputes it correctly.

    [ ] Group by:
        - `sale_date`
          Business logic: The date was derived once in Task 8 and carried through Task 17.

        - `store_id`
          Business logic: Group by the ID only, so a store's day stays on one row even if
          its name changes.

    [ ] Calculate Aggregations:
        - `total_revenue`: `round(sum(total_amount), 2)`
        - `total_transactions`: `count(transaction_id)`
        - `total_items_sold`: `sum(quantity)`
        - `unique_customers`: `countDistinct(customer_id)`
        Business logic: These four numbers replace the store managers' noon email of
        yesterday's sales.
    
    [ ] Add the store name: left-join `silver_stores_current` (`SILVER_STORES_CURRENT`)
        on `store_id` and take its `name` as `store_name`.
        Business logic: The name shown is the store's current one; it is attached after
        the aggregation so it cannot split a store's totals.

    [ ] Final Select: the columns listed in data_contract.md > gold_daily_sales_by_store,
        in that order.
        Business logic: Dashboards and exports are built against the data contract;
        matching its names and order means a `SELECT *` consumer never breaks.

Tips:
    - Run the pipeline after this step to validate aggregations and reporting logic.
    - If stuck, see the solution below.
"""
### ---------------------
### Write Your Code Here ###


### ---------------------
### Solution Is Below 
# @dp.materialized_view(
#     name=GOLD_DAILY_SALES_BY_STORE,
#     comment="Daily store sales summary"
# )
# def gold_daily_sales_by_store():
#     df = spark.read.table(GOLD_DENORMALIZED_SALES_FACTS)
#     stores = spark.read.table(SILVER_STORES_CURRENT).select("store_id", F.col("name").alias("store_name"))
#     agg = (
#         df.groupBy("sale_date", "store_id")
#             .agg(
#                 F.round(F.sum("total_amount"), 2).alias("total_revenue"),
#                 F.count("transaction_id").alias("total_transactions"),
#                 F.sum("quantity").alias("total_items_sold"),
#                 F.countDistinct("customer_id").alias("unique_customers")
#             )
#             .join(stores, "store_id", "left")
#             .select(
#                 "sale_date",
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
Task 19: Create the `03_gold.gold_product_performance` Materialized View
=========================================================================

Business Logic:
    Merchandising's question from the README: which items sell. One row per product,
    ranked by `total_quantity_sold` or `total_revenue`.

Requirements:
    [ ] Read the denormalized sales facts (`GOLD_DENORMALIZED_SALES_FACTS`) as a batch
        source (`spark.read.table`) and declare the table with `@dp.materialized_view`,
        which makes it recomputed on each update.
        Add `comment`: "Product sales performance metrics".
        Business logic: A materialized view recomputes the totals from the full fact
        table on each update, so every sale that has landed is counted.

    [ ] Group by `product_id` only.
        Business logic: A product's sales must stay on one row even if its name or
        category changes.

    [ ] Calculate Aggregations:
        - `total_revenue`: `round(sum(total_amount), 2)`
        - `total_quantity_sold`: `sum(quantity)`
        - `total_orders`: `count(transaction_id)`
        Business logic: `total_revenue` is revenue after discounts, not profit; there is
        no cost column in this lab.

    [ ] Add the product attributes: left-join `silver_products_current`
        (`SILVER_PRODUCTS_CURRENT`) on `product_id` and take its `name` as
        `product_name` and its `category` as `product_category`; select the columns
        listed in data_contract.md > gold_product_performance, in that order.
        Business logic: The name and category shown are the product's current ones.

Tips:
    - Run the pipeline after this step to validate product performance metrics and aggregations.
    - If stuck, see the solution below.
"""

### ---------------------
### Write Your Code Here ###


### ---------------------
### Solution Is Below 
# @dp.materialized_view(
#     name=GOLD_PRODUCT_PERFORMANCE,
#     comment="Product sales performance metrics"
# )
# def gold_product_performance():
#     df = spark.read.table(GOLD_DENORMALIZED_SALES_FACTS)
#     products = spark.read.table(SILVER_PRODUCTS_CURRENT).select(
#         "product_id",
#         F.col("name").alias("product_name"),
#         F.col("category").alias("product_category")
#     )
#     agg = (
#         df.groupBy("product_id")
#           .agg(
#               F.round(F.sum("total_amount"),2).alias("total_revenue"),
#               F.sum("quantity").alias("total_quantity_sold"),
#               F.count("transaction_id").alias("total_orders")
#           )
#           .join(products, "product_id", "left")
#           .select(
#               "product_id",
#               "product_name",
#               "product_category",
#               "total_revenue",
#               "total_quantity_sold",
#               "total_orders"
#           )
#     )
#     return agg


##########################################################################################
##########################################################################################
##########################################################################################
"""
Task 20: Create the `03_gold.gold_customer_lifetime_value` Materialized View
=============================================================================

Business Logic:
    Marketing's customer ranking from the README, built from the sales instead of a
    spreadsheet.

Requirements:
    [ ] Read the denormalized sales facts (`GOLD_DENORMALIZED_SALES_FACTS`) as a batch
        source (`spark.read.table`) and declare the table with `@dp.materialized_view`,
        which makes it recomputed on each update.
        Add `comment`: "Customer lifetime value metrics".
        Business logic: A distinct count over all history cannot be maintained by a plain
        streaming aggregation; a materialized view recomputes it correctly.

    [ ] Group by `customer_id` only, then left-join `silver_customers_current`
        (`SILVER_CUSTOMERS_CURRENT`) on `customer_id` to add its `name` as `customer_name`;
        select the columns listed in data_contract.md > gold_customer_lifetime_value, in
        that order.
        Business logic: A customer's spend must stay on one row when their name changes;
        the name shown is the current one.
    
    [ ] Calculate Aggregations:
        - `total_spend`: `round(sum(total_amount), 2)`
        - `total_orders`: `countDistinct(transaction_id)`
        - `first_purchase_date`: `min(event_time)` cast to date.
        - `last_purchase_date`: `max(event_time)` cast to date.
        - `avg_order_value`: `round(avg(total_amount), 2)`
        Business logic: `total_spend` ranks customers; `last_purchase_date` finds the ones
        who stopped coming back.

Tips:
    - Run the pipeline after this step to validate the CLV aggregations.
    - If stuck, see the solution below.
"""

### ---------------------
### Write Your Code Here ###


### ---------------------
### Solution Is Below 
# @dp.materialized_view(
#     name=GOLD_CUSTOMER_LIFETIME_VALUE,
#     comment="Customer lifetime value metrics"
# )
# def gold_customer_lifetime_value():
#     df = spark.read.table(GOLD_DENORMALIZED_SALES_FACTS)
#     customers = spark.read.table(SILVER_CUSTOMERS_CURRENT).select("customer_id", F.col("name").alias("customer_name"))
#     agg = (
#         df.groupBy("customer_id")
#           .agg(
#               F.round(F.sum("total_amount"),2).alias("total_spend"),
#               F.countDistinct("transaction_id").alias("total_orders"),
#               F.min("event_time").cast("date").alias("first_purchase_date"),
#               F.max("event_time").cast("date").alias("last_purchase_date"),
#               F.round(F.avg("total_amount"),2).alias("avg_order_value")
#           )
#           .join(customers, "customer_id", "left")
#           .select(
#               "customer_id",
#               "customer_name",
#               "total_spend",
#               "total_orders",
#               "first_purchase_date",
#               "last_purchase_date",
#               "avg_order_value"
#           )
#     )
#     return agg

##########################################################################################
# Done with this file? Run a pipeline update, do the Gold checks in
# ProjectPlan.md > Check your work, then step 7 "See it run incrementally" in ProjectPlan.md.
##########################################################################################
