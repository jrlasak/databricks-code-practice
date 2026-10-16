# Databricks notebook source
# MAGIC %md
# MAGIC # Notebook 04: Silver to Gold - Advanced Transformations
# MAGIC
# MAGIC **Exam Coverage**: Section 3 (Data Processing & Transformations)
# MAGIC
# MAGIC **Duration**: about 1 hour
# MAGIC
# MAGIC ---
# MAGIC ## Learning Objectives
# MAGIC By the end of this notebook, you will be able to:
# MAGIC - Create business-level aggregations for reporting
# MAGIC - Apply window functions for advanced analytics
# MAGIC - Calculate customer lifetime value (LTV) with RFM segmentation
# MAGIC - Implement product performance ranking
# MAGIC - Lay out Gold tables with liquid clustering
# MAGIC ---

# COMMAND ----------

# MAGIC %md
# MAGIC ## Section 1: Introduction to Gold Layer
# MAGIC The Gold layer contains **business-level aggregations** optimized for:
# MAGIC - **BI Reports**: Pre-aggregated metrics for dashboards
# MAGIC - **Machine Learning**: Feature tables for ML models
# MAGIC - **Executive Analytics**: KPIs and business metrics
# MAGIC ### 🥇 Gold Layer Characteristics
# MAGIC | Aspect | Approach |
# MAGIC |--------|----------|
# MAGIC | **Structure** | Denormalized (optimized for queries) |
# MAGIC | **Calculations** | Pre-aggregated (computed once) |
# MAGIC | **Layout** | Liquid clustering on filter columns |
# MAGIC | **Names** | Business-friendly terminology |
# MAGIC ### Common Gold Patterns
# MAGIC 1. **Time-series aggregations**: Daily/weekly/monthly summaries
# MAGIC 2. **Snapshot tables**: Point-in-time state capture
# MAGIC 3. **Dimension tables**: Enriched, slowly changing
# MAGIC 4. **Metric tables**: Pre-calculated KPIs

# COMMAND ----------

# MAGIC %run ./variables

# COMMAND ----------

# Set current catalog and schema to Gold
spark.sql(f"USE CATALOG {CATALOG_NAME}")
spark.sql(f"USE SCHEMA {GOLD_SCHEMA}")

print(f"Current Catalog: {spark.catalog.currentCatalog()}")
print(f"Current Schema: {spark.catalog.currentDatabase()}")

# COMMAND ----------

# Import required functions
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from delta.tables import DeltaTable

# COMMAND ----------

# MAGIC %md
# MAGIC ## Section 2: Daily Sales Summary with Rolling Windows
# MAGIC CartHarborHQ's merchandising team wants daily revenue by category on its dashboard: this section builds that table.
# MAGIC
# MAGIC Revenue counts completed orders only: pending orders can still fail payment and cancelled ones were never paid (pending and cancelled orders together are 15% of this data: 10% pending, 5% cancelled).
# MAGIC ### Metrics to Calculate
# MAGIC - Total sales by day and category
# MAGIC - Revenue, order counts, unique customers
# MAGIC - Average order value
# MAGIC - Rolling 7-day and 30-day averages
# MAGIC ### Window Functions for Trends
# MAGIC **Rolling windows** calculate moving averages:
# MAGIC ```python
# MAGIC Window.partitionBy("category").orderBy("date").rowsBetween(-6, 0)  # last 7 rows (= 7 days only if every day has a row)
# MAGIC ```
# MAGIC This captures the current row plus 6 preceding rows. `rowsBetween` counts rows; `rangeBetween` over a day number counts calendar days, so a day with no sales does not stretch the window.

# COMMAND ----------

# Load Silver sales data: completed orders only (pending and cancelled orders are not revenue)
sales_silver = spark.table(SALES_SILVER_TABLE).filter(F.col("order_status") == "completed")

print(f"Total sales records (completed order lines): {sales_silver.count():,}")
sales_silver.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ### 🎯 EXERCISE 1: Create Daily Sales Summary
# MAGIC **Your task**: Aggregate sales data by date and category.
# MAGIC
# MAGIC **Why**: Dashboards hit this table all day; aggregating once means they scan hundreds of rows, not every line item.
# MAGIC
# MAGIC **Requirements:**
# MAGIC 1. Add `sale_date` column (convert `order_timestamp` to date)
# MAGIC 2. Group by `sale_date` and `category`
# MAGIC 3. Calculate aggregations:
# MAGIC    - `total_revenue`: Sum of total_amount
# MAGIC    - `order_count`: number of DISTINCT order_id (each row of sales_clean is one line item, not one order)
# MAGIC    - `unique_customers`: Count distinct customer_id
# MAGIC    - `total_units_sold`: Sum of quantity
# MAGIC 4. Add calculated columns:
# MAGIC    - `avg_order_value`: total_revenue / order_count
# MAGIC    - `revenue_per_customer`: total_revenue / unique_customers
# MAGIC    - `units_per_order`: total_units_sold / order_count
# MAGIC
# MAGIC **Functions needed:**
# MAGIC ```python
# MAGIC F.to_date("timestamp_col")
# MAGIC F.sum(), F.countDistinct()
# MAGIC ```
# MAGIC
# MAGIC **Hint**: Use `.withColumn()` to add sale_date, then `.groupBy().agg()`
# MAGIC
# MAGIC **🔍 Check your output**: one row per `sale_date` x `category`, at most 5 categories per date, and `avg_order_value` = `total_revenue` / `order_count` on every row. The 🔍 cell after the Gold write (below Exercise 2) prints these checks as counts.

# COMMAND ----------

# ✏️ STUDENT EXERCISE
# TODO: Create daily sales summary

daily_sales_summary = sales_silver \
    .withColumn("sale_date", F.to_date(  # TODO: convert order_timestamp
    .groupBy(   # TODO: group by sale_date and category
    .agg(
        # TODO: Add aggregation functions




    )

# TODO: Add calculated metrics
daily_sales_summary = daily_sales_summary \
    .withColumn("avg_order_value",   # TODO: calculate
    .withColumn("revenue_per_customer",   # TODO: calculate
    .withColumn("units_per_order",   # TODO: calculate

display(daily_sales_summary.orderBy("sale_date", "category"))

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC **Solution below** ⬇️

# COMMAND ----------

# ✅ SOLUTION: Daily Sales Summary

# daily_sales_summary = sales_silver \
#     .withColumn("sale_date", F.to_date("order_timestamp")) \
#     .groupBy("sale_date", "category") \
#     .agg(
#         F.sum("total_amount").alias("total_revenue"),
#         F.countDistinct("order_id").alias("order_count"),
#         F.countDistinct("customer_id").alias("unique_customers"),
#         F.sum("quantity").alias("total_units_sold"),
#     )

# daily_sales_summary = daily_sales_summary \
#     .withColumn("avg_order_value", F.col("total_revenue") / F.col("order_count")) \
#     .withColumn("revenue_per_customer", F.col("total_revenue") / F.col("unique_customers")) \
#     .withColumn("units_per_order", F.col("total_units_sold") / F.col("order_count"))

# display(daily_sales_summary.orderBy("sale_date", "category"))

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ### 🎯 EXERCISE 2: Add Rolling Window Calculations
# MAGIC **Your task**: Calculate 7-day and 30-day rolling averages for trend analysis.
# MAGIC
# MAGIC **Why**: A promotion spikes one day; the 7-day average is the trend a business reads.
# MAGIC
# MAGIC **Requirements:**
# MAGIC 1. Create window specs, partitioned by category and ordered by a day number (days since 1970-01-01 of `sale_date`):
# MAGIC    - 7-day: the current day and the 6 calendar days before it
# MAGIC    - 30-day: the current day and the 29 calendar days before it
# MAGIC 2. Calculate rolling metrics:
# MAGIC    - `revenue_7day_avg`: 7-day average revenue
# MAGIC    - `revenue_30day_avg`: 30-day average revenue
# MAGIC    - `orders_7day_total`: 7-day total orders
# MAGIC    - `orders_30day_total`: 30-day total orders
# MAGIC
# MAGIC **Hint**: `rowsBetween(-6, 0)` counts 7 ROWS, which is 7 days only if every date has a row. `rangeBetween(-6, 0)` over a day number covers 7 calendar days even when a day has no sales.
# MAGIC
# MAGIC **🔍 Check your output**: the same rows as Exercise 1 plus four columns; `revenue_7day_avg` is visibly smoother than `total_revenue`, and on each category's first date both averages equal that day's `total_revenue`. The same 🔍 cell prints this check too.

# COMMAND ----------

# ✏️ STUDENT EXERCISE
# TODO: Add rolling window calculations

# Create window specifications
window_7day = Window.partitionBy(   # TODO: add partitionBy, orderBy (day number), rangeBetween

window_30day = Window.partitionBy(   # TODO: add partitionBy, orderBy (day number), rangeBetween

# Calculate rolling metrics
daily_sales_enriched = daily_sales_summary \
    .withColumn("revenue_7day_avg",   # TODO: 7-day avg of total_revenue
    .withColumn("revenue_30day_avg",   # TODO: 30-day avg of total_revenue
    .withColumn("orders_7day_total",   # TODO: 7-day sum of order_count
    .withColumn("orders_30day_total",   # TODO: 30-day sum of order_count

display(daily_sales_enriched.orderBy("sale_date", "category").limit(20))

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC **Solution below** ⬇️

# COMMAND ----------

# ✅ SOLUTION: Rolling Window Calculations

# # A day number (days since 1970-01-01) lets rangeBetween count calendar days, not rows
# day_number = F.datediff(F.col("sale_date"), F.lit("1970-01-01").cast("date"))
# window_7day = Window.partitionBy("category").orderBy(day_number).rangeBetween(-6, 0)
# window_30day = Window.partitionBy("category").orderBy(day_number).rangeBetween(-29, 0)

# daily_sales_enriched = daily_sales_summary \
#     .withColumn("revenue_7day_avg", F.avg("total_revenue").over(window_7day)) \
#     .withColumn("revenue_30day_avg", F.avg("total_revenue").over(window_30day)) \
#     .withColumn("orders_7day_total", F.sum("order_count").over(window_7day)) \
#     .withColumn("orders_30day_total", F.sum("order_count").over(window_30day))

# display(daily_sales_enriched.orderBy("sale_date", "category").limit(20))

# COMMAND ----------

# Write to Gold table with liquid clustering on the columns dashboards filter by
daily_sales_enriched.write \
    .format("delta") \
    .mode("overwrite") \
    .clusterBy("sale_date", "category") \
    .option("overwriteSchema", "true") \
    .saveAsTable(DAILY_SALES_SUMMARY_TABLE)

print(f"✅ Created Gold table: {DAILY_SALES_SUMMARY_TABLE}")

# COMMAND ----------

# 🔍 Verify Exercises 1 and 2: the "Check your output" rules, as counts
dss = spark.table(DAILY_SALES_SUMMARY_TABLE)
print(f"Rows: {dss.count():,} (expect 910: 182 dates x 5 categories)")
print(f"Repeated sale_date + category rows: {dss.count() - dss.select('sale_date', 'category').distinct().count()} (expect 0)")
print(f"Dates with more than 5 categories: {dss.groupBy('sale_date').count().filter(F.col('count') > 5).count()} (expect 0)")
print(f"Rows where avg_order_value != total_revenue / order_count: "
      f"{dss.filter(F.abs(F.col('avg_order_value') - F.col('total_revenue') / F.col('order_count')) > 0.001).count()} (expect 0)")

# On each category's first date the window holds that one day, so both averages equal total_revenue
first_dates = dss.groupBy("category").agg(F.min("sale_date").alias("sale_date"))
first_rows = dss.join(first_dates, ["category", "sale_date"])
first_off = first_rows.filter(
    (F.abs(F.col("revenue_7day_avg") - F.col("total_revenue")) > 0.001) |
    (F.abs(F.col("revenue_30day_avg") - F.col("total_revenue")) > 0.001)
).count()
print(f"Categories whose first-date averages differ from total_revenue: {first_off} (expect 0)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Section 3: Customer Lifetime Value with RFM Segmentation
# MAGIC CartHarborHQ's loyalty team wants RFM segments to pick who gets a retention offer. You build one row per customer with spend, order and recency metrics.
# MAGIC
# MAGIC Orders from customers missing from the Silver `customers_clean` table (`customer_name` = `Unknown customer`) still count as revenue in the daily summary, but they are left out here: the loyalty team cannot target a customer the CRM does not have.
# MAGIC ### What is RFM?
# MAGIC **RFM Segmentation** scores customers on three dimensions:
# MAGIC | Metric | Question | Score Range |
# MAGIC |--------|----------|-------------|
# MAGIC | **Recency** | How recently did they buy? | 1-5 (5 = most recent) |
# MAGIC | **Frequency** | How often do they buy? | 1-5 (5 = most frequent) |
# MAGIC | **Monetary** | How much do they spend? | 1-5 (5 = highest spend) |
# MAGIC
# MAGIC **Combined RFM score**: 3-15 (sum of R+F+M)
# MAGIC
# MAGIC ### Customer Segments
# MAGIC - **Champions** (13-15): Best customers
# MAGIC - **Loyal** (11-12): Regular buyers
# MAGIC - **Potential Loyalists** (9-10): Growing engagement
# MAGIC - **At Risk** (7-8): Declining activity
# MAGIC - **Hibernating** (5-6): Inactive
# MAGIC - **Lost** (3-4): Haven't returned
# MAGIC
# MAGIC ### Value Tiers
# MAGIC `ltv_category` ranks customers by `total_spend`: **High Value** = the top 10%, **Medium Value** = the next 20%, **Low Value** = the rest. The cut-offs are the 90th and 70th percentiles of `total_spend`, computed from the data in the segment cell, so they follow the data instead of a fixed dollar amount.

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ### 🎯 EXERCISE 3: Calculate Customer Lifetime Value
# MAGIC **Your task**: Aggregate customer purchase history and calculate time-based metrics.
# MAGIC
# MAGIC **Why**: every RFM score in Exercise 4 ranks these columns: a wrong `days_since_last_purchase` sends the retention offer to the wrong customers.
# MAGIC
# MAGIC **Part 1 - Aggregations:**
# MAGIC Group by customer (the provided start already drops the `Unknown customer` lines) and calculate:
# MAGIC - `total_spend`: Sum of total_amount
# MAGIC - `total_orders`: count of distinct order_id
# MAGIC - `first_purchase_date`: earliest order date (`to_date` of order_timestamp, so the column is a DATE, not the timestamp string)
# MAGIC - `last_purchase_date`: latest order date (`to_date` of order_timestamp)
# MAGIC - `total_items_purchased`: Sum of quantity
# MAGIC - `categories_purchased`: Count distinct category
# MAGIC
# MAGIC Then add `avg_order_value`: total_spend / total_orders (after the aggregation).
# MAGIC
# MAGIC **Part 2 - Time Metrics:**
# MAGIC Add calculated columns:
# MAGIC - `days_since_first_purchase`: days between the first purchase and `as_of_date`, the latest order date in the data
# MAGIC - `days_since_last_purchase`: days between the last purchase and `as_of_date` (Recency!)
# MAGIC - `customer_tenure_days`: Days between first and last purchase
# MAGIC
# MAGIC Measure against the data's own latest date, not today: a report re-run next month must not age every customer by 30 days. `as_of_date` is provided in the cell below.
# MAGIC
# MAGIC **Functions:**
# MAGIC ```python
# MAGIC F.to_date("timestamp_col")
# MAGIC F.datediff(F.lit(as_of_date), F.col("date_col"))
# MAGIC ```
# MAGIC
# MAGIC **🔍 Check your output**: one row per customer; `first_purchase_date` and `last_purchase_date` are dates; `days_since_last_purchase` <= `days_since_first_purchase` on every row, and `avg_order_value` x `total_orders` matches `total_spend` (up to rounding). The 🔍 cell after the Gold write (below Exercise 4) prints these checks as counts.

# COMMAND ----------

# ✏️ STUDENT EXERCISE
# TODO: Calculate customer lifetime value metrics

# Provided: the latest order date in the data, the "today" of this report
as_of_date = sales_silver.agg(F.max(F.to_date("order_timestamp"))).first()[0]

customer_ltv = sales_silver.filter(F.col("customer_name") != "Unknown customer") \
    .groupBy("customer_id", "customer_name", "loyalty_tier") \
    .agg(
        # TODO: Add aggregations






    )

# TODO: Add avg_order_value and the time-based metrics
customer_ltv = customer_ltv \
    .withColumn("avg_order_value",   # TODO
    .withColumn("days_since_first_purchase",   # TODO
    .withColumn("days_since_last_purchase",   # TODO (this is Recency!)
    .withColumn("customer_tenure_days",   # TODO

display(customer_ltv.orderBy(F.desc("total_spend")).limit(20))

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC **Solution below** ⬇️

# COMMAND ----------

# ✅ SOLUTION: Customer Lifetime Value

# # The latest order date in the data is the "today" of this report
# as_of_date = sales_silver.agg(F.max(F.to_date("order_timestamp"))).first()[0]

# customer_ltv = sales_silver.filter(F.col("customer_name") != "Unknown customer") \
#     .groupBy("customer_id", "customer_name", "loyalty_tier") \
#     .agg(
#         F.sum("total_amount").alias("total_spend"),
#         F.countDistinct("order_id").alias("total_orders"),
#         F.min(F.to_date("order_timestamp")).alias("first_purchase_date"),
#         F.max(F.to_date("order_timestamp")).alias("last_purchase_date"),
#         F.sum("quantity").alias("total_items_purchased"),
#         F.countDistinct("category").alias("categories_purchased")
#     )

# customer_ltv = (customer_ltv
#     .withColumn("avg_order_value", F.col("total_spend") / F.col("total_orders"))
#     .withColumn("days_since_first_purchase",
#                 F.datediff(F.lit(as_of_date), F.col("first_purchase_date")))
#     .withColumn("days_since_last_purchase",
#                 F.datediff(F.lit(as_of_date), F.col("last_purchase_date")))
#     .withColumn("customer_tenure_days",
#                 F.datediff(F.col("last_purchase_date"), F.col("first_purchase_date")))
#     )

# display(customer_ltv.orderBy(F.desc("total_spend")).limit(20))

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ### 🎯 EXERCISE 4: Calculate RFM Scores and Segments
# MAGIC **Your task**: Starting from `customer_ltv`, write `customer_ltv_rfm` yourself: three RFM scores with `ntile(5)` and their sum. The work cell below lists only the requirements, as comments.
# MAGIC
# MAGIC **Why**: RFM decides who gets a retention offer; reverse the recency score and the offer goes to your most active customers.
# MAGIC
# MAGIC **Requirements:**
# MAGIC - `recency_score`, `frequency_score`, `monetary_score`: 5 quintiles each, 5 = best (most recent buyer, most orders, highest spend); add `customer_id` ascending as the second sort key in each of the three windows. `ntile` splits rows by position, so customers with the same value can land in different buckets; the second key fixes their order, so each customer gets the same score on every run.
# MAGIC - `rfm_score`: recency_score + frequency_score + monetary_score
# MAGIC - Display the top 20 customers by `rfm_score`
# MAGIC
# MAGIC Part 2 (`customer_segment` from the RFM score, and `ltv_category`) is provided in the cell after the solution: run it and read it.
# MAGIC
# MAGIC **Hint**: ntile hands bucket 1 to the first rows in the window's order, so order each window so the best customers come last. For recency, lower days is better: sort days since last purchase from most to fewest, and the bucket IS the score.
# MAGIC
# MAGIC **🔍 Check your output**: every score is between 1 and 5 and every `rfm_score` between 3 and 15; a customer who bought on `as_of_date` has `recency_score` 5. The same 🔍 cell prints these checks too.

# COMMAND ----------

# ✏️ STUDENT EXERCISE
# TODO: Calculate RFM scores using ntile (write the whole cell yourself)
# - recency_score, frequency_score, monetary_score: ntile(5), 5 = best, ties broken on customer_id ascending
# - rfm_score: recency_score + frequency_score + monetary_score
# - display the top 20 by rfm_score

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC **Solution below** ⬇️

# COMMAND ----------

# ✅ SOLUTION: RFM Scores

# # Longest-lapsed customers land in bucket 1, the most recent in bucket 5: the bucket IS the score
# # customer_id (ascending) breaks ties so the scores are repeatable
# customer_ltv_rfm = customer_ltv \
#     .withColumn("recency_score",
#                 F.ntile(5).over(Window.orderBy(F.desc("days_since_last_purchase"), "customer_id"))) \
#     .withColumn("frequency_score",
#                 F.ntile(5).over(Window.orderBy("total_orders", "customer_id"))) \
#     .withColumn("monetary_score",
#                 F.ntile(5).over(Window.orderBy("total_spend", "customer_id")))

# customer_ltv_rfm = customer_ltv_rfm \
#     .withColumn("rfm_score", 
#                 F.col("recency_score") + F.col("frequency_score") + F.col("monetary_score"))

# display(customer_ltv_rfm.orderBy(F.desc("rfm_score")).limit(20))

# COMMAND ----------

# Part 2 (provided): customer segments from the RFM score, value tiers from the spend distribution
# Value tier cut-offs from the data: the 90th percentile (top 10%) and the 70th (next 20%)
p70, p90 = customer_ltv_rfm.agg(
    F.expr("percentile(CAST(total_spend AS DOUBLE), array(0.7, 0.9))")
).first()[0]
print(f"ltv_category cut-offs: Medium Value from {p70:,.2f}, High Value from {p90:,.2f}")

customer_ltv_segmented = customer_ltv_rfm \
    .withColumn("customer_segment",
        F.when(F.col("rfm_score") >= 13, "Champions")
         .when(F.col("rfm_score") >= 11, "Loyal Customers")
         .when(F.col("rfm_score") >= 9, "Potential Loyalists")
         .when(F.col("rfm_score") >= 7, "At Risk")
         .when(F.col("rfm_score") >= 5, "Hibernating")
         .otherwise("Lost")
    ) \
    .withColumn("ltv_category",
        F.when(F.col("total_spend") >= p90, "High Value")
         .when(F.col("total_spend") >= p70, "Medium Value")
         .otherwise("Low Value")
    )

display(customer_ltv_segmented.orderBy(F.desc("rfm_score")).limit(20))

# COMMAND ----------

# View segment distribution
display(
    customer_ltv_segmented.groupBy("customer_segment", "ltv_category")
    .agg(
        F.count("*").alias("customer_count"),
        F.sum("total_spend").alias("segment_revenue"),
        F.avg("total_spend").alias("avg_customer_value")
    )
    .orderBy(F.desc("segment_revenue"))
    .limit(10)
)

# COMMAND ----------

# Write to Gold table
customer_ltv_segmented.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(CUSTOMER_LTV_TABLE)

print(f"✅ Created Gold table: {CUSTOMER_LTV_TABLE}")

# COMMAND ----------

# 🔍 Verify Exercises 3 and 4: the "Check your output" rules, as counts
clv = spark.table(CUSTOMER_LTV_TABLE)
print(f"Rows: {clv.count():,} (expect 7,385 known customers)")
print(f"Repeated customer_id rows: {clv.count() - clv.select('customer_id').distinct().count()} (expect 0)")
print(f"first_purchase_date / last_purchase_date types: {dict(clv.dtypes)['first_purchase_date']} / {dict(clv.dtypes)['last_purchase_date']} (expect date / date)")
print(f"Rows with days_since_last_purchase > days_since_first_purchase: "
      f"{clv.filter(F.col('days_since_last_purchase') > F.col('days_since_first_purchase')).count()} (expect 0)")
print(f"Rows where avg_order_value x total_orders != total_spend: "
      f"{clv.filter(F.abs(F.col('avg_order_value') * F.col('total_orders') - F.col('total_spend')) > 0.01).count()} (expect 0)")
score_out_of_range = ~F.col("recency_score").between(1, 5) | ~F.col("frequency_score").between(1, 5) | ~F.col("monetary_score").between(1, 5)
print(f"Rows with a score outside 1-5: {clv.filter(score_out_of_range).count()} (expect 0)")
print(f"Rows where rfm_score != the sum of the three scores: "
      f"{clv.filter(F.col('rfm_score') != F.col('recency_score') + F.col('frequency_score') + F.col('monetary_score')).count()} (expect 0)")
print(f"Buyers on as_of_date without recency_score 5: "
      f"{clv.filter((F.col('days_since_last_purchase') == 0) & (F.col('recency_score') != 5)).count()} (expect 0)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Section 4: Product Performance with Ranking
# MAGIC CartHarborHQ's category managers want their products ranked by revenue. Analyze product performance using window functions.
# MAGIC ### Ranking Functions
# MAGIC | Function | Behavior | Use Case |
# MAGIC |----------|----------|----------|
# MAGIC | `RANK()` | Gaps in ranking for ties | Competition ranking |
# MAGIC | `DENSE_RANK()` | No gaps for ties | Consecutive ranks |
# MAGIC | `ROW_NUMBER()` | Unique numbers, no ties | Pagination |
# MAGIC
# MAGIC **Example:**
# MAGIC ```
# MAGIC Revenue: [100, 100, 90, 80]
# MAGIC RANK():       1,   1,  3,  4  (gap at 2)
# MAGIC DENSE_RANK(): 1,   1,  2,  3  (no gap)
# MAGIC ROW_NUMBER(): 1,   2,  3,  4  (arbitrary order for ties)
# MAGIC ```

# COMMAND ----------

# Calculate product performance metrics
product_performance = sales_silver \
    .groupBy("product_id", "product_name", "category", "subcategory") \
    .agg(
        F.sum("total_amount").alias("total_revenue"),
        F.sum("quantity").alias("total_units_sold"),
        F.countDistinct("order_id").alias("total_orders"),
        F.countDistinct("customer_id").alias("unique_customers"),
        F.avg("unit_price").alias("avg_selling_price"),
    )

product_performance = product_performance \
    .withColumn("revenue_per_unit", F.col("total_revenue") / F.col("total_units_sold")) \
    .withColumn("orders_per_customer", F.col("total_orders") / F.col("unique_customers"))

display(product_performance.orderBy(F.desc("total_revenue")).limit(20))

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ### 🎯 EXERCISE 5: Add Product Rankings and Market Share
# MAGIC **Your task**: Rank products within categories.
# MAGIC
# MAGIC **Why**: rank vs row_number decides whether two tied products both make the top 5.
# MAGIC
# MAGIC **Part 1 - Ranking:**
# MAGIC Add columns:
# MAGIC - `revenue_rank_in_category`: RANK() partitioned by category, ordered by `total_revenue` descending
# MAGIC - `revenue_row_number`: ROW_NUMBER() with same partition and order
# MAGIC
# MAGIC Part 2 (market share: `category_total_revenue`, `market_share_pct`, `cumulative_market_share`) is provided in the cell after the solution: run it and read it.
# MAGIC
# MAGIC **Hint**: one window spec serves both columns; `rank` and `row_number` differ only in what they do with two products on the same revenue.
# MAGIC
# MAGIC **🔍 Check your output**: one row per product, `revenue_rank_in_category` = 1 appears once per category unless two products tie, and `revenue_row_number` never repeats within a category. The 🔍 cell after the Gold write prints these checks as counts.

# COMMAND ----------

# ✏️ STUDENT EXERCISE
# TODO: Add ranking within category

category_window = Window.partitionBy(   # TODO: partition by category, order by total_revenue desc

product_performance_ranked = product_performance \
    .withColumn("revenue_rank_in_category",   # TODO: use F.rank()
    .withColumn("revenue_row_number",   # TODO: use F.row_number()

display(product_performance_ranked.filter("revenue_rank_in_category <= 10").orderBy("category", "revenue_rank_in_category"))

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC **Solution below** ⬇️

# COMMAND ----------

# ✅ SOLUTION: Product Ranking

# category_window = Window.partitionBy("category").orderBy(F.desc("total_revenue"))

# product_performance_ranked = product_performance \
#     .withColumn("revenue_rank_in_category", F.rank().over(category_window)) \
#     .withColumn("revenue_row_number", F.row_number().over(category_window))

# display(product_performance_ranked.filter("revenue_rank_in_category <= 10").orderBy("category", "revenue_rank_in_category"))

# COMMAND ----------

# Calculate market share within category
category_totals_window = Window.partitionBy("category")

product_performance_share = (
    product_performance_ranked 
    .withColumn("category_total_revenue", F.sum("total_revenue").over(category_totals_window)) 
    .withColumn("market_share_pct", 
                (F.col("total_revenue") / F.col("category_total_revenue") * 100))
    .withColumn("cumulative_market_share",
                F.sum("market_share_pct").over(
                    Window.partitionBy("category").orderBy(F.desc("total_revenue"))
                    .rowsBetween(Window.unboundedPreceding, Window.currentRow)
                ))
    )

display(product_performance_share.filter("revenue_rank_in_category <= 5").orderBy("category", "revenue_rank_in_category"))

# COMMAND ----------

# Identify performance tiers
product_performance_final = product_performance_share \
    .withColumn("performance_tier",
        F.when(F.col("revenue_rank_in_category") <= 5, "Top Performer")
         .when(F.col("revenue_rank_in_category") <= 20, "Strong Performer")
         .when(F.col("cumulative_market_share") <= 80, "Average Performer")
         .otherwise("Underperformer")
    )

display(product_performance_final.orderBy("category", "revenue_rank_in_category").limit(50))

# COMMAND ----------

# Write to Gold table
product_performance_final.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(PRODUCT_PERFORMANCE_TABLE)

print(f"✅ Created Gold table: {PRODUCT_PERFORMANCE_TABLE}")

# COMMAND ----------

# 🔍 Verify Exercise 5: the "Check your output" rules, as counts
pp = spark.table(PRODUCT_PERFORMANCE_TABLE)
print(f"Repeated product_id rows: {pp.count() - pp.select('product_id').distinct().count()} (expect 0)")
print(f"Categories with a rank-1 product: {pp.filter('revenue_rank_in_category = 1').select('category').distinct().count()} (expect 5)")
print(f"Repeated row numbers within a category: {pp.count() - pp.select('category', 'revenue_row_number').distinct().count()} (expect 0)")
top = pp.filter('revenue_row_number = 1')
print(f"Row-number-1 products below their category's top revenue: {top.join(pp.groupBy('category').agg(F.max('total_revenue').alias('mx')), 'category').filter(F.col('total_revenue') < F.col('mx')).count()} (expect 0)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Section 5: Data Layout with Liquid Clustering
# MAGIC **Liquid clustering** co-locates rows with similar values of the clustering keys, so queries that filter on those keys skip files.
# MAGIC ### When to Use
# MAGIC | Layout | Best For | Example |
# MAGIC |--------------|----------|--------|
# MAGIC | **Liquid clustering** (`CLUSTER BY`) | Default for new tables; keys are the columns you filter on, changeable later with `ALTER TABLE ... CLUSTER BY` (then `OPTIMIZE ... FULL` to move the existing rows) | sale_date, category |
# MAGIC | **`CLUSTER BY AUTO`** | UC managed tables with predictive optimization; Databricks picks the keys | - |
# MAGIC | **Partitioning / ZORDER** | Legacy: don't partition under 1 TB; above it Databricks still recommends liquid clustering first; ZORDER cannot be combined with clustering | - |
# MAGIC ### Pattern
# MAGIC ```sql
# MAGIC ALTER TABLE t CLUSTER BY (col1, col2);
# MAGIC OPTIMIZE t FULL;  -- FULL reclusters the rows written before the keys were set; plain OPTIMIZE only clusters data that still needs it
# MAGIC ```
# MAGIC **Best practice**: liquid clustering for every new table; no partitioning under 1 TB.

# COMMAND ----------

# Optimize daily sales (clustered by sale_date and category when it was written)
display(spark.sql(f"""
    OPTIMIZE {DAILY_SALES_SUMMARY_TABLE}
"""))

# COMMAND ----------

# Cluster customer LTV on the columns the loyalty team filters by, then optimize
spark.sql(f"""
    ALTER TABLE {CUSTOMER_LTV_TABLE}
    CLUSTER BY (customer_segment, ltv_category)
""")

display(spark.sql(f"""
    OPTIMIZE {CUSTOMER_LTV_TABLE} FULL
"""))

# COMMAND ----------

# Cluster product performance on the columns category managers filter by, then optimize
spark.sql(f"""
    ALTER TABLE {PRODUCT_PERFORMANCE_TABLE}
    CLUSTER BY (category, performance_tier)
""")

display(spark.sql(f"""
    OPTIMIZE {PRODUCT_PERFORMANCE_TABLE} FULL
"""))

# COMMAND ----------

# View optimization details
# Check clusteringColumns, numFiles and sizeInBytes
display(spark.sql(f"DESCRIBE DETAIL {DAILY_SALES_SUMMARY_TABLE}"))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Additional Optimization Commands
# MAGIC **VACUUM** - Remove old file versions:
# MAGIC ```sql
# MAGIC VACUUM table_name RETAIN 168 HOURS  -- Keep 7 days for time travel
# MAGIC ```
# MAGIC **ANALYZE** - Update statistics:
# MAGIC ```sql
# MAGIC ANALYZE TABLE table_name COMPUTE STATISTICS
# MAGIC ```

# COMMAND ----------

# Best Practice: VACUUM to remove old file versions
# Removes files not required by versions older than retention threshold
# Default retention is 7 days (168 hours) - balance time travel vs storage costs
# Note: VACUUM permanently deletes files - cannot time travel beyond retention period

spark.sql(f"""
    VACUUM {DAILY_SALES_SUMMARY_TABLE} RETAIN 168 HOURS
"""
)
print(f"✅ VACUUM ran on {DAILY_SALES_SUMMARY_TABLE}: removed unreferenced files older than 168 hours")



# COMMAND ----------

# Best Practice: ANALYZE TABLE for query optimization
# Updates table statistics used by cost-based optimizer (CBO)
# ANALYZE itself returns no rows; the statistics it wrote show in DESCRIBE TABLE EXTENDED
spark.sql(f"""
    ANALYZE TABLE {CUSTOMER_LTV_TABLE} COMPUTE STATISTICS
"""
)
display(
    spark.sql(f"DESCRIBE TABLE EXTENDED {CUSTOMER_LTV_TABLE}")
        .filter("col_name = 'Statistics'")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Section 6: Summary and Checkpoint
# MAGIC ### 🎯 Key Concepts Covered
# MAGIC **1. Gold Layer Design**
# MAGIC - Business-level aggregations
# MAGIC - Denormalized for query performance
# MAGIC - Pre-calculated metrics
# MAGIC
# MAGIC **2. Advanced Aggregations**
# MAGIC - GROUP BY with multiple agg functions
# MAGIC - Rolling window calculations
# MAGIC - Time-series summaries
# MAGIC
# MAGIC **3. Window Functions**
# MAGIC - `RANK()`, `ROW_NUMBER()`, `DENSE_RANK()`
# MAGIC - `NTILE()` for quintile scoring
# MAGIC - Cumulative calculations
# MAGIC - Partitioned windows
# MAGIC
# MAGIC **4. Business Analytics**
# MAGIC - Customer lifetime value (LTV)
# MAGIC - RFM segmentation
# MAGIC - Product performance ranking
# MAGIC - Market share analysis
# MAGIC
# MAGIC **5. Optimization**
# MAGIC - Liquid clustering (CLUSTER BY), OPTIMIZE and VACUUM
# MAGIC ### ✅ Exam Checklist
# MAGIC Can you:
# MAGIC - [ ] Write complex GROUP BY with multiple aggregations?
# MAGIC - [ ] Use RANK(), ROW_NUMBER(), NTILE()?
# MAGIC - [ ] Create rolling windows with rowsBetween and rangeBetween?
# MAGIC - [ ] Choose clustering keys with CLUSTER BY?
# MAGIC - [ ] Explain why partitioning is legacy under 1 TB?
# MAGIC - [ ] Implement cumulative calculations?
# MAGIC ### 📚 Next Steps
# MAGIC **Notebook 05** covers:
# MAGIC - Job parameters with widgets
# MAGIC - Retry logic for transient failures
# MAGIC - Wiring notebooks 02-04 into a 3-task job
# MAGIC ---
# MAGIC **🎉 Notebook Complete!**
# MAGIC Your three Gold tables are written and clustered: `daily_sales_summary`, `customer_lifetime_value` and `product_performance`. Next: Notebook 05 turns notebooks 02 to 04 into a 3-task job.
