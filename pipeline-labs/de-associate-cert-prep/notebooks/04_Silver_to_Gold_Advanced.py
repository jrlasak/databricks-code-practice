# Databricks notebook source
# MAGIC %md
# MAGIC #Notebook 04: Silver to Gold - Advanced Transformations
# MAGIC
# MAGIC **Exam Coverage**: Section 3 (Incremental Data Processing)
# MAGIC
# MAGIC **Duration**: 60-75 minutes
# MAGIC
# MAGIC ---
# MAGIC ## Learning Objectives
# MAGIC By the end of this notebook, you will be able to:
# MAGIC - Create business-level aggregations for reporting
# MAGIC - Apply window functions for advanced analytics
# MAGIC - Calculate customer lifetime value (LTV) with RFM segmentation
# MAGIC - Implement product performance ranking
# MAGIC - Build funnel analysis from event data
# MAGIC - Optimize Gold tables with Z-ordering and partitioning
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
# MAGIC | **Partitioning** | Time-series optimized |
# MAGIC | **Indexing** | Z-ordered for co-location |
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
# MAGIC Create a daily sales summary for business dashboards.
# MAGIC ### Metrics to Calculate
# MAGIC - Total sales by day and category
# MAGIC - Revenue, order counts, unique customers
# MAGIC - Average order value
# MAGIC - Rolling 7-day and 30-day averages
# MAGIC ### Window Functions for Trends
# MAGIC **Rolling windows** calculate moving averages:
# MAGIC ```python
# MAGIC Window.partitionBy("category").orderBy("date").rowsBetween(-6, 0)  # 7 days
# MAGIC ```
# MAGIC This captures the current row plus 6 preceding rows.

# COMMAND ----------

# Load Silver sales data
sales_silver = spark.table(SALES_SILVER_TABLE)

print(f"Total sales records: {sales_silver.count():,}")
sales_silver.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ### 🎯 EXERCISE 1: Create Daily Sales Summary
# MAGIC **Your task**: Aggregate sales data by date and category.
# MAGIC **Requirements:**
# MAGIC 1. Add `sale_date` column (convert `order_timestamp` to date)
# MAGIC 2. Group by `sale_date` and `category`
# MAGIC 3. Calculate aggregations:
# MAGIC   - `total_revenue`: Sum of total_amount
# MAGIC   - `order_count`: Count of order_id
# MAGIC   - `unique_customers`: Count distinct customer_id
# MAGIC   - `avg_order_value`: Average of total_amount
# MAGIC   - `total_units_sold`: Sum of quantity
# MAGIC 4. Add calculated columns:
# MAGIC   - `revenue_per_customer`: total_revenue / unique_customers
# MAGIC   - `units_per_order`: total_units_sold / order_count
# MAGIC
# MAGIC **Functions needed:**
# MAGIC ```python
# MAGIC F.to_date("timestamp_col")
# MAGIC F.sum(), F.count(), F.countDistinct(), F.avg()
# MAGIC ```
# MAGIC
# MAGIC **Hint**: Use `.withColumn()` to add sale_date, then `.groupBy().agg()`

# COMMAND ----------

# TODO: Create daily sales summary

daily_sales_summary = sales_silver \
    .withColumn("sale_date", F.to_date(  # TODO: convert order_timestamp
    .groupBy(   # TODO: group by sale_date and category
    .agg(
        # TODO: Add aggregation functions
        
        
        
        
        
    )

# TODO: Add calculated metrics
daily_sales_summary = daily_sales_summary \
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
#         F.count("order_id").alias("order_count"),
#         F.countDistinct("customer_id").alias("unique_customers"),
#         F.avg("total_amount").alias("avg_order_value"),
#         F.sum("quantity").alias("total_units_sold"),
#     )

# daily_sales_summary = daily_sales_summary \
#     .withColumn("revenue_per_customer", F.col("total_revenue") / F.col("unique_customers")) \
#     .withColumn("units_per_order", F.col("total_units_sold") / F.col("order_count"))

# display(daily_sales_summary.orderBy("sale_date", "category"))

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ### 🎯 EXERCISE 2: Add Rolling Window Calculations
# MAGIC **Your task**: Calculate 7-day and 30-day rolling averages for trend analysis.
# MAGIC
# MAGIC **Requirements:**
# MAGIC 1. Create window specs:
# MAGIC - 7-day: Partition by category, order by sale_date, rows -6 to 0
# MAGIC - 30-day: Partition by category, order by sale_date, rows -29 to 0
# MAGIC 2. Calculate rolling metrics:
# MAGIC - `revenue_7day_avg`: 7-day average revenue
# MAGIC - `revenue_30day_avg`: 30-day average revenue
# MAGIC - `orders_7day_total`: 7-day total orders
# MAGIC - `orders_30day_total`: 30-day total orders
# MAGIC **Window pattern:**
# MAGIC ```python
# MAGIC window = Window.partitionBy("col").orderBy("col").rowsBetween(-N, 0)
# MAGIC df.withColumn("rolling_avg", F.avg("col").over(window))
# MAGIC ```
# MAGIC **Hint**: `.rowsBetween(-6, 0)` includes current row + 6 previous = 7 total

# COMMAND ----------

# TODO: Add rolling window calculations

# Create window specifications
window_7day = Window.partitionBy(   # TODO: add partitionBy, orderBy, rowsBetween

window_30day = Window.partitionBy(   # TODO: add partitionBy, orderBy, rowsBetween

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

# window_7day = Window.partitionBy("category").orderBy("sale_date").rowsBetween(-6, 0)
# window_30day = Window.partitionBy("category").orderBy("sale_date").rowsBetween(-29, 0)

# daily_sales_enriched = daily_sales_summary \
#     .withColumn("revenue_7day_avg", F.avg("total_revenue").over(window_7day)) \
#     .withColumn("revenue_30day_avg", F.avg("total_revenue").over(window_30day)) \
#     .withColumn("orders_7day_total", F.sum("order_count").over(window_7day)) \
#     .withColumn("orders_30day_total", F.sum("order_count").over(window_30day))

# display(daily_sales_enriched.orderBy("sale_date", "category").limit(20))

# COMMAND ----------

# Write to Gold table with date partitioning
daily_sales_enriched.write \
    .format("delta") \
    .mode("overwrite") \
    .partitionBy("sale_date") \
    .option("overwriteSchema", "true") \
    .saveAsTable(DAILY_SALES_SUMMARY_TABLE)

print(f"✅ Created Gold table: {DAILY_SALES_SUMMARY_TABLE}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Section 3: Customer Lifetime Value with RFM Segmentation
# MAGIC Calculate comprehensive customer metrics.
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

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ### 🎯 EXERCISE 3: Calculate Customer Lifetime Value
# MAGIC **Your task**: Aggregate customer purchase history and calculate time-based metrics.
# MAGIC
# MAGIC **Part 1 - Aggregations:**
# MAGIC Group by customer and calculate:
# MAGIC - `total_spend`: Sum of total_amount
# MAGIC - `total_orders`: Count of order_id
# MAGIC - `avg_order_value`: Average of total_amount
# MAGIC - `first_purchase_date`: Min of order_timestamp
# MAGIC - `last_purchase_date`: Max of order_timestamp
# MAGIC - `total_items_purchased`: Sum of quantity
# MAGIC - `categories_purchased`: Count distinct category
# MAGIC
# MAGIC **Part 2 - Time Metrics:**
# MAGIC Add calculated columns:
# MAGIC - `days_since_first_purchase`: Days between first purchase and today
# MAGIC - `days_since_last_purchase`: Days between last purchase and today (Recency!)
# MAGIC - `customer_tenure_days`: Days between first and last purchase
# MAGIC **Functions:**
# MAGIC ```python
# MAGIC F.datediff(F.current_date(), F.to_date("timestamp_col"))
# MAGIC ```

# COMMAND ----------

# TODO: Calculate customer lifetime value metrics

customer_ltv = sales_silver \
    .groupBy("customer_id", "customer_name", "loyalty_tier") \
    .agg(
        # TODO: Add aggregations
        
        
        
        
        
        
    )

# TODO: Add time-based metrics
customer_ltv = customer_ltv \
    .withColumn("days_since_first_purchase",   # TODO
    .withColumn("days_since_last_purchase",   # TODO (this is Recency!)
    .withColumn("customer_tenure_days",   # TODO

display(customer_ltv.orderBy(F.desc("total_spend"))).limit(20)

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC **Solution below** ⬇️

# COMMAND ----------

# ✅ SOLUTION: Customer Lifetime Value

# customer_ltv = sales_silver \
#     .groupBy("customer_id", "customer_name", "loyalty_tier") \
#     .agg(
#         F.sum("total_amount").alias("total_spend"),
#         F.count("order_id").alias("total_orders"),
#         F.avg("total_amount").alias("avg_order_value"),
#         F.min("order_timestamp").alias("first_purchase_date"),
#         F.max("order_timestamp").alias("last_purchase_date"),
#         F.sum("quantity").alias("total_items_purchased"),
#         F.countDistinct("category").alias("categories_purchased")
#     )

# customer_ltv = (customer_ltv
#     .withColumn("days_since_first_purchase", 
#                 F.datediff(F.current_date(), F.to_date("first_purchase_date")))
#     .withColumn("days_since_last_purchase",
#                 F.datediff(F.current_date(), F.to_date("last_purchase_date")))
#     .withColumn("customer_tenure_days",
#                 F.datediff(F.col("last_purchase_date"), F.col("first_purchase_date")))
#     )

# display(customer_ltv.orderBy(F.desc("total_spend")).limit(20))

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ### 🎯 EXERCISE 4: Calculate RFM Scores and Segments
# MAGIC **Your task**: Create RFM scores using ntile() and assign segments.
# MAGIC
# MAGIC **Part 1 - RFM Scores:**
# MAGIC Use `F.ntile(5)` to create 5 buckets (quintiles):
# MAGIC - `recency_score`: REVERSE of days_since_last_purchase (lower days = better)
# MAGIC - Use: `6 - F.ntile(5).over(Window.orderBy(F.desc("days_since_last_purchase")))`
# MAGIC - `frequency_score`: Based on total_orders (higher = better)
# MAGIC - Use: `F.ntile(5).over(Window.orderBy("total_orders"))`
# MAGIC - `monetary_score`: Based on total_spend (higher = better)
# MAGIC - Use: `F.ntile(5).over(Window.orderBy("total_spend"))`
# MAGIC - `rfm_score`: Sum of R + F + M
# MAGIC
# MAGIC **Part 2 - Segments:**
# MAGIC Create `customer_segment` based on RFM score:
# MAGIC - '>= 13': Champions
# MAGIC - '>= 11': Loyal Customers
# MAGIC - '>= 9': Potential Loyalists
# MAGIC - '>= 7': At Risk
# MAGIC - '>= 5': Hibernating
# MAGIC - 'else': Lost
# MAGIC
# MAGIC **Hint**: `ntile(5)` splits data into 5 equal-sized buckets.

# COMMAND ----------

# TODO: Calculate RFM scores using ntile

customer_ltv_rfm = customer_ltv \
    .withColumn("recency_score", 
                # TODO: 6 minus ntile of days_since_last_purchase (desc)
                
    .withColumn("frequency_score",
                # TODO: ntile of total_orders
                
    .withColumn("monetary_score",
                # TODO: ntile of total_spend
                

# TODO: Calculate combined RFM score
customer_ltv_rfm = customer_ltv_rfm \
    .withColumn("rfm_score",   # TODO: sum of R + F + M

display(customer_ltv_rfm.orderBy(F.desc("rfm_score")).limit(20))

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC **Solution below** ⬇️

# COMMAND ----------

# ✅ SOLUTION: RFM Scores

# customer_ltv_rfm = customer_ltv \
#     .withColumn("recency_score", 
#                 6 - F.ntile(5).over(Window.orderBy(F.desc("days_since_last_purchase")))) \
#     .withColumn("frequency_score",
#                 F.ntile(5).over(Window.orderBy("total_orders"))) \
#     .withColumn("monetary_score",
#                 F.ntile(5).over(Window.orderBy("total_spend")))

# customer_ltv_rfm = customer_ltv_rfm \
#     .withColumn("rfm_score", 
#                 F.col("recency_score") + F.col("frequency_score") + F.col("monetary_score"))

# display(customer_ltv_rfm.orderBy(F.desc("rfm_score")).limit(20))

# COMMAND ----------

# Create customer segments based on RFM score
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
        F.when(F.col("total_spend") >= 10000, "High Value")
         .when(F.col("total_spend") >= 5000, "Medium Value")
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

# MAGIC %md
# MAGIC ## Section 4: Product Performance with Ranking
# MAGIC Analyze product performance using window functions.
# MAGIC ### Ranking Functions
# MAGIC | Function | Behavior | Use Case |
# MAGIC |----------|----------|----------|
# MAGIC | `RANK()` | Gaps in ranking for ties | Competition ranking |
# MAGIC | `DENSE_RANK()` | No gaps for ties | Consecutive ranks |
# MAGIC | `ROW_NUMBER()` | Unique numbers, no ties | Pagination |
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
        F.count("order_id").alias("total_orders"),
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
# MAGIC **Your task**: Rank products within categories and calculate market share.
# MAGIC
# MAGIC **Part 1 - Ranking:**
# MAGIC Add columns:
# MAGIC - `revenue_rank_in_category`: RANK() partitioned by category, ordered by revenue desc
# MAGIC - `revenue_row_number`: ROW_NUMBER() with same partition and order
# MAGIC
# MAGIC **Part 2 - Market Share:**
# MAGIC Add columns:
# MAGIC - `category_total_revenue`: Sum of revenue within category
# MAGIC - `market_share_pct`: (product revenue / category total) * 100
# MAGIC - `cumulative_market_share`: Running sum of market_share_pct
# MAGIC
# MAGIC **Patterns:**
# MAGIC ```python
# MAGIC # Ranking
# MAGIC window = Window.partitionBy("category").orderBy(F.desc("revenue"))
# MAGIC df.withColumn("rank", F.rank().over(window))
# MAGIC # Cumulative sum
# MAGIC .rowsBetween(Window.unboundedPreceding, Window.currentRow)
# MAGIC ```

# COMMAND ----------

# TODO: Add ranking within category

category_window = Window.partitionBy(   # TODO: partition by category, order by revenue desc

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

# MAGIC %md
# MAGIC ## Section 6: Optimize with Z-Ordering
# MAGIC **Z-ordering** co-locates related data for faster queries.
# MAGIC ### When to Use
# MAGIC | Optimization | Best For | Example |
# MAGIC |--------------|----------|--------|
# MAGIC | **Partitioning** | Low cardinality, time-series | date, region |
# MAGIC | **Z-ordering** | High cardinality, frequent filters | customer_id, product_id, category |
# MAGIC ### Pattern
# MAGIC ```sql
# MAGIC OPTIMIZE table_name ZORDER BY (column1, column2)
# MAGIC ```
# MAGIC **Best practice**: Partition by date, Z-order by frequently queried columns.

# COMMAND ----------

# Optimize daily sales (already partitioned by sale_date)
display(spark.sql(f"""
    OPTIMIZE {DAILY_SALES_SUMMARY_TABLE}
    ZORDER BY (category)
"""))

# COMMAND ----------

# Optimize customer LTV by segment
display(spark.sql(f"""
    OPTIMIZE {CUSTOMER_LTV_TABLE}
    ZORDER BY (customer_segment, ltv_category)
"""))

#print(f"✅ Optimized {CUSTOMER_LTV_TABLE}")

# COMMAND ----------

# Optimize product performance by category
display(spark.sql(f"""
    OPTIMIZE {PRODUCT_PERFORMANCE_TABLE}
    ZORDER BY (category, performance_tier)
"""))

# COMMAND ----------

# View optimization details
# Take a look at "partitionColumns", "clustering Columns", "numFiles", "sizeInBytes", etc
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

spark.sql(f"""
    VACUUM {DAILY_SALES_SUMMARY_TABLE} RETAIN 168 HOURS
"""
)
print(f"✅ Cleaned old files from {DAILY_SALES_SUMMARY_TABLE}")



# COMMAND ----------

# Best Practice: ANALYZE TABLE for query optimization
# Updates table statistics used by cost-based optimizer (CBO)
display(spark.sql(f"""
    ANALYZE TABLE {CUSTOMER_LTV_TABLE} COMPUTE STATISTICS
"""
))

# Note: VACUUM permanently deletes files - cannot time travel beyond retention period

# COMMAND ----------

# MAGIC %md
# MAGIC ## Section 7: Summary and Checkpoint
# MAGIC ### 🎯 Key Concepts Covered
# MAGIC **1. Gold Layer Design**
# MAGIC - Business-level aggregations
# MAGIC - Denormalized for query performance
# MAGIC - Pre-calculated metrics
# MAGIC **2. Advanced Aggregations**
# MAGIC - GROUP BY with multiple agg functions
# MAGIC - Rolling window calculations
# MAGIC - Time-series summaries
# MAGIC **3. Window Functions**
# MAGIC - `RANK()`, `ROW_NUMBER()`, `DENSE_RANK()`
# MAGIC - `NTILE()` for quintile scoring
# MAGIC - Cumulative calculations
# MAGIC - Partitioned windows
# MAGIC **4. Business Analytics**
# MAGIC - Customer lifetime value (LTV)
# MAGIC - RFM segmentation
# MAGIC - Product performance ranking
# MAGIC - Market share analysis
# MAGIC - Funnel conversion rates
# MAGIC **5. Optimization**
# MAGIC - Partitioning (low cardinality)
# MAGIC - Z-ordering (high cardinality)
# MAGIC - OPTIMIZE and VACUUM
# MAGIC ### ✅ Exam Checklist
# MAGIC Can you:
# MAGIC - [ ] Write complex GROUP BY with multiple aggregations?
# MAGIC - [ ] Use RANK(), ROW_NUMBER(), NTILE()? 
# MAGIC - [ ] Create rolling windows with rowsBetween?
# MAGIC - [ ] Calculate conversion rates and percentages?
# MAGIC - [ ] Optimize tables with ZORDER BY?
# MAGIC - [ ] Explain partitioning vs Z-ordering?
# MAGIC - [ ] Implement cumulative calculations?
# MAGIC ---
# MAGIC **🎉 Notebook Complete!**
# MAGIC The Gold layer is complete with optimized analytics tables ready for BI tools and dashboards. Proceed to Notebook 05.
