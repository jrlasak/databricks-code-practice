# Databricks notebook source
# MAGIC %md
# MAGIC # 03: Gold Layer - Business-Focused Aggregations
# MAGIC
# MAGIC **Zenith Online pipeline, notebook 3 of 3.** This notebook aggregates `silver_enriched_activity` into two Gold tables, `gold_daily_product_performance` and `customer_purchase_summary`, then answers three business questions with SQL. Run `02_Silver_Enrichment` first.
# MAGIC
# MAGIC **Duration**: ~30 min
# MAGIC
# MAGIC ---
# MAGIC ## Learning Objectives
# MAGIC By the end of this notebook, you will be able to:
# MAGIC - Measure data skew, write the two-stage salted aggregation, and say when salting pays off
# MAGIC - Rank rows within groups with a window function and write a partitioned Delta table
# MAGIC - Gather a data quality metric on serverless compute and query Gold tables with Spark SQL
# MAGIC ---

# COMMAND ----------

# MAGIC %md
# MAGIC ## Pipeline Initialization
# MAGIC
# MAGIC Import the libraries this notebook uses, then load the shared catalog, schema, path and table names from `variables`.

# COMMAND ----------

import pyspark.sql.functions as F
from pyspark.sql.window import Window

# In Databricks, the SparkSession `spark` is already created for you.

# COMMAND ----------

# MAGIC %run ./variables

# COMMAND ----------

# MAGIC %md
# MAGIC ### 🔎 Diagnose the skew first
# MAGIC Run the cell below before Task 6. It counts Silver rows per product. You should see the 5 hot products (`product_id` 101, 210, 315, 420, 55) at about 7,500 rows each against about 25 for a typical product (the printed median): a 300x imbalance. Each row is one event, and about 70% of a hot product's events are views, so the same products show about 5,200 views each in Task 6. In a grouped aggregation, all rows of one key share one group. For counts and sums, partial aggregation shrinks that to one row per task before the shuffle, so each hot product reaches its reducer as a handful of partial rows, not thousands; a two-stage salt pays off only where Spark has no map-side partial step and the function can still be split into two stages, such as a grouped-aggregate Pandas UDF, which loads every row of its group into one Python worker.

# COMMAND ----------

# 🔎 Rows per product in Silver: the skew Task 6 works on
skew_check = spark.read.table(SILVER_TABLE).groupBy("product_id").count()
display(skew_check.orderBy(F.col("count").desc()).limit(10))
print(f"Median rows per product: {skew_check.approxQuantile('count', [0.5], 0.01)[0]:.0f}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 03 - Gold Layer: Business-Focused Aggregations
# MAGIC
# MAGIC **Goal:** Build aggregated, denormalized tables that directly answer key business questions and are optimized for BI tools.
# MAGIC
# MAGIC **Target Schema:** `03_gold`
# MAGIC
# MAGIC **Input:** Silver layer table (`silver_enriched_activity`).
# MAGIC
# MAGIC **Output:**
# MAGIC - `gold_daily_product_performance`
# MAGIC - `customer_purchase_summary`
# MAGIC
# MAGIC ---
# MAGIC ### Daily Product Performance (Handling Data Skew)
# MAGIC Aggregate Silver data to compute daily product metrics, practising the two-stage salting pattern on the skewed popular products.
# MAGIC
# MAGIC ---
# MAGIC ### 🎯 TASK 6: Create `gold_daily_product_performance` Aggregate Table
# MAGIC
# MAGIC **Why:** Produces daily product performance metrics (views, funnel progression, revenue) optimized for BI dashboards and trend analyses. The two-stage salted aggregation is the standard pattern for a skewed key: it spreads each hot product's rows over 5 groups before the final aggregation, and the totals (so the ranking too) are identical to an unsalted run.
# MAGIC
# MAGIC **End Goal:** A query-ready Delta table (`gold_daily_product_performance`), written with `partitionBy` to practice the DataFrameWriter API, giving insight into product engagement and monetization by day and category with embedded revenue ranking.
# MAGIC
# MAGIC **Key Concepts:**
# MAGIC - **Data Skew**: When one key has significantly more data (e.g., popular products)
# MAGIC - **Salting**: add a random salt to the grouping key so a hot key's rows spread over several groups; it helps only where Spark has no map-side partial aggregation (a grouped-aggregate Pandas UDF) and the aggregate can be split into two stages
# MAGIC - **Two-Stage Aggregation**: Aggregate with salt, then remove salt and re-aggregate
# MAGIC - **Window Functions**: Rank products by revenue within each category
# MAGIC - **partitionBy**: writes one directory per `event_date`; the exam tests the API, but on Databricks a table under 1 TB should not be partitioned (liquid clustering is the recommendation for managed tables)
# MAGIC
# MAGIC **Requirements:**
# MAGIC - `gold_daily_product_performance` has one row per `event_date` and product with `total_views`, `total_adds_to_cart`, `total_purchases` and `total_revenue` (the price of purchase events only)
# MAGIC - The metrics come from a two-stage aggregation: first grouped with a random salt from 0 to 4 added to the key, then re-aggregated without it, so each hot product's rows are split across 5 groups
# MAGIC - Each row carries a `revenue_rank` within its day and category, and the table is overwritten (schema included) and partitioned by `event_date`

# COMMAND ----------

# ✏️ STUDENT EXERCISE
# TODO 1: Print "Starting Gold Layer: Daily Product Performance Aggregation" and read SILVER_TABLE into silver_df.
# TODO 2: Add a salt column: a random integer from 0 to 4 (salting factor 5).
# TODO 3: First aggregation, grouped by event_date, product_id, product_name, category and salt:
#         views (count of action "view_product"), adds_to_cart (count of "add_to_cart"),
#         purchases (count of "purchase"), daily_revenue (sum of price for "purchase", otherwise 0).
# TODO 4: Final aggregation without the salt, grouped by event_date, product_id, product_name, category:
#         total_views, total_adds_to_cart, total_purchases, total_revenue (sums of the four metrics).
# TODO 5: Add revenue_rank: the rank by total_revenue, highest first, within each event_date and category.
# TODO 6: Write to GOLD_DAILY_PRODUCT_TABLE: format "delta", mode "overwrite",
#         option "overwriteSchema" = "true", partitioned by event_date. Print a completion message.


# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC **Solution below** ⬇️

# COMMAND ----------

# ✅ SOLUTION: Create `gold_daily_product_performance` Aggregate Table
# print("Starting Gold Layer: Daily Product Performance Aggregation")
# silver_df = spark.read.table(SILVER_TABLE)
#
# SALTING_FACTOR = 5
# salted_df = silver_df.withColumn("salt", (F.rand() * SALTING_FACTOR).cast("int"))
#
# salted_agg_df = (
#     salted_df.groupBy("event_date", "product_id", "product_name", "category", "salt")
#     .agg(
#         F.count(F.when(F.col("action") == "view_product", 1)).alias("views"),
#         F.count(F.when(F.col("action") == "add_to_cart", 1)).alias("adds_to_cart"),
#         F.count(F.when(F.col("action") == "purchase", 1)).alias("purchases"),
#         F.sum(F.when(F.col("action") == "purchase", F.col("price")).otherwise(0)).alias("daily_revenue")
#     )
# )
#
# daily_product_performance_df = (
#     salted_agg_df.groupBy("event_date", "product_id", "product_name", "category")
#     .agg(
#         F.sum("views").alias("total_views"),
#         F.sum("adds_to_cart").alias("total_adds_to_cart"),
#         F.sum("purchases").alias("total_purchases"),
#         F.sum("daily_revenue").alias("total_revenue")
#     )
# )
#
# window_spec = Window.partitionBy("event_date", "category").orderBy(F.col("total_revenue").desc())
# final_product_gold_df = daily_product_performance_df.withColumn("revenue_rank", F.rank().over(window_spec))
#
# (
#     final_product_gold_df.write
#     .format("delta")
#     .mode("overwrite")
#     .option("overwriteSchema", "true")
#     .partitionBy("event_date")
#     .saveAsTable(GOLD_DAILY_PRODUCT_TABLE)
# )
#
# print(f"Gold Layer: Wrote to `{GOLD_DAILY_PRODUCT_TABLE}` (two-stage salted aggregation, partitioned by event_date).")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 🔍 Verify Your Results
# MAGIC Run the cell below to list the 10 most-viewed product rows. You should see:
# MAGIC - **Row count**: about 500 after one setup run, one row per product per day. All the events of a generator run fall within 30 seconds, so they share one `event_date`, and the 49,900 Silver events touch every one of the 500 products (the delisted 501-505 never reached Silver).
# MAGIC - **Key columns**: `event_date`, `product_id`, `product_name`, `category`, `total_views`, `total_adds_to_cart`, `total_purchases`, `total_revenue`, `revenue_rank`.
# MAGIC - **Partitioning**: the second table, from `DESCRIBE DETAIL`, lists `event_date` under `partitionColumns`, the proof that your `partitionBy` reached the table.
# MAGIC - **What to look for**: the 5 hot products (`product_id` 101, 210, 315, 420 and 55) at the top with about 5,200 views each, against roughly 17 for any other product (most between 12 and 23: the other 25% of events spread over all 500 products). That is the skew the generator planted (75% of all events go to those 5), and the hot products' views, adds to cart and purchases fall in funnel order (about 5,200 / 1,500 / 750). A tail product has so few events (about 17 / 5 / 2.5) that some show more purchases than adds to cart: buy-now purchases skip the cart, and the counts are small enough for noise to flip them.
# MAGIC
# MAGIC **What this means**: counts and sums are pre-aggregated on every task before the shuffle (partial aggregation). Without the salt, each hot product reaches its reducer as one partial row per task, not as thousands of rows, so this exact query would not straggle; on this data the salted version even costs one extra shuffle. The two-stage pattern you just wrote is the one the exam asks about. It pays off when Spark cannot pre-aggregate on the map side, for example a grouped-aggregate Pandas UDF, which loads all rows of a group into one Python worker, and only if the function can be split into two stages, as a sum can. Holistic aggregates such as `collect_list` gain nothing from it: the final stage still gathers every value of the hot key on one task. The totals are identical to an unsalted aggregation.

# COMMAND ----------

# 🔍 Verification: the hot products at the top of the views ranking
gold_products = spark.read.table(GOLD_DAILY_PRODUCT_TABLE)
print(f"✅ {GOLD_DAILY_PRODUCT_TABLE}: {gold_products.count():,} rows")
display(
    gold_products
    .select("event_date", "product_id", "product_name", "category",
            "total_views", "total_adds_to_cart", "total_purchases", "total_revenue", "revenue_rank")
    .orderBy(F.col("total_views").desc())
    .limit(10)
)
display(spark.sql(f"DESCRIBE DETAIL {GOLD_DAILY_PRODUCT_TABLE}").select("partitionColumns", "numFiles"))

# COMMAND ----------

# MAGIC %md
# MAGIC ### 🔎 See what the salt bought
# MAGIC The check above reads the same totals with or without the salt, so it cannot show the salt did anything. Run the cell below: it builds the same daily aggregation from Silver with one plain `groupBy` (no salt), prints its physical plan, then compares it with the table you just wrote. You should see:
# MAGIC - **The physical plan of the unsalted aggregation**: read it from the bottom up. Free Edition serverless runs this query on Photon, so the nodes carry Photon names. Just above `PhotonScan` sits a `PhotonGroupingAgg` whose functions list `partial_count` (three times) and `partial_sum`, and above it the shuffle (`PhotonShuffleExchangeSink` and `PhotonShuffleExchangeSource`). That first `PhotonGroupingAgg` is the map-side step: every task collapses each hot product to one partial row before the shuffle. The second `PhotonGroupingAgg`, above the shuffle, lists `finalmerge_count` and `finalmerge_sum`: it only adds those partial rows up. On a cluster without Photon the same two steps print as a pair of `HashAggregate` nodes around an `Exchange hashpartitioning`, the lower one with `partial_count` and `partial_sum`.
# MAGIC - **The last two lines, Rows only in the salted table: 0** and **Rows only in the unsalted version: 0**: `exceptAll` in both directions finds no difference (revenue is rounded to cents on both sides, because summing doubles in a different order can move the last digits). A non-zero count means your Task 6 table differs from a plain aggregation: check that `daily_revenue` uses `otherwise(0)` (TODO 3), that the final aggregation sums the four metrics rather than counting them (TODO 4), and that `revenue_rank` uses `rank` (TODO 5). An error saying `salt` cannot be resolved means the salt column is still in your table: drop it from the final grouping (TODO 4).
# MAGIC
# MAGIC **What this means**: for counts and sums, Spark already does on every task what the salt was meant to do, so the salted version produces the same table with one extra shuffle. Salting matters only for aggregates that cannot pre-combine on the map side, such as a grouped-aggregate Pandas UDF.

# COMMAND ----------

# 🔎 The same aggregation without the salt: identical table, and the plan shows why
unsalted_agg_df = (
    spark.read.table(SILVER_TABLE)
    .groupBy("event_date", "product_id", "product_name", "category")
    .agg(
        F.count(F.when(F.col("action") == "view_product", 1)).alias("total_views"),
        F.count(F.when(F.col("action") == "add_to_cart", 1)).alias("total_adds_to_cart"),
        F.count(F.when(F.col("action") == "purchase", 1)).alias("total_purchases"),
        F.sum(F.when(F.col("action") == "purchase", F.col("price")).otherwise(0)).alias("total_revenue")
    )
)
unsalted_window = Window.partitionBy("event_date", "category").orderBy(F.col("total_revenue").desc())
unsalted_gold = unsalted_agg_df.withColumn("revenue_rank", F.rank().over(unsalted_window))

salted_gold = spark.read.table(GOLD_DAILY_PRODUCT_TABLE)
salted_cmp = salted_gold.withColumn("total_revenue", F.round("total_revenue", 2))
unsalted_cmp = unsalted_gold.select(*salted_gold.columns).withColumn("total_revenue", F.round("total_revenue", 2))
only_salted = salted_cmp.exceptAll(unsalted_cmp).count()
only_unsalted = unsalted_cmp.exceptAll(salted_cmp).count()

print("Physical plan of the unsalted aggregation (read bottom-up):")
unsalted_agg_df.explain()

print(f"Rows only in the salted table: {only_salted}")
print(f"Rows only in the unsalted version: {only_unsalted}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Customer Purchase Summary
# MAGIC Aggregate customer-level purchase metrics and perform a data quality check for missing regions.
# MAGIC
# MAGIC ---
# MAGIC ### 🎯 TASK 7: Create `customer_purchase_summary` Aggregate Table
# MAGIC
# MAGIC **Why:** Consolidates purchase behavior per customer to support top-customer identification and region-level marketing segmentation (lifetime value needs months of purchase history, which one sample run does not have). The null region count surfaces data completeness issues early.
# MAGIC
# MAGIC **End Goal:** A Gold table (`customer_purchase_summary`) listing monetization KPIs per customer, ready to rank for business stakeholders.
# MAGIC
# MAGIC **Key Concepts:**
# MAGIC - **Data Quality Checks**: Monitor NULL values before aggregation
# MAGIC - **Customer Segmentation**: Group by customer and region for analysis
# MAGIC - **Approximate Aggregations**: Use `approx_count_distinct` for large-scale distinct counts
# MAGIC - **Serverless Compatibility**: Accumulators need the SparkContext, which serverless compute does not expose, so the metric comes from a filtered `count` action.
# MAGIC
# MAGIC **Requirements:**
# MAGIC - Before aggregating, the number of Silver events with a null `region` is counted with a filtered `count` action and printed as a data quality metric
# MAGIC - `customer_purchase_summary` has one row per `user_id` and `region`, built from purchase events only, with `total_purchase_value`, `total_purchases`, `distinct_products_purchased` (via `approx_count_distinct`) and `last_purchase_timestamp`
# MAGIC - The table is written as Delta in overwrite mode; readers rank it with `ORDER BY total_purchase_value DESC` (as the check below and Business Question 3 do), because a table has no row order of its own

# COMMAND ----------

# ✏️ STUDENT EXERCISE
# TODO 1: Print "Starting Gold Layer: Customer Purchase Summary" and read SILVER_TABLE into silver_df.
# TODO 2: Data quality check: count the silver_df rows where region is null into unknown_location_count, then print
#         f"Data Quality Check: Found {unknown_location_count} events with unknown customer locations."
# TODO 3: Keep only rows with action "purchase" and group by user_id and region:
#         total_purchase_value (sum of price), total_purchases (count of event_id),
#         distinct_products_purchased (approx_count_distinct of product_id), last_purchase_timestamp (max of event_dt).
# TODO 4: Name the result customer_summary_df (no sort: a table has no row order; readers sort with ORDER BY).
# TODO 5: Write customer_summary_df to GOLD_CUSTOMER_SUMMARY_TABLE: format "delta", mode "overwrite". Print a completion message.


# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC **Solution below** ⬇️

# COMMAND ----------

# ✅ SOLUTION: Create `customer_purchase_summary` Aggregate Table
# print("Starting Gold Layer: Customer Purchase Summary")
# silver_df = spark.read.table(SILVER_TABLE)
#
# # # Data quality metric: a filtered count action, which serverless compute supports.
# unknown_location_count = silver_df.filter(F.col("region").isNull()).count()
# print(f"Data Quality Check: Found {unknown_location_count} events with unknown customer locations.")
#
# customer_summary_df = (
#     silver_df.filter(F.col("action") == "purchase")
#     .groupBy("user_id", "region")
#     .agg(
#         F.sum("price").alias("total_purchase_value"),
#         F.count("event_id").alias("total_purchases"),
#         F.approx_count_distinct("product_id").alias("distinct_products_purchased"),
#         F.max("event_dt").alias("last_purchase_timestamp")
#     )
# )
#
# (
#     customer_summary_df.write
#     .format("delta")
#     .mode("overwrite")
#     .saveAsTable(GOLD_CUSTOMER_SUMMARY_TABLE)
# )
# print(f"Gold Layer: Wrote to `{GOLD_CUSTOMER_SUMMARY_TABLE}`.")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 🔍 Verify Your Results
# MAGIC Run the cell below to list the 10 customers with the highest purchase value. You should see:
# MAGIC - **Row count**: about 1,012 after one setup run, one row per customer and region with at least one purchase. About 10% of the Silver events are purchases (around 5,000), spread over 1,000 customers, so only a handful of customers (about 3) end up with no purchase and no row; about 15 more rows belong to late sign-ups (`user_id` above 1,000) with a null region. The data quality line printed by your solution should read about 249: the late sign-ups' events, which the left join kept with no location.
# MAGIC - **Key columns**: `user_id`, `region`, `total_purchase_value`, `total_purchases`, `distinct_products_purchased`, `last_purchase_timestamp`.
# MAGIC - **What to look for**: the cell sorts by `total_purchase_value`, highest first (the top customer spends about $2,550), and `distinct_products_purchased` sits below `total_purchases` for most of the top 10: 75% of purchases go to the 5 hot products, so a customer with several purchases usually buys one of them more than once.
# MAGIC
# MAGIC **What this means**: marketing can read the top customers and their region straight from this table, without joining raw events. Business Question 3 below queries exactly this ranking with SQL.

# COMMAND ----------

# 🔍 Verification: the top 10 customers by purchase value
customer_summary = spark.read.table(GOLD_CUSTOMER_SUMMARY_TABLE)
print(f"✅ {GOLD_CUSTOMER_SUMMARY_TABLE}: {customer_summary.count():,} rows")
display(
    customer_summary
    .orderBy(F.col("total_purchase_value").desc())
    .limit(10)
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Answering Business Questions
# MAGIC
# MAGIC Now that our Gold tables exist in Unity Catalog, we can query them directly with SQL. The three questions below are worked examples, not exercises: read each query, run it, and compare the output with **You should see**.
# MAGIC
# MAGIC ### Business Question 1:
# MAGIC What are the top 5 most purchased products for the most recent day?
# MAGIC
# MAGIC Query the Gold product performance table to find the most popular products.
# MAGIC
# MAGIC **You should see**: 5 rows, all on the same `event_date` after one setup run: the 5 hot products (`product_id` 101, 210, 315, 420 and 55), each with roughly 720-780 purchases, against at most 9 for any other product. The hot-product draw applies to purchases too, so the most-viewed products are also the best sellers. `product_id` is in the output so two rows can always be told apart.

# COMMAND ----------

print("--- Top 5 Purchased Products (Most Recent Day) ---")
spark.sql(f"""
    SELECT
        event_date,
        product_id,
        product_name,
        category,
        total_purchases,
        total_revenue
    FROM {GOLD_DAILY_PRODUCT_TABLE}
    WHERE event_date = (SELECT MAX(event_date) FROM {GOLD_DAILY_PRODUCT_TABLE})
    ORDER BY total_purchases DESC
    LIMIT 5
""").show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Business Question 2:
# MAGIC What is the daily sales revenue per category?
# MAGIC
# MAGIC Summarize daily revenue by product category for trend analysis.
# MAGIC
# MAGIC **You should see**: 4 rows, one per category, about $604K in total after one setup run: Electronics about $215K, Furniture about $152K, Home Decor about $131K and Office Supplies about $106K. Electronics leads because hot product 101, a keyboard at about $175, sells about 750 times; the 5 hot products carry about half of all revenue. One setup run is one day, so this is one row per category; every further run on a later date adds 4 rows, which is when this becomes a trend.

# COMMAND ----------

print("--- Daily Sales Revenue per Category ---")
spark.sql(f"""
    SELECT
        event_date,
        category,
        SUM(total_revenue) as category_revenue
    FROM {GOLD_DAILY_PRODUCT_TABLE}
    GROUP BY event_date, category
    ORDER BY event_date DESC, category_revenue DESC
""").show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Business Question 3:
# MAGIC What are the top 10 Customers by purchase value?
# MAGIC
# MAGIC Identify the highest-value customers based on total purchase value.
# MAGIC
# MAGIC **You should see**: the same 10 customers, in the same order, as the Task 7 verification above. Spark SQL and the DataFrame API read the same Delta table, so the answer cannot differ: pick whichever the consumer reads more easily.

# COMMAND ----------

print("--- Top 10 Customers by Purchase Value ---")
spark.sql(f"""
    SELECT
        user_id,
        region,
        total_purchase_value,
        total_purchases,
        last_purchase_timestamp
    FROM {GOLD_CUSTOMER_SUMMARY_TABLE}
    ORDER BY total_purchase_value DESC
    LIMIT 10
""").show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### 🔄 See the pipeline run incrementally
# MAGIC 1. Re-run `00_Setup_Environment`: it lands one more JSON Lines file of 50,500 lines (50,000 events plus 500 retries).
# MAGIC 2. Re-run Tasks 1 and 5 and their 🔍 cells (Task 4 first, if the notebook 02 session restarted, because Task 5 calls `categorize_region`): `bronze_user_events` grows to 101,000 rows and `silver_enriched_activity` to 99,800, with 498 rows without a region. The checkpoints remembered the first file, so only the new one was read.
# MAGIC 3. Re-run Tasks 6 and 7: if both setup runs fell on the same UTC date, every product's totals double (the seeded generator draws the same mix again) while the row counts stay at about 500 and about 1,012.
# MAGIC
# MAGIC A batch read of the landing folder would have re-read both files every time; that is the cost the checkpoint saves on a feed that grows all day.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary and Checkpoint
# MAGIC You built the whole Zenith Online pipeline: three Bronze tables, one de-duplicated and enriched Silver table, two Gold tables, and SQL answers to marketing's three questions.
# MAGIC
# MAGIC ### ✅ Exam checklist
# MAGIC Without scrolling up, can you:
# MAGIC - [ ] Explain what salting changes, what it never changes, and why a count or sum does not need it?
# MAGIC - [ ] Write a two-stage aggregation and a `rank` over a window partitioned by two columns?
# MAGIC - [ ] Explain why accumulators are unavailable on serverless compute and what replaces them?
# MAGIC - [ ] Query a Delta table from Python with `spark.sql`?
