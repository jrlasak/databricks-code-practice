# Databricks notebook source
# MAGIC %md
# MAGIC # 02: Enrich With Dimensions  -  "Stream-Static Joins: Adding Business Context"
# MAGIC **Exam Coverage**: DE Associate (Medallion, MERGE) + DE Professional (Stream-Static Joins)
# MAGIC
# MAGIC **Duration**: 75 minutes
# MAGIC
# MAGIC ---
# MAGIC ## Learning Objectives
# MAGIC By the end of this notebook, you will be able to:
# MAGIC - Enrich streaming transactions with static customer and merchant data
# MAGIC - Keep payments whose merchant is not in the dimension yet (late-arriving dimension rows) in stream-static joins
# MAGIC - Implement SCD Type 1 MERGE for merchant profile updates
# MAGIC - Flag geographic mismatches as potential fraud indicators
# MAGIC ---

# COMMAND ----------

# MAGIC %md
# MAGIC ## Section 1: Stream-Static Enrichment
# MAGIC Enriched transaction data allows for more accurate fraud detection. We'll join our Bronze transaction stream with our customer and merchant dimensions and write the result to Silver.
# MAGIC
# MAGIC ### Key Concepts
# MAGIC - **Stream-Static Join**: Joins a streaming DataFrame with a static DataFrame.
# MAGIC - **Join Behavior**: Each micro-batch joins against the static side as it is when that batch runs. With a static Delta table that is the table's latest version, so an update to that table (a MERGE like the one in Exercise 3) reaches the next micro-batch without restarting the stream. This notebook joins the raw dimension export files, so the merchants Exercise 3 adds do not reach this run's Silver rows.
# MAGIC - **Performance**: The join is stateless: no state store and no watermark, because each micro-batch is joined on its own. That is what makes it cheap compared with a stream-stream join.

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.types import *

# COMMAND ----------

# MAGIC %md
# MAGIC Import shared variables

# COMMAND ----------

# MAGIC %run ./variables

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ### 🎯 EXERCISE 1: Customer Enrichment
# MAGIC **Your task**: Read the customers dimension (CSV) and join it with the Bronze transaction stream, handling missing customer data gracefully.
# MAGIC
# MAGIC **Requirements:**
# MAGIC - Load customers from `CUSTOMERS_RAW_PATH` (CSV with headers) using schema `customer_id STRING, name STRING, email STRING, country STRING, risk_tier STRING, created_at DATE`. Load the Bronze transaction stream from `TABLE_BRONZE_TRANSACTIONS`.
# MAGIC - Perform a **left join** on `customer_id` so transactions with unknown customers are preserved.
# MAGIC - Keep only `customer_id`, `customer_name` (renamed from `name`), `customer_country` (renamed from `country`) and `risk_tier` in `customers_df` before joining. After the join, fill nulls: "Unknown Customer" for `customer_name`, "Unknown" for `customer_country`. `risk_tier` stays as it is.
# MAGIC
# MAGIC **Aha Moment**: In fintech, dropping a transaction because a customer isn't in your DB yet is a compliance gap  -  every transaction must be traceable, even if the customer profile is incomplete.
# MAGIC
# MAGIC **Hint**: Remember that `spark.read` creates a static DataFrame, while `spark.readStream` creates a streaming DataFrame.
# MAGIC
# MAGIC **Quick check before you go on**: nothing verifies this join until the Silver write in Exercise 4, so in your work cell count the rows and the distinct `customer_id` values of `customers_df`: 110,000 / 110,000. Fewer distinct ids than rows means a duplicated key, and every duplicate would multiply a customer's payments in the join.

# COMMAND ----------

# ✏️ STUDENT EXERCISE

# TODO: Load static customers and perform a left join with null handling
# customers_df = ...
# bronze_txns_stream = ...
# enriched_txns_df = ...

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC **Solution below** ⬇️

# COMMAND ----------

# ✅ SOLUTION: Customer Enrichment

# customer_schema = "customer_id STRING, name STRING, email STRING, country STRING, risk_tier STRING, created_at DATE"
# customers_df = (spark.read
#   .option("header", "true")
#   .schema(customer_schema)
#   .csv(CUSTOMERS_RAW_PATH)
#   .withColumnRenamed("name", "customer_name")
#   .withColumnRenamed("country", "customer_country")
#   .select("customer_id", "customer_name", "customer_country", "risk_tier"))
#
# bronze_txns_stream = (spark.readStream
#   .table(TABLE_BRONZE_TRANSACTIONS))
#
# enriched_txns_df = (bronze_txns_stream
#   .join(customers_df, "customer_id", "left")
#   .withColumn("customer_name", F.coalesce(F.col("customer_name"), F.lit("Unknown Customer")))
#   .withColumn("customer_country", F.coalesce(F.col("customer_country"), F.lit("Unknown"))))

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ### 🎯 EXERCISE 2: Merchant Enrichment with Null Handling
# MAGIC **Your task**: Join the merchants dimension (JSON) and handle records with missing merchant details.
# MAGIC
# MAGIC **Requirements:**
# MAGIC - Load merchants from `MERCHANTS_RAW_PATH` (JSON) into `merchants_df` using schema `merchant_id STRING, name STRING, category_code STRING, country STRING, risk_score INT, onboarded_at DATE`.
# MAGIC - Rename `name` to `merchant_name`, `country` to `merchant_country` and `risk_score` to `merchant_risk_score`, then keep exactly `merchant_id`, `merchant_name`, `category_code`, `merchant_country`, `merchant_risk_score` in `merchants_df`. The cell before Exercise 3 saves this DataFrame as the Silver merchants table; `merchant_risk_score` feeds the risk score in Notebook 03 and `category_code` the daily report in Notebook 04.
# MAGIC - Left join `enriched_txns_df` to `merchants_df` on `merchant_id`, then fill missing `merchant_name` with "Unknown Merchant" and `merchant_country` with "Unknown".
# MAGIC
# MAGIC **Aha Moment**: About 1% of payments go to merchants onboarded after the last dimension export (ids `5000`-`5009`). An inner join silently drops them, so they vanish from the rules engine and every downstream total, and a fraudster who finds that gap has an unmonitored channel. Keeping them as "Unknown Merchant" means they still reach the rules engine and the daily report, and ops can pull `merchant_name = 'Unknown Merchant'` as a review queue. Once a merchant reaches the dimension, its new payments pick up the details; the rows already in Silver keep "Unknown Merchant" until a backfill (a MERGE from the refreshed dimension) repairs them.
# MAGIC
# MAGIC **Quick check before you go on**: count the rows of `merchants_df` in your work cell: 5,000, one per `merchant_id`. The provided cell before Exercise 3 prints the same 5,000 when it saves this DataFrame.

# COMMAND ----------

# ✏️ STUDENT EXERCISE

# TODO: Load static merchants and perform a left join with null handling
# merchants_df = ...
# enriched_full_df = ...

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC **Solution below** ⬇️

# COMMAND ----------

# ✅ SOLUTION: Merchant Enrichment with Null Handling

# merchant_schema = "merchant_id STRING, name STRING, category_code STRING, country STRING, risk_score INT, onboarded_at DATE"
# merchants_df = (spark.read
#   .schema(merchant_schema)
#   .json(MERCHANTS_RAW_PATH)
#   .withColumnRenamed("name", "merchant_name")
#   .withColumnRenamed("country", "merchant_country")
#   .withColumnRenamed("risk_score", "merchant_risk_score")
#   .select("merchant_id", "merchant_name", "category_code", "merchant_country", "merchant_risk_score"))
#
# enriched_full_df = (enriched_txns_df
#   .join(merchants_df, "merchant_id", "left")
#   .withColumn("merchant_name", F.coalesce(F.col("merchant_name"), F.lit("Unknown Merchant")))
#   .withColumn("merchant_country", F.coalesce(F.col("merchant_country"), F.lit("Unknown"))))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Section 2: Dimension Management with MERGE
# MAGIC Merchant details (risk scores, categories) change over time, and new merchants onboard every day. The merchant team sends a daily file with those changes. We'll apply it to our Silver merchants table as an SCD Type 1 update, using Delta Lake's `MERGE INTO`.
# MAGIC
# MAGIC ### Key Concepts
# MAGIC - **SCD Type 1**: Overwrites the old value with the new value (no history maintained).
# MAGIC - **MERGE INTO**: An upsert operation that updates existing records and inserts new ones in a single atomic transaction.

# COMMAND ----------

# MAGIC %md
# MAGIC ### 📦 Provided: Initial Load and Today's Updates
# MAGIC Run the two cells below. The first saves `merchants_df` (your Exercise 2 DataFrame) as `TABLE_SILVER_MERCHANTS`: the dimension as of the last full export, 5,000 merchants. The second reads today's file from the merchant team (`MERCHANT_UPDATES_RAW_PATH`) into `merchant_updates_df`, with the same columns as `merchants_df`: 3 existing merchants re-scored by the risk team and the 10 merchants onboarded since the export (`5000`-`5009`).

# COMMAND ----------

# Initial load: the merchant dimension as of the last full export
merchants_df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(TABLE_SILVER_MERCHANTS)
print(f"✅ {TABLE_SILVER_MERCHANTS}: {spark.read.table(TABLE_SILVER_MERCHANTS).count()} merchants before the MERGE")

# COMMAND ----------

# Today's merchant updates, in the same shape as merchants_df
merchant_updates_df = (spark.read
  .schema("merchant_id STRING, name STRING, category_code STRING, country STRING, risk_score INT, onboarded_at DATE")
  .json(MERCHANT_UPDATES_RAW_PATH)
  .selectExpr("merchant_id", "name AS merchant_name", "category_code",
              "country AS merchant_country", "risk_score AS merchant_risk_score"))

print(f"Merchant updates: {merchant_updates_df.count()} rows")
display(merchant_updates_df.orderBy(F.col("merchant_id").cast("int")))

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ### 🎯 EXERCISE 3: MERGE Merchant Updates
# MAGIC **Your task**: Apply today's updates to the Silver `merchants` table in one atomic operation, leaving the merchants that did not change alone.
# MAGIC
# MAGIC **Requirements:**
# MAGIC - MERGE `merchant_updates_df` (source) into `TABLE_SILVER_MERCHANTS` (target), matching on `merchant_id`.
# MAGIC - When a merchant matches, update all its columns (SCD Type 1: the new risk score replaces the old one).
# MAGIC - When a merchant does not match, insert it.
# MAGIC
# MAGIC **Hint**: You can use the `DeltaTable` API to perform an upsert (merge) operation.

# COMMAND ----------

# ✏️ STUDENT EXERCISE

# TODO: MERGE today's merchant updates into the Silver merchants table
# 1. Load the Silver merchants table as a DeltaTable object
# 2. Merge merchant_updates_df into it, matching on merchant_id
#    Update matched merchants and insert new ones in the same operation

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC **Solution below** ⬇️

# COMMAND ----------

# ✅ SOLUTION: MERGE Merchant Updates

# from delta.tables import DeltaTable
#
# silver_merchants_dt = DeltaTable.forName(spark, TABLE_SILVER_MERCHANTS)
#
# (silver_merchants_dt.alias("target")
#   .merge(merchant_updates_df.alias("source"), "target.merchant_id = source.merchant_id")
#   .whenMatchedUpdateAll()
#   .whenNotMatchedInsertAll()
#   .execute())

# COMMAND ----------

# MAGIC %md
# MAGIC ### 🔍 Verify Your Results
# MAGIC Run the cell below to inspect your output. You should see:
# MAGIC - **Row count**: 5,010 merchants: the 5,000 from the export plus the 10 onboarded since.
# MAGIC - **Before and after** (first grid): the 13 merchants in today's file. The 3 re-scored merchants show `score_before_merge` at 80 or below and `merchant_risk_score` 95. The 10 new merchants (`5000`-`5009`) show `score_before_merge` as `null`, because they were not in the export.
# MAGIC - **MERGE metrics** (second grid): the latest table history entry has `operation` = `MERGE`, and its `operationMetrics` show `numSourceRows` = 13, `numTargetRowsUpdated` = 3 and `numTargetRowsInserted` = 10: the MERGE changed 13 rows, not 5,010. `numTargetRowsCopied` counts unchanged rows that were rewritten only because they share a file with an updated row: 0 when the table has deletion vectors on, the rest of that file when it does not.
# MAGIC - **Ran your MERGE twice?** The second run matches all 13 merchants, so history shows `numTargetRowsUpdated` = 13 and `numTargetRowsInserted` = 0. The table is still right: this MERGE is idempotent because it replaces values on a stable key and you fed it the same 13 rows. A MERGE that adds to a value (`SET total = total + source.total`) would double count on the second run. To see the 3 / 10 split again, re-run the two provided cells above (they reset the table to the export), then your MERGE once.
# MAGIC
# MAGIC **What this means**: Your Silver merchants table is now an updateable dimension: one MERGE applies today's inserts and updates in a single atomic transaction, and only the files that hold a matched row are touched (with deletion vectors MERGE marks the old rows and leaves the rewrite to a later `OPTIMIZE`). A source that holds just today's changes keeps that set small. When the source is a full snapshot instead, an unconditional update-all matches every merchant, so production MERGEs add a change condition to touch only the rows that actually changed.

# COMMAND ----------

# 🔍 Verification query
merchants_result = spark.read.table(TABLE_SILVER_MERCHANTS)
print(f"✅ {TABLE_SILVER_MERCHANTS}: {merchants_result.count()} merchants")

# Today's 13 merchants: risk score in the export (before) next to the score in the Silver table (after)
display(merchant_updates_df.select("merchant_id")
  .join(merchants_result, "merchant_id", "left")
  .join(merchants_df.select("merchant_id", F.col("merchant_risk_score").alias("score_before_merge")), "merchant_id", "left")
  .select("merchant_id", "merchant_name", "score_before_merge", "merchant_risk_score")
  .orderBy(F.col("merchant_id").cast("int")))

display(spark.sql(f"DESCRIBE HISTORY {TABLE_SILVER_MERCHANTS} LIMIT 1").select("version", "operation", "operationMetrics"))

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ### 🎯 EXERCISE 4: Standardize and Write Silver
# MAGIC **Your task**: Finalize the enriched transaction stream by keeping only clean rows, standardizing data, flagging geographic mismatches, and writing to Silver.
# MAGIC
# MAGIC **Requirements:**
# MAGIC - Keep only rows that parsed cleanly (`_rescued_data` IS NULL), then drop the `_rescued_data` column: rescued rows stay in Bronze for the integration team to review, and Silver does not carry a column that is always null.
# MAGIC - Standardize `currency` to uppercase and create a boolean `is_geo_mismatch` flag (true when `ip_country` != `customer_country`).
# MAGIC - Write the resulting stream to `TABLE_SILVER_TRANSACTIONS` using append mode, checkpoint at `CHECKPOINT_PATH + "/silver_txns"`, and `availableNow=True` trigger.
# MAGIC
# MAGIC **Hint**: Same streaming write as Notebook 01 Exercise 3, including waiting for the query to finish before the verification cell.

# COMMAND ----------

# ✏️ STUDENT EXERCISE

# TODO: Keep clean rows, standardize, flag geo-mismatch, and write the enriched stream to Silver
# silver_txns_df = ...
# (silver_txns_df
#   .writeStream
#   ...)

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC **Solution below** ⬇️

# COMMAND ----------

# ✅ SOLUTION: Standardize and Write Silver

# silver_txns_df = (enriched_full_df
#   .filter("_rescued_data IS NULL")
#   .drop("_rescued_data")
#   .withColumn("currency", F.upper(F.col("currency")))
#   .withColumn("is_geo_mismatch", F.col("ip_country") != F.col("customer_country")))
#
# (silver_txns_df
#   .writeStream
#   .format("delta")
#   .outputMode("append")
#   .option("checkpointLocation", CHECKPOINT_PATH + "/silver_txns")
#   .trigger(availableNow=True)
#   .table(TABLE_SILVER_TRANSACTIONS))
#
# for s in spark.streams.active:
#     s.awaitTermination()

# COMMAND ----------

# MAGIC %md
# MAGIC ### 🔍 Verify Your Results
# MAGIC Run the cell below to inspect your output. You should see:
# MAGIC - **Row count**: 50,141 if you ran each cell once: your Bronze count (50,155) minus the 14 rescued rows you counted in Notebook 01 Exercise 4. The left joins drop nothing else.
# MAGIC - **New columns**: `customer_name`, `customer_country`, `risk_tier`, `merchant_name`, `category_code`, `merchant_country`, `merchant_risk_score`, `is_geo_mismatch`.
# MAGIC - **`_rescued_data`**: not among Silver's columns in the grid (you dropped it in requirement 1). If you still see it, add the drop to your work cell and follow the README's recovery step: re-run `00_Setup_Environment.py`, then the notebooks in order. Re-running only the corrected streaming cell does nothing, because its checkpoint already marked the files as processed.
# MAGIC - **`is_geo_mismatch`**: the printed geo-mismatch count is about 2,500 (1 payment in 20): the customer's registered country differs from the IP country, usually travel, VPNs or cross-border online shopping. On its own that is a weak signal, which is why Notebook 03 combines it with other rules.
# MAGIC - **Null handling**: the printed `'Unknown Merchant'` count is about 500 (about 1% of rows): payments at merchants `5000`-`5009`, onboarded after the export you joined against (Exercise 3 inserted them into the Silver merchants table, but this join read the export). `"Unknown Customer"` should not appear here, because every customer id in this data exists in the dimension; the left join is what protects you on the day one doesn't.
# MAGIC - **`currency`**: the printed list is `['USD']`. The gateway settles in USD, and the lowercase `usd` some integrations send is now standardized.
# MAGIC
# MAGIC **What this means**: Your Silver layer now has business-ready, enriched transactions. Every row carries the context needed for fraud detection: who the customer is, who the merchant is, and whether the geography looks suspicious. This is the dataset the rules engine in Notebook 03 will score.

# COMMAND ----------

# 🔍 Verification query
silver_result = spark.read.table(TABLE_SILVER_TRANSACTIONS)
print(f"✅ {TABLE_SILVER_TRANSACTIONS}: {silver_result.count()} rows")
print(f"'Unknown Merchant' rows: {silver_result.filter(F.col('merchant_name') == 'Unknown Merchant').count()}")
print(f"Geo-mismatch rows: {silver_result.filter('is_geo_mismatch').count()}")
print(f"Currencies: {sorted(r.currency for r in silver_result.select('currency').distinct().collect())}")
display(silver_result.limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary and Checkpoint
# MAGIC ### 🎯 Key Concepts Covered
# MAGIC - **Stream-Static Joins**: Efficiently adding customer and merchant data to a live stream.
# MAGIC - **Null Handling**: Keeping payments at not-yet-known merchants, labeled "Unknown Merchant", instead of letting the join drop them.
# MAGIC - **MERGE INTO**: Applying dimension inserts and updates in one atomic transaction.
# MAGIC
# MAGIC ### ✅ Exam Checklist
# MAGIC Can you:
# MAGIC - [ ] Explain why a LEFT join is safer than an INNER join for merchant enrichment?
# MAGIC - [ ] Predict what the next micro-batch sees if a merchant's risk score changes in a static Delta dimension table?
# MAGIC - [ ] Identify the benefits of `MERGE` over `OVERWRITE` for large dimension tables?
# MAGIC
# MAGIC ### 📚 Next Steps
# MAGIC **Notebook 03** covers:
# MAGIC - **Windowed Aggregations** (Tumbling & Sliding) for velocity detection.
# MAGIC - **Multi-Rule Risk Scoring** for fraud detection.
# MAGIC ---
# MAGIC **🎉 Notebook Complete!**
