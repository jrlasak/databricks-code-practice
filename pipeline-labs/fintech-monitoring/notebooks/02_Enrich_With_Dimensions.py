# Databricks notebook source
# MAGIC %md
# MAGIC # 02: Enrich With Dimensions  -  "Stream-Static Joins: Adding Business Context"
# MAGIC **Exam Coverage**: DE Associate (Medallion, MERGE) + DE Professional (Stream-Static Joins)
# MAGIC **Duration**: 60 minutes
# MAGIC ---
# MAGIC ## Learning Objectives
# MAGIC By the end of this notebook, you will be able to:
# MAGIC - Enrich streaming transactions with static customer and merchant data
# MAGIC - Handle null foreign keys in stream-static joins
# MAGIC - Implement SCD Type 1 MERGE for merchant profile updates
# MAGIC - Flag geographic mismatches as potential fraud indicators
# MAGIC ---

# COMMAND ----------

# MAGIC %md
# MAGIC ## Section 1: Stream-Static Enrichment
# MAGIC Enriched transaction data allows for more accurate fraud detection. We'll join our transaction stream (Silver) with our customer and merchant dimensions.
# MAGIC
# MAGIC ### Key Concepts
# MAGIC - **Stream-Static Join**: Joins a streaming DataFrame with a static DataFrame.
# MAGIC - **Join Behavior**: The static side is read once when the stream starts.
# MAGIC - **Performance**: Very efficient, as only the streaming side needs to be shuffled.

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
# MAGIC - Rename `name` to `customer_name` and `country` to `customer_country`, selecting only needed columns before joining. Fill null values with defaults: "Unknown Customer" for name and "Unknown" for country.
# MAGIC
# MAGIC **Aha Moment**: In Fintech, dropping a transaction because a customer isn't in your DB yet is a compliance gap  -  every transaction must be traceable, even if the customer profile is incomplete.
# MAGIC
# MAGIC **Hint**: Remember that `spark.read` creates a static DataFrame, while `spark.readStream` creates a streaming DataFrame.

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
# MAGIC ### 🔄 STEP: Generate New Data
# MAGIC Run the cell below to simulate new transactions.

# COMMAND ----------

# MAGIC %run ./data_generator

# COMMAND ----------

# Generate a new batch of data
generate_batch(n_files=2, records_per_file=100)

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ### 🎯 EXERCISE 2: Merchant Enrichment with Null Handling
# MAGIC **Your task**: Join the merchants dimension (JSON) and handle records with missing merchant details.
# MAGIC
# MAGIC **Requirements:**
# MAGIC - Load merchants from `MERCHANTS_RAW_PATH` (JSON) using schema `merchant_id STRING, name STRING, category_code STRING, country STRING, risk_score INT, onboarded_at DATE`.
# MAGIC - Perform a **left join** on `merchant_id` with the enriched stream from Exercise 1, renaming `name` to `merchant_name`, `country` to `merchant_country`, and `risk_score` to `merchant_risk_score`.
# MAGIC - Fill missing `merchant_name` with "Unknown Merchant" and `merchant_country` with "Unknown". Select only the columns you need from the merchants side before joining to avoid column name collisions.
# MAGIC
# MAGIC **Aha Moment**: In Fintech, dropping a transaction because a merchant isn't in your DB yet is a lost revenue event. Seniors route these to a "pending_review" category instead.

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
# MAGIC Merchant details (risk scores, categories) change over time. We'll implement an SCD Type 1 update for our Silver merchants table using Delta Lake's `MERGE INTO`.
# MAGIC
# MAGIC ### Key Concepts
# MAGIC - **SCD Type 1**: Overwrites the old value with the new value (no history maintained).
# MAGIC - **MERGE INTO**: An upsert operation that updates existing records and inserts new ones in a single atomic transaction.

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ### 🎯 EXERCISE 3: MERGE Merchant Updates
# MAGIC **Your task**: Update the Silver `merchants` table with the latest data from the landing volume.
# MAGIC
# MAGIC **Requirements:**
# MAGIC - Save `merchants_df` to `TABLE_SILVER_MERCHANTS` (overwrite to ensure schema consistency).
# MAGIC - Use the Delta `MERGE` operation to update existing merchants and insert new ones.
# MAGIC - Match on `merchant_id`.
# MAGIC - Handle both updates for existing merchants and inserts for new ones in a single operation.
# MAGIC
# MAGIC **Hint**: You can use the `DeltaTable` API to perform an upsert (merge) operation.

# COMMAND ----------

# ✏️ STUDENT EXERCISE

# TODO: Implement the MERGE logic to update the silver merchants table
# 1. Save merchants_df to TABLE_SILVER_MERCHANTS (overwrite mode)
# 2. Load the Silver merchants table as a DeltaTable object
# 3. Merge merchants_df into the DeltaTable, matching on merchant_id
#    Handle both updates for existing rows and inserts for new rows

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC **Solution below** ⬇️

# COMMAND ----------

# ✅ SOLUTION: MERGE Merchant Updates

# merchants_df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(TABLE_SILVER_MERCHANTS)
#
# from delta.tables import DeltaTable
#
# silver_merchants_dt = DeltaTable.forName(spark, TABLE_SILVER_MERCHANTS)
#
# (silver_merchants_dt.alias("target")
#   .merge(merchants_df.alias("source"), "target.merchant_id = source.merchant_id")
#   .whenMatchedUpdateAll()
#   .whenNotMatchedInsertAll()
#   .execute())

# COMMAND ----------

# MAGIC %md
# MAGIC ### 🔍 Verify Your Results
# MAGIC Run the cell below to inspect your output. You should see:
# MAGIC - **Row count**: ~5,000 merchants.
# MAGIC - **Key columns**: `merchant_id`, `name`, `category_code`, `country`, `risk_score`, `onboarded_at`.
# MAGIC - **`risk_score`**: Values between 0-100. Merchants with scores > 80 are considered "high risk"  -  these will factor into fraud detection in Notebook 03.
# MAGIC
# MAGIC **What this means**: Your Silver merchants table is now a live, updateable dimension. When new merchants onboard or risk scores change, the MERGE operation handles both inserts and updates in a single atomic transaction  -  no full table rewrites needed.

# COMMAND ----------

# 🔍 Verification query
merchants_result = spark.read.table(TABLE_SILVER_MERCHANTS)
print(f"✅ {TABLE_SILVER_MERCHANTS}: {merchants_result.count()} merchants")
display(merchants_result.limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ### 🎯 EXERCISE 4: Standardize and Write Silver
# MAGIC **Your task**: Finalize the enriched transaction stream by standardizing data, flagging geographic mismatches, and writing to Silver.
# MAGIC
# MAGIC **Requirements:**
# MAGIC - Standardize `currency` to uppercase and create a boolean `is_geo_mismatch` flag (true when `ip_country` != `customer_country`).
# MAGIC - Write the resulting stream to `TABLE_SILVER_TRANSACTIONS` using append mode, checkpoint at `CHECKPOINT_PATH + "/silver_txns"`, and `availableNow=True` trigger.
# MAGIC
# MAGIC **Hint**: This follows the same streaming write pattern from Notebook 01.

# COMMAND ----------

# ✏️ STUDENT EXERCISE

# TODO: Standardize, flag geo-mismatch, and write the enriched stream to Silver
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
# MAGIC - **Row count**: Same as your Bronze table (enrichment doesn't drop rows thanks to left joins).
# MAGIC - **New columns**: `customer_name`, `customer_country`, `risk_tier`, `merchant_name`, `merchant_country`, `merchant_risk_score`, `is_geo_mismatch`.
# MAGIC - **`is_geo_mismatch`**: Some rows should show `true`  -  meaning the customer's registered country doesn't match the IP country of the transaction. These are potential fraud indicators.
# MAGIC - **Null handling**: Look for `"Unknown Customer"` or `"Unknown Merchant"` values  -  these are transactions where the foreign key didn't match a dimension record. In Fintech, we preserve them instead of dropping.
# MAGIC - **`currency`**: Should all be uppercase (e.g., `USD`, `EUR`) after standardization.
# MAGIC
# MAGIC **What this means**: Your Silver layer now has business-ready, enriched transactions. Every row carries the context needed for fraud detection  -  who the customer is, who the merchant is, and whether the geography looks suspicious. This is the dataset the rules engine in Notebook 03 will score.

# COMMAND ----------

# 🔍 Verification query
silver_result = spark.read.table(TABLE_SILVER_TRANSACTIONS)
print(f"✅ {TABLE_SILVER_TRANSACTIONS}: {silver_result.count()} rows")
display(silver_result.limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary and Checkpoint
# MAGIC ### 🎯 Key Concepts Covered
# MAGIC - **Stream-Static Joins**: Efficiently adding customer and merchant data to a live stream.
# MAGIC - **Null Handling**: Routing unknown merchants to a review category instead of dropping data.
# MAGIC - **MERGE INTO**: Upserting dimension updates without full table rewrites.
# MAGIC
# MAGIC ### ✅ Exam Checklist
# MAGIC Can you:
# MAGIC - [ ] Explain why a LEFT join is safer than an INNER join for merchant enrichment?
# MAGIC - [ ] Predict what happens if you add a new merchant to the source volume while the stream is running?
# MAGIC - [ ] Identify the benefits of `MERGE` over `OVERWRITE` for large dimension tables?
# MAGIC
# MAGIC ### 📚 Next Steps
# MAGIC **Notebook 03** covers:
# MAGIC - **Windowed Aggregations** (Tumbling & Sliding) for velocity detection.
# MAGIC - **Multi-Rule Risk Scoring** for fraud detection.
# MAGIC ---
# MAGIC **🎉 Notebook Complete!**
