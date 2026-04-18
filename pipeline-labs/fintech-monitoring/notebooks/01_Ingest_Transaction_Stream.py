# Databricks notebook source
# MAGIC %md
# MAGIC # 01: Ingest Transaction Stream  -  "From Raw Events to Bronze"
# MAGIC **Exam Coverage**: DE Associate (Auto Loader) + DE Professional (Streaming, Watermarks)
# MAGIC **Duration**: 40 minutes
# MAGIC ---
# MAGIC ## Learning Objectives
# MAGIC By the end of this notebook, you will be able to:
# MAGIC - Configure Auto Loader for streaming JSON ingestion with rescued data capture
# MAGIC - Deduplicate payment gateway retries using watermarked dedup
# MAGIC - Build a robust Bronze transaction table
# MAGIC ---

# COMMAND ----------

# MAGIC %md
# MAGIC ## Section 1: Ingesting Raw Payments
# MAGIC ModernPaymentsABC receives transaction data as JSON files via a payment gateway. These files are landed in a cloud volume. To ingest these files incrementally, we use **Auto Loader** (`cloudFiles`).
# MAGIC
# MAGIC ### Key Concepts
# MAGIC - **Auto Loader**: Efficiently ingests new files as they arrive in cloud storage.
# MAGIC - **Rescued Data Column**: Captures records that don't match the schema (e.g., malformed JSON, type mismatches) so they aren't lost.
# MAGIC - **Schema Inference**: Automatically detects the structure of your JSON data.

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
# MAGIC ### 🎯 EXERCISE 1: Configure Auto Loader with Rescue
# MAGIC **Your task**: Set up a streaming DataFrame to read JSON files from the transactions volume using Auto Loader.
# MAGIC
# MAGIC **Requirements:**
# MAGIC - Define the transaction schema using `StructType`. Refer to **`data_contract.md`** in the project root for field names and types.
# MAGIC - Use the `cloudFiles` format with `json` as the source format.
# MAGIC - Provide the schema to the reader.
# MAGIC - Enable the rescued data column to capture malformed records (name it `_rescued_data`).
# MAGIC - Define the schema location for Auto Loader in the `_system` volume (use `CHECKPOINT_PATH + "/schema_bronze"`).
# MAGIC
# MAGIC **Hint**: Make sure to use the `TRANSACTIONS_RAW_PATH` variable defined in `variables.py`.

# COMMAND ----------

# ✏️ STUDENT EXERCISE

# TODO: Define the transaction schema (Refer to data_contract.md)
# txn_schema = StructType([ ... ])

# TODO: Define the streaming reader using cloudFiles
# raw_txns_df = (spark.readStream
#   .format("...")
#   ...)

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC **Solution below** ⬇️

# COMMAND ----------

# ✅ SOLUTION: Configure Auto Loader with Rescue

# txn_schema = StructType([
#     StructField("txn_id", StringType(), True),
#     StructField("customer_id", StringType(), True),
#     StructField("merchant_id", StringType(), True),
#     StructField("amount", DoubleType(), True),
#     StructField("currency", StringType(), True),
#     StructField("channel", StringType(), True),
#     StructField("ip_country", StringType(), True),
#     StructField("timestamp", TimestampType(), True)
# ])
#
# raw_txns_df = (spark.readStream
#   .format("cloudFiles")
#   .option("cloudFiles.format", "json")
#   .schema(txn_schema)
#   .option("cloudFiles.schemaLocation", CHECKPOINT_PATH + "/schema_bronze")
#   .option("cloudFiles.rescuedDataColumn", "_rescued_data")
#   .load(TRANSACTIONS_RAW_PATH))
#
# # display(raw_txns_df, checkpointLocation=CHECKPOINT_PATH + "/display_raw_txns")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 🔄 STEP: Generate New Data
# MAGIC Run the cell below to simulate new payment files arriving in the source volume. This will trigger your streaming pipeline to process new records.

# COMMAND ----------

# MAGIC %run ./data_generator

# COMMAND ----------

# Generate a new batch of data
generate_batch(n_files=2, records_per_file=50)

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ### 🎯 EXERCISE 2: Inspect Rescued Data
# MAGIC **Your task**: Query the `_rescued_data` column to see if any records were malformed during ingestion.
# MAGIC
# MAGIC **Requirements:**
# MAGIC - Filter the `raw_txns_df` for records where `_rescued_data` is NOT null.
# MAGIC - Display the count and the content of the rescued data.
# MAGIC
# MAGIC **Hint**: In a real production environment, you would monitor this count to detect upstream schema changes or integration bugs.

# COMMAND ----------

# ✏️ STUDENT EXERCISE

# TODO: Filter for rescued records
# rescued_df = raw_txns_df.filter("__________ IS NOT NULL")
# display(rescued_df, checkpointLocation=CHECKPOINT_PATH + "/display_rescued")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC **Solution below** ⬇️

# COMMAND ----------

# ✅ SOLUTION: Inspect Rescued Data

# rescued_df = raw_txns_df.filter("_rescued_data IS NOT NULL")
# # display(rescued_df, checkpointLocation=CHECKPOINT_PATH + "/display_rescued")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Section 2: Technical Deduplication
# MAGIC In Fintech, "at-least-once" delivery from payment gateways often results in duplicate records (retries). These are technical duplicates (same `txn_id`) and should be handled at the Bronze layer before business logic is applied.
# MAGIC
# MAGIC ### Key Concepts
# MAGIC - **Watermarks**: A threshold that tells Spark how long to wait for late-arriving data.
# MAGIC - **Deduplication within Watermark**: Removes duplicates based on specific columns within a time window.

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ### 🎯 EXERCISE 3: Deduplicate Payment Retries
# MAGIC **Your task**: Use a watermark and `dropDuplicatesWithinWatermark` to ensure each `txn_id` is only processed once.
# MAGIC
# MAGIC **Requirements:**
# MAGIC - Add a watermark of 5 minutes on the `timestamp` column.
# MAGIC - Remove duplicates based on the transaction ID (`txn_id`) within that watermark.
# MAGIC - This ensures that if the same transaction arrives twice within 5 minutes, only the first one is kept.
# MAGIC
# MAGIC **Hint**: Use the streaming deduplication function that works with watermarks.

# COMMAND ----------

# ✏️ STUDENT EXERCISE

# TODO: Deduplicate the stream
# deduped_txns_df = (raw_txns_df
#   .withWatermark(...)
#   .dropDuplicatesWithinWatermark(...))

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC **Solution below** ⬇️

# COMMAND ----------

# ✅ SOLUTION: Deduplicate Payment Retries

# deduped_txns_df = (raw_txns_df
#   .withWatermark("timestamp", "5 minutes")
#   .dropDuplicatesWithinWatermark(["txn_id"]))

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ### 🎯 EXERCISE 4: Write Bronze Transaction Table
# MAGIC **Your task**: Write the deduplicated stream to a Bronze Delta table.
# MAGIC
# MAGIC **Requirements:**
# MAGIC - Add a column `_ingestion_timestamp` for the current ingestion time.
# MAGIC - Write to the Delta table defined in `TABLE_BRONZE_TRANSACTIONS`.
# MAGIC - Use "append" output mode.
# MAGIC - Define a checkpoint location in the `_system` volume (use `CHECKPOINT_PATH + "/bronze_txns"`).
# MAGIC - Use a trigger to process all available data in one batch (`availableNow=True`).
# MAGIC
# MAGIC **Hint**: Trigger `availableNow=True` is the senior way to process all available data and then stop.

# COMMAND ----------

# ✏️ STUDENT EXERCISE

# TODO: Add ingestion metadata and write to Delta
# (deduped_txns_df
#   ...)

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC **Solution below** ⬇️

# COMMAND ----------

# ✅ SOLUTION: Write Bronze Transaction Table

# (deduped_txns_df
#   .withColumn("_ingestion_timestamp", F.current_timestamp())
#   .writeStream
#   .format("delta")
#   .outputMode("append")
#   .option("checkpointLocation", CHECKPOINT_PATH + "/bronze_txns")
#   .trigger(availableNow=True)
#   .table(TABLE_BRONZE_TRANSACTIONS))
#
# # Wait for the stream to finish before proceeding
# for s in spark.streams.active:
#     s.awaitTermination()

# COMMAND ----------

# MAGIC %md
# MAGIC ### 🔍 Verify Your Results
# MAGIC Run the cell below to inspect your output. You should see:
# MAGIC - **Row count**: ~200+ rows (depends on how many batches you generated  -  each `generate_batch` call adds ~100 transactions).
# MAGIC - **Key columns**: `txn_id`, `customer_id`, `merchant_id`, `amount`, `currency`, `timestamp`, `_rescued_data`, `_ingestion_timestamp`.
# MAGIC - **`_rescued_data`**: Most rows should show `null` here  -  meaning they parsed cleanly. Any non-null values indicate records with unexpected fields.
# MAGIC - **`_ingestion_timestamp`**: Every row should have a timestamp from when YOU ran the pipeline  -  this is your lineage metadata for auditing.
# MAGIC
# MAGIC **What this means**: Your Bronze layer is now a faithful, append-only record of every payment event  -  including retries that were deduplicated and malformed records that were captured (not dropped). This is the foundation every downstream layer depends on.

# COMMAND ----------

# 🔍 Verification query
bronze_result = spark.read.table(TABLE_BRONZE_TRANSACTIONS)
print(f"✅ {TABLE_BRONZE_TRANSACTIONS}: {bronze_result.count()} rows")
display(bronze_result.limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary and Checkpoint
# MAGIC ### 🎯 Key Concepts Covered
# MAGIC - **Auto Loader** with schema inference and rescued data columns.
# MAGIC - **Watermarked Deduplication** to handle "at-least-once" delivery retries.
# MAGIC - **Streaming to Delta** with metadata and checkpoints.
# MAGIC
# MAGIC ### ✅ Exam Checklist
# MAGIC Can you:
# MAGIC - [ ] Identify why the rescued data column is critical for payment pipelines?
# MAGIC - [ ] Explain the difference between `dropDuplicates()` and `dropDuplicatesWithinWatermark()`?
# MAGIC - [ ] Configure a streaming trigger for cost-effective processing?
# MAGIC
# MAGIC ### 📚 Next Steps
# MAGIC **Notebook 02** covers:
# MAGIC - **Stream-Static Joins** for customer and merchant enrichment.
# MAGIC - **MERGE INTO** for merchant risk profile updates.
# MAGIC ---
# MAGIC **🎉 Notebook Complete!**
