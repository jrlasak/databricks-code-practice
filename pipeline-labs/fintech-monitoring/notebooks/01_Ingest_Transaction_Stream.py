# Databricks notebook source
# MAGIC %md
# MAGIC # 01: Ingest Transaction Stream  -  "From Raw Events to Bronze"
# MAGIC **Exam Coverage**: DE Associate (Auto Loader) + DE Professional (Streaming, Watermarks)
# MAGIC
# MAGIC **Duration**: 75 minutes
# MAGIC
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
# MAGIC - **Rescued Data Column**: Keeps values that do not fit your schema (a type mismatch, a field the schema does not have, a column-name case mismatch) as a JSON blob in `_rescued_data`, instead of losing them as silent NULLs.
# MAGIC - **Explicit schema**: You hand Auto Loader the schema from the data contract instead of letting it infer one, so a gateway that starts sending `amount` as text cannot silently change your column type. With a provided schema Auto Loader does not evolve it (new columns are ignored) and adds no rescued data column unless you ask for one, which is why Exercise 1 sets it explicitly.

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
# MAGIC - Define the transaction schema as a `StructType` with the field names and types of the Bronze `transactions` table in **`data_contract.md`** (project root).
# MAGIC - Use the `cloudFiles` format with `json` as the source format, passing your schema and an Auto Loader schema location.
# MAGIC - Enable the rescued data column (name it `_rescued_data`) so values that do not match the schema are kept instead of silently nulled.
# MAGIC
# MAGIC **Hint**: Read from `TRANSACTIONS_RAW_PATH` and put the schema location in the `_system` volume at `CHECKPOINT_PATH + "/schema_bronze"` (both defined in `variables.py`).

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
#   .option("rescuedDataColumn", "_rescued_data")
#   .load(TRANSACTIONS_RAW_PATH))

# COMMAND ----------

# MAGIC %md
# MAGIC ### 🔄 STEP: Generate New Data
# MAGIC Run the two cells below to land 2 more payment files (100 transactions) in the source volume. Nothing processes them yet: your Exercise 3 write picks them up together with the setup files, because Auto Loader records every file it has already ingested in its checkpoint and processes any file it has not seen yet.

# COMMAND ----------

# MAGIC %run ./data_generator

# COMMAND ----------

# Generate a new batch of data
generate_batch(n_files=2, records_per_file=50)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Section 2: Technical Deduplication
# MAGIC In fintech, "at-least-once" delivery from payment gateways often results in duplicate records (retries). These are technical duplicates (same `txn_id`) and should be handled at the Bronze layer before business logic is applied.
# MAGIC
# MAGIC ### Key Concepts
# MAGIC - **Watermarks**: A threshold that tells Spark how long to wait for late-arriving data.
# MAGIC - **Deduplication within Watermark**: Removes duplicates based on specific columns within a time window.
# MAGIC - **Why not plain `dropDuplicates`**: on a stream it remembers every `txn_id` it has ever seen. At 500K payments a day that state grows until the job runs out of memory. With a watermark, Spark drops a `txn_id` from state once the watermark passes it, so state holds only a few minutes of traffic.

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ### 🎯 EXERCISE 2: Deduplicate Payment Retries
# MAGIC **Your task**: Make sure each `txn_id` reaches Bronze only once, using a watermark and Spark's watermark-aware streaming deduplication.
# MAGIC
# MAGIC **Requirements:**
# MAGIC - Add a watermark of 5 minutes on the `timestamp` column.
# MAGIC - Remove duplicates on the transaction ID (`txn_id`) alone, within that watermark: two copies of a `txn_id` whose `timestamp` values are less than 5 minutes apart are kept once (the first copy Spark processes).
# MAGIC
# MAGIC **Why `txn_id` alone, and why 5 minutes**: the gateway stamps each retry about a minute after the original, so `timestamp` cannot be part of the dedup key. A 5-minute watermark covers every retry and lets Spark forget a `txn_id` soon after.
# MAGIC
# MAGIC **Hint**: Use the streaming deduplication function that works with watermarks.

# COMMAND ----------

# ✏️ STUDENT EXERCISE

# TODO: Deduplicate the stream
# deduped_txns_df = (raw_txns_df
#   ...)

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
# MAGIC ### 🎯 EXERCISE 3: Write Bronze Transaction Table
# MAGIC **Your task**: Write the deduplicated stream to a Bronze Delta table.
# MAGIC
# MAGIC **Requirements:**
# MAGIC - Add a column `_ingestion_timestamp` for the current ingestion time.
# MAGIC - Append to the Delta table defined in `TABLE_BRONZE_TRANSACTIONS`, with the checkpoint in the `_system` volume at `CHECKPOINT_PATH + "/bronze_txns"`.
# MAGIC - Use a trigger that processes all available data and then stops (`availableNow=True`).
# MAGIC
# MAGIC **Hint**: A streaming write starts in the background and the cell returns at once. Block until the query has finished (the query handle you get back has a method for that) before you run the verification cell. Otherwise it can read a table that does not exist yet or is only half written. Notebooks 02 and 03 reuse this pattern.

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
# MAGIC - **Row count**: 50,155 rows if you ran each cell once: 50,000 transactions from the 5 setup batches, 41 transactions from 6 suspicious customers (`9990`-`9995`) that setup also landed, 100 from this notebook's `generate_batch` call, and 14 records with values that did not match the schema (2 per generated file). The 1,002 gateway retries (2% of each batch, same `txn_id` one minute later) are gone: that is your deduplication at work.
# MAGIC - **Key columns**: `txn_id`, `customer_id`, `merchant_id`, `amount`, `currency`, `timestamp`, `_rescued_data`, `_ingestion_timestamp`.
# MAGIC - **`_rescued_data`**: Almost every row shows `null` here  -  meaning it parsed cleanly. Non-null values are records with a type mismatch or an unexpected field; Exercise 4 inspects them.
# MAGIC - **`_ingestion_timestamp`**: Every row should have a timestamp from when YOU ran the pipeline  -  this is your lineage metadata for auditing.
# MAGIC
# MAGIC **What this means**: Your Bronze layer is now an append-only record of the payment events, each exactly once (the dedup dropped the gateway retries), with values that did not match the schema rescued instead of lost. The watermark has a cost: the dedup may drop a payment whose `timestamp` is more than 5 minutes behind the latest one it has seen, so in production you size the watermark to the gateway's real delivery delay, not just to the retry gap.

# COMMAND ----------

# 🔍 Verification query
bronze_result = spark.read.table(TABLE_BRONZE_TRANSACTIONS)
print(f"✅ {TABLE_BRONZE_TRANSACTIONS}: {bronze_result.count()} rows")
display(bronze_result.limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ### 🎯 EXERCISE 4: Inspect Rescued Data
# MAGIC **Your task**: Now that Bronze is written, query it to see which records did not match the schema during ingestion.
# MAGIC
# MAGIC **Requirements:**
# MAGIC - Read `TABLE_BRONZE_TRANSACTIONS` as a batch (static) DataFrame and keep only the rows where `_rescued_data` is NOT null.
# MAGIC - Print the count of rescued rows and display them.
# MAGIC
# MAGIC **🔍 What you should see**: 14 rescued rows, 2 from every generated file (5 setup batches + 2 files from this notebook). Their `txn_id` starts with `MALF_`:
# MAGIC - `..._1` (customer `9998`) sent `amount` as the text `"one hundred"`. The contract says double, so `amount` is `null` and the original value sits in `_rescued_data`.
# MAGIC - `..._2` (customer `9999`) sent a field the contract does not have (`unexpected_nested`). The rest of the row parsed normally; the extra field sits in `_rescued_data`.
# MAGIC - A line that is not valid JSON at all would not appear here: rescue keeps values from records the parser could read. Truly corrupt lines need a separate mechanism (a corrupt-record column or `badRecordsPath`).
# MAGIC
# MAGIC **Why a batch query**: a streaming display never finishes on its own. Bronze already holds every rescued record, so a batch read gives you the count and the rows in seconds.
# MAGIC
# MAGIC **In production**: monitor this count; a jump means an upstream schema change or an integration bug.

# COMMAND ----------

# ✏️ STUDENT EXERCISE

# TODO: Read Bronze as a batch DataFrame and keep only the rescued records
# rescued_df = ...

# TODO: Print the count and display the rescued rows

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC **Solution below** ⬇️

# COMMAND ----------

# ✅ SOLUTION: Inspect Rescued Data

# rescued_df = (spark.read
#   .table(TABLE_BRONZE_TRANSACTIONS)
#   .filter("_rescued_data IS NOT NULL"))
#
# print(f"Rescued records in Bronze: {rescued_df.count()}")
# display(rescued_df.select("txn_id", "customer_id", "amount", "timestamp", "_rescued_data"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary and Checkpoint
# MAGIC ### 🎯 Key Concepts Covered
# MAGIC - **Auto Loader** with an explicit schema and an opt-in rescued data column.
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