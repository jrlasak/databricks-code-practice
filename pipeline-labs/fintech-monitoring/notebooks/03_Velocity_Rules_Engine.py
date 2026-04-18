# Databricks notebook source
# MAGIC %md
# MAGIC # 03: Velocity Rules Engine  -  "Detecting Suspicious Patterns"
# MAGIC **Exam Coverage**: DE Professional (Windowing, Watermarks, Advanced Aggregations)
# MAGIC **Duration**: 55 minutes
# MAGIC ---
# MAGIC ## Learning Objectives
# MAGIC By the end of this notebook, you will be able to:
# MAGIC - Implement tumbling and sliding window aggregations for velocity detection
# MAGIC - Design multi-dimensional fraud scoring rules using watermarks
# MAGIC - Understand how window type affects detection accuracy
# MAGIC - Build a real-time alerting stream for high-risk transactions
# MAGIC ---

# COMMAND ----------

# MAGIC %md
# MAGIC ## Section 1: Windowed Velocity Detection
# MAGIC Fraud often manifests as a "velocity spike"  -  many transactions from the same customer or merchant in a very short time. To detect this, we use **Windowed Aggregations**.
# MAGIC
# MAGIC ### Key Concepts
# MAGIC - **Tumbling Windows**: Fixed-size, non-overlapping time intervals (e.g., 12:00-12:10, 12:10-12:20).
# MAGIC - **Sliding Windows**: Overlapping intervals that "slide" over the data (e.g., a 10-minute window that slides every 5 minutes).
# MAGIC - **Watermarks**: Required for stateful aggregations to know when late data can be discarded.

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
# MAGIC ### 🎯 EXERCISE 1: Tumbling Window Velocity
# MAGIC **Your task**: Count transactions per customer in 10-minute tumbling windows and flag customers with > 5 transactions.
# MAGIC
# MAGIC **Requirements:**
# MAGIC - Load the Silver transaction stream from `TABLE_SILVER_TRANSACTIONS` with a 10-minute watermark on `timestamp`.
# MAGIC - Group by `customer_id` and a 10-minute tumbling window, counting transactions as `txn_count`.
# MAGIC - Add a boolean flag `is_velocity_spike` that is true when `txn_count` exceeds 5.
# MAGIC
# MAGIC **Hint**: First define a late data threshold on the stream, then group by the customer and a time window to count transactions.

# COMMAND ----------

# ✏️ STUDENT EXERCISE

# TODO: Implement tumbling window aggregation per Requirements
# silver_txns_stream = ...
# tumbling_velocity_df = ...

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC **Solution below** ⬇️

# COMMAND ----------

# ✅ SOLUTION: Tumbling Window Velocity

# silver_txns_stream = (spark.readStream
#   .table(TABLE_SILVER_TRANSACTIONS))
#
# tumbling_velocity_df = (silver_txns_stream
#   .withWatermark("timestamp", "10 minutes")
#   .groupBy("customer_id", F.window("timestamp", "10 minutes"))
#   .agg(F.count("*").alias("txn_count"))
#   .withColumn("is_velocity_spike", F.col("txn_count") > 5))
#
# # In a notebook, display() starts a temporary stream
# # display(tumbling_velocity_df, checkpointLocation=CHECKPOINT_PATH + "/display_tumbling_velocity")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 🔄 STEP: Generate New Data
# MAGIC Run the cell below to simulate new transactions.

# COMMAND ----------

# MAGIC %run ./data_generator

# COMMAND ----------

# Generate a new batch of data
generate_batch(n_files=3, records_per_file=100)

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ### 🎯 EXERCISE 2: Sliding Window Amount Threshold
# MAGIC **Your task**: Calculate the rolling total transaction amount per customer over a 1-hour sliding window that updates every 5 minutes.
# MAGIC
# MAGIC **Requirements:**
# MAGIC - Using the same Silver stream (with a 1-hour watermark), group by `customer_id` and a **sliding window** on `timestamp` (size = 1 hour, slide = 5 minutes).
# MAGIC - Sum the transaction `amount` as `total_amount`.
# MAGIC - Add a boolean flag `is_amount_threshold_exceeded` that is true when `total_amount` exceeds $5,000.
# MAGIC
# MAGIC **Aha Moment**: A sliding window catches patterns that straddle tumbling boundaries (e.g., 3 txns at the end of one tumbling window and 3 at the start of the next).

# COMMAND ----------

# ✏️ STUDENT EXERCISE

# TODO: Implement sliding window aggregation per Requirements
# sliding_amount_df = ...

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC **Solution below** ⬇️

# COMMAND ----------

# ✅ SOLUTION: Sliding Window Amount Threshold

# sliding_amount_df = (silver_txns_stream
#   .withWatermark("timestamp", "1 hour")
#   .groupBy("customer_id", F.window("timestamp", "1 hour", "5 minutes"))
#   .agg(F.sum("amount").alias("total_amount"))
#   .withColumn("is_amount_threshold_exceeded", F.col("total_amount") > 5000))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Section 2: Composite Risk Scoring
# MAGIC Real fraud detection doesn't rely on a single rule. We'll combine our velocity metrics, amount thresholds, and geographic mismatch flags into a single **Risk Score**.
# MAGIC
# MAGIC ### Key Concepts
# MAGIC - **Composite Score**: A weighted calculation that aggregates multiple risk indicators.
# MAGIC - **Joining Streams**: In a real scenario, you would join these aggregated results back to individual transactions to flag them. (For this lab, we'll simplify by applying scoring directly).

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ### 🎯 EXERCISE 3: Multi-Rule Risk Scoring
# MAGIC **Your task**: Combine risk indicators into a composite score (0-100) and flag transactions for alerting.
# MAGIC
# MAGIC **Requirements:**
# MAGIC - Start with the base `silver_txns_stream`.
# MAGIC - Add a `risk_score` column with weighted logic for indicators like geographic mismatch, high amount, merchant risk, and channel.
# MAGIC - Create a boolean flag `is_alert` based on a risk score threshold.
# MAGIC
# MAGIC **Hint**: You can use conditional logic to sum up the scores for different indicators.

# COMMAND ----------

# ✏️ STUDENT EXERCISE

# TODO: Define composite scoring logic
# 1. Create a 'risk_score' column using the following weights:
#    - Geographic Mismatch: +30 points (is_geo_mismatch == true)
#    - High Amount: +20 points (amount > 800)
#    - High Risk Merchant: +25 points (merchant_risk_score > 80)
#    - Mobile Channel: +15 points (channel == 'mobile')
# 2. Add an 'is_alert' boolean flag (risk_score > 70)

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC **Solution below** ⬇️

# COMMAND ----------

# ✅ SOLUTION: Multi-Rule Risk Scoring

# alerted_txns_df = (silver_txns_stream
#   .withColumn("risk_score", 
#       F.expr("case when is_geo_mismatch then 30 else 0 end") +
#       F.expr("case when amount > 800 then 20 else 0 end") +
#       F.expr("case when merchant_risk_score > 80 then 25 else 0 end") +
#       F.expr("case when channel = 'mobile' then 15 else 0 end")
#   )
#   .withColumn("is_alert", F.col("risk_score") > 70))

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ### 🎯 EXERCISE 4: Write Flagged Alerts
# MAGIC **Your task**: Output high-risk transactions (risk_score > 70) to a dedicated Silver alerting table.
# MAGIC
# MAGIC **Requirements:**
# MAGIC - Filter the scored stream for transactions where `is_alert` is true.
# MAGIC - Write the filtered stream to `TABLE_SILVER_ALERTS` using append mode, a checkpoint at `CHECKPOINT_PATH + "/fraud_alerts"`, and `availableNow=True` trigger.
# MAGIC
# MAGIC **Hint**: This follows the same write pattern you used in Notebook 01 for writing to Bronze.

# COMMAND ----------

# ✏️ STUDENT EXERCISE

# TODO: 
# 1. Filter for transactions where is_alert is true
# 2. Write the resulting stream to TABLE_SILVER_ALERTS using availableNow trigger

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC **Solution below** ⬇️

# COMMAND ----------

# ✅ SOLUTION: Write Flagged Alerts

# (alerted_txns_df
#   .filter("is_alert = true")
#   .writeStream
#   .format("delta")
#   .outputMode("append")
#   .option("checkpointLocation", CHECKPOINT_PATH + "/fraud_alerts")
#   .trigger(availableNow=True)
#   .table(TABLE_SILVER_ALERTS))
#
# for s in spark.streams.active:
#     s.awaitTermination()

# COMMAND ----------

# MAGIC %md
# MAGIC ### 🔍 Verify Your Results
# MAGIC Run the cell below to inspect your output. You should see:
# MAGIC - **Row count**: A fraction of your total Silver transactions  -  only the ones with `risk_score > 70` (typically 5-15% depending on data distribution).
# MAGIC - **Key columns**: All Silver transaction columns PLUS `risk_score` and `is_alert`.
# MAGIC - **`risk_score`**: Every row should be > 70 (that's your filter). Look at the values  -  a score of 75 means 3 risk factors triggered, while 90 means nearly all factors fired.
# MAGIC - **`is_geo_mismatch`**: Most flagged transactions should show `true` here (worth +30 points alone).
# MAGIC - **What triggered the alert?**: Cross-reference `is_geo_mismatch`, `amount > 800`, `merchant_risk_score > 80`, and `channel = 'mobile'` to understand WHY each transaction was flagged.
# MAGIC
# MAGIC **What this means**: You've just built a real-time fraud detection pipeline. In production, this alerts table would feed an ops dashboard where analysts review flagged transactions within minutes  -  not the next morning. The composite scoring approach reduces false positives compared to single-rule systems (a geo mismatch alone isn't enough to trigger an alert).

# COMMAND ----------

# 🔍 Verification query
alerts_result = spark.read.table(TABLE_SILVER_ALERTS)
print(f"✅ {TABLE_SILVER_ALERTS}: {alerts_result.count()} flagged transactions")
display(alerts_result.limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary and Checkpoint
# MAGIC ### 🎯 Key Concepts Covered
# MAGIC - **Windowed Aggregations**: Tumbling and sliding windows for time-based velocity detection.
# MAGIC - **Watermarks**: Managing stream state for late-arriving data.
# MAGIC - **Risk Scoring Engine**: Combining multiple signals into a single actionable metric.
# MAGIC
# MAGIC ### ✅ Exam Checklist
# MAGIC Can you:
# MAGIC - [ ] Explain why sliding windows are superior for fraud detection despite higher compute costs?
# MAGIC - [ ] Identify the minimum watermark size required if you expect data to be up to 10 minutes late?
# MAGIC - [ ] Describe how a composite score reduces "false positives" compared to single-rule alerts?
# MAGIC
# MAGIC ### 📚 Next Steps
# MAGIC **Notebook 04** covers:
# MAGIC - **Batch Aggregations** for regulatory reporting.
# MAGIC - **Liquid Clustering** for optimized report performance.
# MAGIC ---
# MAGIC **🎉 Notebook Complete!**
