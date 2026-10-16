# Databricks notebook source
# MAGIC %md
# MAGIC # 03: Velocity Rules Engine  -  "Detecting Suspicious Patterns"
# MAGIC **Exam Coverage**: DE Professional (Windowing, Watermarks, Advanced Aggregations)
# MAGIC
# MAGIC **Duration**: 90 minutes
# MAGIC
# MAGIC ---
# MAGIC ## Learning Objectives
# MAGIC By the end of this notebook, you will be able to:
# MAGIC - Implement tumbling and sliding window aggregations for velocity detection
# MAGIC - Design multi-dimensional fraud scoring rules
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
# MAGIC - **Watermarks**: Tell Spark how long to wait for late data, so it can finalize a window and drop its state. Append mode refuses an aggregation without one, and without a watermark aggregation state is never dropped.

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
# MAGIC **Your task**: Count transactions per customer in 10-minute tumbling windows, flag customers with > 5 transactions, and save the spikes to their own Silver table.
# MAGIC
# MAGIC **Requirements:**
# MAGIC - Load the Silver transaction stream from `TABLE_SILVER_TRANSACTIONS` into `silver_txns_stream` with no watermark (Exercises 2 and 3 reuse it), then apply a 10-minute watermark on `timestamp` for this aggregation.
# MAGIC - Group by `customer_id` and a 10-minute tumbling window, counting transactions as `txn_count`, and add a boolean flag `is_velocity_spike` that is true when `txn_count` exceeds 5.
# MAGIC - Write only the spike rows to `TABLE_SILVER_VELOCITY_SPIKES`: append mode, checkpoint at `CHECKPOINT_PATH + "/velocity_spikes"`, `availableNow=True` trigger.
# MAGIC
# MAGIC **Hint**: Define the late data threshold on the DataFrame you aggregate (not on `silver_txns_stream` itself), then group by the customer and a time window to count transactions. In append mode, Spark emits a window only once the watermark has passed its end, so every row you write is final. As in Notebook 01, wait for the query to finish before you run the verification cell.

# COMMAND ----------

# ✏️ STUDENT EXERCISE

# TODO: Implement tumbling window aggregation per Requirements
# silver_txns_stream = ...
# tumbling_velocity_df = ...

# TODO: Write the velocity spikes to Silver
# (tumbling_velocity_df
#   ...)

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
# # Persist only the spikes: an availableNow write finishes, an unbounded display() never does
# (tumbling_velocity_df
#   .filter("is_velocity_spike = true")
#   .writeStream
#   .format("delta")
#   .outputMode("append")
#   .option("checkpointLocation", CHECKPOINT_PATH + "/velocity_spikes")
#   .trigger(availableNow=True)
#   .table(TABLE_SILVER_VELOCITY_SPIKES))
#
# for s in spark.streams.active:
#     s.awaitTermination()

# COMMAND ----------

# MAGIC %md
# MAGIC ### 🔍 Verify Your Results
# MAGIC Run the cell below to inspect your output. You should see:
# MAGIC - **Row count**: 5 rows on a fresh setup: customers `9990` to `9994`, each with `txn_count` = 7 in the same 10-minute window. Every one of those 7 payments was an online (card-not-present) payment within 60 seconds. Background customers average half a payment each over the 3 hours of background traffic (a handful make 4 or 5, spread across those hours), so none gets near 5 in one 10-minute window.
# MAGIC - **Customer `9995` is not here**: their six payments split 3 and 3 across two 10-minute windows. Exercise 2 is where they show up.
# MAGIC - **Key columns**: `customer_id`, `window` (a struct with `start` and `end`), `txn_count`, `is_velocity_spike`.
# MAGIC
# MAGIC **What this means**: This is the burst pattern of a compromised account being drained: many payments in minutes. Here each of those 35 payments also trips every per-transaction rule in Exercise 3, so the scoring would catch them anyway. A careful fraudster keeps each payment under the amount and merchant-risk thresholds, and then the burst is the only signal left. That is why velocity lives in its own Silver table, and why Notebook 04 counts these windows per customer in the SAR candidate queue.

# COMMAND ----------

# 🔍 Verification query
velocity_result = spark.read.table(TABLE_SILVER_VELOCITY_SPIKES)
print(f"✅ {TABLE_SILVER_VELOCITY_SPIKES}: {velocity_result.count()} velocity spikes")
display(velocity_result.orderBy(F.desc("txn_count"), "customer_id"))

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ### 🎯 EXERCISE 2: Sliding Window Amount Threshold
# MAGIC **Your task**: Calculate the rolling total transaction amount per customer over a 1-hour sliding window that updates every 5 minutes, and save the breaches to their own Silver table.
# MAGIC
# MAGIC **Requirements:**
# MAGIC - Starting again from `silver_txns_stream`, apply a 1-hour watermark, then group by `customer_id` and a **sliding window** on `timestamp` (size = 1 hour, slide = 5 minutes), summing `amount` as `total_amount`.
# MAGIC - Add a boolean flag `is_amount_threshold_exceeded` that is true when `total_amount` exceeds $5,000.
# MAGIC - Write only the flagged rows to `TABLE_SILVER_AMOUNT_SPIKES`: append mode, checkpoint at `CHECKPOINT_PATH + "/amount_spikes"`, `availableNow=True` trigger.
# MAGIC
# MAGIC **Aha Moment**: A sliding window catches a total that straddles a tumbling boundary: $2,700 in the last minutes of one hour and $2,700 in the first minutes of the next never breach a 1-hour tumbling window.
# MAGIC
# MAGIC **Hint**: Same write pattern as Exercise 1. Wait for the query to finish before the verification cell.

# COMMAND ----------

# ✏️ STUDENT EXERCISE

# TODO: Implement sliding window aggregation per Requirements
# sliding_amount_df = ...

# TODO: Write the amount threshold breaches to Silver
# (sliding_amount_df
#   ...)

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
#
# (sliding_amount_df
#   .filter("is_amount_threshold_exceeded = true")
#   .writeStream
#   .format("delta")
#   .outputMode("append")
#   .option("checkpointLocation", CHECKPOINT_PATH + "/amount_spikes")
#   .trigger(availableNow=True)
#   .table(TABLE_SILVER_AMOUNT_SPIKES))
#
# for s in spark.streams.active:
#     s.awaitTermination()

# COMMAND ----------

# MAGIC %md
# MAGIC ### 🔍 Verify Your Results
# MAGIC Run the cell below to inspect your output. You should see:
# MAGIC - **Row count**: 71 rows on a fresh setup. 60 belong to the same 5 customers (`9990` to `9994`), 12 rows each: their 7-payment burst fits inside 12 overlapping 1-hour windows (one starting every 5 minutes), and every one of those windows breaches the threshold. The other 11 belong to customer `9995`.
# MAGIC - **`total_amount`**: between $6,370 and $6,426 for `9990` to `9994`: 7 payments of $850-$978 each.
# MAGIC - **Customer `9995`**: in no other Notebook 03 table. Six $900 payments, three on each side of the hour: a 1-hour tumbling window sees $2,700 twice and stays silent, the per-transaction score gives each payment 65, and only the sliding window sees $5,400. 11 of the sliding windows hold all six payments.
# MAGIC - **Key columns**: `customer_id`, `window`, `total_amount`, `is_amount_threshold_exceeded`.
# MAGIC
# MAGIC **What this means**: One burst, 12 rows. That is the price of sliding windows: they never miss a pattern that straddles a boundary, but they report it once per overlapping window, and they keep 12 times the window state of a tumbling window. Downstream consumers roll results up by customer, as the SAR candidate queue in Notebook 04 does with these rows and the fraud alerts.

# COMMAND ----------

# 🔍 Verification query
amount_result = spark.read.table(TABLE_SILVER_AMOUNT_SPIKES)
print(f"✅ {TABLE_SILVER_AMOUNT_SPIKES}: {amount_result.count()} amount threshold breaches")
display(amount_result.orderBy(F.desc("total_amount"), "customer_id", "window"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Section 2: Composite Risk Scoring
# MAGIC Real fraud detection doesn't rely on a single rule. The two window tables you just wrote are customer-level signals; now we score every individual transaction by combining its own indicators (geographic mismatch, amount, merchant risk, channel) into a single **Risk Score**.
# MAGIC
# MAGIC ### Key Concepts
# MAGIC - **Composite Score**: A weighted calculation that aggregates multiple risk indicators.
# MAGIC - **Two kinds of signal**: The window tables flag customers; the score flags single payments. In this lab they meet in Notebook 04, where the SAR candidate queue counts each customer's spike windows next to their alerts, so a customer caught only by a window rule (like `9995`) still reaches compliance.

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ### 🎯 EXERCISE 3: Multi-Rule Risk Scoring
# MAGIC **Your task**: Combine risk indicators into a composite score (0-90) and flag transactions for alerting.
# MAGIC
# MAGIC **Requirements:**
# MAGIC - Start with the base `silver_txns_stream` and name the scored result `alerted_txns_df` (Exercise 4 writes it).
# MAGIC - Add a `risk_score` column that sums four weights: +30 when `is_geo_mismatch` is true, +20 when `amount` is above 800, +25 when `merchant_risk_score` is above 80, +15 when `channel` is `'online'`.
# MAGIC - Create a boolean flag `is_alert` that is true when `risk_score` is above 70.
# MAGIC
# MAGIC **Why these indicators**: card-not-present payments carry most card fraud because the card never touches a terminal; a high-risk merchant and an IP outside the customer's country each add evidence. No single one is enough.
# MAGIC
# MAGIC **Hint**: Turn each indicator into its own term (its points when the condition holds, else 0) and add the four terms together.

# COMMAND ----------

# ✏️ STUDENT EXERCISE

# TODO: Define composite scoring logic
# 1. Create a 'risk_score' column using the following weights:
#    - Geographic Mismatch: +30 points (is_geo_mismatch == true)
#    - High Amount: +20 points (amount > 800)
#    - High Risk Merchant: +25 points (merchant_risk_score > 80)
#    - Card-not-present: +15 points (channel == 'online')
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
#       F.expr("case when channel = 'online' then 15 else 0 end")
#   )
#   .withColumn("is_alert", F.col("risk_score") > 70))

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ### 🎯 EXERCISE 4: Write Flagged Alerts
# MAGIC **Your task**: Output high-risk transactions (risk_score > 70) to a dedicated Silver alerting table.
# MAGIC
# MAGIC **Requirements:**
# MAGIC - Filter `alerted_txns_df` for transactions where `is_alert` is true.
# MAGIC - Write the filtered stream to `TABLE_SILVER_ALERTS` using append mode, a checkpoint at `CHECKPOINT_PATH + "/fraud_alerts"`, and `availableNow=True` trigger.
# MAGIC
# MAGIC **Hint**: Same streaming write as Notebook 01 Exercise 3, including waiting for the query to finish before the verification cell.

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
# MAGIC - **Row count**: about 120 rows (somewhere between 100 and 160; the background part depends on the generator's random draws), roughly 0.25% of your 50,141 Silver transactions. With these weights only two combinations clear 70: geo mismatch + amount > 800 + merchant risk > 80 (75 points), or all four rules (90 points). About 90 come from background traffic (5% geo mismatch x about 20% over $800 x about 19% at high-risk merchants), and the 35 payments of customers `9990` to `9994` from the velocity table are all here.
# MAGIC - **Customer `9995` is not here**: each of their payments scores 30 + 20 + 15 = 65 (the merchant is not high risk), below the threshold of 70. Only the sliding window caught them.
# MAGIC - **Key columns**: All Silver transaction columns PLUS `risk_score` and `is_alert`.
# MAGIC - **`risk_score`**: Only two values appear: 75 (three factors fired, everything except the online channel) and 90 (all four fired). About half the rows are 90: the 35 payments of customers `9990` to `9994` plus about a third of the background alerts, because about a third of payments are online.
# MAGIC - **`is_geo_mismatch`**: Every flagged transaction shows `true` here: without the 30 geo points the maximum score is 60.
# MAGIC - **What triggered the alert?**: Cross-reference `is_geo_mismatch`, `amount > 800`, `merchant_risk_score > 80`, and `channel = 'online'` to understand WHY each transaction was flagged.
# MAGIC
# MAGIC **What this means**: In production, this alerts table feeds an ops dashboard where analysts review flagged transactions. Every write here uses `availableNow=True`: each run processes what has landed and stops. Scheduled as a job every few minutes, the same code gives the ops team alerts within minutes instead of the next morning, and you only pay for compute while it runs. The composite scoring approach reduces false positives compared to single-rule systems (a geo mismatch alone isn't enough to trigger an alert).

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
# MAGIC - [ ] Explain when a sliding window is worth its duplicate rows and extra state compared with a tumbling window?
# MAGIC - [ ] Identify the minimum watermark size required if you expect data to be up to 10 minutes late?
# MAGIC - [ ] Describe how a composite score reduces "false positives" compared to single-rule alerts?
# MAGIC
# MAGIC ### 📚 Next Steps
# MAGIC **Notebook 04** covers:
# MAGIC - **Batch Aggregations** for regulatory reporting.
# MAGIC - **Liquid Clustering** for optimized report performance.
# MAGIC ---
# MAGIC **🎉 Notebook Complete!**
