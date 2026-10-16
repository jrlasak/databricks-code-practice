# Databricks notebook source
# MAGIC %md
# MAGIC # 04: Regulatory Reporting  -  "Gold Layer: From Alerts to Action"
# MAGIC **Exam Coverage**: DE Associate (Medallion, Gold) + DE Professional (Optimization, Liquid Clustering)
# MAGIC
# MAGIC **Duration**: 60 minutes
# MAGIC
# MAGIC ---
# MAGIC ## Learning Objectives
# MAGIC By the end of this notebook, you will be able to:
# MAGIC - Build batch aggregation tables for regulatory reporting
# MAGIC - Build a daily summary per merchant category (MCC) from Silver transactions
# MAGIC - Build a SAR candidate queue that combines per-payment alerts with window-rule hits
# MAGIC - Understand when Gold should be streaming vs. batch
# MAGIC - Lay out Gold tables with Liquid Clustering so date and category filters skip files as the table grows
# MAGIC ---

# COMMAND ----------

# MAGIC %md
# MAGIC ## Section 1: Daily Category Summary
# MAGIC The risk and finance teams start every morning with yesterday's volume per merchant category (MCC): how many payments, how much money, and the average ticket. A category whose volume or average ticket jumps overnight is where the next investigation starts.
# MAGIC
# MAGIC ### Key Concepts
# MAGIC - **Batch Gold Layer**: For regulatory reports that are only needed daily, scheduled batch processing is more cost-effective than continuous streaming.
# MAGIC - **Aggregation Functions**: Counts, sums and averages per group: the business-ready numbers a daily report needs.

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
# MAGIC ### 🎯 EXERCISE 1: Daily Transaction Summary
# MAGIC **Your task**: Aggregate transactions by merchant category for a daily summary report.
# MAGIC
# MAGIC **Requirements:**
# MAGIC - Load Silver transactions (static) from `TABLE_SILVER_TRANSACTIONS` and derive `report_date` by casting `timestamp` to date.
# MAGIC - Group by `report_date` and `category_code`, calculating `total_txns` (count), `total_amount` (sum), and `avg_amount` (average).
# MAGIC
# MAGIC **Hint**: For a daily report, read the whole Delta table as a static (batch) DataFrame  -  no streaming needed.

# COMMAND ----------

# ✏️ STUDENT EXERCISE

# TODO: Load silver transactions and perform daily aggregation
# silver_txns_df = ...
# daily_summary_df = ...

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC **Solution below** ⬇️

# COMMAND ----------

# ✅ SOLUTION: Daily Transaction Summary

# silver_txns_df = (spark.read.table(TABLE_SILVER_TRANSACTIONS))
#
# daily_summary_df = (silver_txns_df
#   .withColumn("report_date", F.to_date(F.col("timestamp")))
#   .groupBy("report_date", "category_code")
#   .agg(
#       F.count("*").alias("total_txns"),
#       F.sum("amount").alias("total_amount"),
#       F.avg("amount").alias("avg_amount")
#   ))

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ### 🎯 EXERCISE 2: Optimize with Liquid Clustering
# MAGIC **Your task**: Write the daily summary to a Delta table and enable **Liquid Clustering** to optimize for reports.
# MAGIC
# MAGIC **Requirements:**
# MAGIC - Write `daily_summary_df` to `TABLE_GOLD_DAILY_SUMMARY`.
# MAGIC - Use the `overwrite` save mode.
# MAGIC - Enable Liquid Clustering on the `report_date` and `category_code` columns.
# MAGIC
# MAGIC **Hint**: Liquid Clustering replaces partitioning and ZORDER and is Databricks' recommended layout for new Delta tables. You set the clustering columns on the writer, before saving the table.

# COMMAND ----------

# ✏️ STUDENT EXERCISE

# TODO: Write to Gold with Liquid Clustering enabled on report_date and category_code
# (daily_summary_df
#   .write
#   ...)

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC **Solution below** ⬇️

# COMMAND ----------

# ✅ SOLUTION: Optimize with Liquid Clustering

# (daily_summary_df
#   .write.mode("overwrite")
#   .clusterBy("report_date", "category_code")
#   .saveAsTable(TABLE_GOLD_DAILY_SUMMARY))

# COMMAND ----------

# MAGIC %md
# MAGIC ### 🔍 Verify Your Results
# MAGIC Run the cell below to inspect your output. You should see:
# MAGIC - **Row count**: One row per `(report_date, category_code)` combination: 7 per date (6 merchant categories plus one `null` category for payments at merchants not yet in the dimension). Your data spans about 6 hours (the payments of customers `9990`-`9995` sit one to three hours before the background traffic), so expect 7 rows when it all falls on one date. If it crosses midnight, each date gets one row per category it has payments in, up to 14 rows.
# MAGIC - **Key columns**: `report_date`, `category_code`, `total_txns`, `total_amount`, `avg_amount`.
# MAGIC - **`category_code`**: The six Merchant Category Codes the generator uses: `5411` (grocery stores), `5812` (restaurants), `5912` (drug stores), `4814` (telecom services), `7011` (hotels), `4121` (taxis and rideshare). Each category has its own daily summary.
# MAGIC - **`total_amount`** is net: about 1% of payments are $0 voids or negative refunds that the gateway sends through the same feed.
# MAGIC - **`avg_amount`**: Close to $500 in every category, because this synthetic gateway draws amounts uniformly up to $1,000 regardless of category. In production data, this column is where category differences show up (a hotel stay averages far more than a pharmacy visit).
# MAGIC - **Clustering columns**: `['report_date', 'category_code']`. An empty list means the table was written without Liquid Clustering: fix your write and run it again.
# MAGIC
# MAGIC **What this means**: This is the table a compliance dashboard queries: a handful of pre-aggregated rows instead of ~50,000 Silver transactions, which is why it answers in under a second. Liquid Clustering on `report_date` and `category_code` is what keeps it fast once it holds years of dates, because filters on those keys skip files. At 7 rows it changes nothing yet: a write this small is not clustered on write, and `OPTIMIZE` (or Predictive Optimization) clusters the data as the table grows.

# COMMAND ----------

# 🔍 Verification query
daily_result = spark.read.table(TABLE_GOLD_DAILY_SUMMARY)
print(f"✅ {TABLE_GOLD_DAILY_SUMMARY}: {daily_result.count()} rows")
clustering_cols = spark.sql(f"DESCRIBE DETAIL {TABLE_GOLD_DAILY_SUMMARY}").first()["clusteringColumns"]
print(f"Clustering columns: {clustering_cols}")
display(daily_result)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Section 2: SAR Candidate Queue
# MAGIC Compliance analysts decide, case by case, whether a customer's activity needs a Suspicious Activity Report (SAR). What they need every morning is the queue of candidates, pre-filled with who, how much and when. Some candidates come from per-payment alerts; others only from the window rules in Notebook 03, so the queue needs both.
# MAGIC
# MAGIC ### Key Concepts
# MAGIC - **SAR Pre-Fill**: Rolling alerts and window-rule hits up by customer to show the full scope of their suspicious behavior, with the date range a SAR asks for.
# MAGIC - **Full outer join**: Keeps a customer who appears in only one of the inputs, which is exactly the customer a single-rule view would miss.
# MAGIC - **Aha Moment**: Silver alerts stream; the Gold SAR dataset is batch. Pick the latency per consumer: ops needs alerts within minutes, compliance needs a daily queue.

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ### 🎯 EXERCISE 3: SAR Candidate Queue
# MAGIC **Your task**: Build one row per suspicious customer from the fraud alerts and the two spike tables, so compliance analysts see every candidate and why it is there.
# MAGIC
# MAGIC **Requirements:**
# MAGIC - Roll up the Silver alerts (`TABLE_SILVER_ALERTS`, read as a batch) per `customer_id`: `total_incidents` (count), `total_suspicious_amount` (sum of amount), `max_risk_score` (max of risk_score), and the activity window `first_activity_ts` / `last_activity_ts` (min / max of `timestamp`).
# MAGIC - Count the rows per `customer_id` in `TABLE_SILVER_VELOCITY_SPIKES` as `velocity_spike_windows` and in `TABLE_SILVER_AMOUNT_SPIKES` as `amount_spike_windows`.
# MAGIC - Full outer join the three on `customer_id`, so a customer caught only by a window rule still appears, and fill the missing counts, amounts and scores with 0 (not null).
# MAGIC
# MAGIC **Hint**: Each input becomes one row per customer before the joins. Join on the column name, not on an equality condition: with a condition, a full outer join keeps both `customer_id` columns, and a customer who exists on only one side gets a null id in the other. Write the result to `TABLE_GOLD_SAR_PREFILL` in overwrite mode, as in Exercise 2.

# COMMAND ----------

# ✏️ STUDENT EXERCISE

# TODO: Build the SAR candidate queue
# 1. Roll up static Silver alerts per customer_id:
#    total_incidents, total_suspicious_amount, max_risk_score, first_activity_ts, last_activity_ts
# 2. Count spike rows per customer_id: velocity_spike_windows, amount_spike_windows
# 3. Full outer join the three on customer_id and fill missing numbers with 0
# 4. Write the result to TABLE_GOLD_SAR_PREFILL in overwrite mode
# sar_prefill_df = ...

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC **Solution below** ⬇️

# COMMAND ----------

# ✅ SOLUTION: SAR Candidate Queue

# alert_rollup_df = (spark.read.table(TABLE_SILVER_ALERTS)
#   .groupBy("customer_id")
#   .agg(
#       F.count("*").alias("total_incidents"),
#       F.sum("amount").alias("total_suspicious_amount"),
#       F.max("risk_score").alias("max_risk_score"),
#       F.min("timestamp").alias("first_activity_ts"),
#       F.max("timestamp").alias("last_activity_ts")
#   ))
#
# velocity_counts_df = (spark.read.table(TABLE_SILVER_VELOCITY_SPIKES)
#   .groupBy("customer_id")
#   .agg(F.count("*").alias("velocity_spike_windows")))
#
# amount_counts_df = (spark.read.table(TABLE_SILVER_AMOUNT_SPIKES)
#   .groupBy("customer_id")
#   .agg(F.count("*").alias("amount_spike_windows")))
#
# sar_prefill_df = (alert_rollup_df
#   .join(velocity_counts_df, "customer_id", "full_outer")
#   .join(amount_counts_df, "customer_id", "full_outer")
#   .na.fill(0, ["total_incidents", "total_suspicious_amount", "max_risk_score",
#                "velocity_spike_windows", "amount_spike_windows"]))
#
# (sar_prefill_df
#   .write.mode("overwrite")
#   .saveAsTable(TABLE_GOLD_SAR_PREFILL))

# COMMAND ----------

# MAGIC %md
# MAGIC ### 🔍 Verify Your Results
# MAGIC Run the cell below to inspect your output. You should see:
# MAGIC - **Row count**: about 95 customers (between 70 and 130): about 90 background customers with one alert each, customers `9990` to `9994`, and customer `9995`.
# MAGIC - **Key columns**: `customer_id`, `total_incidents`, `total_suspicious_amount`, `max_risk_score`, `first_activity_ts`, `last_activity_ts`, `velocity_spike_windows`, `amount_spike_windows`.
# MAGIC - **Sorted by `total_incidents`** (the grid below): customers `9990` to `9994` sit at the top with 7 incidents, 1 velocity window, 12 amount windows and `max_risk_score` 90; everyone else almost always has 1 incident and no spike windows. A customer with 7 incidents in one minute is very different from one with a single borderline alert.
# MAGIC - **Now sort by `amount_spike_windows`** (click its header, descending): customer `9995` appears right below customers `9990` to `9994` with 0 incidents, 0 velocity windows, 11 amount windows and `max_risk_score` 0. No single payment of theirs was alerted, so a queue built from alerts alone would never show them.
# MAGIC - **`total_suspicious_amount`**: The dollar exposure from alerted payments: between $6,370 and $6,426 for the top 5; everyone else carries one payment of $800-$1,000; `9995` shows 0, because their $5,400 was never alerted payment by payment. Use it to size a case, never as the only sort key: sort on the spike-window counts too, as you just did, or `9995` drops to the bottom of the queue.
# MAGIC - **`first_activity_ts` / `last_activity_ts`**: the date range a SAR asks for; for `9990` to `9994` it is one minute. For `9995` both are `null`: the analyst pulls their payments from Silver for the time range their amount spike windows cover.
# MAGIC
# MAGIC **What this means**: This queue saves compliance analysts hours of manual work. Instead of querying raw alerts and window tables and joining them in spreadsheets, they start from one row per candidate, with the evidence counted and the dates filled in, and decide which cases become a filed SAR. This is the final output of the pipeline, from raw JSON files to regulatory action.

# COMMAND ----------

# 🔍 Verification query
sar_result = spark.read.table(TABLE_GOLD_SAR_PREFILL)
print(f"✅ {TABLE_GOLD_SAR_PREFILL}: {sar_result.count()} SAR candidates")
display(sar_result.orderBy(F.desc("total_incidents"), F.desc("amount_spike_windows"), "customer_id"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary and Checkpoint
# MAGIC ### 🎯 Key Concepts Covered
# MAGIC - **Batch Gold Aggregations**: Building business-ready reporting tables.
# MAGIC - **Liquid Clustering**: The layout Databricks recommends for new Delta tables, replacing partitioning and Z-Ordering.
# MAGIC - **SAR Candidate Queue**: One row per customer, combining per-payment alerts with window-rule hits.
# MAGIC - **Reporting SLA**: Understanding that Gold layers don't always need to be streaming.
# MAGIC
# MAGIC ### ✅ Exam Checklist
# MAGIC Can you:
# MAGIC - [ ] Identify why Gold reporting is often batch while Silver alerting is streaming?
# MAGIC - [ ] Explain the advantage of Liquid Clustering over Z-Order?
# MAGIC - [ ] Describe how the SAR dataset simplifies the workflow for a compliance analyst?
# MAGIC
# MAGIC ### 📚 Next Steps
# MAGIC **Try the incremental payoff**: run Notebooks 01 to 04 again, top to bottom, without re-running 00. Notebook 01's 🔄 cell lands 100 new payments (plus 2 gateway retries and 4 records that do not match the schema), and each streaming checkpoint ingests only the new files. Bronze grows by 104 rows to 50,259 (the 2 retries are deduplicated away), the rescued count reads 18, Silver grows by exactly the 100 new payments to 50,241, and the daily summary's `total_txns` add up to 100 more. The spike tables stay at 5 and 71 rows. If any count jumped by 50,000 instead, that stream lost its checkpoint and reprocessed everything.
# MAGIC
# MAGIC ---
# MAGIC **🎉 Congratulations! You have completed the ModernPaymentsABC Fintech Transaction Monitoring lab!**
