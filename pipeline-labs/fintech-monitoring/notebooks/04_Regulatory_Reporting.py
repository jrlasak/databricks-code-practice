# Databricks notebook source
# MAGIC %md
# MAGIC # 04: Regulatory Reporting  -  "Gold Layer: From Alerts to Action"
# MAGIC **Exam Coverage**: DE Associate (Medallion, Gold) + DE Professional (Optimization, Liquid Clustering)
# MAGIC **Duration**: 30 minutes
# MAGIC ---
# MAGIC ## Learning Objectives
# MAGIC By the end of this notebook, you will be able to:
# MAGIC - Build batch aggregation tables for regulatory reporting
# MAGIC - Create merchant risk scorecards from streaming data
# MAGIC - Understand when Gold should be streaming vs. batch
# MAGIC - Optimize Gold tables with Liquid Clustering for sub-second reporting performance
# MAGIC ---

# COMMAND ----------

# MAGIC %md
# MAGIC ## Section 1: Merchant Risk Scorecard
# MAGIC To comply with financial regulations, ModernPaymentsABC needs a daily "scorecard" per merchant that summarizes their risk profile.
# MAGIC
# MAGIC ### Key Concepts
# MAGIC - **Batch Gold Layer**: For regulatory reports that are only needed daily, scheduled batch processing is more cost-effective than continuous streaming.
# MAGIC - **Aggregation Functions**: Calculating averages, counts, and percentages for business-ready insights.

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
# MAGIC **Hint**: For a daily report, we use `spark.read.table()` to read the entire Delta table as a static DataFrame  -  no streaming needed.

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
# MAGIC **Hint**: Liquid Clustering simplifies Z-Ordering and is the modern way to optimize Delta tables in Unity Catalog.

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
# MAGIC - **Row count**: One row per `(report_date, category_code)` combination  -  far fewer than Silver (this is the power of Gold aggregation).
# MAGIC - **Key columns**: `report_date`, `category_code`, `total_txns`, `total_amount`, `avg_amount`.
# MAGIC - **`category_code`**: Merchant categories like `5411` (grocery), `5812` (restaurants), `4789` (transportation). Each category has its own daily summary.
# MAGIC - **`avg_amount`**: Compare across categories  -  restaurant transactions typically average lower than electronics purchases.
# MAGIC
# MAGIC **What this means**: This is the table a compliance dashboard queries. Instead of scanning millions of Silver rows, analysts get pre-aggregated daily breakdowns with sub-second response times thanks to Liquid Clustering on `report_date` and `category_code`. This is why Gold layers exist  -  to serve specific business consumers efficiently.

# COMMAND ----------

# 🔍 Verification query
daily_result = spark.read.table(TABLE_GOLD_DAILY_SUMMARY)
print(f"✅ {TABLE_GOLD_DAILY_SUMMARY}: {daily_result.count()} rows")
display(daily_result)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Section 2: SAR Pre-Fill Dataset
# MAGIC Compliance analysts file Suspicious Activity Reports (SARs) when fraud is detected. We'll build a Gold table that pre-fills the data they need.
# MAGIC
# MAGIC ### Key Concepts
# MAGIC - **SAR Pre-Fill**: Aggregating alerts by customer to show the full scope of their suspicious behavior.
# MAGIC - **Aha Moment**: While Silver alerts are streaming, the Gold SAR dataset is batch. Seniors design dual-layer architectures based on consumer needs (Ops vs Compliance).

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ### 🎯 EXERCISE 3: SAR Pre-Fill Dataset
# MAGIC **Your task**: Aggregate fraud alerts by customer to show total suspicious activity for compliance analysts.
# MAGIC
# MAGIC **Requirements:**
# MAGIC - Load Silver alerts (static) from `TABLE_SILVER_ALERTS` and group by `customer_id`.
# MAGIC - Calculate `total_incidents` (count), `total_suspicious_amount` (sum of amount), and `max_risk_score` (max of risk_score).
# MAGIC - Write the results to `TABLE_GOLD_SAR_PREFILL` in overwrite mode.

# COMMAND ----------

# ✏️ STUDENT EXERCISE

# TODO: Aggregate by customer_id
# 1. Load static Silver alerts from TABLE_SILVER_ALERTS
# 2. Group by customer_id
# 3. Calculate:
#    - total_incidents (count)
#    - total_suspicious_amount (sum of amount)
#    - max_risk_score (max of risk_score)
# 4. Write resulting DataFrame to TABLE_GOLD_SAR_PREFILL in overwrite mode

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC **Solution below** ⬇️

# COMMAND ----------

# ✅ SOLUTION: SAR Pre-Fill Dataset

# alerts_df = (spark.read.table(TABLE_SILVER_ALERTS))
#
# sar_prefill_df = (alerts_df
#   .groupBy("customer_id")
#   .agg(
#       F.count("*").alias("total_incidents"),
#       F.sum("amount").alias("total_suspicious_amount"),
#       F.max("risk_score").alias("max_risk_score")
#   ))
#
# (sar_prefill_df
#   .write.mode("overwrite")
#   .saveAsTable(TABLE_GOLD_SAR_PREFILL))

# COMMAND ----------

# MAGIC %md
# MAGIC ### 🔍 Verify Your Results
# MAGIC Run the cell below to inspect your output. You should see:
# MAGIC - **Row count**: One row per flagged customer  -  significantly fewer than the alerts table (multiple alerts per customer are rolled up).
# MAGIC - **Key columns**: `customer_id`, `total_incidents`, `total_suspicious_amount`, `max_risk_score`.
# MAGIC - **`total_incidents`**: Customers with multiple incidents are higher priority for investigation. A customer with 5+ incidents in one day is very different from one with a single borderline alert.
# MAGIC - **`total_suspicious_amount`**: The aggregate dollar exposure per customer. Compliance analysts use this to prioritize which SARs to file first.
# MAGIC - **`max_risk_score`**: The worst single transaction score for that customer  -  indicates peak severity.
# MAGIC
# MAGIC **What this means**: You've just built the dataset that saves compliance analysts hours of manual work. Instead of querying raw alerts and aggregating in spreadsheets, they get a pre-built report showing exactly which customers need Suspicious Activity Reports filed. This is the final output of your entire pipeline  -  from raw JSON files to regulatory action.

# COMMAND ----------

# 🔍 Verification query
sar_result = spark.read.table(TABLE_GOLD_SAR_PREFILL)
print(f"✅ {TABLE_GOLD_SAR_PREFILL}: {sar_result.count()} customers flagged")
display(sar_result)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary and Checkpoint
# MAGIC ### 🎯 Key Concepts Covered
# MAGIC - **Batch Gold Aggregations**: Building business-ready reporting tables.
# MAGIC - **Liquid Clustering**: Modern optimization for Delta tables that simplifies Z-Ordering.
# MAGIC - **Reporting SLA**: Understanding that Gold layers don't always need to be streaming.
# MAGIC
# MAGIC ### ✅ Exam Checklist
# MAGIC Can you:
# MAGIC - [ ] Identify why Gold reporting is often batch while Silver alerting is streaming?
# MAGIC - [ ] Explain the advantage of Liquid Clustering over Z-Order?
# MAGIC - [ ] Describe how the SAR dataset simplifies the workflow for a compliance analyst?
# MAGIC
# MAGIC ### 📚 Next Steps
# MAGIC **Review the README** to see the full project lifecycle you just built!
# MAGIC ---
# MAGIC **🎉 Congratulations! You have completed the ModernPaymentsABC Fintech Transaction Monitoring lab!**
