# Databricks notebook source
# COMMAND ----------
# MAGIC %md
# MAGIC # Medallion Architecture
# MAGIC **Topic**: ELT | **Exercises**: 8 | **Total Time**: ~90 min
# MAGIC
# MAGIC Practice the bronze/silver/gold medallion pattern end to end: raw ingestion,
# MAGIC metadata enrichment, deduplication, data cleaning, aggregation, incremental
# MAGIC processing with MERGE, and cross-tier quality reporting. Every exercise reads
# MAGIC from Delta tables in Unity Catalog and writes results to Delta output tables.
# MAGIC
# MAGIC **Solutions**: If stuck, see `solutions/medallion-architecture-solutions.py` for hints and answers.
# MAGIC
# MAGIC **Source tables** (created by setup notebook):
# MAGIC
# MAGIC | Exercise | Source Table(s) | Output Table | Rows |
# MAGIC |----------|----------------|--------------|------|
# MAGIC | 1 (easy) | `raw_orders_ex1` | `bronze_orders_ex1` | 10 |
# MAGIC | 2 (easy) | `raw_orders_ex2` | `bronze_orders_ex2` | 8 |
# MAGIC | 3 (medium) | `bronze_orders_ex3` | `silver_orders_ex3` | 14 -> 10 |
# MAGIC | 4 (medium) | `bronze_orders_ex4` | `silver_orders_ex4` | 12 -> 9 |
# MAGIC | 5 (medium) | `silver_orders_ex5` | `gold_daily_summary_ex5` | 20 -> 8 |
# MAGIC | 6 (hard) | `silver_orders_ex6` + `bronze_batch_ex6` | `silver_orders_ex6_merged` | 6+8 -> 9 |
# MAGIC | 7 (hard) | `gold_summary_ex7` + `silver_new_ex7` | `gold_summary_ex7_merged` | 5+12 -> 8 |
# MAGIC | 8 (hard) | `qc_bronze_ex8` + `qc_silver_ex8` + `qc_gold_ex8` | `quality_report_ex8` | -> 6 rows |
# MAGIC
# MAGIC **Schema** (`raw_orders_ex1`):
# MAGIC | Column | Type | Notes |
# MAGIC |--------|------|-------|
# MAGIC | order_id | STRING | Primary key (has duplicates in raw) |
# MAGIC | customer_id | STRING | FK to customers (some nulls) |
# MAGIC | product_id | STRING | Product identifier |
# MAGIC | amount | DOUBLE | Order total in USD (includes $0) |
# MAGIC | status | STRING | completed, pending, shipped, cancelled |
# MAGIC | order_date | DATE | When order was placed |
# MAGIC | updated_at | TIMESTAMP | Last modification time |

# COMMAND ----------

# MAGIC %run ./00_Setup

# COMMAND ----------

# MAGIC %run ./setup/medallion-architecture-setup

# COMMAND ----------
# MAGIC %md
# MAGIC **Setup complete.** All source and output tables are in `db_code.medallion_architecture`.
# MAGIC Each exercise has its own independent source tables and writes to its own output table.
# MAGIC - Ex 1-2 (easy): Raw to bronze - append and metadata enrichment
# MAGIC - Ex 3-4 (medium): Bronze to silver - dedup and cleaning
# MAGIC - Ex 5 (medium): Silver to gold - aggregation
# MAGIC - Ex 6-7 (hard): Incremental MERGE at silver and gold tiers
# MAGIC - Ex 8 (hard): Cross-tier data quality report

# COMMAND ----------
# MAGIC %md
# MAGIC ## Exercise 1: Raw Append to Bronze Preserving All Fields
# MAGIC **Difficulty**: Easy | **Time**: ~5 min
# MAGIC
# MAGIC Append raw order data to a bronze Delta table, preserving all fields including
# MAGIC messy data. Bronze is the raw layer: no dedup, no cleaning, no transformation.
# MAGIC Every row from the source lands in bronze exactly as-is.
# MAGIC
# MAGIC **Source** (`raw_orders_ex1`): 10 rows including a duplicate (ORD-003 appears twice),
# MAGIC a null customer_id (ORD-007), and a $0 amount (ORD-009).
# MAGIC
# MAGIC **Target**: Write to `db_code.medallion_architecture.bronze_orders_ex1`. Expected 10 rows.
# MAGIC
# MAGIC **Requirements**:
# MAGIC 1. Create the target table using CREATE OR REPLACE TABLE ... AS SELECT from `raw_orders_ex1`
# MAGIC 2. Include ALL columns: order_id, customer_id, product_id, amount, status, order_date, updated_at
# MAGIC 3. Do NOT deduplicate, do NOT filter nulls - bronze preserves raw data exactly
# MAGIC
# MAGIC **Constraints**:
# MAGIC - Output must be a Delta table in Unity Catalog
# MAGIC - All 10 rows must appear (including the duplicate ORD-003 and the null customer_id ORD-007)

# COMMAND ----------

# EXERCISE_KEY: medallion_ex1
# TODO: Create bronze table from raw orders preserving all rows and all columns

# Your code here


# COMMAND ----------

# Validate Exercise 1
result = spark.table(f"{CATALOG}.{SCHEMA}.bronze_orders_ex1")

assert result.count() == MEDAL_EX1_RAW_COUNT, \
    f"Expected {MEDAL_EX1_RAW_COUNT} rows (all raw rows), got {result.count()}"
# All columns must be present
for col in ["order_id", "customer_id", "product_id", "amount", "status", "order_date", "updated_at"]:
    assert col in result.columns, f"Missing column: {col}"
# Duplicate ORD-003 must be preserved (2 rows)
ord003_count = result.filter("order_id = 'ORD-003'").count()
assert ord003_count == 2, f"ORD-003 should appear twice in bronze (raw duplicate), got {ord003_count}"
# Null customer_id row must be preserved
null_cust = result.filter("order_id = 'ORD-007' AND customer_id IS NULL").count()
assert null_cust == 1, "ORD-007 with NULL customer_id must be preserved in bronze"
# Zero amount must be preserved
zero_amt = result.filter("order_id = 'ORD-009' AND amount = 0.0").count()
assert zero_amt == 1, "ORD-009 with $0 amount must be preserved in bronze"

print("Exercise 1 passed!")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Exercise 2: Bronze with Ingestion Metadata Columns
# MAGIC **Difficulty**: Easy | **Time**: ~5 min
# MAGIC
# MAGIC Append raw orders to a bronze table, adding two ingestion metadata columns
# MAGIC that track when and where the data arrived. Production bronze tables always
# MAGIC carry metadata for lineage and debugging.
# MAGIC
# MAGIC **Source** (`raw_orders_ex2`): 8 rows of raw orders.
# MAGIC
# MAGIC **Target**: Write to `db_code.medallion_architecture.bronze_orders_ex2`. Expected 8 rows.
# MAGIC
# MAGIC **Requirements**:
# MAGIC 1. SELECT all columns from `raw_orders_ex2`
# MAGIC 2. Add `_ingested_at` column set to `current_timestamp()`
# MAGIC 3. Add `_source_file` column set to the literal string `'raw_orders_batch_001'`
# MAGIC 4. Write as Delta table using CREATE OR REPLACE TABLE ... AS SELECT
# MAGIC
# MAGIC **Constraints**:
# MAGIC - All 8 rows must appear (no filtering)
# MAGIC - `_ingested_at` must be TIMESTAMP type
# MAGIC - `_source_file` must be STRING type
# MAGIC - Original columns must remain unchanged

# COMMAND ----------

# EXERCISE_KEY: medallion_ex2
# TODO: Create bronze table from raw orders with _ingested_at and _source_file metadata

# Your code here


# COMMAND ----------

# Validate Exercise 2
result = spark.table(f"{CATALOG}.{SCHEMA}.bronze_orders_ex2")

assert result.count() == MEDAL_EX2_RAW_COUNT, \
    f"Expected {MEDAL_EX2_RAW_COUNT} rows, got {result.count()}"
# Must have metadata columns
assert "_ingested_at" in result.columns, "Missing '_ingested_at' - bronze must include ingestion timestamp"
assert "_source_file" in result.columns, "Missing '_source_file' - bronze must include source file identifier"
# Must have original columns
for col in ["order_id", "customer_id", "product_id", "amount", "status", "order_date", "updated_at"]:
    assert col in result.columns, f"Missing original column: {col}"
# _ingested_at must be TIMESTAMP
ts_type = str(result.schema["_ingested_at"].dataType)
assert "Timestamp" in ts_type, f"_ingested_at should be TIMESTAMP, got {ts_type}"
# _source_file must have the expected value
src_files = [row._source_file for row in result.select("_source_file").distinct().collect()]
assert "raw_orders_batch_001" in src_files, \
    f"_source_file should be 'raw_orders_batch_001', got {src_files}"
# No nulls in _ingested_at
null_ts = result.filter("_ingested_at IS NULL").count()
assert null_ts == 0, f"Found {null_ts} rows with NULL _ingested_at"

print("Exercise 2 passed!")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Exercise 3: Bronze-to-Silver Dedup Using ROW_NUMBER
# MAGIC **Difficulty**: Medium | **Time**: ~10 min
# MAGIC
# MAGIC Deduplicate bronze orders, keeping the latest version per order_id based on
# MAGIC `_ingested_at`. This is the standard bronze-to-silver dedup pattern using
# MAGIC ROW_NUMBER window function or Databricks QUALIFY clause.
# MAGIC
# MAGIC **Source** (`bronze_orders_ex3`): 14 rows with duplicates:
# MAGIC - ORD-301 appears 2 times (different _ingested_at)
# MAGIC - ORD-305 appears 2 times
# MAGIC - ORD-308 appears 3 times
# MAGIC - 2 rows have NULL customer_id (ORD-304, ORD-310) - keep them (dedup only, no null filtering)
# MAGIC
# MAGIC **Target**: Write to `db_code.medallion_architecture.silver_orders_ex3`. Expected 10 rows.
# MAGIC
# MAGIC **Requirements**:
# MAGIC 1. Deduplicate on order_id, keeping the row with the latest `_ingested_at`
# MAGIC 2. Select: order_id, customer_id, product_id, amount, status, order_date
# MAGIC 3. Do NOT filter out NULL customer_ids - this exercise is dedup only
# MAGIC 4. Write as Delta table at `{CATALOG}.{SCHEMA}.silver_orders_ex3`
# MAGIC
# MAGIC **Constraints**:
# MAGIC - Use ROW_NUMBER() OVER (PARTITION BY ... ORDER BY ...) or QUALIFY
# MAGIC - No duplicate order_ids in the output
# MAGIC - ORD-308 should appear exactly once (latest _ingested_at: batch_07.json)
# MAGIC - NULL customer_id rows (ORD-304, ORD-310) must be present

# COMMAND ----------

# EXERCISE_KEY: medallion_ex3
# TODO: Dedup bronze orders keeping latest _ingested_at per order_id

# Your code here


# COMMAND ----------

# Validate Exercise 3
result = spark.table(f"{CATALOG}.{SCHEMA}.silver_orders_ex3")

assert result.count() == MEDAL_EX3_EXPECTED_COUNT, \
    f"Expected {MEDAL_EX3_EXPECTED_COUNT} rows after dedup, got {result.count()}"
# No duplicate order_ids
distinct_ids = result.select("order_id").distinct().count()
assert result.count() == distinct_ids, \
    f"Found duplicate order_ids: {result.count()} rows but {distinct_ids} distinct IDs"
# ORD-308 appears exactly once
ord308_count = result.filter("order_id = 'ORD-308'").count()
assert ord308_count == 1, f"ORD-308 should appear exactly once after dedup, got {ord308_count}"
# ORD-301 appears exactly once
ord301_count = result.filter("order_id = 'ORD-301'").count()
assert ord301_count == 1, f"ORD-301 should appear exactly once after dedup, got {ord301_count}"
# NULL customer_id rows preserved (ORD-304, ORD-310)
null_cust = result.filter("customer_id IS NULL").count()
assert null_cust == 2, f"Expected 2 rows with NULL customer_id (ORD-304, ORD-310), got {null_cust}"

print("Exercise 3 passed!")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Exercise 4: Silver Data Cleaning - Nulls, Types, Standardization
# MAGIC **Difficulty**: Medium | **Time**: ~10 min
# MAGIC
# MAGIC Clean dirty bronze data for the silver layer: filter null keys, cast types,
# MAGIC and standardize string values. Silver is the clean, conformed layer where
# MAGIC downstream consumers can trust every row.
# MAGIC
# MAGIC **Source** (`bronze_orders_ex4`): 12 rows with quality issues:
# MAGIC - 2 rows with NULL order_id (rows 4, 9) - filter out
# MAGIC - 1 row with NULL customer_id (ORD-407) - filter out
# MAGIC - `amount` stored as STRING (needs CAST to DOUBLE)
# MAGIC - `order_date` stored as STRING (needs CAST to DATE)
# MAGIC - `status` has inconsistent formatting: whitespace and mixed case
# MAGIC   (e.g., ' Completed ', 'PENDING', 'shipped', '  Pending  ')
# MAGIC
# MAGIC **Target**: Write to `db_code.medallion_architecture.silver_orders_ex4`. Expected 9 rows.
# MAGIC
# MAGIC **Requirements**:
# MAGIC 1. Filter out rows where order_id IS NULL
# MAGIC 2. Filter out rows where customer_id IS NULL
# MAGIC 3. CAST amount from STRING to DOUBLE
# MAGIC 4. CAST order_date from STRING to DATE
# MAGIC 5. Standardize status: TRIM whitespace, then UPPER (e.g., ' Completed ' -> 'COMPLETED')
# MAGIC 6. Select: order_id, customer_id, product_id, amount (DOUBLE), status (standardized), order_date (DATE)
# MAGIC 7. Write as Delta table at `{CATALOG}.{SCHEMA}.silver_orders_ex4`
# MAGIC
# MAGIC **Constraints**:
# MAGIC - No NULL order_ids or customer_ids in output
# MAGIC - amount must be DOUBLE type, not STRING
# MAGIC - order_date must be DATE type, not STRING
# MAGIC - All status values must be uppercase with no leading/trailing whitespace

# COMMAND ----------

# EXERCISE_KEY: medallion_ex4
# TODO: Clean bronze data - filter nulls, cast types, standardize status

# Your code here


# COMMAND ----------

# Validate Exercise 4
result = spark.table(f"{CATALOG}.{SCHEMA}.silver_orders_ex4")

assert result.count() == MEDAL_EX4_EXPECTED_COUNT, \
    f"Expected {MEDAL_EX4_EXPECTED_COUNT} rows after cleaning, got {result.count()}"
# No NULL keys
null_oid = result.filter("order_id IS NULL").count()
assert null_oid == 0, f"Found {null_oid} NULL order_ids - should be filtered out"
null_cid = result.filter("customer_id IS NULL").count()
assert null_cid == 0, f"Found {null_cid} NULL customer_ids - should be filtered out"
# amount must be DOUBLE type
amt_type = str(result.schema["amount"].dataType)
assert "Double" in amt_type, f"amount should be DOUBLE, got {amt_type}"
# order_date must be DATE type
dt_type = str(result.schema["order_date"].dataType)
assert "Date" in dt_type, f"order_date should be DATE, got {dt_type}"
# All statuses must be uppercase and trimmed
statuses = [row.status for row in result.select("status").distinct().collect()]
for s in statuses:
    assert s == s.strip().upper(), f"Status '{s}' is not trimmed+uppercased"
# Spot check: ORD-401 status should be 'COMPLETED' (was ' Completed ')
ord401 = result.filter("order_id = 'ORD-401'").collect()
assert len(ord401) == 1, "ORD-401 should exist"
assert ord401[0].status == "COMPLETED", f"ORD-401 status should be 'COMPLETED', got '{ord401[0].status}'"
# Spot check: amount is numeric
ord402 = result.filter("order_id = 'ORD-402'").collect()
assert ord402[0].amount == 142.50, f"ORD-402 amount should be 142.50, got {ord402[0].amount}"

print("Exercise 4 passed!")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Exercise 5: Silver-to-Gold Aggregation - Daily/Regional Summaries
# MAGIC **Difficulty**: Medium | **Time**: ~10 min
# MAGIC
# MAGIC Aggregate clean silver order data into a gold table with daily regional
# MAGIC summaries. Gold tables serve dashboards and analysts directly with
# MAGIC pre-computed metrics.
# MAGIC
# MAGIC **Source** (`silver_orders_ex5`): 20 clean rows across 3 dates (2026-03-20, -21, -22)
# MAGIC and 3 regions (US-East, US-West, EU-West). Not all date/region combos have data.
# MAGIC
# MAGIC **Target**: Write to `db_code.medallion_architecture.gold_daily_summary_ex5`. Expected 8 rows.
# MAGIC
# MAGIC **Requirements**:
# MAGIC 1. GROUP BY order_date and region
# MAGIC 2. Calculate: order_count (COUNT), total_revenue (SUM of amount),
# MAGIC    avg_order_amount (AVG of amount, rounded to 2 decimal places)
# MAGIC 3. Write as Delta table at `{CATALOG}.{SCHEMA}.gold_daily_summary_ex5`
# MAGIC
# MAGIC **Constraints**:
# MAGIC - One row per (order_date, region) combination
# MAGIC - 2026-03-20/US-East should have: 3 orders, 280.00 revenue, 93.33 avg
# MAGIC - 2026-03-22 has no US-West data (only 8 combos total, not 9)
# MAGIC - avg_order_amount must be ROUND(..., 2) to avoid floating point issues

# COMMAND ----------

# EXERCISE_KEY: medallion_ex5
# TODO: Aggregate silver orders into gold daily/regional summary

# Your code here


# COMMAND ----------

# Validate Exercise 5
result = spark.table(f"{CATALOG}.{SCHEMA}.gold_daily_summary_ex5")

assert result.count() == MEDAL_EX5_EXPECTED_COUNT, \
    f"Expected {MEDAL_EX5_EXPECTED_COUNT} rows, got {result.count()}"
# Required columns
for col in ["order_date", "region", "order_count", "total_revenue", "avg_order_amount"]:
    assert col in result.columns, f"Missing column: {col}"
# Spot check: US-East on 2026-03-20 = 3 orders, 280.00 revenue, 93.33 avg
us_east_20 = result.filter("region = 'US-East' AND order_date = '2026-03-20'").collect()
assert len(us_east_20) == 1, "Should have exactly 1 row for US-East on 2026-03-20"
row = us_east_20[0]
assert row.order_count == 3, f"US-East 2026-03-20: expected 3 orders, got {row.order_count}"
assert abs(row.total_revenue - 280.00) < 0.01, \
    f"US-East 2026-03-20: expected 280.00 revenue, got {row.total_revenue}"
assert abs(row.avg_order_amount - 93.33) < 0.01, \
    f"US-East 2026-03-20: expected 93.33 avg, got {row.avg_order_amount}"
# No US-West on 2026-03-22
us_west_22 = result.filter("region = 'US-West' AND order_date = '2026-03-22'").count()
assert us_west_22 == 0, "Should have no row for US-West on 2026-03-22 (no source data)"
# EU-West on 2026-03-22: 3 orders, 275.00 revenue
eu_west_22 = result.filter("region = 'EU-West' AND order_date = '2026-03-22'").collect()
assert len(eu_west_22) == 1, "Should have 1 row for EU-West on 2026-03-22"
assert eu_west_22[0].order_count == 3, \
    f"EU-West 2026-03-22: expected 3 orders, got {eu_west_22[0].order_count}"
assert abs(eu_west_22[0].total_revenue - 275.00) < 0.01, \
    f"EU-West 2026-03-22: expected 275.00 revenue, got {eu_west_22[0].total_revenue}"

print("Exercise 5 passed!")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Exercise 6: Incremental Bronze-to-Silver with MERGE
# MAGIC **Difficulty**: Hard | **Time**: ~20 min
# MAGIC
# MAGIC Process a new batch of bronze orders and MERGE them into an existing silver table.
# MAGIC Production medallion pipelines process only new data and upsert into silver
# MAGIC instead of full reprocessing.
# MAGIC
# MAGIC **Source** (`silver_orders_ex6`): Existing silver table with 6 rows (ORD-601 through ORD-606).
# MAGIC **Source** (`bronze_batch_ex6`): 8 new bronze rows containing:
# MAGIC - 3 updates: ORD-601 (status pending->completed), ORD-603 (shipped->delivered),
# MAGIC   ORD-605 (amount 210->215) with newer updated_at
# MAGIC - 3 new orders: ORD-607, ORD-608, ORD-609
# MAGIC - 2 duplicates within batch: ORD-607 appears 3 times with different _ingested_at
# MAGIC
# MAGIC **Target**: Write to `db_code.medallion_architecture.silver_orders_ex6_merged`. Expected 9 rows.
# MAGIC
# MAGIC **Requirements**:
# MAGIC 1. Copy `silver_orders_ex6` to `silver_orders_ex6_merged` (your working table)
# MAGIC 2. Deduplicate `bronze_batch_ex6` on order_id (keep latest _ingested_at)
# MAGIC 3. MERGE the deduped batch into `silver_orders_ex6_merged`:
# MAGIC    - WHEN MATCHED AND source updated_at > target updated_at THEN UPDATE all fields
# MAGIC    - WHEN NOT MATCHED THEN INSERT all fields
# MAGIC 4. Set processed_at to current_timestamp() for all inserted/updated rows
# MAGIC
# MAGIC **Constraints**:
# MAGIC - Must use Delta MERGE INTO (not DELETE + INSERT)
# MAGIC - After MERGE: 9 total rows (6 original + 3 new)
# MAGIC - Updated rows (ORD-601, ORD-603, ORD-605) must have the newer values from the batch
# MAGIC - ORD-607 appears exactly once (dedup before MERGE)
# MAGIC - ORD-602 must be unchanged (not in the batch)

# COMMAND ----------

# EXERCISE_KEY: medallion_ex6
# TODO: Copy silver, dedup new batch, MERGE into silver incrementally

# Your code here


# COMMAND ----------

# Validate Exercise 6
result = spark.table(f"{CATALOG}.{SCHEMA}.silver_orders_ex6_merged")

assert result.count() == MEDAL_EX6_EXPECTED_COUNT, \
    f"Expected {MEDAL_EX6_EXPECTED_COUNT} rows, got {result.count()}"
# No duplicate order_ids
distinct_ids = result.select("order_id").distinct().count()
assert result.count() == distinct_ids, \
    f"Duplicate order_ids found: {result.count()} rows but {distinct_ids} distinct"
# ORD-601 must have updated status (completed, not pending)
ord601 = result.filter("order_id = 'ORD-601'").collect()[0]
assert ord601.status == "completed", \
    f"ORD-601 should have updated status 'completed', got '{ord601.status}'"
assert str(ord601.updated_at) >= "2026-03-25 10:00:00", \
    f"ORD-601 should have updated_at >= 10:00, got {ord601.updated_at}"
# ORD-605 must have updated amount
ord605 = result.filter("order_id = 'ORD-605'").collect()[0]
assert ord605.amount == 215.00, f"ORD-605 amount should be 215.00, got {ord605.amount}"
# ORD-607 must exist exactly once (deduped from 3 in batch)
ord607_count = result.filter("order_id = 'ORD-607'").count()
assert ord607_count == 1, f"ORD-607 should appear exactly once, got {ord607_count}"
# ORD-602 must be unchanged
ord602 = result.filter("order_id = 'ORD-602'").collect()[0]
assert str(ord602.updated_at) == "2026-03-25 08:05:00", \
    f"ORD-602 should be unchanged (08:05:00), got {ord602.updated_at}"
# New events must exist
for eid in ["ORD-608", "ORD-609"]:
    assert result.filter(f"order_id = '{eid}'").count() == 1, f"{eid} should be inserted by MERGE"

print("Exercise 6 passed!")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Exercise 7: Gold Incremental Refresh with MERGE
# MAGIC **Difficulty**: Hard | **Time**: ~15 min
# MAGIC
# MAGIC MERGE new silver records into an existing gold aggregation table. Instead of
# MAGIC recomputing gold from scratch, aggregate only the new silver batch and merge
# MAGIC the results: update existing date+region combos with new totals, insert combos
# MAGIC that didn't exist before.
# MAGIC
# MAGIC **Source** (`gold_summary_ex7`): Existing gold table with 5 aggregate rows.
# MAGIC **Source** (`silver_new_ex7`): 12 new silver rows that aggregate into 6 combos:
# MAGIC - 3 match existing gold combos (UPDATE with new aggregates)
# MAGIC - 3 are new combos (INSERT)
# MAGIC
# MAGIC **Target**: Write to `db_code.medallion_architecture.gold_summary_ex7_merged`. Expected 8 rows.
# MAGIC
# MAGIC **Requirements**:
# MAGIC 1. Copy `gold_summary_ex7` to `gold_summary_ex7_merged` (your working table)
# MAGIC 2. Aggregate `silver_new_ex7` by order_date and region to get new batch totals:
# MAGIC    order_count, total_revenue, avg_order_amount (ROUND to 2 decimals)
# MAGIC 3. MERGE the new aggregates into `gold_summary_ex7_merged`:
# MAGIC    - WHEN MATCHED: UPDATE order_count, total_revenue, avg_order_amount with new batch values
# MAGIC    - WHEN NOT MATCHED: INSERT the new aggregate row
# MAGIC
# MAGIC **Constraints**:
# MAGIC - Must use Delta MERGE INTO
# MAGIC - Match on both order_date AND region
# MAGIC - After MERGE: 8 total rows (5 original + 3 new combos, 3 existing updated)
# MAGIC - 2026-03-20/EU-West and 2026-03-21/US-West must be unchanged (no new silver for them)
# MAGIC - 2026-03-21/US-East updated: order_count=3, total_revenue=265.00

# COMMAND ----------

# EXERCISE_KEY: medallion_ex7
# TODO: Copy gold, aggregate new silver, MERGE into gold incrementally

# Your code here


# COMMAND ----------

# Validate Exercise 7
result = spark.table(f"{CATALOG}.{SCHEMA}.gold_summary_ex7_merged")

assert result.count() == MEDAL_EX7_EXPECTED_COUNT, \
    f"Expected {MEDAL_EX7_EXPECTED_COUNT} rows, got {result.count()}"
# Required columns
for col in ["order_date", "region", "order_count", "total_revenue", "avg_order_amount"]:
    assert col in result.columns, f"Missing column: {col}"
# Updated combo: 2026-03-21/US-East = 3 orders, 265.00 revenue
us_east_21 = result.filter("region = 'US-East' AND order_date = '2026-03-21'").collect()
assert len(us_east_21) == 1, "Should have 1 row for US-East on 2026-03-21"
assert us_east_21[0].order_count == 3, \
    f"US-East 2026-03-21: expected 3 orders (updated), got {us_east_21[0].order_count}"
assert abs(us_east_21[0].total_revenue - 265.00) < 0.01, \
    f"US-East 2026-03-21: expected 265.00 revenue (updated), got {us_east_21[0].total_revenue}"
# Updated combo: 2026-03-20/US-West = 1 order, 200.00
us_west_20 = result.filter("region = 'US-West' AND order_date = '2026-03-20'").collect()
assert len(us_west_20) == 1, "Should have 1 row for US-West on 2026-03-20"
assert us_west_20[0].order_count == 1, \
    f"US-West 2026-03-20: expected 1 order (updated), got {us_west_20[0].order_count}"
# Unchanged combo: 2026-03-20/EU-West should still be original values
eu_west_20 = result.filter("region = 'EU-West' AND order_date = '2026-03-20'").collect()
assert len(eu_west_20) == 1, "Should have 1 row for EU-West on 2026-03-20"
assert eu_west_20[0].order_count == 3, \
    f"EU-West 2026-03-20: expected 3 orders (unchanged), got {eu_west_20[0].order_count}"
assert abs(eu_west_20[0].total_revenue - 255.00) < 0.01, \
    f"EU-West 2026-03-20: expected 255.00 revenue (unchanged), got {eu_west_20[0].total_revenue}"
# New combo: 2026-03-22/US-East must exist
us_east_22 = result.filter("region = 'US-East' AND order_date = '2026-03-22'").collect()
assert len(us_east_22) == 1, "Should have 1 row for US-East on 2026-03-22 (new insert)"
assert us_east_22[0].order_count == 2, \
    f"US-East 2026-03-22: expected 2 orders (new), got {us_east_22[0].order_count}"
# New combo: 2026-03-22/US-West must exist
us_west_22 = result.filter("region = 'US-West' AND order_date = '2026-03-22'").collect()
assert len(us_west_22) == 1, "Should have 1 row for US-West on 2026-03-22 (new insert)"

print("Exercise 7 passed!")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Exercise 8: Data Quality Report Across Tiers
# MAGIC **Difficulty**: Hard | **Time**: ~15 min
# MAGIC
# MAGIC Build a quality report table that summarizes data quality issues across all three
# MAGIC medallion tiers. Production pipelines need automated quality gates to catch
# MAGIC problems before they propagate downstream.
# MAGIC
# MAGIC **Source** (`qc_bronze_ex8`): 12 rows with issues:
# MAGIC - 3 rows with NULL order_id
# MAGIC - 2 rows with order_date outside valid range (before 2026-01-01 or after 2027-12-31)
# MAGIC - 2 rows with negative amount
# MAGIC
# MAGIC **Source** (`qc_silver_ex8`): 10 rows with issues:
# MAGIC - 4 rows with orphan customer_ids (C-899, C-898, C-897, C-896 not in qc_customers_ex8)
# MAGIC
# MAGIC **Source** (`qc_gold_ex8`): 8 rows with issues:
# MAGIC - 2 rows with zero order_count
# MAGIC - 1 row with negative total_revenue
# MAGIC
# MAGIC **Target**: Write to `db_code.medallion_architecture.quality_report_ex8`. Expected 6 rows.
# MAGIC
# MAGIC **Requirements**:
# MAGIC 1. Bronze check 1: count NULL order_ids (check_name = 'null_order_id')
# MAGIC 2. Bronze check 2: count out-of-range dates (check_name = 'invalid_date_range',
# MAGIC    valid: order_date >= '2026-01-01' AND order_date <= '2027-12-31')
# MAGIC 3. Bronze check 3: count negative amounts (check_name = 'negative_amount')
# MAGIC 4. Silver check: count orphan customer_ids via LEFT ANTI JOIN with qc_customers_ex8
# MAGIC    (check_name = 'orphan_customer_id')
# MAGIC 5. Gold check 1: count zero order_counts (check_name = 'zero_order_count')
# MAGIC 6. Gold check 2: count negative revenue (check_name = 'negative_revenue')
# MAGIC 7. Build quality_report_ex8 with columns:
# MAGIC    - tier (STRING): 'bronze', 'silver', or 'gold'
# MAGIC    - check_name (STRING): as specified above
# MAGIC    - total_rows (INT): total rows in that tier's table
# MAGIC    - failed_rows (INT): rows that failed this check
# MAGIC    - pass_rate (DOUBLE): (total_rows - failed_rows) / total_rows as decimal
# MAGIC
# MAGIC **Constraints**:
# MAGIC - Report has exactly 6 rows (3 bronze + 1 silver + 2 gold)
# MAGIC - pass_rate between 0.0 and 1.0
# MAGIC - Bronze null_order_id: 3 failed / 12 total (pass_rate = 0.75)
# MAGIC - Bronze invalid_date_range: 2 failed / 12 total (pass_rate ~= 0.8333)
# MAGIC - Bronze negative_amount: 2 failed / 12 total (pass_rate ~= 0.8333)
# MAGIC - Silver orphan_customer_id: 4 failed / 10 total (pass_rate = 0.6)
# MAGIC - Gold zero_order_count: 2 failed / 8 total (pass_rate = 0.75)
# MAGIC - Gold negative_revenue: 1 failed / 8 total (pass_rate = 0.875)

# COMMAND ----------

# EXERCISE_KEY: medallion_ex8
# TODO: Run quality checks across bronze, silver, gold tiers and write report

# Your code here


# COMMAND ----------

# Validate Exercise 8
result = spark.table(f"{CATALOG}.{SCHEMA}.quality_report_ex8")

assert result.count() == 6, f"Expected 6 quality report rows, got {result.count()}"
for col in ["tier", "check_name", "total_rows", "failed_rows", "pass_rate"]:
    assert col in result.columns, f"Missing column: {col}"

# Bronze: null_order_id
bronze_null = result.filter("tier = 'bronze' AND check_name = 'null_order_id'").collect()
assert len(bronze_null) == 1, "Missing row for bronze null_order_id check"
assert bronze_null[0].total_rows == 12, f"Bronze total_rows should be 12, got {bronze_null[0].total_rows}"
assert bronze_null[0].failed_rows == MEDAL_EX8_BRONZE_NULL_IDS, \
    f"Bronze null IDs: expected {MEDAL_EX8_BRONZE_NULL_IDS}, got {bronze_null[0].failed_rows}"
assert abs(bronze_null[0].pass_rate - 0.75) < 0.01, \
    f"Bronze null_order_id pass_rate should be 0.75, got {bronze_null[0].pass_rate}"

# Bronze: invalid_date_range
bronze_date = result.filter("tier = 'bronze' AND check_name = 'invalid_date_range'").collect()
assert len(bronze_date) == 1, "Missing row for bronze invalid_date_range check"
assert bronze_date[0].failed_rows == MEDAL_EX8_BRONZE_BAD_DATES, \
    f"Bronze bad dates: expected {MEDAL_EX8_BRONZE_BAD_DATES}, got {bronze_date[0].failed_rows}"
assert abs(bronze_date[0].pass_rate - 0.8333) < 0.01, \
    f"Bronze invalid_date_range pass_rate should be ~0.8333, got {bronze_date[0].pass_rate}"

# Bronze: negative_amount
bronze_neg = result.filter("tier = 'bronze' AND check_name = 'negative_amount'").collect()
assert len(bronze_neg) == 1, "Missing row for bronze negative_amount check"
assert bronze_neg[0].failed_rows == MEDAL_EX8_BRONZE_NEG_AMOUNTS, \
    f"Bronze negative amounts: expected {MEDAL_EX8_BRONZE_NEG_AMOUNTS}, got {bronze_neg[0].failed_rows}"
assert abs(bronze_neg[0].pass_rate - 0.8333) < 0.01, \
    f"Bronze negative_amount pass_rate should be ~0.8333, got {bronze_neg[0].pass_rate}"

# Silver: orphan_customer_id
silver_orphan = result.filter("tier = 'silver' AND check_name = 'orphan_customer_id'").collect()
assert len(silver_orphan) == 1, "Missing row for silver orphan_customer_id check"
assert silver_orphan[0].total_rows == 10, f"Silver total_rows should be 10, got {silver_orphan[0].total_rows}"
assert silver_orphan[0].failed_rows == MEDAL_EX8_SILVER_ORPHANS, \
    f"Silver orphans: expected {MEDAL_EX8_SILVER_ORPHANS}, got {silver_orphan[0].failed_rows}"
assert abs(silver_orphan[0].pass_rate - 0.6) < 0.01, \
    f"Silver orphan pass_rate should be 0.6, got {silver_orphan[0].pass_rate}"

# Gold: zero_order_count
gold_zero = result.filter("tier = 'gold' AND check_name = 'zero_order_count'").collect()
assert len(gold_zero) == 1, "Missing row for gold zero_order_count check"
assert gold_zero[0].total_rows == 8, f"Gold total_rows should be 8, got {gold_zero[0].total_rows}"
assert gold_zero[0].failed_rows == MEDAL_EX8_GOLD_ZERO_COUNTS, \
    f"Gold zero counts: expected {MEDAL_EX8_GOLD_ZERO_COUNTS}, got {gold_zero[0].failed_rows}"
assert abs(gold_zero[0].pass_rate - 0.75) < 0.01, \
    f"Gold zero_order_count pass_rate should be 0.75, got {gold_zero[0].pass_rate}"

# Gold: negative_revenue
gold_neg = result.filter("tier = 'gold' AND check_name = 'negative_revenue'").collect()
assert len(gold_neg) == 1, "Missing row for gold negative_revenue check"
assert gold_neg[0].failed_rows == MEDAL_EX8_GOLD_NEG_REVENUE, \
    f"Gold negative revenue: expected {MEDAL_EX8_GOLD_NEG_REVENUE}, got {gold_neg[0].failed_rows}"
assert abs(gold_neg[0].pass_rate - 0.875) < 0.01, \
    f"Gold negative_revenue pass_rate should be 0.875, got {gold_neg[0].pass_rate}"

print("Exercise 8 passed!")
