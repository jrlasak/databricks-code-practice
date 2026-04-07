# Databricks notebook source
# COMMAND ----------
# MAGIC %md
# MAGIC ### Setup: Auto Loader Ingestion
# MAGIC Creates source JSON/CSV files in Unity Catalog Volumes and target tables for each exercise.
# MAGIC Run automatically via `%run` from the exercise notebook.

# COMMAND ----------

CATALOG = "db_code"
SCHEMA = "auto_loader"
BASE_SCHEMA = "elt"

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA}")

# Volume for source files and checkpoints
spark.sql(f"CREATE VOLUME IF NOT EXISTS {CATALOG}.{SCHEMA}.source_files")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {CATALOG}.{SCHEMA}.checkpoints")

VOLUME_BASE = f"/Volumes/{CATALOG}/{SCHEMA}/source_files"
CHECKPOINT_BASE = f"/Volumes/{CATALOG}/{SCHEMA}/checkpoints"

# COMMAND ----------

# -- Clean previous runs --
# Remove old files and tables so setup is idempotent
import os

for ex_dir in [
    "ex1_files", "ex2_files",
    "ex3_files_batch1", "ex3_files_batch2",
    "ex4_files", "ex5_files", "ex6_files",
]:
    dbutils.fs.rm(f"{VOLUME_BASE}/{ex_dir}", recurse=True)

# Clean checkpoint directories
dbutils.fs.rm(CHECKPOINT_BASE, recurse=True)
spark.sql(f"CREATE VOLUME IF NOT EXISTS {CATALOG}.{SCHEMA}.checkpoints")

# Drop exercise output tables
for i in range(1, 7):
    spark.sql(f"DROP TABLE IF EXISTS {CATALOG}.{SCHEMA}.ex{i}_output")
spark.sql(f"DROP TABLE IF EXISTS {CATALOG}.{SCHEMA}.ex6_target")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Exercise 1: Basic cloudFiles read
# MAGIC 5 JSON files with a simple, consistent schema. User reads them with Auto Loader.

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType, LongType
from datetime import datetime

# 10 order records across 5 JSON files (2 records each)
ex1_data = [
    ("ORD-201", "CUST-001", 99.50, "completed", datetime(2026, 3, 1, 10, 0, 0)),
    ("ORD-202", "CUST-002", 150.00, "pending", datetime(2026, 3, 1, 10, 5, 0)),
    ("ORD-203", "CUST-003", 75.25, "completed", datetime(2026, 3, 1, 10, 10, 0)),
    ("ORD-204", "CUST-001", 200.00, "shipped", datetime(2026, 3, 1, 10, 15, 0)),
    ("ORD-205", "CUST-004", 45.00, "completed", datetime(2026, 3, 1, 10, 20, 0)),
    ("ORD-206", "CUST-002", 310.75, "pending", datetime(2026, 3, 1, 10, 25, 0)),
    ("ORD-207", "CUST-005", 89.99, "shipped", datetime(2026, 3, 1, 10, 30, 0)),
    ("ORD-208", "CUST-003", 125.00, "completed", datetime(2026, 3, 1, 10, 35, 0)),
    ("ORD-209", "CUST-001", 60.00, "cancelled", datetime(2026, 3, 1, 10, 40, 0)),
    ("ORD-210", "CUST-004", 175.50, "completed", datetime(2026, 3, 1, 10, 45, 0)),
]

ex1_schema = StructType([
    StructField("order_id", StringType()),
    StructField("customer_id", StringType()),
    StructField("amount", DoubleType()),
    StructField("status", StringType()),
    StructField("order_ts", TimestampType()),
])

# Write 2 records per file to simulate multiple files landing
for i in range(0, 10, 2):
    batch = spark.createDataFrame(ex1_data[i:i+2], schema=ex1_schema)
    batch.coalesce(1).write.mode("overwrite").json(f"{VOLUME_BASE}/ex1_files/batch_{i//2}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Exercise 2: Schema hints for type control
# MAGIC JSON files where `amount` is an integer in the JSON (no decimal point).
# MAGIC Auto Loader will infer LONG by default. User must use schema hints to force DOUBLE.

# COMMAND ----------

# amount stored as whole numbers (no decimal point) - Auto Loader infers LONG
ex2_data = [
    ("ORD-301", "CUST-001", 100, "completed", datetime(2026, 3, 2, 9, 0, 0)),
    ("ORD-302", "CUST-002", 250, "pending", datetime(2026, 3, 2, 9, 5, 0)),
    ("ORD-303", "CUST-003", 75, "shipped", datetime(2026, 3, 2, 9, 10, 0)),
    ("ORD-304", "CUST-004", 500, "completed", datetime(2026, 3, 2, 9, 15, 0)),
    ("ORD-305", "CUST-001", 0, "cancelled", datetime(2026, 3, 2, 9, 20, 0)),
    ("ORD-306", "CUST-005", 189, "completed", datetime(2026, 3, 2, 9, 25, 0)),
]

ex2_schema = StructType([
    StructField("order_id", StringType()),
    StructField("customer_id", StringType()),
    StructField("amount", LongType()),
    StructField("status", StringType()),
    StructField("order_ts", TimestampType()),
])

df_ex2 = spark.createDataFrame(ex2_data, schema=ex2_schema)
df_ex2.coalesce(1).write.mode("overwrite").json(f"{VOLUME_BASE}/ex2_files")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Exercise 3: Schema evolution handling
# MAGIC Two batches of files. Batch 1 has base schema. Batch 2 adds a `priority` column.
# MAGIC User must configure Auto Loader to evolve the schema when new columns appear.

# COMMAND ----------

# Batch 1: base schema (4 columns)
ex3_batch1 = [
    ("ORD-401", "CUST-001", 120.00, "completed", datetime(2026, 3, 3, 8, 0, 0)),
    ("ORD-402", "CUST-002", 85.50, "pending", datetime(2026, 3, 3, 8, 5, 0)),
    ("ORD-403", "CUST-003", 210.00, "shipped", datetime(2026, 3, 3, 8, 10, 0)),
    ("ORD-404", "CUST-004", 55.00, "completed", datetime(2026, 3, 3, 8, 15, 0)),
]

ex3_batch1_schema = StructType([
    StructField("order_id", StringType()),
    StructField("customer_id", StringType()),
    StructField("amount", DoubleType()),
    StructField("status", StringType()),
    StructField("order_ts", TimestampType()),
])

df_b1 = spark.createDataFrame(ex3_batch1, schema=ex3_batch1_schema)
df_b1.coalesce(1).write.mode("overwrite").json(f"{VOLUME_BASE}/ex3_files_batch1")

# Batch 2: adds priority column (schema evolution)
ex3_batch2 = [
    ("ORD-405", "CUST-001", 300.00, "pending", datetime(2026, 3, 3, 12, 0, 0), "high"),
    ("ORD-406", "CUST-005", 45.00, "completed", datetime(2026, 3, 3, 12, 5, 0), "low"),
    ("ORD-407", "CUST-002", 175.00, "shipped", datetime(2026, 3, 3, 12, 10, 0), "medium"),
]

ex3_batch2_schema = StructType([
    StructField("order_id", StringType()),
    StructField("customer_id", StringType()),
    StructField("amount", DoubleType()),
    StructField("status", StringType()),
    StructField("order_ts", TimestampType()),
    StructField("priority", StringType()),
])

df_b2 = spark.createDataFrame(ex3_batch2, schema=ex3_batch2_schema)
df_b2.coalesce(1).write.mode("overwrite").json(f"{VOLUME_BASE}/ex3_files_batch2")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Exercise 4: Rescued data column
# MAGIC JSON files where some records have a mismatched type (`amount` is a string instead of number).
# MAGIC Auto Loader should capture these in the rescued data column.

# COMMAND ----------

import json

# Write JSON manually to control exact content (mix of valid and invalid types)
ex4_records_good = [
    {"order_id": "ORD-501", "customer_id": "CUST-001", "amount": 99.50, "status": "completed"},
    {"order_id": "ORD-502", "customer_id": "CUST-002", "amount": 150.00, "status": "pending"},
    {"order_id": "ORD-503", "customer_id": "CUST-003", "amount": 75.25, "status": "shipped"},
    {"order_id": "ORD-504", "customer_id": "CUST-004", "amount": 200.00, "status": "completed"},
]

# These records have amount as STRING - will be rescued
ex4_records_bad = [
    {"order_id": "ORD-505", "customer_id": "CUST-005", "amount": "INVALID", "status": "pending"},
    {"order_id": "ORD-506", "customer_id": "CUST-001", "amount": "N/A", "status": "cancelled"},
]

# Write good records as one file
good_path = f"{VOLUME_BASE}/ex4_files/good_records.json"
bad_path = f"{VOLUME_BASE}/ex4_files/bad_records.json"

# Use dbutils to write JSON lines (one JSON object per line)
good_lines = "\n".join([json.dumps(r) for r in ex4_records_good])
bad_lines = "\n".join([json.dumps(r) for r in ex4_records_bad])

dbutils.fs.put(good_path, good_lines, overwrite=True)
dbutils.fs.put(bad_path, bad_lines, overwrite=True)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Exercise 5: Auto Loader with CSV format
# MAGIC CSV files with a pipe delimiter and header row. User must configure cloudFiles for CSV format
# MAGIC with custom parsing options.

# COMMAND ----------

# CSV records with pipe delimiter
ex5_csv_header = "order_id|customer_id|amount|status|order_ts"
ex5_csv_rows_file1 = [
    "ORD-551|CUST-001|88.50|completed|2026-03-05T10:00:00",
    "ORD-552|CUST-002|215.00|pending|2026-03-05T10:05:00",
    "ORD-553|CUST-003|42.75|shipped|2026-03-05T10:10:00",
]
ex5_csv_rows_file2 = [
    "ORD-554|CUST-004|330.00|completed|2026-03-05T10:15:00",
    "ORD-555|CUST-001|67.25|pending|2026-03-05T10:20:00",
]

file1_content = ex5_csv_header + "\n" + "\n".join(ex5_csv_rows_file1)
file2_content = ex5_csv_header + "\n" + "\n".join(ex5_csv_rows_file2)

dbutils.fs.put(f"{VOLUME_BASE}/ex5_files/orders_batch1.csv", file1_content, overwrite=True)
dbutils.fs.put(f"{VOLUME_BASE}/ex5_files/orders_batch2.csv", file2_content, overwrite=True)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Exercise 6: Auto Loader + MERGE for incremental upsert
# MAGIC Source JSON files with order updates (some new, some updates to existing).
# MAGIC User reads with Auto Loader and MERGEs into a target table.

# COMMAND ----------

# Create the target table with initial data (existing orders)
spark.sql(f"""
    CREATE OR REPLACE TABLE {CATALOG}.{SCHEMA}.ex6_target (
        order_id STRING,
        customer_id STRING,
        amount DOUBLE,
        status STRING,
        order_ts TIMESTAMP
    ) USING DELTA
""")

spark.sql(f"""
    INSERT INTO {CATALOG}.{SCHEMA}.ex6_target VALUES
        ('ORD-601', 'CUST-001', 80.00, 'pending', TIMESTAMP '2026-03-04 09:00:00'),
        ('ORD-602', 'CUST-002', 120.00, 'pending', TIMESTAMP '2026-03-04 09:05:00'),
        ('ORD-603', 'CUST-003', 95.50, 'pending', TIMESTAMP '2026-03-04 09:10:00')
""")

# Source files: 2 updated orders + 2 new orders
ex6_data = [
    ("ORD-601", "CUST-001", 80.00, "shipped", datetime(2026, 3, 4, 14, 0, 0)),
    ("ORD-602", "CUST-002", 120.00, "completed", datetime(2026, 3, 4, 14, 5, 0)),
    ("ORD-604", "CUST-004", 250.00, "pending", datetime(2026, 3, 4, 14, 10, 0)),
    ("ORD-605", "CUST-005", 175.00, "pending", datetime(2026, 3, 4, 14, 15, 0)),
]

ex6_schema = StructType([
    StructField("order_id", StringType()),
    StructField("customer_id", StringType()),
    StructField("amount", DoubleType()),
    StructField("status", StringType()),
    StructField("order_ts", TimestampType()),
])

df_ex6 = spark.createDataFrame(ex6_data, schema=ex6_schema)
df_ex6.coalesce(1).write.mode("overwrite").json(f"{VOLUME_BASE}/ex6_files")

# COMMAND ----------

print(f"Setup complete for Auto Loader Ingestion")
print(f"  Schema: {CATALOG}.{SCHEMA}")
print(f"  Volume: {VOLUME_BASE}")
print(f"  Checkpoints: {CHECKPOINT_BASE}")
print(f"  Exercise 1: 10 records across 5 JSON file batches in ex1_files/")
print(f"  Exercise 2: 6 records with integer amounts in ex2_files/")
print(f"  Exercise 3: 4 records (batch1) + 3 records with extra column (batch2)")
print(f"  Exercise 4: 4 good + 2 type-mismatched records in ex4_files/")
print(f"  Exercise 5: 5 CSV records (pipe-delimited, 2 files) in ex5_files/")
print(f"  Exercise 6: 4 source records (2 updates + 2 new) in ex6_files/, target table with 3 rows")
