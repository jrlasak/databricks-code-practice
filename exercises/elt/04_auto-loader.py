# Databricks notebook source
# COMMAND ----------
# MAGIC %md
# MAGIC # Auto Loader Ingestion
# MAGIC **Topic**: ELT | **Exercises**: 6 | **Total Time**: ~75 min
# MAGIC
# MAGIC Practice Auto Loader (`cloudFiles`): basic file ingestion from Volumes, schema hints for type
# MAGIC control, schema evolution for new columns, rescued data for type mismatches, CSV format with
# MAGIC custom options, and combining Auto Loader with MERGE INTO for incremental upsert pipelines.
# MAGIC
# MAGIC **Solutions**: If stuck, see `solutions/auto-loader-solutions.py` for hints and answers.
# MAGIC
# MAGIC **Key concepts**:
# MAGIC - `cloudFiles` format reads files incrementally from a directory
# MAGIC - `trigger(availableNow=True)` processes all available files and stops
# MAGIC - Checkpoints track which files have been processed
# MAGIC - Schema inference, hints, and evolution control how Auto Loader handles schema changes
# MAGIC - Rescued data column captures records that don't match the inferred/expected schema
# MAGIC - `foreachBatch` enables MERGE-based upsert pipelines with streaming sources

# COMMAND ----------

# MAGIC %run ./00_Setup

# COMMAND ----------

# MAGIC %run ./setup/auto-loader-setup

# COMMAND ----------
# MAGIC %md
# MAGIC **Setup complete.** Exercise files and tables are in `db_code.auto_loader`.
# MAGIC
# MAGIC **Source files** (in Unity Catalog Volumes):
# MAGIC - Ex 1: `/Volumes/db_code/auto_loader/source_files/ex1_files/` - JSON, 10 order records across 5 batches
# MAGIC - Ex 2: `/Volumes/db_code/auto_loader/source_files/ex2_files/` - JSON, 6 records with integer amounts
# MAGIC - Ex 3: `/Volumes/db_code/auto_loader/source_files/ex3_files_batch1/` + `ex3_files_batch2/` - JSON, 4 + 3 records (batch 2 adds `priority` column)
# MAGIC - Ex 4: `/Volumes/db_code/auto_loader/source_files/ex4_files/` - JSON, 4 good + 2 type-mismatched records
# MAGIC - Ex 5: `/Volumes/db_code/auto_loader/source_files/ex5_files/` - CSV (pipe-delimited), 5 records across 2 files
# MAGIC - Ex 6: `/Volumes/db_code/auto_loader/source_files/ex6_files/` - JSON, 4 records (2 updates, 2 new). Target table `ex6_target` has 3 existing rows.
# MAGIC
# MAGIC **Checkpoints**: Use `CHECKPOINT_BASE` variable (`/Volumes/db_code/auto_loader/checkpoints/`) for all checkpoint locations.
# MAGIC
# MAGIC **Pattern**: Each exercise reads from a source directory, writes to `db_code.auto_loader.exN_output` (except Ex 6 which MERGEs into `ex6_target`).

# COMMAND ----------
# MAGIC %md
# MAGIC ## Exercise 1: Basic cloudFiles Read
# MAGIC **Difficulty**: Easy | **Time**: ~10 min
# MAGIC
# MAGIC Read JSON files from a Volume directory using Auto Loader and write them to a Delta table.
# MAGIC This is the foundational Auto Loader pattern: `cloudFiles` format, Volume source path,
# MAGIC `trigger(availableNow=True)` to process all available files and stop.
# MAGIC
# MAGIC **Source**: JSON files in `/Volumes/db_code/auto_loader/source_files/ex1_files/` (5 batches, 10 records total)
# MAGIC
# MAGIC **Schema** (per JSON record):
# MAGIC | Column | Type | Notes |
# MAGIC |--------|------|-------|
# MAGIC | order_id | STRING | ORD-201 through ORD-210 |
# MAGIC | customer_id | STRING | CUST-001 through CUST-005 |
# MAGIC | amount | DOUBLE | Order total |
# MAGIC | status | STRING | completed, pending, shipped, cancelled |
# MAGIC | order_ts | TIMESTAMP | Order timestamp |
# MAGIC
# MAGIC **Target**: Write to `db_code.auto_loader.ex1_output`. Expected 10 rows.
# MAGIC
# MAGIC **Requirements**:
# MAGIC 1. Use `spark.readStream.format("cloudFiles")` with `cloudFiles.format` = `json`
# MAGIC 2. Read from the ex1_files source path
# MAGIC 3. Set a checkpoint location under `CHECKPOINT_BASE`
# MAGIC 4. Write as Delta to the target table using `trigger(availableNow=True)`
# MAGIC 5. Wait for the stream to finish with `.awaitTermination()`
# MAGIC
# MAGIC **Constraints**:
# MAGIC - Use `trigger(availableNow=True)`, NOT `trigger(once=True)`
# MAGIC - Checkpoint path must be in a Volume

# COMMAND ----------

# EXERCISE_KEY: auto_loader_ex1
# TODO: Read JSON files with Auto Loader and write to Delta table

# Your code here


# COMMAND ----------

# Validate Exercise 1
result = spark.table(f"{CATALOG}.{SCHEMA}.ex1_output")

assert result.count() == 10, f"Expected 10 rows, got {result.count()}"
assert "order_id" in result.columns, "Missing order_id column"
assert "amount" in result.columns, "Missing amount column"
assert result.filter("order_id = 'ORD-201'").count() == 1, "ORD-201 should exist"
assert result.filter("order_id = 'ORD-210'").count() == 1, "ORD-210 should exist"

print("Exercise 1 passed!")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Exercise 2: Schema Hints for Type Control
# MAGIC **Difficulty**: Medium | **Time**: ~10 min
# MAGIC
# MAGIC Auto Loader infers schema from sampled JSON files. When JSON stores numbers without
# MAGIC decimal points (e.g., `100` instead of `100.0`), Auto Loader infers LONG instead of DOUBLE.
# MAGIC Use schema hints to override the inferred type for `amount` to DOUBLE.
# MAGIC
# MAGIC **Source**: JSON files in `/Volumes/db_code/auto_loader/source_files/ex2_files/` (6 records, `amount` is integer in JSON)
# MAGIC
# MAGIC **Target**: Write to `db_code.auto_loader.ex2_output`. Expected 6 rows. `amount` column must be DOUBLE type.
# MAGIC
# MAGIC **Requirements**:
# MAGIC 1. Use `cloudFiles` format with `cloudFiles.format` = `json`
# MAGIC 2. Add a schema hint to force `amount` to DOUBLE
# MAGIC 3. Write to the target table with `trigger(availableNow=True)`
# MAGIC 4. The `amount` column in the output must be DoubleType, not LongType
# MAGIC
# MAGIC **Constraints**:
# MAGIC - Do NOT provide a full schema - use schema inference with hints
# MAGIC - The hint must handle the `amount` column specifically

# COMMAND ----------

# EXERCISE_KEY: auto_loader_ex2
# TODO: Read JSON with schema hints to force amount to DOUBLE

# Your code here


# COMMAND ----------

# Validate Exercise 2
from pyspark.sql.types import DoubleType as _DoubleType

result = spark.table(f"{CATALOG}.{SCHEMA}.ex2_output")

assert result.count() == 6, f"Expected 6 rows, got {result.count()}"
amount_type = [f.dataType for f in result.schema.fields if f.name == "amount"][0]
assert isinstance(amount_type, _DoubleType), f"amount should be DoubleType, got {amount_type}"
assert result.filter("order_id = 'ORD-305'").select("amount").collect()[0][0] == 0.0, \
    "ORD-305 amount should be 0.0 (zero order)"
assert result.filter("order_id = 'ORD-304'").select("amount").collect()[0][0] == 500.0, \
    "ORD-304 amount should be 500.0"

print("Exercise 2 passed!")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Exercise 3: Schema Evolution Handling
# MAGIC **Difficulty**: Medium | **Time**: ~15 min
# MAGIC
# MAGIC In production, upstream sources add new columns over time. Auto Loader can detect
# MAGIC schema changes and evolve the target table automatically. This exercise uses two batches
# MAGIC of files: batch 1 has the base schema, batch 2 adds a `priority` column.
# MAGIC
# MAGIC **Source**: Two directories -
# MAGIC - `/Volumes/db_code/auto_loader/source_files/ex3_files_batch1/` (4 records, base schema)
# MAGIC - `/Volumes/db_code/auto_loader/source_files/ex3_files_batch2/` (3 records, adds `priority` column)
# MAGIC
# MAGIC **Target**: Write to `db_code.auto_loader.ex3_output`. Expected 7 rows total. `priority` column must exist.
# MAGIC
# MAGIC **Requirements**:
# MAGIC 1. Configure Auto Loader with schema evolution mode set to `addNewColumns`
# MAGIC 2. Process batch 1 first to establish the base schema, then process batch 2 which has the new column
# MAGIC 3. The target table should have the `priority` column after processing batch 2
# MAGIC 4. Rows from batch 1 should have `priority` = null
# MAGIC
# MAGIC **Approach**: Process the files in two steps. First stream reads batch 1 to create the table
# MAGIC with the base schema. Second stream reads batch 2 which has the new column. Use separate
# MAGIC schema locations and separate checkpoints for each stream. The `addNewColumns` schema
# MAGIC evolution mode tells Auto Loader to accept new columns, and `mergeSchema` on the write
# MAGIC side lets Delta evolve the target table schema.
# MAGIC
# MAGIC **Constraints**:
# MAGIC - Use `cloudFiles.schemaEvolutionMode` option
# MAGIC - Both batches must end up in the same target table
# MAGIC - Do NOT manually ALTER TABLE to add the column

# COMMAND ----------

# EXERCISE_KEY: auto_loader_ex3
# TODO: Configure Auto Loader with schema evolution to handle new columns

# Your code here


# COMMAND ----------

# Validate Exercise 3
result = spark.table(f"{CATALOG}.{SCHEMA}.ex3_output")

assert result.count() == 7, f"Expected 7 rows, got {result.count()}"
assert "priority" in result.columns, "Target should have 'priority' column after schema evolution"
# Batch 1 rows should have null priority
batch1_nulls = result.filter("priority IS NULL").count()
assert batch1_nulls == 4, f"Expected 4 rows with null priority (batch 1), got {batch1_nulls}"
# Batch 2 rows should have priority values
batch2_vals = result.filter("priority IS NOT NULL").count()
assert batch2_vals == 3, f"Expected 3 rows with priority values (batch 2), got {batch2_vals}"
assert result.filter("order_id = 'ORD-405'").select("priority").collect()[0][0] == "high", \
    "ORD-405 should have priority='high'"

print("Exercise 3 passed!")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Exercise 4: Rescued Data Column
# MAGIC **Difficulty**: Medium | **Time**: ~10 min
# MAGIC
# MAGIC When Auto Loader encounters records that don't match the expected schema (e.g., a string
# MAGIC value in a numeric column), it can rescue those records into a special column instead of
# MAGIC dropping them silently. This is critical in production - you need to know what data failed
# MAGIC to parse and why.
# MAGIC
# MAGIC **Source**: JSON files in `/Volumes/db_code/auto_loader/source_files/ex4_files/`
# MAGIC - 4 records with valid `amount` (numeric)
# MAGIC - 2 records with invalid `amount` ("INVALID", "N/A" - strings instead of numbers)
# MAGIC
# MAGIC **Target**: Write to `db_code.auto_loader.ex4_output`. Expected 6 rows.
# MAGIC - 4 rows with `amount` populated and `_rescued_data` = null
# MAGIC - 2 rows with `amount` = null and `_rescued_data` containing the mismatched value
# MAGIC
# MAGIC **Requirements**:
# MAGIC 1. Provide a schema that expects `amount` as DOUBLE
# MAGIC 2. Enable the rescued data column
# MAGIC 3. All 6 records should appear in the output (none dropped)
# MAGIC 4. Records with invalid `amount` should have their original data in `_rescued_data`
# MAGIC
# MAGIC **Constraints**:
# MAGIC - You MUST provide a schema (not rely on inference) so Auto Loader knows `amount` should be DOUBLE
# MAGIC - The rescued data column name is `_rescued_data` by default

# COMMAND ----------

# EXERCISE_KEY: auto_loader_ex4
# TODO: Read JSON with rescued data column enabled

# Your code here


# COMMAND ----------

# Validate Exercise 4
result = spark.table(f"{CATALOG}.{SCHEMA}.ex4_output")

assert result.count() == 6, f"Expected 6 rows, got {result.count()}"
assert "_rescued_data" in result.columns, "Output should have _rescued_data column"
# Valid records: amount populated, no rescued data
valid = result.filter("amount IS NOT NULL AND _rescued_data IS NULL")
assert valid.count() == 4, f"Expected 4 valid rows (amount NOT NULL, no rescued data), got {valid.count()}"
# Invalid records: amount null, rescued data populated
rescued = result.filter("amount IS NULL AND _rescued_data IS NOT NULL")
assert rescued.count() == 2, f"Expected 2 rescued rows (amount NULL, rescued data present), got {rescued.count()}"
# Verify specific rescued record
rescued_505 = result.filter("order_id = 'ORD-505'").select("_rescued_data").collect()[0][0]
assert rescued_505 is not None, "ORD-505 should have rescued data (amount was 'INVALID')"

print("Exercise 4 passed!")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Exercise 5: Auto Loader with CSV Format and Custom Options
# MAGIC **Difficulty**: Medium | **Time**: ~10 min
# MAGIC
# MAGIC Auto Loader supports multiple file formats beyond JSON. CSV files require additional
# MAGIC configuration: delimiter, header handling, and schema specification. This exercise reads
# MAGIC pipe-delimited CSV files with headers.
# MAGIC
# MAGIC **Source**: CSV files in `/Volumes/db_code/auto_loader/source_files/ex5_files/` (2 files, 5 records total)
# MAGIC
# MAGIC **File format**: Pipe-delimited (`|`), with header row
# MAGIC ```
# MAGIC order_id|customer_id|amount|status|order_ts
# MAGIC ORD-551|CUST-001|88.50|completed|2026-03-05T10:00:00
# MAGIC ```
# MAGIC
# MAGIC **Target**: Write to `db_code.auto_loader.ex5_output`. Expected 5 rows.
# MAGIC
# MAGIC **Requirements**:
# MAGIC 1. Use `cloudFiles` format with `cloudFiles.format` = `csv`
# MAGIC 2. Set the delimiter to pipe (`|`)
# MAGIC 3. Enable header detection so column names come from the first row
# MAGIC 4. Use a schema hint to cast `amount` as DOUBLE (CSV reads everything as STRING by default)
# MAGIC 5. Write to the target table with `trigger(availableNow=True)`
# MAGIC
# MAGIC **Constraints**:
# MAGIC - Do NOT hardcode column names in a full schema - use header + hints
# MAGIC - The delimiter option for CSV in cloudFiles is `delimiter` (not `sep`)

# COMMAND ----------

# EXERCISE_KEY: auto_loader_ex5
# TODO: Read CSV files with Auto Loader using pipe delimiter and header

# Your code here


# COMMAND ----------

# Validate Exercise 5
result = spark.table(f"{CATALOG}.{SCHEMA}.ex5_output")

assert result.count() == 5, f"Expected 5 rows, got {result.count()}"
assert "order_id" in result.columns, "Missing order_id column"
assert "amount" in result.columns, "Missing amount column"
assert result.filter("order_id = 'ORD-551'").count() == 1, "ORD-551 should exist"
assert result.filter("order_id = 'ORD-555'").count() == 1, "ORD-555 should exist"
# Verify amount is numeric (not string)
amt_551 = result.filter("order_id = 'ORD-551'").select("amount").collect()[0][0]
assert float(amt_551) == 88.50, f"ORD-551 amount should be 88.50, got {amt_551}"
amt_554 = result.filter("order_id = 'ORD-554'").select("amount").collect()[0][0]
assert float(amt_554) == 330.00, f"ORD-554 amount should be 330.00, got {amt_554}"

print("Exercise 5 passed!")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Exercise 6: Auto Loader + MERGE for Incremental Dedup Upsert
# MAGIC **Difficulty**: Hard | **Time**: ~20 min
# MAGIC
# MAGIC The real production pattern: read new files incrementally with Auto Loader, then MERGE
# MAGIC them into a target Delta table to handle both inserts and updates. This combines streaming
# MAGIC ingestion with Delta Lake's MERGE for a complete incremental pipeline.
# MAGIC
# MAGIC **Source**: JSON files in `/Volumes/db_code/auto_loader/source_files/ex6_files/` (4 records)
# MAGIC - ORD-601 (status update: pending -> shipped)
# MAGIC - ORD-602 (status update: pending -> completed)
# MAGIC - ORD-604 (new order)
# MAGIC - ORD-605 (new order)
# MAGIC
# MAGIC **Target** (`db_code.auto_loader.ex6_target`): 3 existing rows
# MAGIC | order_id | customer_id | amount | status | order_ts |
# MAGIC |----------|-------------|--------|--------|----------|
# MAGIC | ORD-601 | CUST-001 | 80.00 | pending | 2026-03-04 09:00 |
# MAGIC | ORD-602 | CUST-002 | 120.00 | pending | 2026-03-04 09:05 |
# MAGIC | ORD-603 | CUST-003 | 95.50 | pending | 2026-03-04 09:10 |
# MAGIC
# MAGIC **Expected Output**: `ex6_target` should have 5 rows after MERGE.
# MAGIC - ORD-601: status='shipped' (updated)
# MAGIC - ORD-602: status='completed' (updated)
# MAGIC - ORD-603: status='pending' (unchanged, not in source)
# MAGIC - ORD-604: new row inserted
# MAGIC - ORD-605: new row inserted
# MAGIC
# MAGIC **Requirements**:
# MAGIC 1. Read source files using Auto Loader (`cloudFiles` format)
# MAGIC 2. Use `foreachBatch` to process each micro-batch with a MERGE
# MAGIC 3. MERGE on `order_id`: update matched, insert not matched
# MAGIC 4. Use `trigger(availableNow=True)` and `.awaitTermination()`
# MAGIC
# MAGIC **Constraints**:
# MAGIC - MERGE into `ex6_target` (already has 3 rows from setup)
# MAGIC - Do NOT overwrite the target table - MERGE only

# COMMAND ----------

# EXERCISE_KEY: auto_loader_ex6
# TODO: Auto Loader read + foreachBatch MERGE into target table

# Your code here


# COMMAND ----------

# Validate Exercise 6
result = spark.table(f"{CATALOG}.{SCHEMA}.ex6_target")

assert result.count() == 5, f"Expected 5 rows, got {result.count()}"
# Updated rows
assert result.filter("order_id = 'ORD-601'").select("status").collect()[0][0] == "shipped", \
    "ORD-601 should be updated to status='shipped'"
assert result.filter("order_id = 'ORD-602'").select("status").collect()[0][0] == "completed", \
    "ORD-602 should be updated to status='completed'"
# Unchanged row
assert result.filter("order_id = 'ORD-603'").select("status").collect()[0][0] == "pending", \
    "ORD-603 should be unchanged (not in source)"
# New rows
assert result.filter("order_id = 'ORD-604'").count() == 1, "ORD-604 should be inserted"
assert result.filter("order_id = 'ORD-605'").count() == 1, "ORD-605 should be inserted"
assert result.filter("order_id = 'ORD-604'").select("amount").collect()[0][0] == 250.00, \
    "ORD-604 amount should be 250.00"

print("Exercise 6 passed!")
