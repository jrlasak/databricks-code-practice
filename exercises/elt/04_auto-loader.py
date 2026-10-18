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
# MAGIC - The rescued data column keeps values that don't match the schema (wrong type, unknown column) instead of losing them as silent NULLs
# MAGIC - `foreachBatch` enables MERGE-based upsert pipelines with streaming sources

# COMMAND ----------

# MAGIC %run ./00_Setup

# COMMAND ----------

# MAGIC %run ./setup/auto-loader-setup

# COMMAND ----------
# MAGIC %md
# MAGIC Exercise files and tables are in `db_code.auto_loader`.
# MAGIC
# MAGIC **Source files** (in Unity Catalog Volumes):
# MAGIC - Ex 1: `/Volumes/db_code/auto_loader/source_files/ex1_files/` - JSON, 10 order records across 5 batches
# MAGIC - Ex 2: `/Volumes/db_code/auto_loader/source_files/ex2_files/` - JSON, 6 records with integer amounts
# MAGIC - Ex 3: `/Volumes/db_code/auto_loader/source_files/ex3_files_batch1/` + `ex3_files_batch2/` - JSON, 4 + 3 records (batch 2 adds `priority` column)
# MAGIC - Ex 4: `/Volumes/db_code/auto_loader/source_files/ex4_files/` - JSON, 4 good + 2 type-mismatched records
# MAGIC - Ex 5: `/Volumes/db_code/auto_loader/source_files/ex5_files/` - CSV (pipe-delimited), 5 records across 2 files
# MAGIC - Ex 6: `/Volumes/db_code/auto_loader/source_files/ex6_files/` - JSON, 4 records (2 updates, 2 new). Target table `ex6_target` has 3 existing rows.
# MAGIC
# MAGIC **Every exercise**:
# MAGIC - Give each stream its own checkpoint folder under `CHECKPOINT_BASE` (set by the setup cell to `/Volumes/db_code/auto_loader/checkpoints`, no trailing slash, so join with `f"{CHECKPOINT_BASE}/ex2"`).
# MAGIC - Every exercise that infers its schema (all but Ex 4) also needs `cloudFiles.schemaLocation` under `CHECKPOINT_BASE`, in a folder of its own. Auto Loader stores the inferred schema there.
# MAGIC - Write with `trigger(availableNow=True)`, not the deprecated `trigger(once=True)`, and end with `.awaitTermination()` so the check cell reads the finished table.
# MAGIC - Write to `db_code.auto_loader.exN_output`, except Ex 6, which MERGEs into `db_code.auto_loader.ex6_target`.
# MAGIC
# MAGIC **In your code**: TODO cells are Python. The setup cell defines `CATALOG` (`db_code`), `SCHEMA` (`auto_loader`), `VOLUME_BASE` (`/Volumes/db_code/auto_loader/source_files`) and `CHECKPOINT_BASE`.
# MAGIC
# MAGIC **Retrying an exercise**: rerun the `%run ./setup/auto-loader-setup` cell first. A checkpoint remembers which files it has read, so a retry on the same checkpoint reads nothing. Setup deletes all checkpoints and every `exN_output` table and rebuilds `ex6_target`, so rerun any exercise you want to keep.

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
# MAGIC **Schema** (values in the JSON records; with default inference every column lands in `ex1_output` as STRING):
# MAGIC | Column | JSON value | Notes |
# MAGIC |--------|------------|-------|
# MAGIC | order_id | string | ORD-201 through ORD-210 |
# MAGIC | customer_id | string | CUST-001 through CUST-005 |
# MAGIC | amount | number | Order total |
# MAGIC | status | string | completed, pending, shipped, cancelled |
# MAGIC | order_ts | string (ISO timestamp) | Order timestamp |
# MAGIC
# MAGIC **Target**: Write to `db_code.auto_loader.ex1_output`. Expected 10 rows.
# MAGIC
# MAGIC **Requirements**:
# MAGIC 1. Use `spark.readStream.format("cloudFiles")` with `cloudFiles.format` = `json`
# MAGIC 2. Read from `/Volumes/db_code/auto_loader/source_files/ex1_files/`
# MAGIC 3. Set `cloudFiles.schemaLocation` (where Auto Loader stores the inferred schema) and the write's `checkpointLocation`, both under `CHECKPOINT_BASE`, one folder each (for example `/Volumes/db_code/auto_loader/checkpoints/ex1_schema` and `/Volumes/db_code/auto_loader/checkpoints/ex1`)
# MAGIC 4. Write as Delta to the target table using `trigger(availableNow=True)`
# MAGIC 5. Wait for the stream to finish with `.awaitTermination()`

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
# MAGIC Auto Loader infers the schema from sampled JSON files. By default it infers every JSON column as STRING; with `cloudFiles.inferColumnTypes` set to `true` it infers `100` (no decimal point) as LONG. Either way, use a schema hint to pin `amount` to DOUBLE.
# MAGIC
# MAGIC **Source**: JSON files in `/Volumes/db_code/auto_loader/source_files/ex2_files/` (6 records, `amount` is integer in JSON)
# MAGIC
# MAGIC **Target**: Write to `db_code.auto_loader.ex2_output`. Expected 6 rows. `amount` column must be DOUBLE type.
# MAGIC
# MAGIC **Requirements**:
# MAGIC 1. Use `cloudFiles` format with `cloudFiles.format` = `json`
# MAGIC 2. Add a schema hint to force `amount` to DOUBLE
# MAGIC
# MAGIC **Constraints**:
# MAGIC - Do NOT provide a full schema - use schema inference with hints

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
    "ORD-305 amount does not match its JSON value (a zero order)"
assert result.filter("order_id = 'ORD-304'").select("amount").collect()[0][0] == 500.0, \
    "ORD-304 amount does not match its JSON value"

print("Exercise 2 passed!")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Exercise 3: Schema Evolution Handling
# MAGIC **Difficulty**: Medium | **Time**: ~15 min
# MAGIC
# MAGIC In production, upstream sources add new columns over time. Auto Loader evolves the schema it reads with, and `mergeSchema` on the write lets the Delta table take the new column. This exercise uses two batches of files: batch 1 has the base schema, batch 2 adds a `priority` column.
# MAGIC
# MAGIC **Source**: Two directories -
# MAGIC - `/Volumes/db_code/auto_loader/source_files/ex3_files_batch1/` (4 records, base schema)
# MAGIC - `/Volumes/db_code/auto_loader/source_files/ex3_files_batch2/` (3 records, adds `priority` column)
# MAGIC
# MAGIC **Target**: Write to `db_code.auto_loader.ex3_output`. Expected 7 rows total. `priority` column must exist.
# MAGIC
# MAGIC **Requirements**:
# MAGIC 1. Run a first stream on `ex3_files_batch1/` that creates `db_code.auto_loader.ex3_output` with the base schema.
# MAGIC 2. Run a second stream on `ex3_files_batch2/` that writes to the same table and adds the `priority` column.
# MAGIC 3. Give each stream its own schema location and its own checkpoint under `CHECKPOINT_BASE`, so the second stream infers `priority` from batch 2 when it starts.
# MAGIC 4. Set `cloudFiles.schemaEvolutionMode` to `addNewColumns` on both streams (already the default when Auto Loader infers the schema; set it so the intent is visible), and set `mergeSchema` to `true` on the write so the Delta table adds `priority`.
# MAGIC
# MAGIC After both streams, the 4 batch 1 rows have `priority` = null and the 3 batch 2 rows have a value (ORD-405 is `high`).
# MAGIC
# MAGIC **Note**: with one long-lived stream and one schema location, `addNewColumns` stops the stream once with `UNKNOWN_FIELD_EXCEPTION` after recording the new column, and the next run, usually a job retry, continues with it.
# MAGIC
# MAGIC **Constraints**:
# MAGIC - Both batches end up in the same target table
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
# MAGIC When Auto Loader parses a value that doesn't match the schema (e.g., a string in a numeric column), the row still lands with that column NULL. Without a rescued data column the original value is lost; with one, Auto Loader keeps it in the rescued data column as JSON, together with the source file path. This is critical in production - you need to know what data failed to parse and why.
# MAGIC
# MAGIC **Source**: JSON files in `/Volumes/db_code/auto_loader/source_files/ex4_files/`
# MAGIC - Each record has `order_id`, `customer_id` and `status` (all STRING) and `amount`; there is no `order_ts`
# MAGIC - 4 records have a numeric `amount`, 2 have a string ("INVALID" on ORD-505, "N/A" on ORD-506)
# MAGIC
# MAGIC **Target**: Write to `db_code.auto_loader.ex4_output`. Expected 6 rows.
# MAGIC - 4 rows with `amount` populated and `_rescued_data` = null
# MAGIC - 2 rows with `amount` = null and `_rescued_data` containing the mismatched value
# MAGIC
# MAGIC **Requirements**:
# MAGIC 1. Provide a schema with all four columns, `amount` as DOUBLE (a column you leave out also lands in `_rescued_data`)
# MAGIC 2. Add the rescued data column: with your own schema it only appears when you set its name with the `rescuedDataColumn` reader option
# MAGIC 3. Records with invalid `amount` should have their original data in `_rescued_data`
# MAGIC
# MAGIC **Constraints**:
# MAGIC - Name the column `_rescued_data` (the name Auto Loader uses when it infers the schema)

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
# MAGIC
# MAGIC **Constraints**:
# MAGIC - Do NOT hardcode column names in a full schema - use header + hints
# MAGIC - Set the separator with `sep` or its alias `delimiter`

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
# Verify amount is DOUBLE (schema hint applied)
from pyspark.sql.types import DoubleType as _DoubleType
amount_type = [f.dataType for f in result.schema.fields if f.name == "amount"][0]
assert isinstance(amount_type, _DoubleType), f"amount should be DoubleType, got {amount_type}"
amt_551 = result.filter("order_id = 'ORD-551'").select("amount").collect()[0][0]
assert amt_551 == 88.50, f"ORD-551 amount should be 88.50, got {amt_551}"
amt_554 = result.filter("order_id = 'ORD-554'").select("amount").collect()[0][0]
assert amt_554 == 330.00, f"ORD-554 amount should be 330.00, got {amt_554}"

print("Exercise 5 passed!")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Exercise 6: Auto Loader + MERGE for Incremental Upsert
# MAGIC **Difficulty**: Hard | **Time**: ~20 min
# MAGIC
# MAGIC This is the production pattern: read new files incrementally with Auto Loader, then MERGE them into a target Delta table so one run handles both inserts and updates.
# MAGIC
# MAGIC **Source**: JSON files in `/Volumes/db_code/auto_loader/source_files/ex6_files/` (4 records)
# MAGIC - ORD-601 (status update: pending -> shipped)
# MAGIC - ORD-602 (status update: pending -> completed)
# MAGIC - ORD-604 (new order)
# MAGIC - ORD-605 (new order)
# MAGIC
# MAGIC With default inference every JSON value arrives as STRING, while `ex6_target` stores `amount` as DOUBLE and `order_ts` as TIMESTAMP: make the source types match the target before the MERGE.
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
# MAGIC 2. Use `foreachBatch` to process each micro-batch with a MERGE; inside the function, run the MERGE through the batch's own session (`batch_df.sparkSession`), not the notebook's `spark`.
# MAGIC 3. MERGE on `order_id`: update matched, insert not matched
# MAGIC
# MAGIC **Constraints**:
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
