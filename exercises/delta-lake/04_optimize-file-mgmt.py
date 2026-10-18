# Databricks notebook source
# COMMAND ----------
# MAGIC %md
# MAGIC # OPTIMIZE & File Management
# MAGIC **Topic**: Delta Lake | **Exercises**: 6 | **Checkpoints**: 2 | **Total Time**: ~55 min
# MAGIC
# MAGIC Practice Delta Lake table maintenance: inspect file metrics, compact small files
# MAGIC with OPTIMIZE, co-locate data with ZORDER, clean up old files with VACUUM, and
# MAGIC analyze maintenance operations from table history.
# MAGIC
# MAGIC **Solutions**: If stuck, see `solutions/optimize-file-mgmt-solutions.py` for hints and answers.
# MAGIC
# MAGIC **Tables used** (from `00_Setup.py`):
# MAGIC - `db_code.delta_lake.orders` - order records
# MAGIC
# MAGIC **Schema** (`orders`):
# MAGIC | Column | Type | Notes |
# MAGIC |--------|------|-------|
# MAGIC | order_id | STRING | Order key (repeats in the base table: ORD-042 and ORD-067 appear twice) |
# MAGIC | customer_id | STRING | FK to customers (some nulls) |
# MAGIC | product_id | STRING | FK to products |
# MAGIC | amount | DOUBLE | Order total in USD |
# MAGIC | status | STRING | completed, pending, shipped, cancelled |
# MAGIC | order_date | DATE | Order placement date |
# MAGIC | updated_at | TIMESTAMP | Last modification time |

# COMMAND ----------

# MAGIC %run ./00_Setup

# COMMAND ----------

# MAGIC %run ./setup/optimize-file-mgmt-setup

# COMMAND ----------
# MAGIC %md
# MAGIC **Setup complete.** Every exercise table is in `db_code.optimize_file_mgmt`. Write your output tables there too, by full name (for example `db_code.optimize_file_mgmt.opt_ex1_detail`). In Python cells, the variables `CATALOG` and `SCHEMA` hold `db_code` and `optimize_file_mgmt`. The base table (`orders`) is in `db_code.delta_lake`.
# MAGIC
# MAGIC Keep the `# EXERCISE_KEY: ...` line as the first line of each answer cell. The answer cells are Python: run SQL with `spark.sql()`.
# MAGIC
# MAGIC Exercise tables:
# MAGIC - `opt_ex1_orders`, `opt_ex2_orders`, `opt_ex3_orders`, `opt_ex4_orders`: 20 rows each (`ORD-001` to `ORD-020`), written by 10 two-row INSERTs, so 10 small files each
# MAGIC - `opt_ex5_orders`, `opt_ex7_orders`: 5 rows each (`ORD-001` to `ORD-005`), written in one CTAS
# MAGIC - `opt_ex6_orders`: 4 rows; setup updated `ORD-001` and `ORD-002` and deleted `ORD-005`
# MAGIC - `opt_ex8_orders`: 20 rows written by 10 two-row INSERTs, then compacted by one `OPTIMIZE` in setup

# COMMAND ----------
# MAGIC %md
# MAGIC ## Checkpoint 1: Inspect File Count and Size
# MAGIC **Time**: ~5 min
# MAGIC
# MAGIC Use `DESCRIBE DETAIL` to read how many data files a Delta table has and how many bytes they take.
# MAGIC
# MAGIC **Table** (`db_code.optimize_file_mgmt.opt_ex1_orders`): 20 rows (`ORD-001` to `ORD-020`) written by 10 separate 2-row INSERTs, so 10 small files.
# MAGIC
# MAGIC **Expected Output**: `opt_ex1_detail` has 1 row: `num_files` is the `numFiles` you read (10) and `size_bytes` is the `sizeInBytes` you read.
# MAGIC
# MAGIC **Requirements**:
# MAGIC 1. Add a new cell and run `DESCRIBE DETAIL db_code.optimize_file_mgmt.opt_ex1_orders`
# MAGIC 2. Read `numFiles` and `sizeInBytes` from the output
# MAGIC 3. Put them in `num_files` and `size_bytes` in the code cell below and run it

# COMMAND ----------

# EXERCISE_KEY: opt_ex1
# TODO: Run DESCRIBE DETAIL on opt_ex1_orders, then fill in what you observe

num_files = 0      # Replace: numFiles from DESCRIBE DETAIL
size_bytes = 0     # Replace: sizeInBytes from DESCRIBE DETAIL

spark.sql(f"""
    CREATE OR REPLACE TABLE {CATALOG}.{SCHEMA}.opt_ex1_detail AS
    SELECT CAST({num_files} AS LONG) AS num_files, CAST({size_bytes} AS LONG) AS size_bytes
""")

# COMMAND ----------

# Validate Checkpoint 1
result = spark.table(f"{CATALOG}.{SCHEMA}.opt_ex1_detail").collect()[0]
actual = spark.sql(f"DESCRIBE DETAIL {CATALOG}.{SCHEMA}.opt_ex1_orders").collect()[0]

assert result.num_files == actual.numFiles, f"num_files should be the numFiles that DESCRIBE DETAIL shows, got {result.num_files}"
assert result.size_bytes == actual.sizeInBytes, f"size_bytes should be the sizeInBytes that DESCRIBE DETAIL shows, got {result.size_bytes}"

print("Checkpoint 1 passed!")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Exercise 2: Run OPTIMIZE to Compact Files
# MAGIC **Difficulty**: Easy | **Time**: ~5 min
# MAGIC
# MAGIC OPTIMIZE compacts many small files into a few larger ones without changing the data.
# MAGIC
# MAGIC **Table** (`db_code.optimize_file_mgmt.opt_ex2_orders`): 20 rows in 10 small files.
# MAGIC
# MAGIC **Expected Output**: After OPTIMIZE, `opt_ex2_orders` has 1-2 files instead of 10 and still holds the same 20 rows.
# MAGIC
# MAGIC **Requirements**:
# MAGIC 1. Run `OPTIMIZE` on `opt_ex2_orders`

# COMMAND ----------

# EXERCISE_KEY: opt_ex2
# TODO: Run OPTIMIZE on opt_ex2_orders

# Your code here


# COMMAND ----------

# Validate Exercise 2
detail = spark.sql(f"DESCRIBE DETAIL {CATALOG}.{SCHEMA}.opt_ex2_orders").collect()[0]

assert detail.numFiles <= 2, f"After OPTIMIZE, expected 1-2 files, got {detail.numFiles}"
# Data should be unchanged
row_count = spark.table(f"{CATALOG}.{SCHEMA}.opt_ex2_orders").count()
assert row_count == 20, f"opt_ex2_orders has {row_count} rows: OPTIMIZE must not add or remove rows"

print("Exercise 2 passed!")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Exercise 3: OPTIMIZE with ZORDER
# MAGIC **Difficulty**: Easy | **Time**: ~5 min
# MAGIC
# MAGIC ZORDER co-locates rows with similar values in the same files, so a filter on that column can skip files. It pays off on high-cardinality filter columns such as `customer_id`.
# MAGIC
# MAGIC **Note**: Liquid Clustering is the recommended replacement for ZORDER on new tables.
# MAGIC ZORDER is still valid on existing non-clustered tables; `05_liquid-clustering.py` covers the replacement.
# MAGIC
# MAGIC **Table** (`db_code.optimize_file_mgmt.opt_ex3_orders`): 20 rows in 10 small files.
# MAGIC
# MAGIC **Expected Output**: `opt_ex3_orders` is compacted to 1-2 files, and `DESCRIBE HISTORY` shows an `OPTIMIZE` row Z-ordered by `customer_id`.
# MAGIC
# MAGIC **Requirements**:
# MAGIC 1. Run `OPTIMIZE` with `ZORDER BY (customer_id)` on `opt_ex3_orders`

# COMMAND ----------

# EXERCISE_KEY: opt_ex3
# TODO: Run OPTIMIZE with ZORDER BY on the customer_id column

# Your code here


# COMMAND ----------

# Validate Exercise 3
detail = spark.sql(f"DESCRIBE DETAIL {CATALOG}.{SCHEMA}.opt_ex3_orders").collect()[0]
assert detail.numFiles <= 2, f"Table should be compacted, got {detail.numFiles} files"

# Every OPTIMIZE row has a zOrderBy parameter; it is '[]' when no ZORDER BY was used
hist = spark.sql(f"DESCRIBE HISTORY {CATALOG}.{SCHEMA}.opt_ex3_orders")
zorder_ops = [r for r in hist.filter("operation = 'OPTIMIZE'").collect()
              if r.operationParameters.get("zOrderBy", "[]") != "[]"]
assert len(zorder_ops) > 0, "No Z-ordered OPTIMIZE in the history: add ZORDER BY to your OPTIMIZE"
assert "customer_id" in zorder_ops[0].operationParameters["zOrderBy"], \
    f"The Z-order column is not the one the requirement names: got {zorder_ops[0].operationParameters['zOrderBy']}"

print("Exercise 3 passed!")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Exercise 4: Measure File Metrics Before and After OPTIMIZE
# MAGIC **Difficulty**: Medium | **Time**: ~10 min
# MAGIC
# MAGIC Record the file count of a fragmented table before and after `OPTIMIZE`.
# MAGIC
# MAGIC **Table** (`db_code.optimize_file_mgmt.opt_ex4_orders`): 20 rows in 10 small files, not yet optimized.
# MAGIC
# MAGIC **Expected Output**: `opt_ex4_comparison` holds one row: `before_files` = 10 and `after_files` = 1 (the check accepts 1 or 2).
# MAGIC
# MAGIC **Requirements** (all in the code cell below, in this order):
# MAGIC 1. Read `numFiles` of `opt_ex4_orders` into `before_files`
# MAGIC 2. Run `OPTIMIZE` on `opt_ex4_orders`
# MAGIC 3. Read `numFiles` again into `after_files`
# MAGIC 4. Run the cell: it saves both values to `opt_ex4_comparison`
# MAGIC
# MAGIC Read the counts in code rather than typing them: once `OPTIMIZE` has run, the before count is gone. If that happens, rerun the `%run ./setup/optimize-file-mgmt-setup` cell to rebuild the fragmented table. That resets the whole notebook: every exercise table is rebuilt and your output tables are dropped, so rerun the answer cells of any exercise you already passed.

# COMMAND ----------

# EXERCISE_KEY: opt_ex4
# TODO: Read numFiles, run OPTIMIZE, read numFiles again

before_files = 0  # Replace: numFiles of opt_ex4_orders before OPTIMIZE
# Write your OPTIMIZE statement here

after_files = 0   # Replace: numFiles of opt_ex4_orders after OPTIMIZE

spark.sql(f"""
    CREATE OR REPLACE TABLE {CATALOG}.{SCHEMA}.opt_ex4_comparison AS
    SELECT CAST({before_files} AS LONG) AS before_files, CAST({after_files} AS LONG) AS after_files
""")

# COMMAND ----------

# Validate Exercise 4
row = spark.table(f"{CATALOG}.{SCHEMA}.opt_ex4_comparison").collect()[0]

assert row.before_files >= 8, f"before_files should be the numFiles of opt_ex4_orders before OPTIMIZE, got {row.before_files}"
assert 1 <= row.after_files <= 2, f"after_files should be the numFiles of opt_ex4_orders after OPTIMIZE (1-2 files), got {row.after_files}"
assert row.before_files > row.after_files, \
    f"before_files ({row.before_files}) should be greater than after_files ({row.after_files})"

detail = spark.sql(f"DESCRIBE DETAIL {CATALOG}.{SCHEMA}.opt_ex4_orders").collect()[0]
assert detail.numFiles <= 2, f"opt_ex4_orders still has {detail.numFiles} files: run OPTIMIZE on it"
assert row.after_files == detail.numFiles, f"after_files should be the current numFiles of opt_ex4_orders, got {row.after_files}"

print("Exercise 4 passed!")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Exercise 5: Set VACUUM Retention Period
# MAGIC **Difficulty**: Easy | **Time**: ~5 min
# MAGIC
# MAGIC Keep removed data files of `opt_ex5_orders` for 30 days instead of the default 7, so time travel can reach a month back.
# MAGIC
# MAGIC **Table** (`db_code.optimize_file_mgmt.opt_ex5_orders`): 5 rows (`ORD-001` to `ORD-005`).
# MAGIC
# MAGIC **Expected Output**: `SHOW TBLPROPERTIES` on `opt_ex5_orders` lists `delta.deletedFileRetentionDuration` = `720 hours`.
# MAGIC
# MAGIC **Requirements**:
# MAGIC 1. With `ALTER TABLE ... SET TBLPROPERTIES`, set the table property that controls VACUUM's retention to `720 hours`

# COMMAND ----------

# EXERCISE_KEY: opt_ex5
# TODO: Set VACUUM's retention on opt_ex5_orders to 720 hours

# Your code here


# COMMAND ----------

# Validate Exercise 5
props = spark.sql(f"SHOW TBLPROPERTIES {CATALOG}.{SCHEMA}.opt_ex5_orders")
retention_rows = props.filter("key = 'delta.deletedFileRetentionDuration'").collect()

assert len(retention_rows) > 0, "Table should have delta.deletedFileRetentionDuration property"
assert retention_rows[0].value == "720 hours", \
    f"Retention should be '720 hours', got '{retention_rows[0].value}'"

print("Exercise 5 passed!")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Exercise 6: Run VACUUM and Verify
# MAGIC **Difficulty**: Medium | **Time**: ~10 min
# MAGIC
# MAGIC VACUUM deletes data files the table no longer references once they are older than the retention period.
# MAGIC
# MAGIC **Table** (`db_code.optimize_file_mgmt.opt_ex6_orders`): 4 rows. Setup ran two UPDATEs and one DELETE on it, so its storage holds files the current version no longer references.
# MAGIC
# MAGIC **Expected Output**: `DESCRIBE HISTORY` on `opt_ex6_orders` shows a `VACUUM START` and a `VACUUM END` row, and `delta.deletedFileRetentionDuration` is back to `168 hours`.
# MAGIC
# MAGIC **Requirements**:
# MAGIC 1. Set `delta.deletedFileRetentionDuration` on `opt_ex6_orders` to `0 hours`
# MAGIC 2. Run `VACUUM` on `opt_ex6_orders`
# MAGIC 3. Set `delta.deletedFileRetentionDuration` back to `168 hours`
# MAGIC
# MAGIC **Approach**: By default, VACUUM keeps unreferenced files for 7 days, so the files setup left behind are too new to delete. Lowering the table's retention lets VACUUM remove them now. A 0-hour retention is for this exercise only: in production, a job still writing to the table can lose files it has not committed yet, and time travel to earlier versions stops working.

# COMMAND ----------

# EXERCISE_KEY: opt_ex6
# TODO: Set retention to 0 hours, run VACUUM, restore retention

# Your code here


# COMMAND ----------

# Validate Exercise 6
hist = spark.sql(f"DESCRIBE HISTORY {CATALOG}.{SCHEMA}.opt_ex6_orders")
vacuum_ops = hist.filter("operation = 'VACUUM END'").count()

assert vacuum_ops > 0, "Should have at least one VACUUM operation in history"

ret = spark.sql(f"SHOW TBLPROPERTIES {CATALOG}.{SCHEMA}.opt_ex6_orders").filter("key = 'delta.deletedFileRetentionDuration'").collect()
assert ret and ret[0].value in ("168 hours", "interval 168 hours"), "Set delta.deletedFileRetentionDuration back to 168 hours"

print("Exercise 6 passed!")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Checkpoint 7: Table Health Report from DESCRIBE DETAIL
# MAGIC **Time**: ~5 min
# MAGIC
# MAGIC Read key metrics from `DESCRIBE DETAIL` and fill in a health report.
# MAGIC
# MAGIC **Table** (`db_code.optimize_file_mgmt.opt_ex7_orders`): 5 rows (`ORD-001` to `ORD-005`).
# MAGIC
# MAGIC **Expected Output**: `opt_ex7_report` holds one row: `table_name` is the `name` you read (`db_code.optimize_file_mgmt.opt_ex7_orders`), and `num_files` and `size_bytes` are the `numFiles` and `sizeInBytes` you read.
# MAGIC
# MAGIC **Requirements**:
# MAGIC 1. Add a new cell and run `DESCRIBE DETAIL db_code.optimize_file_mgmt.opt_ex7_orders`
# MAGIC 2. Find `name`, `numFiles`, `sizeInBytes` in the output
# MAGIC 3. Put them in `table_name`, `num_files` and `size_bytes` in the code cell below and run it

# COMMAND ----------

# EXERCISE_KEY: opt_ex7
# TODO: Run DESCRIBE DETAIL on opt_ex7_orders, then fill in what you observe

table_name = ""        # Replace: the 'name' value from DESCRIBE DETAIL
num_files = 0          # Replace: numFiles
size_bytes = 0         # Replace: sizeInBytes

spark.sql(f"""
    CREATE OR REPLACE TABLE {CATALOG}.{SCHEMA}.opt_ex7_report AS
    SELECT '{table_name}' AS table_name,
           CAST({num_files} AS LONG) AS num_files,
           CAST({size_bytes} AS LONG) AS size_bytes
""")

# COMMAND ----------

# Validate Checkpoint 7
row = spark.table(f"{CATALOG}.{SCHEMA}.opt_ex7_report").collect()[0]
actual = spark.sql(f"DESCRIBE DETAIL {CATALOG}.{SCHEMA}.opt_ex7_orders").collect()[0]

assert row.table_name == actual.name, f"table_name should be the name column of DESCRIBE DETAIL, got '{row.table_name}'"
assert row.num_files == actual.numFiles, f"num_files should be the numFiles that DESCRIBE DETAIL shows, got {row.num_files}"
assert row.size_bytes == actual.sizeInBytes, f"size_bytes should be the sizeInBytes that DESCRIBE DETAIL shows, got {row.size_bytes}"

print("Checkpoint 7 passed!")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Exercise 8: Analyze OPTIMIZE History
# MAGIC **Difficulty**: Medium | **Time**: ~10 min
# MAGIC
# MAGIC Query the table history to find the `OPTIMIZE` operation and read its metrics.
# MAGIC
# MAGIC **Table** (`db_code.optimize_file_mgmt.opt_ex8_orders`): 20 rows built from 10 small INSERT files, then compacted by one `OPTIMIZE` during setup.
# MAGIC
# MAGIC **Expected Output**: `opt_ex8_analysis` holds one row: the OPTIMIZE `version`, `files_added` = 1 and `files_removed` = 10 (the check accepts 1 or more added).
# MAGIC
# MAGIC **Requirements**:
# MAGIC 1. Run `DESCRIBE HISTORY` on `opt_ex8_orders`
# MAGIC 2. Find the row where `operation = 'OPTIMIZE'` (if you see more than one, take the one with the highest `version`)
# MAGIC 3. Read `numAddedFiles` and `numRemovedFiles` from the `operationMetrics` MAP column
# MAGIC 4. Fill in the placeholder values below

# COMMAND ----------

# EXERCISE_KEY: opt_ex8
# TODO: Run DESCRIBE HISTORY, find the OPTIMIZE row, read operationMetrics, fill in values

version = 0            # Replace: version number of the OPTIMIZE operation
files_added = "0"      # Replace: operationMetrics['numAddedFiles']
files_removed = "0"    # Replace: operationMetrics['numRemovedFiles']

spark.sql(f"""
    CREATE OR REPLACE TABLE {CATALOG}.{SCHEMA}.opt_ex8_analysis AS
    SELECT CAST({version} AS LONG) AS version,
           '{files_added}' AS files_added,
           '{files_removed}' AS files_removed
""")

# COMMAND ----------

# Validate Exercise 8
row = spark.table(f"{CATALOG}.{SCHEMA}.opt_ex8_analysis").collect()[0]

assert int(row.files_removed) >= 8, f"files_removed should be operationMetrics['numRemovedFiles'] of the OPTIMIZE row, got {row.files_removed}"
assert int(row.files_added) >= 1, \
    f"OPTIMIZE should have added at least 1 file, got {row.files_added}"

opt_versions = [r.version for r in spark.sql(f"DESCRIBE HISTORY {CATALOG}.{SCHEMA}.opt_ex8_orders").filter("operation = 'OPTIMIZE'").collect()]
assert row.version in opt_versions, f"version should be the version of the OPTIMIZE row in DESCRIBE HISTORY, got {row.version}"

print("Exercise 8 passed!")
