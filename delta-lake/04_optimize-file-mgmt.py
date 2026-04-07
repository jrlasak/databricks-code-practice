# Databricks notebook source
# COMMAND ----------
# MAGIC %md
# MAGIC # OPTIMIZE & File Management
# MAGIC **Topic**: Delta Lake | **Exercises**: 6 | **Checkpoints**: 2 | **Total Time**: ~80 min
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
# MAGIC | order_id | STRING | Primary key |
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
# MAGIC **Setup complete.** Exercise tables are in `{CATALOG}.{SCHEMA}` (optimize_file_mgmt schema).
# MAGIC Base tables (orders) are in `{CATALOG}.{BASE_SCHEMA}` (delta_lake schema).
# MAGIC Exercise tables:
# MAGIC - `opt_ex{1-4}_orders` - intentionally fragmented (10+ small files each) for OPTIMIZE demos
# MAGIC - `opt_ex5_orders` - normal table for setting retention properties
# MAGIC - `opt_ex6_orders` - table with stale files from UPDATE/DELETE operations (for VACUUM)
# MAGIC - `opt_ex7_orders` - normal table for DESCRIBE DETAIL exploration
# MAGIC - `opt_ex8_orders` - pre-optimized fragmented table (for history analysis)

# COMMAND ----------
# MAGIC %md
# MAGIC ## Checkpoint 1: Inspect File Count and Size
# MAGIC **Time**: ~5 min
# MAGIC
# MAGIC Use `DESCRIBE DETAIL` to inspect how many files and how much storage a Delta table uses.
# MAGIC The setup created this table with many small INSERT operations, so it has 10+ small files.
# MAGIC
# MAGIC **Table** (`opt_ex1_orders`): ~20 rows across 10+ small files.
# MAGIC
# MAGIC **Expected Output**: Fill in the observed `numFiles` and `sizeInBytes` values below.
# MAGIC
# MAGIC **Requirements**:
# MAGIC 1. Run `DESCRIBE DETAIL` on `opt_ex1_orders` (add a cell above or use the space below)
# MAGIC 2. Read the `numFiles` and `sizeInBytes` from the output
# MAGIC 3. Fill in the placeholder values and run this cell

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

# Validate Exercise 1
result = spark.table(f"{CATALOG}.{SCHEMA}.opt_ex1_detail").collect()[0]

assert result.num_files >= 8, f"Expected at least 8 files (fragmented table), got {result.num_files}"
assert result.size_bytes > 0, f"sizeInBytes should be positive, got {result.size_bytes}"

print("Exercise 1 passed!")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Exercise 2: Run OPTIMIZE to Compact Files
# MAGIC **Difficulty**: Easy | **Time**: ~5 min
# MAGIC
# MAGIC OPTIMIZE compacts many small files into fewer, larger files. Run it on a fragmented
# MAGIC table and verify the file count decreased.
# MAGIC
# MAGIC **Table** (`opt_ex2_orders`): ~20 rows across 10+ small files (same fragmented pattern as Ex 1).
# MAGIC
# MAGIC **Expected Output**: After OPTIMIZE, `opt_ex2_orders` should have 1-2 files instead of 10+.
# MAGIC
# MAGIC **Requirements**:
# MAGIC 1. Run `OPTIMIZE` on `opt_ex2_orders`
# MAGIC 2. The data should be unchanged (same rows), just in fewer files

# COMMAND ----------

# EXERCISE_KEY: opt_ex2
# TODO: Run OPTIMIZE on the fragmented table

# Your code here


# COMMAND ----------

# Validate Exercise 2
detail = spark.sql(f"DESCRIBE DETAIL {CATALOG}.{SCHEMA}.opt_ex2_orders").collect()[0]

assert detail.numFiles <= 2, f"After OPTIMIZE, expected 1-2 files, got {detail.numFiles}"
# Data should be unchanged
row_count = spark.table(f"{CATALOG}.{SCHEMA}.opt_ex2_orders").count()
assert row_count >= 18, f"Data should be preserved after OPTIMIZE, got {row_count} rows"

print("Exercise 2 passed!")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Exercise 3: OPTIMIZE with ZORDER
# MAGIC **Difficulty**: Medium | **Time**: ~10 min
# MAGIC
# MAGIC ZORDER co-locates rows with similar values in the same files, enabling file skipping
# MAGIC on filtered queries. Run OPTIMIZE with ZORDER BY on the `status` column.
# MAGIC
# MAGIC **Note**: Liquid Clustering is the recommended replacement for ZORDER on new tables.
# MAGIC ZORDER is still valid on existing non-clustered tables. See the Liquid Clustering notebook
# MAGIC for the modern approach.
# MAGIC
# MAGIC **Table** (`opt_ex3_orders`): ~20 rows across 10+ small files.
# MAGIC
# MAGIC **Expected Output**: After OPTIMIZE ZORDER BY (status), the table is compacted AND
# MAGIC data is physically sorted by status within files.
# MAGIC
# MAGIC **Requirements**:
# MAGIC 1. Run `OPTIMIZE` with `ZORDER BY (status)` on `opt_ex3_orders`
# MAGIC
# MAGIC **Approach**: ZORDER is an extension of OPTIMIZE. Same OPTIMIZE command, just add
# MAGIC a ZORDER BY clause specifying which columns to co-locate.

# COMMAND ----------

# EXERCISE_KEY: opt_ex3
# TODO: Run OPTIMIZE with ZORDER BY on the status column

# Your code here


# COMMAND ----------

# Validate Exercise 3
detail = spark.sql(f"DESCRIBE DETAIL {CATALOG}.{SCHEMA}.opt_ex3_orders").collect()[0]
assert detail.numFiles <= 2, f"Table should be compacted, got {detail.numFiles} files"

# Verify ZORDER was used via DESCRIBE HISTORY
hist = spark.sql(f"DESCRIBE HISTORY {CATALOG}.{SCHEMA}.opt_ex3_orders")
optimize_ops = hist.filter("operation = 'OPTIMIZE'").collect()
assert len(optimize_ops) > 0, "Should have at least one OPTIMIZE in history"
# Check operationParameters for zOrderBy
params = optimize_ops[0].operationParameters
assert 'zOrderBy' in str(params), "OPTIMIZE should include zOrderBy in parameters"

print("Exercise 3 passed!")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Exercise 4: Measure File Metrics Before and After OPTIMIZE
# MAGIC **Difficulty**: Medium | **Time**: ~15 min
# MAGIC
# MAGIC Capture the file count before and after OPTIMIZE to quantify the compaction.
# MAGIC
# MAGIC **Table** (`opt_ex4_orders`): ~20 rows across 10+ small files (not yet optimized).
# MAGIC
# MAGIC **Expected Output**: Fill in the before/after file counts in the placeholder below.
# MAGIC
# MAGIC **Requirements**:
# MAGIC 1. Run `DESCRIBE DETAIL` on `opt_ex4_orders` to find current `numFiles`
# MAGIC 2. Run `OPTIMIZE` on `opt_ex4_orders`
# MAGIC 3. Run `DESCRIBE DETAIL` again to find new `numFiles`
# MAGIC 4. Fill in the placeholder values below

# COMMAND ----------

# EXERCISE_KEY: opt_ex4
# TODO: Run DESCRIBE DETAIL, OPTIMIZE, DESCRIBE DETAIL again, then fill in values

before_files = 0  # Replace: numFiles BEFORE optimize
# Write your OPTIMIZE statement here

after_files = 0   # Replace: numFiles AFTER optimize

spark.sql(f"""
    CREATE OR REPLACE TABLE {CATALOG}.{SCHEMA}.opt_ex4_comparison AS
    SELECT CAST({before_files} AS LONG) AS before_files, CAST({after_files} AS LONG) AS after_files
""")

# COMMAND ----------

# Validate Exercise 4
row = spark.table(f"{CATALOG}.{SCHEMA}.opt_ex4_comparison").collect()[0]

assert row.before_files >= 8, f"Before OPTIMIZE should have 8+ files, got {row.before_files}"
assert row.after_files <= 2, f"After OPTIMIZE should have 1-2 files, got {row.after_files}"
assert row.before_files > row.after_files, \
    f"before_files ({row.before_files}) should be greater than after_files ({row.after_files})"

print("Exercise 4 passed!")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Exercise 5: Set VACUUM Retention Period
# MAGIC **Difficulty**: Medium | **Time**: ~10 min
# MAGIC
# MAGIC Set the `delta.deletedFileRetentionDuration` table property to control how long
# MAGIC old files are kept before VACUUM can remove them.
# MAGIC
# MAGIC **Table** (`opt_ex5_orders`): 5 rows from base orders.
# MAGIC
# MAGIC **Expected Output**: The table property `delta.deletedFileRetentionDuration` is set to `168 hours`.
# MAGIC
# MAGIC **Requirements**:
# MAGIC 1. Use `ALTER TABLE SET TBLPROPERTIES` to set the retention duration to `168 hours`
# MAGIC
# MAGIC **Approach**: Delta Lake table properties are set via ALTER TABLE. The retention property
# MAGIC controls the minimum age of files that VACUUM will delete.

# COMMAND ----------

# EXERCISE_KEY: opt_ex5
# TODO: Set the VACUUM retention property to 168 hours

# Your code here


# COMMAND ----------

# Validate Exercise 5
props = spark.sql(f"SHOW TBLPROPERTIES {CATALOG}.{SCHEMA}.opt_ex5_orders")
retention_rows = props.filter("key = 'delta.deletedFileRetentionDuration'").collect()

assert len(retention_rows) > 0, "Table should have delta.deletedFileRetentionDuration property"
assert retention_rows[0].value == "168 hours", \
    f"Retention should be '168 hours', got '{retention_rows[0].value}'"

print("Exercise 5 passed!")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Exercise 6: Run VACUUM and Verify
# MAGIC **Difficulty**: Medium | **Time**: ~10 min
# MAGIC
# MAGIC VACUUM removes old, unreferenced files from a Delta table. The setup performed
# MAGIC UPDATE and DELETE operations that left stale files behind.
# MAGIC
# MAGIC **Table** (`opt_ex6_orders`): 4 rows (was 5, one deleted). Has 3+ stale file versions
# MAGIC from UPDATE/DELETE operations during setup.
# MAGIC
# MAGIC **Expected Output**: VACUUM completes. `DESCRIBE HISTORY` shows a VACUUM operation.
# MAGIC
# MAGIC **Requirements**:
# MAGIC 1. Set the table's file retention to 0 hours (via table property)
# MAGIC 2. Run `VACUUM` to clean up stale files
# MAGIC 3. Restore the retention to 168 hours after
# MAGIC
# MAGIC **Approach**: By default, VACUUM refuses to delete files less than 7 days old. You can
# MAGIC override the retention at the table level using `delta.deletedFileRetentionDuration`.

# COMMAND ----------

# EXERCISE_KEY: opt_ex6
# TODO: Set retention to 0 hours, run VACUUM, restore retention

# Your code here


# COMMAND ----------

# Validate Exercise 6
hist = spark.sql(f"DESCRIBE HISTORY {CATALOG}.{SCHEMA}.opt_ex6_orders")
vacuum_ops = hist.filter("operation = 'VACUUM END'").count()

assert vacuum_ops > 0, "Should have at least one VACUUM operation in history"

print("Exercise 6 passed!")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Checkpoint 7: Table Health Report from DESCRIBE DETAIL
# MAGIC **Time**: ~5 min
# MAGIC
# MAGIC Read key metrics from `DESCRIBE DETAIL` and fill in a health report.
# MAGIC
# MAGIC **Table** (`opt_ex7_orders`): 5 rows from base orders.
# MAGIC
# MAGIC **Requirements**:
# MAGIC 1. Run `DESCRIBE DETAIL` on `opt_ex7_orders`
# MAGIC 2. Find `name`, `numFiles`, `sizeInBytes` in the output
# MAGIC 3. Fill in the placeholder values below

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

# Validate Exercise 7
row = spark.table(f"{CATALOG}.{SCHEMA}.opt_ex7_report").collect()[0]

assert len(row.table_name) > 0, "table_name should not be empty"
assert row.num_files >= 1, f"Should have at least 1 file, got {row.num_files}"
assert row.size_bytes > 0, f"Size should be positive, got {row.size_bytes}"

print("Exercise 7 passed!")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Exercise 8: Analyze OPTIMIZE History
# MAGIC **Difficulty**: Hard | **Time**: ~15 min
# MAGIC
# MAGIC The setup already ran OPTIMIZE on this table. Query the table history to find the
# MAGIC OPTIMIZE operation and read its metrics.
# MAGIC
# MAGIC **Table** (`opt_ex8_orders`): was fragmented, then OPTIMIZE'd by setup.
# MAGIC
# MAGIC **Requirements**:
# MAGIC 1. Run `DESCRIBE HISTORY` on `opt_ex8_orders`
# MAGIC 2. Find the row where `operation = 'OPTIMIZE'`
# MAGIC 3. Look at `operationMetrics` - it's a MAP with keys like `numAddedFiles`, `numRemovedFiles`
# MAGIC 4. Fill in the placeholder values below
# MAGIC
# MAGIC **Approach**: In the DESCRIBE HISTORY output, expand or SELECT the `operationMetrics`
# MAGIC column. Use bracket notation in SQL (e.g., `operationMetrics['numAddedFiles']`) to read map values.

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

assert int(row.files_removed) >= 8, \
    f"OPTIMIZE should have removed 8+ files, got {row.files_removed}"
assert int(row.files_added) >= 1, \
    f"OPTIMIZE should have added at least 1 file, got {row.files_added}"

print("Exercise 8 passed!")
