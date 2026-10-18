# Databricks notebook source
# COMMAND ----------
# MAGIC %md
# MAGIC # OPTIMIZE & File Management - Solutions
# MAGIC **Topic**: Delta Lake | **Exercises**: 6 | **Checkpoints**: 2
# MAGIC
# MAGIC Reference solutions, hints, and common mistakes for each exercise.
# MAGIC Try solving the exercises first before looking here.

# COMMAND ----------
# MAGIC %md
# MAGIC ## Checkpoint 1: Inspect File Count and Size
# MAGIC
# MAGIC **Hints**:
# MAGIC 1. `DESCRIBE DETAIL` returns a single row with table metadata
# MAGIC 2. Run it in a new cell; the columns you need are `numFiles` and `sizeInBytes`
# MAGIC 3. Copy both values into `num_files` and `size_bytes`, then run the cell
# MAGIC
# MAGIC **Common mistakes**:
# MAGIC - Running `DESCRIBE TABLE` instead (it lists columns, not `numFiles` or `sizeInBytes`)
# MAGIC - Leaving `num_files` or `size_bytes` at 0 (the validate cell fails)

# COMMAND ----------

# EXERCISE_KEY: opt_ex1
CATALOG = "db_code"
SCHEMA = "optimize_file_mgmt"
BASE_SCHEMA = "delta_lake"

# Run DESCRIBE DETAIL to see file metrics
detail = spark.sql(f"DESCRIBE DETAIL {CATALOG}.{SCHEMA}.opt_ex1_orders").collect()[0]
num_files = detail.numFiles
size_bytes = detail.sizeInBytes

spark.sql(f"""
    CREATE OR REPLACE TABLE {CATALOG}.{SCHEMA}.opt_ex1_detail AS
    SELECT CAST({num_files} AS LONG) AS num_files, CAST({size_bytes} AS LONG) AS size_bytes
""")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Exercise 2: Run OPTIMIZE to Compact Files
# MAGIC
# MAGIC **Hints**:
# MAGIC 1. OPTIMIZE doesn't change the data, it only reorganizes files on disk
# MAGIC 2. OPTIMIZE is a SQL command; in the Python cell, run it with `spark.sql()`
# MAGIC 3. Syntax: `OPTIMIZE catalog.schema.table_name`
# MAGIC 4. Running OPTIMIZE twice is safe: bin-packing is idempotent, so a second run on a compacted table changes nothing

# COMMAND ----------

# EXERCISE_KEY: opt_ex2
spark.sql(f"OPTIMIZE {CATALOG}.{SCHEMA}.opt_ex2_orders")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Exercise 3: OPTIMIZE with ZORDER
# MAGIC
# MAGIC **Hints**:
# MAGIC 1. ZORDER BY changes how OPTIMIZE lays out the files it rewrites: rows with nearby values end up in the same files
# MAGIC 2. Z-ordering is not its own command: it is a clause of `OPTIMIZE`, written after the table name
# MAGIC 3. `spark.sql("OPTIMIZE db_code.optimize_file_mgmt.opt_ex3_orders ZORDER BY (customer_id)")`
# MAGIC
# MAGIC **Common mistakes**:
# MAGIC - Using ZORDER without OPTIMIZE (ZORDER is a clause OF OPTIMIZE, not a separate command)
# MAGIC - ZORDERing on too many columns (diminishing returns after 2-3 columns)
# MAGIC - ZORDERing on a low-cardinality column (a handful of distinct values, like `status`, gives little file skipping; Z-order pays off on high-cardinality filter columns such as `customer_id`)

# COMMAND ----------

# EXERCISE_KEY: opt_ex3
spark.sql(f"OPTIMIZE {CATALOG}.{SCHEMA}.opt_ex3_orders ZORDER BY (customer_id)")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Exercise 4: Measure File Metrics Before and After OPTIMIZE
# MAGIC
# MAGIC **Hints**:
# MAGIC 1. Use `DESCRIBE DETAIL` to get `numFiles` before and after
# MAGIC 2. Store the before value in a Python variable, then run OPTIMIZE, then get after
# MAGIC 3. Read both counts in code (`spark.sql("DESCRIBE DETAIL ...").first().numFiles`) into `before_files` and `after_files`, then run the cell
# MAGIC
# MAGIC **Common mistakes**:
# MAGIC - Getting both metrics after OPTIMIZE (the "before" is already gone)

# COMMAND ----------

# EXERCISE_KEY: opt_ex4
# Before
before = spark.sql(f"DESCRIBE DETAIL {CATALOG}.{SCHEMA}.opt_ex4_orders").select("numFiles").collect()[0][0]

# Optimize
spark.sql(f"OPTIMIZE {CATALOG}.{SCHEMA}.opt_ex4_orders")

# After
after = spark.sql(f"DESCRIBE DETAIL {CATALOG}.{SCHEMA}.opt_ex4_orders").select("numFiles").collect()[0][0]

# Save comparison
spark.sql(f"""
    CREATE OR REPLACE TABLE {CATALOG}.{SCHEMA}.opt_ex4_comparison AS
    SELECT CAST({before} AS LONG) AS before_files, CAST({after} AS LONG) AS after_files
""")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Exercise 5: Set VACUUM Retention Period
# MAGIC
# MAGIC **Hints**:
# MAGIC 1. Use `ALTER TABLE ... SET TBLPROPERTIES` to set table properties
# MAGIC 2. The property name is `delta.deletedFileRetentionDuration`
# MAGIC 3. The value format is a string like `'720 hours'`
# MAGIC
# MAGIC **Common mistakes**:
# MAGIC - Using a Spark config instead of a table property (table property persists, config doesn't)
# MAGIC - Wrong property name (it's delta.deletedFileRetentionDuration, not delta.logRetentionDuration)
# MAGIC - Setting retention too low in production (risk losing time travel ability)

# COMMAND ----------

# EXERCISE_KEY: opt_ex5
spark.sql(f"""
    ALTER TABLE {CATALOG}.{SCHEMA}.opt_ex5_orders
    SET TBLPROPERTIES ('delta.deletedFileRetentionDuration' = '720 hours')
""")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Exercise 6: Run VACUUM and Verify
# MAGIC
# MAGIC **Hints**:
# MAGIC 1. VACUUM only deletes files the table no longer references, and only once they are older than the retention period (7 days by default)
# MAGIC 2. Use `ALTER TABLE ... SET TBLPROPERTIES` to set the table property `delta.deletedFileRetentionDuration` to `0 hours`
# MAGIC 3. Then run `VACUUM` on the table
# MAGIC 4. Finish with the same `ALTER TABLE`, set to `'168 hours'`
# MAGIC
# MAGIC **Common mistakes**:
# MAGIC - Trying `spark.conf.set` to disable the retention check (not available on serverless)
# MAGIC - Forgetting to restore the retention after (leaves table unprotected)

# COMMAND ----------

# EXERCISE_KEY: opt_ex6
# Set table retention to 0 hours (NEVER do this in production)
spark.sql(f"""
    ALTER TABLE {CATALOG}.{SCHEMA}.opt_ex6_orders
    SET TBLPROPERTIES ('delta.deletedFileRetentionDuration' = '0 hours')
""")

spark.sql(f"VACUUM {CATALOG}.{SCHEMA}.opt_ex6_orders")

# Restore safe retention
spark.sql(f"""
    ALTER TABLE {CATALOG}.{SCHEMA}.opt_ex6_orders
    SET TBLPROPERTIES ('delta.deletedFileRetentionDuration' = '168 hours')
""")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Checkpoint 7: Table Health Report from DESCRIBE DETAIL
# MAGIC
# MAGIC **Hints**:
# MAGIC 1. Run `DESCRIBE DETAIL` and look for: `name`, `numFiles`, `sizeInBytes`
# MAGIC 2. `name` is the table's full three-part name (here `db_code.optimize_file_mgmt.opt_ex7_orders`)
# MAGIC 3. Copy the values you see into the placeholder variables
# MAGIC
# MAGIC **Common mistakes**:
# MAGIC - Leaving a placeholder at `""` or 0 (the validate cell fails)

# COMMAND ----------

# EXERCISE_KEY: opt_ex7
# Run DESCRIBE DETAIL to read the values
detail = spark.sql(f"DESCRIBE DETAIL {CATALOG}.{SCHEMA}.opt_ex7_orders").collect()[0]

table_name = detail.name
num_files = detail.numFiles
size_bytes = detail.sizeInBytes

spark.sql(f"""
    CREATE OR REPLACE TABLE {CATALOG}.{SCHEMA}.opt_ex7_report AS
    SELECT '{table_name}' AS table_name,
           CAST({num_files} AS LONG) AS num_files,
           CAST({size_bytes} AS LONG) AS size_bytes
""")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Exercise 8: Analyze OPTIMIZE History
# MAGIC
# MAGIC **Hints**:
# MAGIC 1. `DESCRIBE HISTORY` returns one row per table version; the `operation` column names what wrote it
# MAGIC 2. You can query it like a table: `SELECT version, operationMetrics FROM (DESCRIBE HISTORY db_code.optimize_file_mgmt.opt_ex8_orders) WHERE operation = 'OPTIMIZE'`
# MAGIC 3. `operationMetrics` values are strings: `operationMetrics['numRemovedFiles']` returns a string such as `'10'`, which is why the placeholders are quoted
# MAGIC
# MAGIC **Common mistakes**:
# MAGIC - Looking for 'ZORDER' as the operation name (it's 'OPTIMIZE', ZORDER is in parameters)
# MAGIC - Not finding the operationMetrics keys (they're strings in a MAP, expand the column)

# COMMAND ----------

# EXERCISE_KEY: opt_ex8
# Run DESCRIBE HISTORY and read the OPTIMIZE row's operationMetrics
hist = spark.sql(f"""
    SELECT version, operationMetrics
    FROM (DESCRIBE HISTORY {CATALOG}.{SCHEMA}.opt_ex8_orders)
    WHERE operation = 'OPTIMIZE'
""").collect()[0]

version = hist.version
files_added = hist.operationMetrics['numAddedFiles']
files_removed = hist.operationMetrics['numRemovedFiles']

spark.sql(f"""
    CREATE OR REPLACE TABLE {CATALOG}.{SCHEMA}.opt_ex8_analysis AS
    SELECT CAST({version} AS LONG) AS version,
           '{files_added}' AS files_added,
           '{files_removed}' AS files_removed
""")
