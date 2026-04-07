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
# MAGIC 2. The result is a DataFrame you can save as a table
# MAGIC 3. Key columns: `numFiles`, `sizeInBytes`, `name`, `format`
# MAGIC
# MAGIC **Common mistakes**:
# MAGIC - Confusing DESCRIBE DETAIL with DESCRIBE TABLE (different commands)
# MAGIC - Not saving the result (just displaying it without writing to a table)

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
# MAGIC 1. Syntax: `OPTIMIZE schema.table_name`
# MAGIC 2. OPTIMIZE is a SQL command, run it with `spark.sql()`
# MAGIC 3. It doesn't change the data, only reorganizes files on disk
# MAGIC
# MAGIC **Common mistakes**:
# MAGIC - Expecting OPTIMIZE to change the data (it only affects file layout)
# MAGIC - Running OPTIMIZE on a table that's already compacted (it's a no-op, not an error)

# COMMAND ----------

# EXERCISE_KEY: opt_ex2
spark.sql(f"OPTIMIZE {CATALOG}.{SCHEMA}.opt_ex2_orders")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Exercise 3: OPTIMIZE with ZORDER
# MAGIC
# MAGIC **Hints**:
# MAGIC 1. Add `ZORDER BY (column)` after the table name in OPTIMIZE
# MAGIC 2. Choose columns that are frequently used in WHERE clauses
# MAGIC 3. ZORDER works within the files created by OPTIMIZE (compaction + ordering)
# MAGIC
# MAGIC **Note**: For new tables, Liquid Clustering (`CLUSTER BY`) is the recommended replacement
# MAGIC for ZORDER. ZORDER remains valid for existing non-clustered tables.
# MAGIC
# MAGIC **Common mistakes**:
# MAGIC - Using ZORDER without OPTIMIZE (ZORDER is a clause OF OPTIMIZE, not a separate command)
# MAGIC - ZORDERing on too many columns (diminishing returns after 2-3 columns)
# MAGIC - ZORDERing on high-cardinality columns like primary keys (no benefit)
# MAGIC - Using ZORDER on new tables instead of Liquid Clustering (the modern approach)

# COMMAND ----------

# EXERCISE_KEY: opt_ex3
spark.sql(f"OPTIMIZE {CATALOG}.{SCHEMA}.opt_ex3_orders ZORDER BY (status)")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Exercise 4: Measure File Metrics Before and After OPTIMIZE
# MAGIC
# MAGIC **Hints**:
# MAGIC 1. Use `DESCRIBE DETAIL` to get `numFiles` before and after
# MAGIC 2. Store the before value in a Python variable, then run OPTIMIZE, then get after
# MAGIC 3. Use `spark.createDataFrame()` or `SELECT ... AS` to build the comparison table
# MAGIC
# MAGIC **Common mistakes**:
# MAGIC - Getting both metrics after OPTIMIZE (the "before" is already gone)
# MAGIC - Forgetting to save to the result table (just printing instead)

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
# MAGIC 3. The value format is a string like `'168 hours'`
# MAGIC
# MAGIC **Common mistakes**:
# MAGIC - Using a Spark config instead of a table property (table property persists, config doesn't)
# MAGIC - Wrong property name (it's delta.deletedFileRetentionDuration, not delta.logRetentionDuration)
# MAGIC - Setting retention too low in production (risk losing time travel ability)

# COMMAND ----------

# EXERCISE_KEY: opt_ex5
spark.sql(f"""
    ALTER TABLE {CATALOG}.{SCHEMA}.opt_ex5_orders
    SET TBLPROPERTIES ('delta.deletedFileRetentionDuration' = '168 hours')
""")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Exercise 6: Run VACUUM and Verify
# MAGIC
# MAGIC **Hints**:
# MAGIC 1. Set the table property `delta.deletedFileRetentionDuration` to `interval 0 hours`
# MAGIC 2. Then run `VACUUM` (no RETAIN clause needed - uses the table's retention setting)
# MAGIC 3. Use `ALTER TABLE SET TBLPROPERTIES` to set the retention
# MAGIC
# MAGIC **Common mistakes**:
# MAGIC - Trying `spark.conf.set` to disable the retention check (not available on serverless)
# MAGIC - Running VACUUM in production with 0-hour retention (never do this - breaks time travel)
# MAGIC - Forgetting to restore the retention after (leaves table unprotected)

# COMMAND ----------

# EXERCISE_KEY: opt_ex6
# Set table retention to 0 hours (NEVER do this in production)
spark.sql(f"""
    ALTER TABLE {CATALOG}.{SCHEMA}.opt_ex6_orders
    SET TBLPROPERTIES ('delta.deletedFileRetentionDuration' = 'interval 0 hours')
""")

spark.sql(f"VACUUM {CATALOG}.{SCHEMA}.opt_ex6_orders")

# Restore safe retention
spark.sql(f"""
    ALTER TABLE {CATALOG}.{SCHEMA}.opt_ex6_orders
    SET TBLPROPERTIES ('delta.deletedFileRetentionDuration' = 'interval 168 hours')
""")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Checkpoint 7: Table Health Report from DESCRIBE DETAIL
# MAGIC
# MAGIC **Hints**:
# MAGIC 1. Run `DESCRIBE DETAIL` and look for: `name`, `numFiles`, `sizeInBytes`
# MAGIC 2. `name` is the full table path (e.g., `spark_catalog.delta_lake.opt_ex7_orders`)
# MAGIC 3. Copy the values you see into the placeholder variables
# MAGIC
# MAGIC **Common mistakes**:
# MAGIC - Leaving placeholder values as 0 (assertions will catch this)

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
# MAGIC 1. Run `DESCRIBE HISTORY` and find the row where `operation = 'OPTIMIZE'`
# MAGIC 2. The `operationMetrics` column is a MAP - look for `numAddedFiles` and `numRemovedFiles`
# MAGIC 3. In SQL, use bracket notation: `operationMetrics['numAddedFiles']`
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
