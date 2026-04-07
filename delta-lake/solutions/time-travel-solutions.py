# Databricks notebook source
# COMMAND ----------
# MAGIC %md
# MAGIC # Time Travel & Restore - Solutions
# MAGIC **Topic**: Delta Lake | **Exercises**: 7 | **Checkpoints**: 1
# MAGIC
# MAGIC Reference solutions, hints, and common mistakes for each exercise.
# MAGIC Try solving the exercises first before looking here.

# COMMAND ----------
# MAGIC %md
# MAGIC ## Exercise 1: Query by Version Number
# MAGIC
# MAGIC **Hints**:
# MAGIC 1. Delta tables store every change as a numbered version starting at 0
# MAGIC 2. Use the `VERSION AS OF` clause in your SELECT statement
# MAGIC 3. Save the result using `CREATE TABLE AS SELECT` or `.write.saveAsTable()`
# MAGIC
# MAGIC **Common mistakes**:
# MAGIC - Using `TIMESTAMP AS OF` instead of `VERSION AS OF` (different syntax)
# MAGIC - Forgetting that version 0 is the initial create, not version 1
# MAGIC - Querying without saving to the output table

# COMMAND ----------

# EXERCISE_KEY: tt_ex1
CATALOG = "db_code"
SCHEMA = "time_travel"
BASE_SCHEMA = "delta_lake"

spark.sql(f"""
    CREATE OR REPLACE TABLE {CATALOG}.{SCHEMA}.tt_ex1_result
    AS SELECT * FROM {CATALOG}.{SCHEMA}.tt_ex1_orders VERSION AS OF 0
""")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Exercise 2: Query by Timestamp
# MAGIC
# MAGIC **Hints**:
# MAGIC 1. `DESCRIBE HISTORY` returns a DataFrame with a `timestamp` column for each version
# MAGIC 2. Filter the history for the UPDATE operation and extract its timestamp with `.collect()`
# MAGIC 3. Use that timestamp in a `TIMESTAMP AS OF` clause (wrap in quotes in SQL)
# MAGIC
# MAGIC **Common mistakes**:
# MAGIC - Hardcoding a timestamp (fragile, depends on when setup ran)
# MAGIC - Filtering by version number instead of operation type (auto-OPTIMIZE may shift version numbers)
# MAGIC - Missing quotes around the timestamp value in the SQL string
# MAGIC - Using `VERSION AS OF` instead of `TIMESTAMP AS OF` (defeats the exercise purpose)

# COMMAND ----------

# EXERCISE_KEY: tt_ex2
# Step 1: Get the UPDATE operation's timestamp from DESCRIBE HISTORY
_hist = spark.sql(f"DESCRIBE HISTORY {CATALOG}.{SCHEMA}.tt_ex2_orders")
_update_ts = str(_hist.filter("operation = 'UPDATE'").select("timestamp").collect()[0][0])

# Step 2: Query using TIMESTAMP AS OF
spark.sql(f"""
    CREATE OR REPLACE TABLE {CATALOG}.{SCHEMA}.tt_ex2_result
    AS SELECT * FROM {CATALOG}.{SCHEMA}.tt_ex2_orders TIMESTAMP AS OF '{_update_ts}'
""")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Checkpoint 3: DESCRIBE HISTORY
# MAGIC
# MAGIC **Hints**:
# MAGIC 1. `DESCRIBE HISTORY table_name` returns one row per version
# MAGIC 2. The `operation` column shows: CREATE TABLE AS SELECT, UPDATE, DELETE, WRITE, MERGE, OPTIMIZE, etc.
# MAGIC 3. Count the rows to get total versions, read the `operation` column for each
# MAGIC 4. On serverless, auto-OPTIMIZE may add extra versions (OPTIMIZE operations) between your DML
# MAGIC 5. DML operations are non-OPTIMIZE operations (CREATE TABLE AS SELECT, UPDATE, DELETE, WRITE, MERGE, RESTORE)
# MAGIC
# MAGIC **Common mistakes**:
# MAGIC - Using `SHOW HISTORY` (wrong command - it's DESCRIBE HISTORY)
# MAGIC - Confusing version numbering (starts at 0, not 1)
# MAGIC - Confusing INSERT operation name (it shows as "WRITE", not "INSERT")
# MAGIC - Assuming version numbers are sequential without gaps (auto-OPTIMIZE can add versions)

# COMMAND ----------

# EXERCISE_KEY: tt_ex3
# Run DESCRIBE HISTORY to observe the output
hist = spark.sql(f"DESCRIBE HISTORY {CATALOG}.{SCHEMA}.tt_ex3_orders")
hist.select("version", "operation").orderBy("version").show()

total_versions = hist.count()
last_dml_operation = hist.orderBy(hist.version.desc()).select("operation").collect()[0][0]
# Count DML operations (everything except OPTIMIZE)
num_dml_operations = hist.filter("operation != 'OPTIMIZE'").count()

spark.sql(f"""
    CREATE OR REPLACE TABLE {CATALOG}.{SCHEMA}.tt_ex3_history AS
    SELECT CAST({total_versions} AS INT) AS total_versions,
           '{last_dml_operation}' AS last_dml_operation,
           CAST({num_dml_operations} AS INT) AS num_dml_operations
""")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Exercise 4: RESTORE to Previous Version
# MAGIC
# MAGIC **Hints**:
# MAGIC 1. The RESTORE command reverts a table to a specific version or timestamp
# MAGIC 2. Syntax: `RESTORE TABLE table_name TO VERSION AS OF N`
# MAGIC 3. RESTORE creates a new version (it doesn't delete history)
# MAGIC
# MAGIC **Common mistakes**:
# MAGIC - Using `ALTER TABLE` or `CREATE OR REPLACE` instead of RESTORE
# MAGIC - Thinking RESTORE deletes intermediate versions (history is preserved)
# MAGIC - Trying to RESTORE to a version that's been VACUUM'd (data files may be gone)

# COMMAND ----------

# EXERCISE_KEY: tt_ex4
spark.sql(f"""
    RESTORE TABLE {CATALOG}.{SCHEMA}.tt_ex4_orders TO VERSION AS OF 0
""")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Exercise 5: Diff Two Versions
# MAGIC
# MAGIC **Hints**:
# MAGIC 1. Query both versions using `VERSION AS OF` and compare them
# MAGIC 2. EXCEPT finds rows in one set but not the other (set difference)
# MAGIC 3. Union the differences from both directions to catch adds, deletes, and changes
# MAGIC
# MAGIC **Common mistakes**:
# MAGIC - Only checking one direction (misses either added or deleted rows)
# MAGIC - Comparing only order_ids instead of full rows (misses modifications)
# MAGIC - Using INTERSECT instead of EXCEPT (INTERSECT finds matches, not differences)

# COMMAND ----------

# EXERCISE_KEY: tt_ex5
# Compare full rows in both directions, then extract distinct order_ids
spark.sql(f"""
    CREATE OR REPLACE TABLE {CATALOG}.{SCHEMA}.tt_ex5_changes AS
    WITH v0_data AS (SELECT * FROM {CATALOG}.{SCHEMA}.tt_ex5_orders VERSION AS OF 0),
         current_data AS (SELECT * FROM {CATALOG}.{SCHEMA}.tt_ex5_orders),
         in_v0_not_current AS (SELECT * FROM v0_data EXCEPT SELECT * FROM current_data),
         in_current_not_v0 AS (SELECT * FROM current_data EXCEPT SELECT * FROM v0_data)
    SELECT DISTINCT order_id FROM (
        SELECT order_id FROM in_v0_not_current
        UNION
        SELECT order_id FROM in_current_not_v0
    )
""")

# Alternative: FULL OUTER JOIN approach
# spark.sql(f"""
#     CREATE OR REPLACE TABLE {CATALOG}.{SCHEMA}.tt_ex5_changes AS
#     SELECT COALESCE(v0.order_id, cur.order_id) AS order_id
#     FROM (SELECT * FROM {CATALOG}.{SCHEMA}.tt_ex5_orders VERSION AS OF 0) v0
#     FULL OUTER JOIN {CATALOG}.{SCHEMA}.tt_ex5_orders cur
#       ON v0.order_id = cur.order_id
#     WHERE v0.order_id IS NULL OR cur.order_id IS NULL
#        OR v0.amount != cur.amount OR v0.status != cur.status
# """)

# COMMAND ----------
# MAGIC %md
# MAGIC ## Exercise 6: Audit Trail
# MAGIC
# MAGIC **Hints**:
# MAGIC 1. First, find the bad row: `SELECT * FROM tt_ex6_orders WHERE amount > 1000`
# MAGIC 2. Use `DESCRIBE HISTORY` to get all version numbers (don't hardcode - auto-OPTIMIZE may add extra versions)
# MAGIC 3. Check each version: `SELECT COUNT(*) FROM tt_ex6_orders VERSION AS OF {v} WHERE order_id = '...'`
# MAGIC 4. The version where the count goes from 0 to 1 is when it was introduced
# MAGIC
# MAGIC **Common mistakes**:
# MAGIC - Only checking the current version (need to trace back through history)
# MAGIC - Starting from the latest version instead of v0 (find the FIRST appearance)
# MAGIC - Hardcoding version numbers (use DESCRIBE HISTORY to get actual versions)

# COMMAND ----------

# EXERCISE_KEY: tt_ex6
# Step 1: Find the bad order
bad_order_id = spark.sql(f"""
    SELECT order_id FROM {CATALOG}.{SCHEMA}.tt_ex6_orders WHERE amount > 1000
""").collect()[0][0]

# Step 2: Check each version to find when it first appeared
num_versions = spark.sql(f"DESCRIBE HISTORY {CATALOG}.{SCHEMA}.tt_ex6_orders").count()
bad_version = None
for v in range(num_versions):
    count = spark.sql(f"""
        SELECT COUNT(*) FROM {CATALOG}.{SCHEMA}.tt_ex6_orders VERSION AS OF {v}
        WHERE order_id = '{bad_order_id}'
    """).collect()[0][0]
    if count > 0:
        bad_version = v
        break

# Step 3: Save result
spark.sql(f"""
    CREATE OR REPLACE TABLE {CATALOG}.{SCHEMA}.tt_ex6_audit AS
    SELECT CAST({bad_version} AS INT) AS bad_version, '{bad_order_id}' AS bad_order_id
""")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Exercise 7: Selective Undo via MERGE + Time Travel
# MAGIC
# MAGIC **Hints**:
# MAGIC 1. Use `VERSION AS OF 0` as the source table in a MERGE statement
# MAGIC 2. The WHEN MATCHED condition should target only the incorrectly cancelled rows
# MAGIC 3. This is more surgical than RESTORE - you pick which rows to fix
# MAGIC
# MAGIC **Common mistakes**:
# MAGIC - Using RESTORE instead of MERGE (RESTORE reverts everything, not just specific rows)
# MAGIC - Matching on order_id alone without filtering for cancelled status (would update all matched rows)
# MAGIC - Forgetting that time travel reads are version-locked (v0 data doesn't change)

# COMMAND ----------

# EXERCISE_KEY: tt_ex7
spark.sql(f"""
    MERGE INTO {CATALOG}.{SCHEMA}.tt_ex7_orders t
    USING (SELECT * FROM {CATALOG}.{SCHEMA}.tt_ex7_orders VERSION AS OF 0) v0
    ON t.order_id = v0.order_id
    WHEN MATCHED AND t.status = 'cancelled' AND t.amount < 100 THEN UPDATE SET *
""")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Exercise 8: Reproducible Reporting with Time Travel
# MAGIC
# MAGIC **Hints**:
# MAGIC 1. The setup stored the INSERT version number in `TT_EX8_INSERT_V` - use it with `VERSION AS OF`
# MAGIC 2. Aggregate with `COUNT(*)` and `SUM(amount)` in a single query
# MAGIC 3. Save the result with the exact column names the assertions expect
# MAGIC
# MAGIC **Common mistakes**:
# MAGIC - Querying the current version instead of the INSERT version (current has corrupted data)
# MAGIC - Hardcoding `VERSION AS OF 1` (auto-OPTIMIZE may shift version numbers - use `TT_EX8_INSERT_V`)
# MAGIC - Using wrong column aliases (must be `order_count` and `total_amount`)
# MAGIC - Forgetting that `COUNT(*)` returns LONG and `SUM(amount)` returns DOUBLE

# COMMAND ----------

# EXERCISE_KEY: tt_ex8
spark.sql(f"""
    CREATE OR REPLACE TABLE {CATALOG}.{SCHEMA}.tt_ex8_report AS
    SELECT COUNT(*) AS order_count, SUM(amount) AS total_amount
    FROM {CATALOG}.{SCHEMA}.tt_ex8_orders VERSION AS OF {TT_EX8_INSERT_V}
""")
