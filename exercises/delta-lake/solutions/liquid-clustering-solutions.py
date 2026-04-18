# Databricks notebook source
# COMMAND ----------
# MAGIC %md
# MAGIC # Liquid Clustering - Solutions
# MAGIC **Topic**: Delta Lake | **Exercises**: 6 | **Checkpoints**: 1
# MAGIC
# MAGIC Reference solutions, hints, and common mistakes for each exercise.
# MAGIC Try solving the exercises first before looking here.

# COMMAND ----------
# MAGIC %md
# MAGIC ## Exercise 1: Create a Liquid Clustered Table
# MAGIC
# MAGIC **Hints**:
# MAGIC 1. Add `CLUSTER BY (column)` between the table name and AS SELECT
# MAGIC 2. The syntax is: `CREATE TABLE name CLUSTER BY (col) AS SELECT ...`
# MAGIC 3. Liquid clustering replaces both PARTITION BY and ZORDER
# MAGIC
# MAGIC **Common mistakes**:
# MAGIC - Using `PARTITION BY` instead of `CLUSTER BY` (different feature)
# MAGIC - Putting CLUSTER BY after AS SELECT (must come before)

# COMMAND ----------

# EXERCISE_KEY: lc_ex1
CATALOG = "db_code"
SCHEMA = "liquid_clustering"
BASE_SCHEMA = "delta_lake"

spark.sql(f"""
    CREATE OR REPLACE TABLE {CATALOG}.{SCHEMA}.lc_ex1_orders
    CLUSTER BY (status)
    AS SELECT * FROM {CATALOG}.{BASE_SCHEMA}.orders
    WHERE order_id IN ('ORD-001', 'ORD-002', 'ORD-003', 'ORD-004', 'ORD-005')
""")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Exercise 2: Trigger Clustering with OPTIMIZE
# MAGIC
# MAGIC **Hints**:
# MAGIC 1. Same OPTIMIZE syntax as regular compaction
# MAGIC 2. No ZORDER BY clause needed - liquid clustering is automatic
# MAGIC 3. OPTIMIZE both compacts files AND applies clustering layout
# MAGIC
# MAGIC **Common mistakes**:
# MAGIC - Adding ZORDER BY on a liquid clustered table (not needed, may conflict)
# MAGIC - Expecting clustering without running OPTIMIZE (clustering happens during OPTIMIZE)

# COMMAND ----------

# EXERCISE_KEY: lc_ex2
spark.sql(f"OPTIMIZE {CATALOG}.{SCHEMA}.lc_ex2_orders")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Checkpoint 3: Verify Clustering Configuration
# MAGIC
# MAGIC **Hints**:
# MAGIC 1. Run `DESCRIBE DETAIL` and look for the `clusteringColumns` column
# MAGIC 2. It's an array of column names used for clustering
# MAGIC 3. Also check `numFiles` to see the result of optimization
# MAGIC
# MAGIC **Common mistakes**:
# MAGIC - Looking for clustering in SHOW TBLPROPERTIES (it's in DESCRIBE DETAIL)
# MAGIC - Confusing `partitionColumns` with `clusteringColumns` (different arrays)

# COMMAND ----------

# EXERCISE_KEY: lc_ex3
detail = spark.sql(f"DESCRIBE DETAIL {CATALOG}.{SCHEMA}.lc_ex3_orders").collect()[0]
clustering_col = detail.clusteringColumns[0]
num_files = detail.numFiles

spark.sql(f"""
    CREATE OR REPLACE TABLE {CATALOG}.{SCHEMA}.lc_ex3_report AS
    SELECT '{clustering_col}' AS clustering_col, CAST({num_files} AS LONG) AS num_files
""")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Exercise 4: Migrate from ZORDER to Liquid Clustering
# MAGIC
# MAGIC **Hints**:
# MAGIC 1. `ALTER TABLE ... CLUSTER BY (column)` adds liquid clustering
# MAGIC 2. Works on any unpartitioned Delta table (partitioned tables need migration)
# MAGIC 3. After ALTER, the next OPTIMIZE will use liquid clustering instead of ZORDER
# MAGIC
# MAGIC **Common mistakes**:
# MAGIC - Trying this on a partitioned table (fails - must be unpartitioned)
# MAGIC - Forgetting to run OPTIMIZE after (ALTER sets the config, OPTIMIZE applies it)

# COMMAND ----------

# EXERCISE_KEY: lc_ex4
spark.sql(f"ALTER TABLE {CATALOG}.{SCHEMA}.lc_ex4_orders CLUSTER BY (status)")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Exercise 5: Change Cluster Keys (Metadata-Only)
# MAGIC
# MAGIC **Hints**:
# MAGIC 1. Run `DESCRIBE DETAIL` to see `numFiles` before any changes
# MAGIC 2. `ALTER TABLE ... CLUSTER BY` is instant - it only changes metadata
# MAGIC 3. Run `DESCRIBE DETAIL` again after ALTER - `numFiles` is identical (proof!)
# MAGIC
# MAGIC **Common mistakes**:
# MAGIC - Expecting files to change after ALTER (they don't until OPTIMIZE)
# MAGIC - Forgetting to OPTIMIZE after changing keys (data stays in old layout)

# COMMAND ----------

# EXERCISE_KEY: lc_ex5
# Step 1: Check files before
files_before_alter = spark.sql(f"DESCRIBE DETAIL {CATALOG}.{SCHEMA}.lc_ex5_orders").select("numFiles").collect()[0][0]

# Step 2: Change clustering key (metadata-only)
spark.sql(f"ALTER TABLE {CATALOG}.{SCHEMA}.lc_ex5_orders CLUSTER BY (order_date)")

# Step 3: Check files after ALTER - should be identical!
files_after_alter = spark.sql(f"DESCRIBE DETAIL {CATALOG}.{SCHEMA}.lc_ex5_orders").select("numFiles").collect()[0][0]

# Step 4: OPTIMIZE to actually re-cluster
spark.sql(f"OPTIMIZE {CATALOG}.{SCHEMA}.lc_ex5_orders")

# Step 5: Check files after OPTIMIZE - now compacted
files_after_optimize = spark.sql(f"DESCRIBE DETAIL {CATALOG}.{SCHEMA}.lc_ex5_orders").select("numFiles").collect()[0][0]

spark.sql(f"""
    CREATE OR REPLACE TABLE {CATALOG}.{SCHEMA}.lc_ex5_proof AS
    SELECT CAST({files_before_alter} AS LONG) AS files_before_alter,
           CAST({files_after_alter} AS LONG) AS files_after_alter,
           CAST({files_after_optimize} AS LONG) AS files_after_optimize
""")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Exercise 6: Multi-Column Cluster Keys
# MAGIC
# MAGIC **Hints**:
# MAGIC 1. List multiple columns in CLUSTER BY: `CLUSTER BY (col1, col2)`
# MAGIC 2. Up to 4 clustering columns are supported
# MAGIC 3. Order matters less than with ZORDER - liquid clustering handles it
# MAGIC
# MAGIC **Common mistakes**:
# MAGIC - Using more than 4 columns (limit is 4)
# MAGIC - Using high-cardinality columns like order_id (low benefit, similar to primary key)

# COMMAND ----------

# EXERCISE_KEY: lc_ex6
spark.sql(f"""
    CREATE OR REPLACE TABLE {CATALOG}.{SCHEMA}.lc_ex6_orders
    CLUSTER BY (status, customer_id)
    AS SELECT * FROM {CATALOG}.{BASE_SCHEMA}.orders
    WHERE order_id IN ('ORD-001', 'ORD-002', 'ORD-003', 'ORD-004', 'ORD-005')
""")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Exercise 7: Migrate Partitioned Table to Liquid Clustering
# MAGIC
# MAGIC **Hints**:
# MAGIC 1. Step 1: CREATE TABLE ... CLUSTER BY (status) AS SELECT * FROM partitioned_table
# MAGIC 2. Step 2: OPTIMIZE the new table
# MAGIC 3. You can't ALTER a partitioned table to add CLUSTER BY - must recreate
# MAGIC
# MAGIC **Common mistakes**:
# MAGIC - Trying ALTER TABLE CLUSTER BY on a partitioned table (fails)
# MAGIC - Forgetting to OPTIMIZE after creation (clustering isn't applied until OPTIMIZE)
# MAGIC - Losing data by not copying all rows (always verify row counts)

# COMMAND ----------

# EXERCISE_KEY: lc_ex7
# Step 1: Create new clustered table with data from partitioned
spark.sql(f"""
    CREATE OR REPLACE TABLE {CATALOG}.{SCHEMA}.lc_ex7_clustered
    CLUSTER BY (status)
    AS SELECT * FROM {CATALOG}.{SCHEMA}.lc_ex7_partitioned
""")

# Step 2: OPTIMIZE to apply clustering
spark.sql(f"OPTIMIZE {CATALOG}.{SCHEMA}.lc_ex7_clustered")
