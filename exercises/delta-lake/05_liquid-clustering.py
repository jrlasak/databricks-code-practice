# Databricks notebook source
# COMMAND ----------
# MAGIC %md
# MAGIC # Liquid Clustering
# MAGIC **Topic**: Delta Lake | **Exercises**: 6 | **Checkpoints**: 1 | **Total Time**: ~70 min
# MAGIC
# MAGIC Practice the recommended data layout for Delta tables. Liquid clustering replaces
# MAGIC partitioning and ZORDER: you pick up to 4 clustering keys instead of partition boundaries,
# MAGIC you can change those keys later without rewriting data, and `OPTIMIZE` organizes the files by them.
# MAGIC
# MAGIC **Solutions**: If stuck, see `solutions/liquid-clustering-solutions.py` for hints and answers.
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

# MAGIC %run ./setup/liquid-clustering-setup

# COMMAND ----------
# MAGIC %md
# MAGIC **Setup complete.** Every exercise table is in `db_code.liquid_clustering`. Write your output tables there too,
# MAGIC by full name (for example `db_code.liquid_clustering.lc_ex1_orders`). In Python cells, the variables `CATALOG`
# MAGIC and `SCHEMA` hold `db_code` and `liquid_clustering`. The base table (`orders`) is in `db_code.delta_lake`.
# MAGIC
# MAGIC Keep the `# EXERCISE_KEY: ...` line as the first line of each answer cell.
# MAGIC
# MAGIC - Exercises 1 and 6: no pre-created table (you create `lc_ex1_orders` and `lc_ex6_orders` from `db_code.delta_lake.orders`)
# MAGIC - `lc_ex2_orders`: 20 rows in about 10 small files, clustered by `status`
# MAGIC - `lc_ex3_orders`: 20 rows, liquid clustered, already optimized
# MAGIC - `lc_ex4_orders`: 5 rows, unpartitioned, previously ZORDER'd by `status`
# MAGIC - `lc_ex5_orders`: 20 rows in about 10 small files, clustered by `status`
# MAGIC - `lc_ex7_partitioned`: 5 rows, partitioned by `status`

# COMMAND ----------
# MAGIC %md
# MAGIC ## Exercise 1: Create a Liquid Clustered Table
# MAGIC **Difficulty**: Easy | **Time**: ~5 min
# MAGIC
# MAGIC Create a Delta table with liquid clustering on the `status` column, filled with 5 rows from
# MAGIC `db_code.delta_lake.orders`.
# MAGIC
# MAGIC **Expected Output**: `lc_ex1_orders` exists with 5 rows and `CLUSTER BY (status)`.
# MAGIC
# MAGIC **Requirements**:
# MAGIC 1. Create `db_code.liquid_clustering.lc_ex1_orders` with `CLUSTER BY (status)`
# MAGIC 2. Copy rows where `order_id IN ('ORD-001', 'ORD-002', 'ORD-003', 'ORD-004', 'ORD-005')` from `db_code.delta_lake.orders`

# COMMAND ----------

# EXERCISE_KEY: lc_ex1
# TODO: Create a liquid clustered table with CLUSTER BY (status)

# Your code here


# COMMAND ----------

# Validate Exercise 1
result = spark.table(f"{CATALOG}.{SCHEMA}.lc_ex1_orders")
assert result.count() == 5, f"Expected 5 rows, got {result.count()}"

detail = spark.sql(f"DESCRIBE DETAIL {CATALOG}.{SCHEMA}.lc_ex1_orders").collect()[0]
clustering = detail.clusteringColumns
assert clustering is not None and len(clustering) > 0, "Table should have clustering columns"
assert "status" in clustering, f"Should be clustered by status, got {clustering}"

print("Exercise 1 passed!")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Exercise 2: Trigger Clustering with OPTIMIZE
# MAGIC **Difficulty**: Easy | **Time**: ~5 min
# MAGIC
# MAGIC Small writes like the setup's 10 INSERTs are not clustered on write: the clustering happens
# MAGIC when you run `OPTIMIZE`, which also compacts the small files.
# MAGIC
# MAGIC **Table** (`lc_ex2_orders`): 20 rows in about 10 small files (one per INSERT), CLUSTER BY (status).
# MAGIC
# MAGIC **Expected Output**: After OPTIMIZE, the table has 1-2 files (compacted and clustered).
# MAGIC
# MAGIC **Requirements**:
# MAGIC 1. Run `OPTIMIZE` on `lc_ex2_orders`

# COMMAND ----------

# EXERCISE_KEY: lc_ex2
# TODO: Run OPTIMIZE to trigger liquid clustering

# Your code here


# COMMAND ----------

# Validate Exercise 2
detail = spark.sql(f"DESCRIBE DETAIL {CATALOG}.{SCHEMA}.lc_ex2_orders").collect()[0]

assert detail.numFiles <= 2, f"After OPTIMIZE, expected 1-2 files, got {detail.numFiles}"

print("Exercise 2 passed!")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Checkpoint 3: Verify Clustering Configuration
# MAGIC **Time**: ~5 min
# MAGIC
# MAGIC Inspect a clustered table's configuration using `DESCRIBE DETAIL`. The output
# MAGIC includes a `clusteringColumns` field showing which columns are used for clustering.
# MAGIC
# MAGIC **Table** (`lc_ex3_orders`): 20 rows, liquid clustered, already optimized.
# MAGIC
# MAGIC **Expected Output**: `lc_ex3_report` holds 1 row whose `clustering_col` and `num_files` equal the
# MAGIC `clusteringColumns` entry and `numFiles` that `DESCRIBE DETAIL` shows for `lc_ex3_orders`.
# MAGIC
# MAGIC **Requirements**:
# MAGIC 1. Run `DESCRIBE DETAIL` on `lc_ex3_orders`
# MAGIC 2. Find the `clusteringColumns` value and the `numFiles` count
# MAGIC 3. Fill in the placeholder values below

# COMMAND ----------

# EXERCISE_KEY: lc_ex3
# TODO: Run DESCRIBE DETAIL on lc_ex3_orders, then fill in what you observe

clustering_col = ""    # Replace: the column name listed in clusteringColumns
num_files = 0          # Replace: numFiles after optimization

spark.sql(f"""
    CREATE OR REPLACE TABLE {CATALOG}.{SCHEMA}.lc_ex3_report AS
    SELECT '{clustering_col}' AS clustering_col, CAST({num_files} AS LONG) AS num_files
""")

# COMMAND ----------

# Validate Checkpoint 3
row = spark.table(f"{CATALOG}.{SCHEMA}.lc_ex3_report").collect()[0]

assert row.clustering_col == "status", f"clustering_col does not match clusteringColumns from DESCRIBE DETAIL, got '{row.clustering_col}'"
actual = spark.sql(f"DESCRIBE DETAIL {CATALOG}.{SCHEMA}.lc_ex3_orders").collect()[0]
assert row.num_files == actual.numFiles, f"num_files should match numFiles from DESCRIBE DETAIL, got {row.num_files}"

print("Checkpoint 3 passed!")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Exercise 4: Migrate from ZORDER to Liquid Clustering
# MAGIC **Difficulty**: Medium | **Time**: ~10 min
# MAGIC
# MAGIC This table was previously maintained with `OPTIMIZE ZORDER BY (status)`.
# MAGIC Migrate it to liquid clustering, which is the recommended replacement.
# MAGIC
# MAGIC **Table** (`lc_ex4_orders`): 5 rows, unpartitioned, previously ZORDER'd by status.
# MAGIC
# MAGIC **Expected Output**: `DESCRIBE DETAIL` on `lc_ex4_orders` shows `clusteringColumns` = `[status]`.
# MAGIC
# MAGIC This exercise only needs the ALTER. In production you follow it with one `OPTIMIZE ... FULL`,
# MAGIC because the ALTER does not cluster the rows already in the table; after that, regular OPTIMIZE
# MAGIC runs use liquid clustering instead of ZORDER.
# MAGIC
# MAGIC **Requirements**:
# MAGIC 1. Use `ALTER TABLE` to enable liquid clustering on the `status` column

# COMMAND ----------

# EXERCISE_KEY: lc_ex4
# TODO: Add liquid clustering to replace ZORDER

# Your code here


# COMMAND ----------

# Validate Exercise 4
detail = spark.sql(f"DESCRIBE DETAIL {CATALOG}.{SCHEMA}.lc_ex4_orders").collect()[0]
clustering = detail.clusteringColumns

assert clustering is not None and len(clustering) > 0, "Table should have clustering columns after migration"
assert "status" in clustering, f"Should be clustered by status, got {clustering}"

print("Exercise 4 passed!")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Exercise 5: Change Clustering Keys (Metadata-Only)
# MAGIC **Difficulty**: Medium | **Time**: ~15 min
# MAGIC
# MAGIC Change the clustering key from `status` to `order_date`. A key advantage of liquid
# MAGIC clustering over partitioning: changing keys is a **metadata-only operation** - the ALTER
# MAGIC rewrites no files, and `OPTIMIZE ... FULL` re-clusters the existing rows with the new key.
# MAGIC
# MAGIC **Table** (`lc_ex5_orders`): 20 rows in about 10 small files (one per INSERT), currently `CLUSTER BY (status)`.
# MAGIC
# MAGIC **Expected Output**: `clusteringColumns` is `[order_date]`. `files_before_alter` and `files_after_alter` are equal
# MAGIC (about 10), and `files_after_optimize` is 1 or 2.
# MAGIC
# MAGIC **Requirements**:
# MAGIC 1. Run `DESCRIBE DETAIL` and read the current `numFiles`
# MAGIC 2. Use `ALTER TABLE` to change the clustering key to `order_date`
# MAGIC 3. Run `DESCRIBE DETAIL` again: `numFiles` is unchanged, because the ALTER only changed metadata
# MAGIC 4. Run `OPTIMIZE ... FULL` to re-cluster all existing data with the new key (plain `OPTIMIZE` is incremental and does not rewrite data already clustered by the old key)
# MAGIC 5. Set each `files_*` variable in the cell below by reading `numFiles` from `DESCRIBE DETAIL` in code at that step (the ALTER and OPTIMIZE run in the same cell, so typed-in numbers cannot be known in advance). To start again from the fragmented table, rerun the `%run ./setup/liquid-clustering-setup` cell; it rebuilds the `lc_*` input tables and drops your output tables, so rerun your answer cells for the other exercises before you revalidate them.

# COMMAND ----------

# EXERCISE_KEY: lc_ex5
# TODO: Record numFiles, change the key, record numFiles, run OPTIMIZE ... FULL, record numFiles

files_before_alter = 0   # Replace: numFiles before ALTER TABLE
# Write your ALTER TABLE CLUSTER BY here

files_after_alter = 0    # Replace: numFiles after ALTER (equals files_before_alter)
# Write your OPTIMIZE ... FULL here

files_after_optimize = 0 # Replace: numFiles after OPTIMIZE FULL

spark.sql(f"""
    CREATE OR REPLACE TABLE {CATALOG}.{SCHEMA}.lc_ex5_proof AS
    SELECT CAST({files_before_alter} AS LONG) AS files_before_alter,
           CAST({files_after_alter} AS LONG) AS files_after_alter,
           CAST({files_after_optimize} AS LONG) AS files_after_optimize
""")

# COMMAND ----------

# Validate Exercise 5
detail = spark.sql(f"DESCRIBE DETAIL {CATALOG}.{SCHEMA}.lc_ex5_orders").collect()[0]
assert "order_date" in detail.clusteringColumns, \
    f"Should be clustered by order_date, got {detail.clusteringColumns}"

proof = spark.table(f"{CATALOG}.{SCHEMA}.lc_ex5_proof").collect()[0]
assert proof.files_before_alter >= 8, \
    f"Setup writes about 10 small files; expected at least 8 before ALTER, got {proof.files_before_alter}"
assert proof.files_after_alter >= 8 and proof.files_before_alter == proof.files_after_alter, \
    f"ALTER is metadata-only: files_after_alter should equal files_before_alter (about 10), got {proof.files_before_alter} vs {proof.files_after_alter}"
assert 1 <= proof.files_after_optimize <= 2, \
    f"After OPTIMIZE FULL, expected 1-2 files, got {proof.files_after_optimize}"
assert proof.files_after_optimize == detail.numFiles, \
    f"files_after_optimize does not match the table's current numFiles from DESCRIBE DETAIL, got {proof.files_after_optimize}"

print("Exercise 5 passed!")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Exercise 6: Multi-Column Clustering Keys
# MAGIC **Difficulty**: Medium | **Time**: ~10 min
# MAGIC
# MAGIC Create a table clustered on multiple columns. Liquid clustering supports up to 4 clustering
# MAGIC keys, in any order, and a filter on any of them can skip files. On tables under 10 TB, each
# MAGIC extra key weakens single-column filters, so cluster only on columns you filter by.
# MAGIC
# MAGIC **Expected Output**: `lc_ex6_orders` exists with 5 rows and `CLUSTER BY (status, customer_id)`.
# MAGIC
# MAGIC **Requirements**:
# MAGIC 1. Create `db_code.liquid_clustering.lc_ex6_orders` with `CLUSTER BY (status, customer_id)`
# MAGIC 2. Copy rows where `order_id IN ('ORD-001', 'ORD-002', 'ORD-003', 'ORD-004', 'ORD-005')` from `db_code.delta_lake.orders`

# COMMAND ----------

# EXERCISE_KEY: lc_ex6
# TODO: Create a table with multi-column liquid clustering

# Your code here


# COMMAND ----------

# Validate Exercise 6
result = spark.table(f"{CATALOG}.{SCHEMA}.lc_ex6_orders")
assert result.count() == 5, f"Expected 5 rows, got {result.count()}"

detail = spark.sql(f"DESCRIBE DETAIL {CATALOG}.{SCHEMA}.lc_ex6_orders").collect()[0]
clustering = detail.clusteringColumns
assert "status" in clustering, f"Should include status in clustering, got {clustering}"
assert "customer_id" in clustering, f"Should include customer_id in clustering, got {clustering}"

print("Exercise 6 passed!")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Exercise 7: Migrate Partitioned Table to Liquid Clustering
# MAGIC **Difficulty**: Hard | **Time**: ~20 min
# MAGIC
# MAGIC A table is currently `PARTITIONED BY (status)`. Plain `ALTER TABLE ... CLUSTER BY` fails on a
# MAGIC partitioned table. On Databricks Runtime 18.1 and above you can convert it in place with
# MAGIC `ALTER TABLE ... REPLACE PARTITIONED BY WITH CLUSTER BY`; here you practice the other path: copy
# MAGIC the data into a new liquid clustered table, which leaves the original untouched until readers switch over.
# MAGIC
# MAGIC **Source** (`lc_ex7_partitioned`): 5 rows, PARTITIONED BY (status).
# MAGIC
# MAGIC **Expected Output**: `lc_ex7_clustered` exists with same 5 rows, CLUSTER BY (status),
# MAGIC and no partitioning, and its `DESCRIBE HISTORY` shows an `OPTIMIZE` row.
# MAGIC
# MAGIC **Requirements**:
# MAGIC 1. Create `db_code.liquid_clustering.lc_ex7_clustered` with `CLUSTER BY (status)` (no partitioning)
# MAGIC 2. Copy all data from `lc_ex7_partitioned`
# MAGIC 3. Run `OPTIMIZE` on the new table

# COMMAND ----------

# EXERCISE_KEY: lc_ex7
# TODO: Create new clustered table, copy data from partitioned, OPTIMIZE

# Your code here


# COMMAND ----------

# Validate Exercise 7
result = spark.table(f"{CATALOG}.{SCHEMA}.lc_ex7_clustered")
assert result.count() == 5, f"Expected 5 rows, got {result.count()}"

detail = spark.sql(f"DESCRIBE DETAIL {CATALOG}.{SCHEMA}.lc_ex7_clustered").collect()[0]
clustering = detail.clusteringColumns
assert "status" in clustering, f"Should be clustered by status, got {clustering}"
# Verify no partitioning
assert len(detail.partitionColumns) == 0, \
    f"Should not be partitioned, got partitionColumns={detail.partitionColumns}"
optimize_rows = spark.sql(f"DESCRIBE HISTORY {CATALOG}.{SCHEMA}.lc_ex7_clustered").filter("operation = 'OPTIMIZE'").count()
assert optimize_rows > 0, "No OPTIMIZE in the history of lc_ex7_clustered: run OPTIMIZE on the new table"

print("Exercise 7 passed!")
