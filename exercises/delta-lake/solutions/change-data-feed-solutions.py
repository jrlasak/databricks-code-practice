# Databricks notebook source
# COMMAND ----------
# MAGIC %md
# MAGIC # Change Data Feed (CDF) - Solutions
# MAGIC **Topic**: Delta Lake | **Exercises**: 7
# MAGIC
# MAGIC Reference solutions, hints, and common mistakes for each exercise.
# MAGIC Try solving the exercises first before looking here.

# COMMAND ----------
# MAGIC %md
# MAGIC ## Exercise 1: Enable CDF on an Existing Table
# MAGIC
# MAGIC **Hints**:
# MAGIC 1. Use `ALTER TABLE ... SET TBLPROPERTIES`
# MAGIC 2. The property name is `delta.enableChangeDataFeed`
# MAGIC 3. Set it to `true` (string value)
# MAGIC
# MAGIC **Common mistakes**:
# MAGIC - Using a Spark config instead of table property (CDF is per-table)
# MAGIC - Misspelling the property name

# COMMAND ----------

# EXERCISE_KEY: cdf_ex1
CATALOG = "db_code"
SCHEMA = "change_data_feed"
BASE_SCHEMA = "delta_lake"

spark.sql(f"""
    ALTER TABLE {CATALOG}.{SCHEMA}.cdf_ex1_orders
    SET TBLPROPERTIES (delta.enableChangeDataFeed = true)
""")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Exercise 2: Read Changes After INSERT
# MAGIC
# MAGIC **Hints**:
# MAGIC 1. `table_changes('catalog.schema.table', start_version)` reads CDF
# MAGIC 2. Version 1 is the INSERT operation (version 0 was the initial CTAS)
# MAGIC 3. Save the result with `CREATE TABLE AS SELECT * FROM table_changes(...)`
# MAGIC
# MAGIC **Common mistakes**:
# MAGIC - Using version 0 (that's the initial data load, not the INSERT)
# MAGIC - Forgetting the quotes around the table name in table_changes()

# COMMAND ----------

# EXERCISE_KEY: cdf_ex2
spark.sql(f"""
    CREATE OR REPLACE TABLE {CATALOG}.{SCHEMA}.cdf_ex2_changes AS
    SELECT * FROM table_changes('{CATALOG}.{SCHEMA}.cdf_ex2_orders', 1)
""")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Exercise 3: Read Changes After UPDATE
# MAGIC
# MAGIC **Hints**:
# MAGIC 1. Same `table_changes()` syntax as Exercise 2
# MAGIC 2. UPDATEs produce TWO rows: `update_preimage` (before) and `update_postimage` (after)
# MAGIC 3. The preimage has the old values, postimage has the new values
# MAGIC
# MAGIC **Common mistakes**:
# MAGIC - Expecting only 1 row per UPDATE (it's 2: pre + post)
# MAGIC - Confusing preimage with postimage (pre = old, post = new)

# COMMAND ----------

# EXERCISE_KEY: cdf_ex3
spark.sql(f"""
    CREATE OR REPLACE TABLE {CATALOG}.{SCHEMA}.cdf_ex3_changes AS
    SELECT * FROM table_changes('{CATALOG}.{SCHEMA}.cdf_ex3_orders', 1)
""")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Exercise 4: Read Changes After DELETE
# MAGIC
# MAGIC **Hints**:
# MAGIC 1. DELETEs produce 1 CDF row per deleted row with `_change_type = 'delete'`
# MAGIC 2. The row contains the deleted values (so you know what was removed)
# MAGIC 3. Same `table_changes()` syntax
# MAGIC
# MAGIC **Common mistakes**:
# MAGIC - Expecting the delete row to be empty (it has the full deleted row's data)

# COMMAND ----------

# EXERCISE_KEY: cdf_ex4
spark.sql(f"""
    CREATE OR REPLACE TABLE {CATALOG}.{SCHEMA}.cdf_ex4_changes AS
    SELECT * FROM table_changes('{CATALOG}.{SCHEMA}.cdf_ex4_orders', 1)
""")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Exercise 5: Filter CDF by Operation Type
# MAGIC
# MAGIC **Hints**:
# MAGIC 1. Read all changes from version 1 onwards
# MAGIC 2. Add a WHERE clause to filter `_change_type = 'insert'`
# MAGIC 3. This is how you build type-specific processing in CDC pipelines
# MAGIC
# MAGIC **Common mistakes**:
# MAGIC - Filtering on `_commit_version` instead of `_change_type` (version tells when, type tells what)
# MAGIC - Including update_postimage (those are updates, not inserts)

# COMMAND ----------

# EXERCISE_KEY: cdf_ex5
spark.sql(f"""
    CREATE OR REPLACE TABLE {CATALOG}.{SCHEMA}.cdf_ex5_inserts AS
    SELECT * FROM table_changes('{CATALOG}.{SCHEMA}.cdf_ex5_orders', 1)
    WHERE _change_type = 'insert'
""")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Exercise 6: Propagate Changes to Downstream Table
# MAGIC
# MAGIC **Hints**:
# MAGIC 1. Read CDF from source, exclude `update_preimage` rows (old values, not needed)
# MAGIC 2. MERGE into target: delete when `_change_type = 'delete'`, update when `update_postimage`, insert when `insert`
# MAGIC 3. Order WHEN MATCHED clauses: delete first, then update (like Exercise 6 in MERGE notebook)
# MAGIC
# MAGIC **Common mistakes**:
# MAGIC - Including update_preimage in the MERGE (causes duplicate key errors)
# MAGIC - Not handling all three change types (missing deletes or updates)
# MAGIC - Using INSERT instead of MERGE (doesn't handle updates or deletes)

# COMMAND ----------

# EXERCISE_KEY: cdf_ex6
spark.sql(f"""
    MERGE INTO {CATALOG}.{SCHEMA}.cdf_ex6_target t
    USING (
        SELECT * FROM table_changes('{CATALOG}.{SCHEMA}.cdf_ex6_source', 1)
        WHERE _change_type != 'update_preimage'
    ) s
    ON t.order_id = s.order_id
    WHEN MATCHED AND s._change_type = 'delete' THEN DELETE
    WHEN MATCHED AND s._change_type = 'update_postimage' THEN UPDATE SET
        customer_id = s.customer_id, product_id = s.product_id, amount = s.amount,
        status = s.status, order_date = s.order_date, updated_at = s.updated_at
    WHEN NOT MATCHED AND s._change_type = 'insert' THEN INSERT (order_id, customer_id, product_id, amount, status, order_date, updated_at)
        VALUES (s.order_id, s.customer_id, s.product_id, s.amount, s.status, s.order_date, s.updated_at)
""")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Exercise 7: Read CDF from a MERGE Operation
# MAGIC
# MAGIC **Hints**:
# MAGIC 1. MERGE produces CDF rows for each affected row: inserts + update pre/post images
# MAGIC 2. Same `table_changes()` syntax, version 1 has the MERGE
# MAGIC 3. Expect 3 rows: 1 update_preimage + 1 update_postimage + 1 insert
# MAGIC
# MAGIC **Common mistakes**:
# MAGIC - Expecting only inserts (MERGE can update and insert in the same operation)
# MAGIC - Not accounting for preimage rows when counting changes

# COMMAND ----------

# EXERCISE_KEY: cdf_ex7
spark.sql(f"""
    CREATE OR REPLACE TABLE {CATALOG}.{SCHEMA}.cdf_ex7_changes AS
    SELECT * FROM table_changes('{CATALOG}.{SCHEMA}.cdf_ex7_orders', 1)
""")
