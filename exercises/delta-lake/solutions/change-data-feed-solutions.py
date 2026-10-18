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
# MAGIC 3. Set it to `true`
# MAGIC
# MAGIC **Common mistakes**:
# MAGIC - Using a Spark config instead of a table property (CDF is set per table)
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
# MAGIC 3. Save the result with `CREATE OR REPLACE TABLE ... AS SELECT * FROM table_changes(...)`
# MAGIC
# MAGIC **Common mistakes**:
# MAGIC - Using version 0 (that's the initial data load, not the INSERT)
# MAGIC - Forgetting the quotes around the table name in `table_changes()`

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
# MAGIC 1. `table_changes()` is a table-valued function, so it goes in the `FROM` clause like a table name
# MAGIC 2. The UPDATE is version 1, so start reading at version 1
# MAGIC 3. Same `table_changes()` call as Exercise 2, pointed at `cdf_ex3_orders`, saved with `CREATE OR REPLACE TABLE ... AS SELECT * FROM table_changes(...)`
# MAGIC
# MAGIC **Common mistakes**:
# MAGIC - Expecting 1 row per UPDATE (CDF writes 2: pre + post)

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
# MAGIC 1. `table_changes()` is a table-valued function, so it goes in the `FROM` clause like a table name
# MAGIC 2. The DELETE is version 1, so start reading at version 1
# MAGIC 3. Same `table_changes()` call as Exercise 2, pointed at `cdf_ex4_orders`, saved with `CREATE OR REPLACE TABLE ... AS SELECT * FROM table_changes(...)`
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
# MAGIC 3. Wrap the filtered query in `CREATE OR REPLACE TABLE ... AS SELECT` to save `cdf_ex5_inserts`
# MAGIC
# MAGIC **Common mistakes**:
# MAGIC - Filtering on `_commit_version` instead of `_change_type` (version tells when, type tells what)
# MAGIC - Including `update_postimage` rows (those are updates, not inserts)

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
# MAGIC 2. Give every WHEN clause its own `_change_type` condition, the NOT MATCHED one included
# MAGIC 3. The three clauses: `WHEN MATCHED AND s._change_type = 'delete' THEN DELETE`, `WHEN MATCHED AND s._change_type = 'update_postimage' THEN UPDATE SET ...`, `WHEN NOT MATCHED AND s._change_type != 'delete' THEN INSERT ...`. Without the `!= 'delete'` test on the insert clause, a `delete` row for a key the target lacks would be inserted as a live row.
# MAGIC
# MAGIC **Common mistakes**:
# MAGIC - Letting `update_preimage` rows reach a `WHEN MATCHED` clause they satisfy, such as an unconditional `WHEN MATCHED THEN UPDATE`: the preimage and the postimage of ORD-001 both match one target row, and the MERGE fails with `DELTA_MULTIPLE_SOURCE_ROW_MATCHING_TARGET_ROW_IN_MERGE`. Filter them out in the `USING` subquery.
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
    WHEN NOT MATCHED AND s._change_type != 'delete' THEN INSERT (order_id, customer_id, product_id, amount, status, order_date, updated_at)
        VALUES (s.order_id, s.customer_id, s.product_id, s.amount, s.status, s.order_date, s.updated_at)
""")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Exercise 7: Count the Changes a MERGE Wrote
# MAGIC
# MAGIC **Hints**:
# MAGIC 1. A MERGE writes CDF rows for every row it touches: one `insert` row per inserted row, plus an `update_preimage` and an `update_postimage` row per updated row
# MAGIC 2. Same `table_changes()` call, pointed at `cdf_ex7_orders` from version 1
# MAGIC 3. `SELECT _change_type, count(*) AS row_count FROM table_changes(...) GROUP BY _change_type`, wrapped in `CREATE OR REPLACE TABLE ... AS`
# MAGIC
# MAGIC **Common mistakes**:
# MAGIC - Expecting only inserts (MERGE can update and insert in the same operation)
# MAGIC - Not accounting for preimage rows when counting changes

# COMMAND ----------

# EXERCISE_KEY: cdf_ex7
spark.sql(f"""
    CREATE OR REPLACE TABLE {CATALOG}.{SCHEMA}.cdf_ex7_counts AS
    SELECT _change_type, count(*) AS row_count
    FROM table_changes('{CATALOG}.{SCHEMA}.cdf_ex7_orders', 1)
    GROUP BY _change_type
""")
