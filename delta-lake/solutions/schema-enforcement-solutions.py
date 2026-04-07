# Databricks notebook source
# COMMAND ----------
# MAGIC %md
# MAGIC # Schema Enforcement & Evolution - Solutions
# MAGIC **Topic**: Delta Lake | **Exercises**: 12
# MAGIC
# MAGIC Reference solutions, hints, and common mistakes for each exercise.
# MAGIC Try solving the exercises first before looking here.

# COMMAND ----------
# MAGIC %md
# MAGIC ## Exercise 1: Write with Matching Schema
# MAGIC
# MAGIC **Hints**:
# MAGIC 1. INSERT INTO ... SELECT * works when schemas match exactly
# MAGIC 2. Delta Lake validates every column name and type against the target
# MAGIC 3. If any column doesn't match, the write fails before any data is written
# MAGIC
# MAGIC **Common mistakes**:
# MAGIC - Using df.write.saveAsTable() which would overwrite instead of append
# MAGIC - Not realizing Delta checks schema on every write, not just the first one

# COMMAND ----------

# EXERCISE_KEY: schema_ex1
CATALOG = "db_code"
SCHEMA = "schema_enforcement"
BASE_SCHEMA = "delta_lake"

spark.sql(f"""
    INSERT INTO {CATALOG}.{SCHEMA}.schema_ex1_target
    SELECT * FROM {CATALOG}.{SCHEMA}.schema_ex1_source
""")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Exercise 2: Handle Schema Mismatch on Write
# MAGIC
# MAGIC **Hints**:
# MAGIC 1. You can't INSERT * because the source has more columns than the target
# MAGIC 2. List the target's columns explicitly in your SELECT
# MAGIC 3. The extra column in the source is simply ignored when you don't select it
# MAGIC
# MAGIC **Common mistakes**:
# MAGIC - Using SELECT * (fails because source has extra column)
# MAGIC - Trying to use mergeSchema here (that adds the column, this exercise drops it)
# MAGIC - Forgetting a column in the explicit list (column count must match exactly)

# COMMAND ----------

# EXERCISE_KEY: schema_ex2
spark.sql(f"""
    INSERT INTO {CATALOG}.{SCHEMA}.schema_ex2_target
    SELECT order_id, customer_id, product_id, amount, status, order_date, updated_at
    FROM {CATALOG}.{SCHEMA}.schema_ex2_source
""")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Exercise 3: Additive Schema Evolution with mergeSchema
# MAGIC
# MAGIC **Hints**:
# MAGIC 1. The DataFrame writer has options you can set with `.option()`
# MAGIC 2. The option name relates to merging schemas together
# MAGIC 3. Use `.write.mode("append").option("mergeSchema", "true").saveAsTable()`
# MAGIC
# MAGIC **Common mistakes**:
# MAGIC - Using mode("overwrite") instead of mode("append") (overwrites all data)
# MAGIC - Misspelling the option name (it's "mergeSchema", camelCase)
# MAGIC - Trying to use this with SQL INSERT (mergeSchema is a DataFrame writer option)

# COMMAND ----------

# EXERCISE_KEY: schema_ex3
spark.table(f"{CATALOG}.{SCHEMA}.schema_ex3_source") \
    .write.mode("append") \
    .option("mergeSchema", "true") \
    .saveAsTable(f"{CATALOG}.{SCHEMA}.schema_ex3_target")

# Alternative: use SQL with schema evolution (for MERGE only, not INSERT)
# spark.sql(f"MERGE WITH SCHEMA EVOLUTION INTO {CATALOG}.{SCHEMA}.schema_ex3_target t USING ...")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Exercise 4: Overwrite Schema for Breaking Change
# MAGIC
# MAGIC **Hints**:
# MAGIC 1. mode("overwrite") alone preserves the target schema and fails on mismatch
# MAGIC 2. You need an additional option to allow schema replacement
# MAGIC 3. The option is "overwriteSchema" set to "true"
# MAGIC
# MAGIC **Common mistakes**:
# MAGIC - Using only mode("overwrite") without the overwriteSchema option (schema mismatch error)
# MAGIC - Confusing mergeSchema with overwriteSchema (merge adds columns, overwrite replaces everything)
# MAGIC - Using CREATE OR REPLACE TABLE instead (valid, but this exercise is about the writer option)

# COMMAND ----------

# EXERCISE_KEY: schema_ex4
spark.table(f"{CATALOG}.{SCHEMA}.schema_ex4_source") \
    .write.mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(f"{CATALOG}.{SCHEMA}.schema_ex4_target")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Exercise 5: Handle Column Type Mismatch
# MAGIC
# MAGIC **Hints**:
# MAGIC 1. Delta rejects type mismatches even for "compatible" types like STRING and DOUBLE
# MAGIC 2. Use CAST() in your SELECT to convert the mismatched column
# MAGIC 3. CAST(amount AS DOUBLE) converts STRING amounts to the target's DOUBLE type
# MAGIC
# MAGIC **Common mistakes**:
# MAGIC - Assuming Spark will auto-cast STRING to DOUBLE (Delta enforces strict types)
# MAGIC - Casting to the wrong type (must match target exactly)
# MAGIC - Forgetting to alias the casted column with the same name

# COMMAND ----------

# EXERCISE_KEY: schema_ex5
spark.sql(f"""
    INSERT INTO {CATALOG}.{SCHEMA}.schema_ex5_target
    SELECT order_id, customer_id, product_id,
           CAST(amount AS DOUBLE) AS amount,
           status, order_date, updated_at
    FROM {CATALOG}.{SCHEMA}.schema_ex5_source
""")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Exercise 6: NOT NULL Constraint Enforcement
# MAGIC
# MAGIC **Hints**:
# MAGIC 1. ALTER TABLE ... ALTER COLUMN lets you modify column properties
# MAGIC 2. Syntax: `ALTER TABLE t ALTER COLUMN col SET NOT NULL`
# MAGIC 3. All existing values must be non-null before the constraint can be added
# MAGIC
# MAGIC **Common mistakes**:
# MAGIC - Trying to add NOT NULL on a column that has existing null values (fails)
# MAGIC - Using ADD CONSTRAINT syntax (that's for CHECK constraints, not NOT NULL)
# MAGIC - Forgetting to actually insert a row after adding the constraint

# COMMAND ----------

# EXERCISE_KEY: schema_ex6
# Step 1: Add NOT NULL constraint
spark.sql(f"""
    ALTER TABLE {CATALOG}.{SCHEMA}.schema_ex6_orders ALTER COLUMN status SET NOT NULL
""")

# Step 2: Insert a valid new order
spark.sql(f"""
    INSERT INTO {CATALOG}.{SCHEMA}.schema_ex6_orders VALUES
    ('ORD-101', 'CUST-010', 'PROD-005', 49.99, 'pending', DATE '2026-03-01', TIMESTAMP '2026-03-01 10:00:00')
""")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Exercise 7: CHECK Constraint Enforcement
# MAGIC
# MAGIC **Hints**:
# MAGIC 1. ALTER TABLE ADD CONSTRAINT creates a named rule for the table
# MAGIC 2. Syntax: `ALTER TABLE t ADD CONSTRAINT name CHECK (expression)`
# MAGIC 3. Name your constraint descriptively (e.g., `positive_amount`)
# MAGIC
# MAGIC **Common mistakes**:
# MAGIC - Forgetting to name the constraint (syntax requires a name)
# MAGIC - Adding a CHECK that existing data violates (fails immediately)
# MAGIC - Confusing CHECK with NOT NULL (different syntax: ADD CONSTRAINT vs ALTER COLUMN)

# COMMAND ----------

# EXERCISE_KEY: schema_ex7
# Step 1: Add CHECK constraint
spark.sql(f"""
    ALTER TABLE {CATALOG}.{SCHEMA}.schema_ex7_orders
    ADD CONSTRAINT positive_amount CHECK (amount > 0)
""")

# Step 2: Insert a valid new order
spark.sql(f"""
    INSERT INTO {CATALOG}.{SCHEMA}.schema_ex7_orders VALUES
    ('ORD-101', 'CUST-010', 'PROD-005', 49.99, 'pending', DATE '2026-03-01', TIMESTAMP '2026-03-01 10:00:00')
""")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Exercise 8: Drop a Constraint
# MAGIC
# MAGIC **Hints**:
# MAGIC 1. Use `ALTER TABLE ... DROP CONSTRAINT` to remove a named constraint
# MAGIC 2. You need the exact constraint name (it's `positive_amount`)
# MAGIC 3. NOT NULL constraints use different syntax (ALTER COLUMN ... DROP NOT NULL)
# MAGIC
# MAGIC **Common mistakes**:
# MAGIC - Trying to drop NOT NULL with DROP CONSTRAINT (different syntax)
# MAGIC - Misspelling the constraint name (must match exactly)
# MAGIC - Dropping the wrong constraint (read the requirements carefully)

# COMMAND ----------

# EXERCISE_KEY: schema_ex8
spark.sql(f"""
    ALTER TABLE {CATALOG}.{SCHEMA}.schema_ex8_orders DROP CONSTRAINT positive_amount
""")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Exercise 9: Query Table Properties for Constraints
# MAGIC
# MAGIC **Hints**:
# MAGIC 1. Use `SHOW TBLPROPERTIES` to see all properties of a Delta table
# MAGIC 2. Delta stores CHECK constraints as properties with prefix `delta.constraints.`
# MAGIC 3. Filter with `WHERE key LIKE 'delta.constraints.%'` to get only constraints
# MAGIC
# MAGIC **Common mistakes**:
# MAGIC - Querying `information_schema.table_constraints` (returns 0 rows for Delta CHECK constraints on Free Edition)
# MAGIC - Forgetting to filter for the `delta.constraints.` prefix (returns all table properties)
# MAGIC - Not aliasing the columns to `constraint_name` and `constraint_expression`

# COMMAND ----------

# EXERCISE_KEY: schema_ex9
props = spark.sql(f"SHOW TBLPROPERTIES {CATALOG}.{SCHEMA}.schema_ex9_orders")
constraints = props.filter("key LIKE 'delta.constraints.%'") \
    .selectExpr("key AS constraint_name", "value AS constraint_expression")
constraints.write.mode("overwrite").saveAsTable(f"{CATALOG}.{SCHEMA}.schema_ex9_constraints")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Exercise 10: Schema Evolution Through MERGE
# MAGIC
# MAGIC **Hints**:
# MAGIC 1. MERGE doesn't support schema evolution by default
# MAGIC 2. Use `MERGE WITH SCHEMA EVOLUTION` to allow the source to add new columns
# MAGIC 3. The syntax goes between MERGE and INTO: `MERGE WITH SCHEMA EVOLUTION INTO ...`
# MAGIC
# MAGIC **Common mistakes**:
# MAGIC - Forgetting WITH SCHEMA EVOLUTION (MERGE fails with schema mismatch)
# MAGIC - Placing WITH SCHEMA EVOLUTION after INTO (wrong position)
# MAGIC - Using .option("mergeSchema") on MERGE (that's a DataFrame writer option, not SQL)

# COMMAND ----------

# EXERCISE_KEY: schema_ex10
spark.sql(f"""
    MERGE WITH SCHEMA EVOLUTION INTO {CATALOG}.{SCHEMA}.schema_ex10_target t
    USING {CATALOG}.{SCHEMA}.schema_ex10_source s
    ON t.order_id = s.order_id
    WHEN MATCHED THEN UPDATE SET *
    WHEN NOT MATCHED THEN INSERT *
""")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Exercise 11: Constraint Violation Handling
# MAGIC
# MAGIC **Hints**:
# MAGIC 1. Create the candidate rows as a temp view or inline VALUES
# MAGIC 2. Filter with WHERE to keep only rows satisfying both constraints
# MAGIC 3. The conditions mirror the table's constraints: status IS NOT NULL AND amount > 0
# MAGIC
# MAGIC **Common mistakes**:
# MAGIC - Trying to INSERT all 3 rows and catching errors (Delta rejects the entire batch, not individual rows)
# MAGIC - Only checking one constraint (need to check both NOT NULL and CHECK)
# MAGIC - Using try/except (filters are cleaner and more production-appropriate)

# COMMAND ----------

# EXERCISE_KEY: schema_ex11
spark.sql(f"""
    INSERT INTO {CATALOG}.{SCHEMA}.schema_ex11_orders
    SELECT * FROM VALUES
        ('ORD-201', 'CUST-020', 'PROD-005', 49.99, 'pending', DATE '2026-03-01', TIMESTAMP '2026-03-01 10:00:00'),
        ('ORD-202', 'CUST-021', 'PROD-003', -10.00, 'completed', DATE '2026-03-01', TIMESTAMP '2026-03-01 10:00:00'),
        ('ORD-203', 'CUST-022', 'PROD-001', 75.00, NULL, DATE '2026-03-01', TIMESTAMP '2026-03-01 10:00:00')
    AS t(order_id, customer_id, product_id, amount, status, order_date, updated_at)
    WHERE status IS NOT NULL AND amount > 0
""")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Exercise 12: MERGE with Constraint-Safe Filtering
# MAGIC
# MAGIC **Hints**:
# MAGIC 1. Use a subquery in the USING clause to pre-filter the source
# MAGIC 2. The WHERE clause should match the target's constraints: status IS NOT NULL AND amount > 0
# MAGIC 3. Rows that fail the filter never reach the MERGE, so no constraint violation occurs
# MAGIC
# MAGIC **Common mistakes**:
# MAGIC - Trying to MERGE without filtering (fails on the first invalid row)
# MAGIC - Filtering in WHEN MATCHED instead of USING (the row already reached the target at that point)
# MAGIC - Forgetting WHEN NOT MATCHED (only updates existing rows, doesn't insert new ones)

# COMMAND ----------

# EXERCISE_KEY: schema_ex12
spark.sql(f"""
    MERGE INTO {CATALOG}.{SCHEMA}.schema_ex12_target t
    USING (
        SELECT * FROM {CATALOG}.{SCHEMA}.schema_ex12_source
        WHERE status IS NOT NULL AND amount > 0
    ) s
    ON t.order_id = s.order_id
    WHEN MATCHED THEN UPDATE SET *
    WHEN NOT MATCHED THEN INSERT *
""")
