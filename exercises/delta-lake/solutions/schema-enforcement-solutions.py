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
# MAGIC 1. Delta checks every write against the target schema: the column count must match, and each type must match or cast safely
# MAGIC 2. Here both tables have the same 7 columns in the same order
# MAGIC 3. `INSERT INTO <target> SELECT * FROM <source>` works when schemas match exactly
# MAGIC
# MAGIC **Common mistakes**:
# MAGIC - Calling `df.write.saveAsTable()` without `mode("append")` (the default mode is `error`, so it fails because the table already exists)

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
# MAGIC 1. `INSERT INTO ... SELECT *` fails because the source has one more column than the target
# MAGIC 2. List the target's columns explicitly in your SELECT
# MAGIC 3. The extra column in the source is simply ignored when you don't select it
# MAGIC
# MAGIC **Common mistakes**:
# MAGIC - Using SELECT * (fails because source has extra column)
# MAGIC - Using mergeSchema here (it adds `discount_pct` to the target; the requirement is to leave it out)
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
# MAGIC - Passing `mergeSchema` to a SQL INSERT (it is a DataFrame writer option; SQL uses a `WITH SCHEMA EVOLUTION` clause instead)

# COMMAND ----------

# EXERCISE_KEY: schema_ex3
spark.table(f"{CATALOG}.{SCHEMA}.schema_ex3_source") \
    .write.mode("append") \
    .option("mergeSchema", "true") \
    .saveAsTable(f"{CATALOG}.{SCHEMA}.schema_ex3_target")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Exercise 4: Overwrite Schema for Breaking Change
# MAGIC
# MAGIC **Hints**:
# MAGIC 1. mode("overwrite") alone preserves the target schema and fails on mismatch
# MAGIC 2. You need an additional option to allow schema replacement
# MAGIC 3. The option is "overwriteSchema" set to "true"
# MAGIC
# MAGIC In SQL, `CREATE OR REPLACE TABLE ... AS SELECT` replaces data and schema in one statement; with the DataFrame writer you need `overwriteSchema`.
# MAGIC
# MAGIC **Common mistakes**:
# MAGIC - Using only mode("overwrite") without the overwriteSchema option (schema mismatch error)
# MAGIC - Confusing mergeSchema with overwriteSchema (merge adds columns, overwrite replaces everything)

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
# MAGIC 1. Databricks only applies safe casts on write (INT into BIGINT); STRING into DOUBLE is not one of them
# MAGIC 2. Use CAST() in your SELECT to convert the mismatched column
# MAGIC 3. CAST(amount AS DOUBLE) converts STRING amounts to the target's DOUBLE type
# MAGIC
# MAGIC **Common mistakes**:
# MAGIC - Assuming the INSERT will cast STRING to DOUBLE for you (it fails: a STRING into a numeric column is not a safe cast)
# MAGIC - Casting to the wrong type (must match target exactly)
# MAGIC - In a DataFrame append, losing the `amount` name on the cast column (DataFrame appends match columns by name; a SQL INSERT matches by position)

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
# MAGIC 1. All existing `status` values are non-null, so the constraint can be added
# MAGIC 2. Nullability is changed with ALTER TABLE ... ALTER COLUMN
# MAGIC 3. Syntax: `ALTER TABLE t ALTER COLUMN col SET NOT NULL`
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
# MAGIC 3. The name must be exactly `positive_amount`: `ALTER TABLE t ADD CONSTRAINT positive_amount CHECK (amount > 0)`
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
# MAGIC 1. Named CHECK constraints and NOT NULL are removed with different statements
# MAGIC 2. You need the exact constraint name: `positive_amount`
# MAGIC 3. Use `ALTER TABLE ... DROP CONSTRAINT positive_amount`; NOT NULL would need `ALTER COLUMN ... DROP NOT NULL`, which you must not run here
# MAGIC
# MAGIC **Common mistakes**:
# MAGIC - Trying to drop NOT NULL with DROP CONSTRAINT (different syntax)
# MAGIC - Misspelling the constraint name (must match exactly)

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
# MAGIC 3. `SHOW TBLPROPERTIES` cannot be used as a subquery: run it with `spark.sql(...)`, filter the DataFrame with `.filter("key LIKE 'delta.constraints.%'")`, then rename `key` and `value` with `selectExpr`
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
# MAGIC - Forgetting WITH SCHEMA EVOLUTION (the MERGE succeeds but silently drops `discount_pct` and `shipping_cost`; the target keeps its 7 columns)
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
# MAGIC - Inserting all 3 rows and catching the error (Delta Lake rejects the whole batch, so ORD-201 is not inserted either)
# MAGIC - Checking only one constraint (filter on both `status IS NOT NULL` and `amount > 0`)

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
# MAGIC - Running the MERGE without filtering (ORD-202 and ORD-203 break the constraints, so the whole MERGE fails and nothing is written)
# MAGIC - Adding the conditions to WHEN MATCHED only (ORD-202 and ORD-203 are new keys, so they reach WHEN NOT MATCHED and fail the MERGE; filtering in USING covers both clauses)
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
