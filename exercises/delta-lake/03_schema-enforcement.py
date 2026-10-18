# Databricks notebook source
# COMMAND ----------
# MAGIC %md
# MAGIC # Schema Enforcement & Evolution
# MAGIC **Topic**: Delta Lake | **Exercises**: 12 | **Total Time**: ~125 min
# MAGIC
# MAGIC Practice Delta Lake schema enforcement, evolution modes, type handling, constraints,
# MAGIC and schema evolution through MERGE. Delta Lake enforces schema by default on every
# MAGIC write - learn how to work with it and when to override it.
# MAGIC
# MAGIC **Solutions**: If stuck, see `solutions/schema-enforcement-solutions.py` for hints and answers.
# MAGIC
# MAGIC **Tables used** (from `00_Setup.py`):
# MAGIC - `db_code.delta_lake.orders` - order records
# MAGIC - `db_code.delta_lake.products` - product catalog
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

# MAGIC %run ./setup/schema-enforcement-setup

# COMMAND ----------
# MAGIC %md
# MAGIC **Setup complete.** Every exercise table is in `db_code.schema_enforcement`. Write your output tables there too, by full name (for example `db_code.schema_enforcement.schema_ex9_constraints`). In Python cells, the variables `CATALOG` and `SCHEMA` hold `db_code` and `schema_enforcement`. The base tables (`orders`, `products`) are in `db_code.delta_lake`.
# MAGIC
# MAGIC Keep the `# EXERCISE_KEY: ...` line as the first line of each answer cell.
# MAGIC
# MAGIC Exercise tables:
# MAGIC - Ex 1 (easy): `schema_ex1_target`, `schema_ex1_source` - matching schemas
# MAGIC - Ex 2 (easy): `schema_ex2_target`, `schema_ex2_source` - source has an extra `discount_pct` column
# MAGIC - Ex 3 (medium): `schema_ex3_target`, `schema_ex3_source` - source has the same extra `discount_pct` column
# MAGIC - Ex 4 (medium): `schema_ex4_target`, `schema_ex4_source` - source has a product schema, not orders
# MAGIC - Ex 5 (medium): `schema_ex5_target`, `schema_ex5_source` - source `amount` is STRING, target `amount` is DOUBLE
# MAGIC - Ex 6 and Ex 7 (medium): `schema_ex6_orders`, `schema_ex7_orders` - no constraints yet
# MAGIC - Ex 8 (medium): `schema_ex8_orders` - `status` NOT NULL + CHECK `positive_amount`
# MAGIC - Ex 9 (medium): `schema_ex9_orders` - CHECK `positive_amount` + CHECK `valid_status`
# MAGIC - Ex 10 (hard): `schema_ex10_target`, `schema_ex10_source` - source has 2 extra columns, `discount_pct` and `shipping_cost`
# MAGIC - Ex 11 (hard): `schema_ex11_orders` - `status` NOT NULL + CHECK `positive_amount`
# MAGIC - Ex 12 (hard): `schema_ex12_target`, `schema_ex12_source` - target has `status` NOT NULL + CHECK `positive_amount`; source has 2 rows that break them

# COMMAND ----------
# MAGIC %md
# MAGIC ## Exercise 1: Write with Matching Schema
# MAGIC **Difficulty**: Easy | **Time**: ~5 min
# MAGIC
# MAGIC Insert rows from a source table with an identical schema into the target.
# MAGIC
# MAGIC **Source** (`schema_ex1_source`): 3 rows - ORD-101, ORD-102, ORD-103. Same schema as target.
# MAGIC
# MAGIC **Target** (`schema_ex1_target`): 5 rows - ORD-001 through ORD-005 from base orders.
# MAGIC
# MAGIC **Expected Output**: `schema_ex1_target` has 8 rows. ORD-101, ORD-102, ORD-103 exist.
# MAGIC
# MAGIC **Requirements**:
# MAGIC 1. Insert all rows from source into target

# COMMAND ----------

# EXERCISE_KEY: schema_ex1
# TODO: Insert source rows into the target table

# Your code here


# COMMAND ----------

# Validate Exercise 1
result = spark.table(f"{CATALOG}.{SCHEMA}.schema_ex1_target")

assert result.count() == 8, f"Expected 8 rows, got {result.count()}"
assert result.filter("order_id = 'ORD-101'").count() == 1, "ORD-101 should be inserted"
assert result.filter("order_id = 'ORD-103'").count() == 1, "ORD-103 should be inserted"

print("Exercise 1 passed!")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Exercise 2: Handle Schema Mismatch on Write
# MAGIC **Difficulty**: Easy | **Time**: ~5 min
# MAGIC
# MAGIC The source has an extra column (`discount_pct`) that doesn't exist in the target.
# MAGIC A direct `INSERT ... SELECT *` would fail because Delta Lake enforces schema by default.
# MAGIC Write the data to the target by selecting only the columns that match.
# MAGIC
# MAGIC **Source** (`schema_ex2_source`): 3 rows with 8 columns (standard 7 + `discount_pct`).
# MAGIC
# MAGIC **Target** (`schema_ex2_target`): 5 rows with 7 columns (standard orders schema).
# MAGIC
# MAGIC **Expected Output**: `schema_ex2_target` has 8 rows. No `discount_pct` column in target.
# MAGIC
# MAGIC **Requirements**:
# MAGIC 1. Insert source rows into target, selecting only the 7 matching columns
# MAGIC 2. The extra `discount_pct` column should not appear in the target

# COMMAND ----------

# EXERCISE_KEY: schema_ex2
# TODO: Insert source rows into target using only the matching columns

# Your code here


# COMMAND ----------

# Validate Exercise 2
result = spark.table(f"{CATALOG}.{SCHEMA}.schema_ex2_target")

assert result.count() == 8, f"Expected 8 rows, got {result.count()}"
assert "discount_pct" not in result.columns, \
    "Target should not have a discount_pct column: insert only the 7 target columns"
assert result.filter("order_id = 'ORD-101'").count() == 1, "ORD-101 should be inserted"

print("Exercise 2 passed!")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Exercise 3: Additive Schema Evolution with mergeSchema
# MAGIC **Difficulty**: Medium | **Time**: ~10 min
# MAGIC
# MAGIC The source has an extra column (`discount_pct`). Instead of dropping it, use schema
# MAGIC evolution to automatically add the new column to the target.
# MAGIC
# MAGIC **Source** (`schema_ex3_source`): 3 rows with 8 columns (standard 7 + `discount_pct`).
# MAGIC
# MAGIC **Target** (`schema_ex3_target`): 5 rows with 7 columns (standard orders schema).
# MAGIC
# MAGIC **Expected Output**: `schema_ex3_target` has 8 rows and a `discount_pct` column.
# MAGIC ORD-001 to ORD-005 have null `discount_pct`. ORD-101 has `discount_pct` = 5.0.
# MAGIC
# MAGIC **Requirements**:
# MAGIC 1. Write source data to target with schema evolution enabled
# MAGIC 2. The target schema should gain the `discount_pct` column
# MAGIC
# MAGIC **Approach**: The PySpark DataFrame writer has an option that tells Delta Lake to accept
# MAGIC new columns during append writes. Use `.write.mode("append")` with the right option.

# COMMAND ----------

# EXERCISE_KEY: schema_ex3
# TODO: Append source to target with schema evolution

# Your code here


# COMMAND ----------

# Validate Exercise 3
result = spark.table(f"{CATALOG}.{SCHEMA}.schema_ex3_target")

assert result.count() == 8, f"Expected 8 rows, got {result.count()}"
assert "discount_pct" in result.columns, \
    "Target should have discount_pct column after schema evolution"
assert result.filter("order_id = 'ORD-001'").select("discount_pct").collect()[0][0] is None, \
    "Existing rows should have null for discount_pct"
assert result.filter("order_id = 'ORD-101'").select("discount_pct").collect()[0][0] == 5.0, \
    "ORD-101 should have discount_pct = 5.0"

print("Exercise 3 passed!")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Exercise 4: Overwrite Schema for Breaking Change
# MAGIC **Difficulty**: Medium | **Time**: ~10 min
# MAGIC
# MAGIC Replace the target table's schema entirely. The source has a completely different
# MAGIC schema (product catalog instead of orders).
# MAGIC
# MAGIC **Source** (`schema_ex4_source`): 3 rows with schema: `product_id`, `product_name`, `category`, `price`.
# MAGIC
# MAGIC **Target** (`schema_ex4_target`): 5 rows with standard orders schema (7 columns).
# MAGIC
# MAGIC **Expected Output**: `schema_ex4_target` has 3 rows with the product schema.
# MAGIC Old orders data and schema are completely replaced.
# MAGIC
# MAGIC **Requirements**:
# MAGIC 1. Overwrite the target with source data AND replace its schema
# MAGIC 2. After the write, target should have `product_id`, `product_name`, `category`, `price`
# MAGIC
# MAGIC **Approach**: A normal overwrite preserves the target schema. The PySpark DataFrame writer
# MAGIC has an option that allows schema replacement during an overwrite.

# COMMAND ----------

# EXERCISE_KEY: schema_ex4
# TODO: Overwrite target with source data and replace the schema

# Your code here


# COMMAND ----------

# Validate Exercise 4
result = spark.table(f"{CATALOG}.{SCHEMA}.schema_ex4_target")

assert result.count() == 3, f"Expected 3 rows, got {result.count()}"
assert set(result.columns) == {"product_id", "product_name", "category", "price"}, \
    f"Schema should be product columns, got {result.columns}"
assert "order_id" not in result.columns, "Old orders schema should be gone"

print("Exercise 4 passed!")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Exercise 5: Handle Column Type Mismatch
# MAGIC **Difficulty**: Medium | **Time**: ~10 min
# MAGIC
# MAGIC The source has `amount` as STRING instead of DOUBLE. Databricks safely casts some types on write (an INT into a BIGINT column), but STRING into DOUBLE is not a safe cast, so the write fails.
# MAGIC Cast the column to match the target schema before writing.
# MAGIC
# MAGIC **Source** (`schema_ex5_source`): 3 rows. `amount` column is STRING type (e.g., '49.99').
# MAGIC
# MAGIC **Target** (`schema_ex5_target`): 5 rows. `amount` column is DOUBLE type.
# MAGIC
# MAGIC **Expected Output**: `schema_ex5_target` has 8 rows. All amounts are DOUBLE.
# MAGIC ORD-101 amount = 49.99 (as DOUBLE, not STRING).
# MAGIC
# MAGIC **Requirements**:
# MAGIC 1. Insert source rows into target with proper type casting
# MAGIC 2. The `amount` column must remain DOUBLE in the target
# MAGIC
# MAGIC **Approach**: Use CAST in your SELECT to convert the mismatched column type
# MAGIC before inserting into the target.

# COMMAND ----------

# EXERCISE_KEY: schema_ex5
# TODO: Insert source rows with type casting for amount

# Your code here


# COMMAND ----------

# Validate Exercise 5
result = spark.table(f"{CATALOG}.{SCHEMA}.schema_ex5_target")

assert result.count() == 8, f"Expected 8 rows, got {result.count()}"
val = result.filter("order_id = 'ORD-101'").select("amount").collect()[0][0]
assert val == 49.99, f"ORD-101 amount should be 49.99, got {val}"
assert isinstance(val, float), f"amount should be DOUBLE (float), got {type(val)}"

print("Exercise 5 passed!")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Exercise 6: NOT NULL Constraint Enforcement
# MAGIC **Difficulty**: Medium | **Time**: ~10 min
# MAGIC
# MAGIC Add a NOT NULL constraint on the `status` column so Delta Lake rejects any write
# MAGIC with a null status. Then insert a valid new order to verify writes still work.
# MAGIC
# MAGIC **Table** (`schema_ex6_orders`): 5 rows from base orders. All rows have non-null status.
# MAGIC
# MAGIC **Expected Output**: `schema_ex6_orders` has 6 rows. The `status` column is NOT NULL, so any later write with a null status fails.
# MAGIC
# MAGIC **Requirements**:
# MAGIC 1. Add a NOT NULL constraint on the `status` column
# MAGIC 2. Insert this row: `('ORD-101', 'CUST-010', 'PROD-005', 49.99, 'pending', DATE '2026-03-01', TIMESTAMP '2026-03-01 10:00:00')`
# MAGIC
# MAGIC **Approach**: ALTER TABLE lets you change column nullability. The constraint can only
# MAGIC be added if all existing values satisfy it.

# COMMAND ----------

# EXERCISE_KEY: schema_ex6
# TODO: Add NOT NULL constraint on status, then insert a valid row

# Your code here


# COMMAND ----------

# Validate Exercise 6
result = spark.table(f"{CATALOG}.{SCHEMA}.schema_ex6_orders")

assert result.count() == 6, f"Expected 6 rows, got {result.count()}"
assert result.filter("order_id = 'ORD-101'").count() == 1, "ORD-101 should be inserted"
# Verify NOT NULL constraint exists
schema_fields = spark.table(f"{CATALOG}.{SCHEMA}.schema_ex6_orders").schema
status_field = [f for f in schema_fields.fields if f.name == "status"][0]
assert not status_field.nullable, "status column should be NOT NULL"

print("Exercise 6 passed!")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Exercise 7: CHECK Constraint Enforcement
# MAGIC **Difficulty**: Medium | **Time**: ~10 min
# MAGIC
# MAGIC Add a CHECK constraint ensuring all order amounts are positive. Then insert a valid
# MAGIC new order to verify writes still work under the constraint.
# MAGIC
# MAGIC **Table** (`schema_ex7_orders`): 5 rows from base orders. All rows have amount > 0.
# MAGIC
# MAGIC **Expected Output**: `schema_ex7_orders` has 6 rows. A CHECK constraint named
# MAGIC `positive_amount` exists (any future write with amount <= 0 would fail).
# MAGIC
# MAGIC **Requirements**:
# MAGIC 1. Add a CHECK constraint named `positive_amount` that enforces `amount > 0`
# MAGIC 2. Insert this row: `('ORD-101', 'CUST-010', 'PROD-005', 49.99, 'pending', DATE '2026-03-01', TIMESTAMP '2026-03-01 10:00:00')`
# MAGIC
# MAGIC **Approach**: ALTER TABLE ADD CONSTRAINT creates a named rule. All existing data
# MAGIC must pass the check before the constraint can be added.

# COMMAND ----------

# EXERCISE_KEY: schema_ex7
# TODO: Add CHECK constraint, then insert a valid row

# Your code here


# COMMAND ----------

# Validate Exercise 7
result = spark.table(f"{CATALOG}.{SCHEMA}.schema_ex7_orders")

assert result.count() == 6, f"Expected 6 rows, got {result.count()}"
assert result.filter("order_id = 'ORD-101'").count() == 1, "ORD-101 should be inserted"
# Verify the positive_amount CHECK constraint exists with the right expression
props = spark.sql(f"SHOW TBLPROPERTIES {CATALOG}.{SCHEMA}.schema_ex7_orders")
check = props.filter("key = 'delta.constraints.positive_amount'").collect()
assert len(check) == 1, "CHECK constraint positive_amount not found"
assert check[0].value.replace(" ", "").strip("()") == "amount>0", \
    f"positive_amount has the wrong expression: {check[0].value}"

print("Exercise 7 passed!")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Exercise 8: Drop a Constraint
# MAGIC **Difficulty**: Medium | **Time**: ~10 min
# MAGIC
# MAGIC Remove one constraint from a table and keep the other.
# MAGIC
# MAGIC **Table** (`schema_ex8_orders`): 5 rows. Constraints: `status` NOT NULL + CHECK `positive_amount` (amount > 0).
# MAGIC
# MAGIC **Expected Output**: `SHOW TBLPROPERTIES` on `schema_ex8_orders` no longer lists `delta.constraints.positive_amount`.
# MAGIC The `status` column is still NOT NULL.
# MAGIC
# MAGIC **Requirements**:
# MAGIC 1. Drop the CHECK constraint named `positive_amount`
# MAGIC 2. Do NOT drop the NOT NULL constraint on `status`

# COMMAND ----------

# EXERCISE_KEY: schema_ex8
# TODO: Drop the positive_amount CHECK constraint

# Your code here


# COMMAND ----------

# Validate Exercise 8
# CHECK constraint should be gone
props = spark.sql(f"SHOW TBLPROPERTIES {CATALOG}.{SCHEMA}.schema_ex8_orders")
check_rows = props.filter("key LIKE 'delta.constraints.positive_amount%'").count()
assert check_rows == 0, "positive_amount CHECK constraint should be dropped"

# NOT NULL should still exist
schema_fields = spark.table(f"{CATALOG}.{SCHEMA}.schema_ex8_orders").schema
status_field = [f for f in schema_fields.fields if f.name == "status"][0]
assert not status_field.nullable, "status NOT NULL constraint should still exist"

print("Exercise 8 passed!")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Exercise 9: Query Table Properties for Constraints
# MAGIC **Difficulty**: Medium | **Time**: ~10 min
# MAGIC
# MAGIC List the CHECK constraints on a table from its table properties.
# MAGIC
# MAGIC **Table** (`schema_ex9_orders`): 5 rows. Has 2 CHECK constraints:
# MAGIC - `positive_amount`: amount > 0
# MAGIC - `valid_status`: status IN ('pending', 'completed', 'shipped', 'cancelled')
# MAGIC
# MAGIC **Expected Output**: Delta table `schema_ex9_constraints` with exactly 2 rows and columns `constraint_name` (the full key, for example `delta.constraints.positive_amount`) and `constraint_expression` (for example `amount > 0`).
# MAGIC
# MAGIC **Requirements**:
# MAGIC 1. Query table properties of `schema_ex9_orders` using `SHOW TBLPROPERTIES`
# MAGIC 2. Filter for properties starting with `delta.constraints.`
# MAGIC 3. Save the result as the Delta table `schema_ex9_constraints` with columns `constraint_name` and `constraint_expression`
# MAGIC
# MAGIC **Approach**: `SHOW TBLPROPERTIES` returns one row per property, with columns `key` and `value`. SQL cannot filter that output, so run it with `spark.sql(...)` and filter the DataFrame.

# COMMAND ----------

# EXERCISE_KEY: schema_ex9
# TODO: Query table properties for constraints and save to schema_ex9_constraints

# Your code here


# COMMAND ----------

# Validate Exercise 9
result = spark.table(f"{CATALOG}.{SCHEMA}.schema_ex9_constraints")

assert result.count() == 2, \
    f"schema_ex9_constraints should hold only the CHECK constraint properties, got {result.count()} rows"
assert "constraint_name" in result.columns, "Should have constraint_name column"
assert "constraint_expression" in result.columns, "Should have constraint_expression column"
constraint_names = set(row.constraint_name for row in result.collect())
assert "delta.constraints.positive_amount" in constraint_names, \
    "constraint_name should hold the full property key for positive_amount"
expressions = set(row.constraint_expression for row in result.collect())
assert any("amount > 0" in expr for expr in expressions), \
    "Should find 'amount > 0' in constraint expressions"

print("Exercise 9 passed!")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Exercise 10: Schema Evolution Through MERGE
# MAGIC **Difficulty**: Hard | **Time**: ~15 min
# MAGIC
# MAGIC MERGE source data with two extra columns (`discount_pct`, `shipping_cost`) into the target using automatic schema evolution.
# MAGIC
# MAGIC **Source** (`schema_ex10_source`): 3 rows with 9 columns (standard 7 + `discount_pct` + `shipping_cost`).
# MAGIC ORD-001 and ORD-002 exist in target (will be updated). ORD-101 is new (will be inserted).
# MAGIC
# MAGIC **Target** (`schema_ex10_target`): 5 rows with 7 columns (standard orders schema).
# MAGIC
# MAGIC **Expected Output**: `schema_ex10_target` has 6 rows. Both `discount_pct` and `shipping_cost` columns exist.
# MAGIC ORD-001 has `discount_pct` = 10.0. ORD-101 has `shipping_cost` = 3.99. ORD-003 has null for both new columns.
# MAGIC
# MAGIC **Requirements**:
# MAGIC 1. MERGE `schema_ex10_source` into `schema_ex10_target` on `order_id` (update matched, insert not matched)
# MAGIC 2. Make that same MERGE statement add `discount_pct` and `shipping_cost` to the target. Free Edition runs on serverless compute, where `spark.conf.set` cannot switch on schema auto-merge.
# MAGIC
# MAGIC **Approach**: By default, MERGE ignores source columns the target does not have: `UPDATE SET *` and `INSERT *` succeed and silently drop `discount_pct` and `shipping_cost`.
# MAGIC Databricks SQL has a form of MERGE that evolves the target schema for that one statement.

# COMMAND ----------

# EXERCISE_KEY: schema_ex10
# TODO: MERGE source into target with schema evolution

# Your code here


# COMMAND ----------

# Validate Exercise 10
result = spark.table(f"{CATALOG}.{SCHEMA}.schema_ex10_target")

assert result.count() == 6, f"Expected 6 rows, got {result.count()}"
assert "discount_pct" in result.columns, "Target should have discount_pct after schema evolution"
assert "shipping_cost" in result.columns, "Target should have shipping_cost after schema evolution"
assert result.filter("order_id = 'ORD-001'").select("discount_pct").collect()[0][0] == 10.0, \
    "ORD-001 should have discount_pct = 10.0"
assert result.filter("order_id = 'ORD-003'").select("discount_pct").collect()[0][0] is None, \
    "ORD-003 should have null discount_pct (not in source)"
assert result.filter("order_id = 'ORD-101'").select("shipping_cost").collect()[0][0] == 3.99, \
    "ORD-101 should have shipping_cost = 3.99"

print("Exercise 10 passed!")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Exercise 11: Constraint Violation Handling
# MAGIC **Difficulty**: Hard | **Time**: ~15 min
# MAGIC
# MAGIC Insert only the valid candidate rows below into `schema_ex11_orders`.
# MAGIC
# MAGIC Optional: insert all 3 first to see the error. Delta Lake rejects the whole batch, so nothing is written.
# MAGIC
# MAGIC **Table** (`schema_ex11_orders`): 5 rows from base orders. Constraints already applied:
# MAGIC - `status` is NOT NULL
# MAGIC - `amount > 0` (CHECK constraint `positive_amount`)
# MAGIC
# MAGIC **Candidate rows** (ORD-202 and ORD-203 each break a constraint). There is no source table: build these rows with `VALUES`, writing `order_date` as `DATE '2026-03-01'` and `updated_at` as `TIMESTAMP '2026-03-01 10:00:00'`:
# MAGIC | order_id | customer_id | product_id | amount | status | order_date | updated_at |
# MAGIC |----------|-------------|------------|--------|--------|------------|------------|
# MAGIC | ORD-201 | CUST-020 | PROD-005 | 49.99 | pending | 2026-03-01 | 2026-03-01 10:00:00 |
# MAGIC | ORD-202 | CUST-021 | PROD-003 | -10.00 | completed | 2026-03-01 | 2026-03-01 10:00:00 |
# MAGIC | ORD-203 | CUST-022 | PROD-001 | 75.00 | NULL | 2026-03-01 | 2026-03-01 10:00:00 |
# MAGIC
# MAGIC **Expected Output**: `schema_ex11_orders` has 6 rows. Only ORD-201 (the valid row) is inserted.
# MAGIC ORD-202 (negative amount) and ORD-203 (null status) are rejected.
# MAGIC
# MAGIC **Requirements**:
# MAGIC 1. Filter out rows that would violate constraints before inserting
# MAGIC 2. Only ORD-201 should be inserted
# MAGIC
# MAGIC **Approach**: Turn each constraint listed above into a filter condition and apply it to the candidate rows in the SELECT that feeds the `INSERT`.

# COMMAND ----------

# EXERCISE_KEY: schema_ex11
# TODO: Filter out constraint-violating rows and insert only valid ones

# Your code here


# COMMAND ----------

# Validate Exercise 11
result = spark.table(f"{CATALOG}.{SCHEMA}.schema_ex11_orders")

assert result.count() == 6, f"Expected 6 rows, got {result.count()}"
assert result.filter("order_id = 'ORD-201'").count() == 1, "ORD-201 (valid) should be inserted"
assert result.filter("order_id = 'ORD-202'").count() == 0, "ORD-202 (negative amount) should NOT be inserted"
assert result.filter("order_id = 'ORD-203'").count() == 0, "ORD-203 (null status) should NOT be inserted"

print("Exercise 11 passed!")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Exercise 12: MERGE with Constraint-Safe Filtering
# MAGIC **Difficulty**: Hard | **Time**: ~15 min
# MAGIC
# MAGIC MERGE source data into a constrained target. The source has a mix of valid and invalid rows.
# MAGIC If any row the MERGE writes breaks a constraint, the whole MERGE fails.
# MAGIC
# MAGIC **Source** (`schema_ex12_source`): 4 rows with the same 7 columns as the target (key columns shown):
# MAGIC | order_id | amount | status | Valid? |
# MAGIC |----------|--------|--------|--------|
# MAGIC | ORD-001 | 120.00 | shipped | Yes (update existing) |
# MAGIC | ORD-201 | 49.99 | pending | Yes (new insert) |
# MAGIC | ORD-202 | -10.00 | completed | No (violates amount > 0) |
# MAGIC | ORD-203 | 75.00 | NULL | No (violates NOT NULL status) |
# MAGIC
# MAGIC **Target** (`schema_ex12_target`): 5 rows. Constraints: `status` NOT NULL + CHECK `positive_amount`.
# MAGIC
# MAGIC **Expected Output**: `schema_ex12_target` has 6 rows. ORD-001 updated (amount = 120.00). ORD-201 inserted.
# MAGIC ORD-202 and ORD-203 excluded.
# MAGIC
# MAGIC **Requirements**:
# MAGIC 1. MERGE source into target (update matched, insert not matched)
# MAGIC 2. Let only the source rows that satisfy both target constraints reach the MERGE
# MAGIC
# MAGIC **Approach**: Write the `USING` clause as a subquery over `schema_ex12_source` that keeps only the rows both target constraints accept.

# COMMAND ----------

# EXERCISE_KEY: schema_ex12
# TODO: MERGE with pre-filtered source to respect constraints

# Your code here


# COMMAND ----------

# Validate Exercise 12
result = spark.table(f"{CATALOG}.{SCHEMA}.schema_ex12_target")

assert result.count() == 6, f"Expected 6 rows, got {result.count()}"
assert result.filter("order_id = 'ORD-001'").select("amount").collect()[0][0] == 120.00, \
    "ORD-001 should be updated to amount 120.00"
assert result.filter("order_id = 'ORD-201'").count() == 1, "ORD-201 (valid) should be inserted"
assert result.filter("order_id = 'ORD-202'").count() == 0, "ORD-202 (negative amount) should NOT be inserted"
assert result.filter("order_id = 'ORD-203'").count() == 0, "ORD-203 (null status) should NOT be inserted"

print("Exercise 12 passed!")
