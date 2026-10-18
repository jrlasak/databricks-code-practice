# Databricks notebook source
# COMMAND ----------
# MAGIC %md
# MAGIC # MERGE Operations
# MAGIC **Topic**: Delta Lake | **Exercises**: 9 | **Total Time**: ~95 min
# MAGIC
# MAGIC Practice Delta Lake MERGE INTO across multiple patterns: basic upserts, insert-only,
# MAGIC update-only, dedup, conditional updates, deletes, multi-condition, SCD Type 2,
# MAGIC and schema evolution.
# MAGIC
# MAGIC **Solutions**: If stuck, see `solutions/merge-operations-solutions.py` for hints and answers.
# MAGIC
# MAGIC **Tables used** (from `00_Setup.py`):
# MAGIC - `db_code.delta_lake.orders` - order records
# MAGIC - `db_code.delta_lake.customers` - customer dimension
# MAGIC
# MAGIC **Schema** (`orders`):
# MAGIC | Column | Type | Notes |
# MAGIC |--------|------|-------|
# MAGIC | order_id | STRING | Order key (repeats in the base table: ORD-042 and ORD-067 appear twice) |
# MAGIC | customer_id | STRING | FK to customers (some nulls) |
# MAGIC | product_id | STRING | FK to products |
# MAGIC | amount | DOUBLE | Order total in USD (includes $0) |
# MAGIC | status | STRING | completed, pending, shipped, cancelled |
# MAGIC | order_date | DATE | Order placement date |
# MAGIC | updated_at | TIMESTAMP | Last modification time |

# COMMAND ----------

# MAGIC %run ./00_Setup

# COMMAND ----------

# MAGIC %run ./setup/merge-operations-setup

# COMMAND ----------
# MAGIC %md
# MAGIC **Setup complete.** Every exercise table is in `db_code.merge_operations`. Write your output tables there too, by full name (for example `db_code.merge_operations.merge_ex1_target`). In Python cells, the variables `CATALOG` and `SCHEMA` hold `db_code` and `merge_operations`. The base tables (`orders`, `customers`) are in `db_code.delta_lake`.
# MAGIC
# MAGIC Keep the `# EXERCISE_KEY: ...` line as the first line of each answer cell.
# MAGIC
# MAGIC Each exercise has its own pair of tables. Every orders target holds ORD-001 to ORD-005 copied from `db_code.delta_lake.orders`.
# MAGIC - Exercise 1: `merge_ex1_target` (5 orders), `merge_ex1_source` (2 changed orders, 2 new)
# MAGIC - Exercise 2: `merge_ex2_target` (5 orders), `merge_ex2_source` (2 changed orders, 2 new)
# MAGIC - Exercise 3: `merge_ex3_target` (5 orders), `merge_ex3_source` (3 changed orders)
# MAGIC - Exercise 4: `merge_ex4_target` (5 orders), `merge_ex4_source` (3 rows, ORD-001 twice)
# MAGIC - Exercise 5: `merge_ex5_target` (5 orders), `merge_ex5_source` (1 newer order, 1 older order, 1 new)
# MAGIC - Exercise 6: `merge_ex6_target` (5 orders), `merge_ex6_source` (1 cancelled order, 1 changed order, 1 new)
# MAGIC - Exercise 7: `merge_ex7_target` (5 orders), `merge_ex7_source` (1 cancelled, 1 newer, 1 older, 2 new)
# MAGIC - Exercise 8: `merge_ex8_target` (3 customers with SCD Type 2 columns), `merge_ex8_source` (3 customers)
# MAGIC - Exercise 9: `merge_ex9_target` (5 orders), `merge_ex9_source` (2 orders with an extra `discount_pct` column)

# COMMAND ----------
# MAGIC %md
# MAGIC ## Exercise 1: Basic Upsert
# MAGIC **Difficulty**: Easy | **Time**: ~5 min
# MAGIC
# MAGIC MERGE `merge_ex1_source` into `merge_ex1_target` to upsert orders.
# MAGIC Update existing orders with new values and insert orders that don't exist yet.
# MAGIC
# MAGIC **Source** (`merge_ex1_source`): 4 rows - ORD-001 and ORD-002 (status='shipped', updated_at 2026-03-01 10:00; amounts unchanged at 109.50 and 175.00), ORD-101 (new), ORD-102 (new)
# MAGIC
# MAGIC **Target** (`merge_ex1_target`): 5 rows - ORD-001 through ORD-005 copied from `db_code.delta_lake.orders` (ORD-001 and ORD-002 have status='completed')
# MAGIC
# MAGIC **Expected Output**: `merge_ex1_target` has 7 rows. ORD-001 and ORD-002 have status='shipped'. ORD-101 and ORD-102 exist.
# MAGIC
# MAGIC **Requirements**:
# MAGIC 1. MERGE source into target matching on `order_id`
# MAGIC 2. When matched: update all columns
# MAGIC 3. When not matched: insert all columns

# COMMAND ----------

# EXERCISE_KEY: merge_ex1
# TODO: Write your MERGE INTO statement

# Your code here


# COMMAND ----------

# Validate Exercise 1
result = spark.table(f"{CATALOG}.{SCHEMA}.merge_ex1_target")

assert result.count() == 7, f"Expected 7 rows, got {result.count()}"
assert result.filter("order_id = 'ORD-101'").count() == 1, "ORD-101 should be inserted"
assert result.filter("order_id = 'ORD-102'").count() == 1, "ORD-102 should be inserted"
assert result.filter("order_id = 'ORD-001'").select("status").collect()[0][0] == "shipped", \
    "ORD-001 was not updated from the source"

print("Exercise 1 passed!")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Exercise 2: Insert-Only Merge
# MAGIC **Difficulty**: Easy | **Time**: ~5 min
# MAGIC
# MAGIC MERGE `merge_ex2_source` into `merge_ex2_target`, but only insert new records.
# MAGIC Existing orders should NOT be updated, even if the source has different values.
# MAGIC
# MAGIC **Source** (`merge_ex2_source`): 4 rows - ORD-001 and ORD-002 (status='shipped'), ORD-101 (new), ORD-102 (new)
# MAGIC
# MAGIC **Target** (`merge_ex2_target`): 5 rows - ORD-001 through ORD-005
# MAGIC
# MAGIC **Expected Output**: 7 rows. ORD-001 keeps status='completed'. ORD-101 and ORD-102 inserted.
# MAGIC
# MAGIC **Requirements**:
# MAGIC 1. MERGE source into target matching on `order_id`
# MAGIC 2. Only insert rows that don't exist in target
# MAGIC 3. Do NOT update any existing records

# COMMAND ----------

# EXERCISE_KEY: merge_ex2
# TODO: Write your MERGE INTO statement

# Your code here


# COMMAND ----------

# Validate Exercise 2
result = spark.table(f"{CATALOG}.{SCHEMA}.merge_ex2_target")

assert result.count() == 7, f"Expected 7 rows, got {result.count()}"
assert result.filter("order_id = 'ORD-101'").count() == 1, "ORD-101 should be inserted"
# ORD-001 should NOT be updated (insert-only merge)
ord001_status = result.filter("order_id = 'ORD-001'").select("status").collect()[0][0]
assert ord001_status != "shipped", \
    f"ORD-001 should not be updated in insert-only merge, but status changed to '{ord001_status}'"

print("Exercise 2 passed!")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Exercise 3: Update-Only Merge
# MAGIC **Difficulty**: Easy | **Time**: ~5 min
# MAGIC
# MAGIC MERGE `merge_ex3_source` into `merge_ex3_target`, but only update existing records.
# MAGIC New records in the source should be ignored entirely.
# MAGIC
# MAGIC **Source** (`merge_ex3_source`): 3 rows - ORD-001, ORD-002, ORD-003 with status='shipped' and amount+10
# MAGIC
# MAGIC **Target** (`merge_ex3_target`): 5 rows - ORD-001 through ORD-005
# MAGIC
# MAGIC **Expected Output**: still 5 rows. ORD-001, ORD-002, ORD-003 have status='shipped'. ORD-004, ORD-005 unchanged.
# MAGIC
# MAGIC **Requirements**:
# MAGIC 1. MERGE source into target matching on `order_id`
# MAGIC 2. Only update rows that exist in both
# MAGIC 3. Do NOT insert any new records

# COMMAND ----------

# EXERCISE_KEY: merge_ex3
# TODO: Write your MERGE INTO statement

# Your code here


# COMMAND ----------

# Validate Exercise 3
result = spark.table(f"{CATALOG}.{SCHEMA}.merge_ex3_target")

assert result.count() == 5, f"Expected 5 rows (no inserts), got {result.count()}"
assert result.filter("status = 'shipped'").count() == 3, \
    "ORD-001, ORD-002, ORD-003 should all have status 'shipped'"
# ORD-004 and ORD-005 should be unchanged (not in source)
assert result.filter("order_id = 'ORD-004'").select("status").collect()[0][0] != "shipped", \
    "ORD-004 should not be modified (not in source)"

print("Exercise 3 passed!")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Exercise 4: Deduplicate Before Merge
# MAGIC **Difficulty**: Medium | **Time**: ~10 min
# MAGIC
# MAGIC `merge_ex4_source` has duplicate `order_id` values. Deduplicate keeping the most
# MAGIC recent record per `order_id`, then MERGE into target.
# MAGIC
# MAGIC **Source** (`merge_ex4_source`): 3 rows - ORD-001 appears TWICE (99.50 at 08:00, 119.50 at 12:00), ORD-101 once
# MAGIC
# MAGIC **Target** (`merge_ex4_target`): 5 rows - ORD-001 through ORD-005
# MAGIC
# MAGIC **Expected Output**: 6 rows. ORD-001 amount=119.50 (the later record). ORD-101 inserted.
# MAGIC
# MAGIC **Requirements**:
# MAGIC 1. Deduplicate source keeping latest `updated_at` per `order_id`
# MAGIC 2. MERGE deduped result into target
# MAGIC 3. Update matched, insert not matched
# MAGIC
# MAGIC **Constraints**:
# MAGIC - Direct MERGE without dedup will fail (duplicate keys in source)

# COMMAND ----------

# EXERCISE_KEY: merge_ex4
# TODO: Dedup the source, then MERGE

# Your code here


# COMMAND ----------

# Validate Exercise 4
result = spark.table(f"{CATALOG}.{SCHEMA}.merge_ex4_target")

assert result.count() == 6, f"Expected 6 rows, got {result.count()}"
assert result.filter("order_id = 'ORD-001'").select("amount").collect()[0][0] == 119.50, \
    "ORD-001 should have amount 119.50 (the later duplicate)"
assert result.filter("order_id = 'ORD-101'").count() == 1, "ORD-101 should be inserted"

print("Exercise 4 passed!")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Exercise 5: Conditional Merge - Only Update If Newer
# MAGIC **Difficulty**: Medium | **Time**: ~10 min
# MAGIC
# MAGIC MERGE `merge_ex5_source` into `merge_ex5_target`, but only update when the
# MAGIC source record is newer. Stale source records should be ignored. New records always inserted.
# MAGIC
# MAGIC **Source** (`merge_ex5_source`): 3 rows - ORD-001 (`updated_at` 2026-03-01, newer than the target, status='shipped'), ORD-002 (`updated_at` 2025-01-01, older than the target, status='returned'), ORD-101 (new)
# MAGIC
# MAGIC **Target** (`merge_ex5_target`): 5 rows - ORD-001 through ORD-005
# MAGIC
# MAGIC **Expected Output**: 6 rows. ORD-001 updated (status='shipped'). ORD-002 unchanged (status='completed'). ORD-101 inserted.
# MAGIC
# MAGIC **Requirements**:
# MAGIC 1. MERGE source into target on `order_id`
# MAGIC 2. Only update matched rows where source `updated_at` is newer than target
# MAGIC 3. Insert rows not in target
# MAGIC 4. Matched rows with older source: do nothing

# COMMAND ----------

# EXERCISE_KEY: merge_ex5
# TODO: Write your conditional MERGE

# Your code here


# COMMAND ----------

# Validate Exercise 5
result = spark.table(f"{CATALOG}.{SCHEMA}.merge_ex5_target")

assert result.count() == 6, f"Expected 6 rows, got {result.count()}"
# ORD-001 should be updated (source is newer)
assert result.filter("order_id = 'ORD-001'").select("status").collect()[0][0] == "shipped", \
    "ORD-001 was not updated (its source row is newer)"
# ORD-002 should NOT be updated (source is older)
ord002_status = result.filter("order_id = 'ORD-002'").select("status").collect()[0][0]
assert ord002_status != "returned", \
    f"ORD-002 should not be updated (source is older), but status changed to '{ord002_status}'"
# ORD-101 should be inserted
assert result.filter("order_id = 'ORD-101'").count() == 1, "ORD-101 should be inserted"

print("Exercise 5 passed!")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Exercise 6: MERGE with DELETE Clause
# MAGIC **Difficulty**: Medium | **Time**: ~10 min
# MAGIC
# MAGIC MERGE `merge_ex6_source` into `merge_ex6_target`. Handle three cases:
# MAGIC delete the target row when the source row has `status = 'cancelled'`, update the other matched orders, insert new orders.
# MAGIC
# MAGIC **Source** (`merge_ex6_source`): 3 rows - ORD-001 (status='cancelled'), ORD-002 (status='shipped'), ORD-101 (new)
# MAGIC
# MAGIC **Target** (`merge_ex6_target`): 5 rows - ORD-001 through ORD-005
# MAGIC
# MAGIC **Expected Output**: 5 rows. ORD-001 deleted. ORD-002 status='shipped'. ORD-101 inserted.
# MAGIC
# MAGIC **Requirements**:
# MAGIC 1. MERGE source into target on `order_id`
# MAGIC 2. Source `status = 'cancelled'`: delete the target row
# MAGIC 3. Update other matched orders
# MAGIC 4. Insert new orders

# COMMAND ----------

# EXERCISE_KEY: merge_ex6
# TODO: Write your MERGE with a DELETE clause

# Your code here


# COMMAND ----------

# Validate Exercise 6
result = spark.table(f"{CATALOG}.{SCHEMA}.merge_ex6_target")

assert result.count() == 5, f"Expected 5 rows (5 - 1 deleted + 1 inserted), got {result.count()}"
assert result.filter("order_id = 'ORD-001'").count() == 0, "ORD-001 should be deleted (cancelled)"
assert result.filter("order_id = 'ORD-002'").select("status").collect()[0][0] == "shipped", \
    "ORD-002 should be updated to 'shipped'"
assert result.filter("order_id = 'ORD-101'").count() == 1, "ORD-101 should be inserted"

print("Exercise 6 passed!")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Exercise 7: Multi-Condition MERGE
# MAGIC **Difficulty**: Hard | **Time**: ~15 min
# MAGIC
# MAGIC MERGE `merge_ex7_source` into `merge_ex7_target` handling all four scenarios
# MAGIC in a single statement: delete, conditional update, skip, and insert.
# MAGIC
# MAGIC **Source** (`merge_ex7_source`): 5 rows - ORD-001 (status='cancelled'), ORD-002 (newer, status='shipped'), ORD-003 (older, amount=50.00), ORD-101 (new), ORD-102 (new)
# MAGIC
# MAGIC **Target** (`merge_ex7_target`): 5 rows - ORD-001 through ORD-005
# MAGIC
# MAGIC **Expected Output**: 6 rows. ORD-001 deleted. ORD-002 updated (status='shipped'). ORD-003 unchanged (amount=45.00). ORD-101, ORD-102 inserted.
# MAGIC
# MAGIC **Requirements**:
# MAGIC 1. MERGE source into target on `order_id`
# MAGIC 2. Source `status = 'cancelled'`: delete the target row, whatever the two `updated_at` values are
# MAGIC 3. Otherwise, source `updated_at` later than the target `updated_at`: update every column of the target row from the source
# MAGIC 4. Otherwise (source `updated_at` the same or earlier): leave the target row as it is
# MAGIC 5. New orders: insert

# COMMAND ----------

# EXERCISE_KEY: merge_ex7
# TODO: Write your multi-condition MERGE

# Your code here


# COMMAND ----------

# Validate Exercise 7
result = spark.table(f"{CATALOG}.{SCHEMA}.merge_ex7_target")

assert result.count() == 6, f"Expected 6 rows, got {result.count()}"
assert result.filter("order_id = 'ORD-001'").count() == 0, "ORD-001 should be deleted (cancelled)"
assert result.filter("order_id = 'ORD-002'").select("status").collect()[0][0] == "shipped", \
    "ORD-002 was not updated (its source row is newer)"
# ORD-003 should not be updated (source timestamp is older)
# Source has amount=50.00 and status='pending' for ORD-003 - verify these values were NOT applied
ord003 = result.filter("order_id = 'ORD-003'").collect()[0]
assert not (ord003.amount == 50.00 and ord003.status == "pending"), \
    "ORD-003 should not be updated (source timestamp is older than target)"
assert result.filter("order_id = 'ORD-101'").count() == 1, "ORD-101 should be inserted"
assert result.filter("order_id = 'ORD-102'").count() == 1, "ORD-102 should be inserted"

print("Exercise 7 passed!")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Exercise 8: SCD Type 2 with MERGE
# MAGIC **Difficulty**: Hard | **Time**: ~20 min
# MAGIC
# MAGIC Implement Slowly Changing Dimension Type 2 on a customer dimension table.
# MAGIC When customer attributes change, expire the old record and insert a new current version.
# MAGIC
# MAGIC **Source** (`merge_ex8_source`): 3 rows
# MAGIC
# MAGIC | customer_id | name | email | region | tier |
# MAGIC |-------------|------|-------|--------|------|
# MAGIC | CUST-001 | Alice Smith | alice.new@example.com | US-East | platinum |
# MAGIC | CUST-003 | Carol Lee | carol@example.com | US-West | silver |
# MAGIC | CUST-010 | New Customer | new@example.com | EU-West | bronze |
# MAGIC
# MAGIC **Target** (`merge_ex8_target`): 3 current rows (effective_start_date = signup date, effective_end_date = 9999-12-31): CUST-001 Alice Smith, alice@example.com, US-East, gold; CUST-002 Bob Johnson, bob@example.com, US-West, silver; CUST-003 Carol Lee, carol@example.com, US-West, gold
# MAGIC
# MAGIC | Column | Type | Notes |
# MAGIC |--------|------|-------|
# MAGIC | customer_id | STRING | Business key |
# MAGIC | name, email, region, tier | STRING | Attributes (may change) |
# MAGIC | is_current | BOOLEAN | true for active version |
# MAGIC | effective_start_date | DATE | When this version became active |
# MAGIC | effective_end_date | DATE | 9999-12-31 for current records |
# MAGIC
# MAGIC **Expected Output**: 6 rows.
# MAGIC CUST-001: 2 rows (old expired + new current with tier='platinum').
# MAGIC CUST-002: 1 row (unchanged). CUST-003: 2 rows (old expired + new current).
# MAGIC CUST-010: 1 row (new).
# MAGIC
# MAGIC **Requirements**:
# MAGIC 1. Expire old records ONLY for customers whose attributes actually changed (is_current=false, effective_end_date=today)
# MAGIC 2. Insert a new current version for changed customers (is_current=true, effective_start_date=today, effective_end_date=9999-12-31)
# MAGIC 3. Insert brand new customers (is_current=true, effective_start_date=today, effective_end_date=9999-12-31)
# MAGIC 4. Leave customers not in source unchanged
# MAGIC 5. Make it idempotent: run your answer cell a second time (without rerunning setup), then rerun the validate cell. It must still find 6 rows: 4 current and 2 expired.
# MAGIC
# MAGIC **Constraints**:
# MAGIC - The attributes are `name`, `email`, `region` and `tier`. A change from NULL to a value, or from a value to NULL, counts as a change.
# MAGIC - A source row whose attributes all match its current target row writes nothing.

# COMMAND ----------

# EXERCISE_KEY: merge_ex8
# TODO: Write your SCD Type 2 MERGE

# Your code here


# COMMAND ----------

# Validate Exercise 8
result = spark.table(f"{CATALOG}.{SCHEMA}.merge_ex8_target")

assert result.count() == 6, f"Expected 6 rows, got {result.count()}"
assert result.filter("is_current = true").count() == 4, \
    "Should have 4 current records (CUST-001 new, CUST-002, CUST-003 new, CUST-010)"
assert result.filter("is_current = false").count() == 2, \
    "Should have 2 expired records (CUST-001 old, CUST-003 old)"
assert result.filter("customer_id = 'CUST-001' AND is_current = true") \
    .select("tier").collect()[0][0] == "platinum", \
    "CUST-001 current version should have tier='platinum'"
assert result.filter("customer_id = 'CUST-003' AND is_current = true") \
    .select("tier").collect()[0][0] == "silver", \
    "CUST-003 current version does not carry the tier from the source"
assert result.filter("is_current = false AND effective_end_date = current_date()").count() == 2, \
    "The 2 expired records should have effective_end_date = today"
assert result.filter("customer_id = 'CUST-010'").count() == 1, \
    "CUST-010 should be inserted as new customer"
assert result.filter("customer_id = 'CUST-002' AND is_current = true").count() == 1, \
    "CUST-002 should be unchanged"

print("Exercise 8 passed!")
print("Idempotency: run your answer cell once more, then this cell. The same assertions must pass.")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Exercise 9: MERGE with Schema Evolution
# MAGIC **Difficulty**: Hard | **Time**: ~15 min
# MAGIC
# MAGIC MERGE `merge_ex9_source` into `merge_ex9_target`. The source has an extra column
# MAGIC (`discount_pct`) that doesn't exist in the target. Make the target schema evolve automatically.
# MAGIC
# MAGIC **Source** (`merge_ex9_source`): 2 rows - same schema as orders + `discount_pct DOUBLE`
# MAGIC - ORD-001 (existing, discount_pct=10.0), ORD-101 (new, discount_pct=5.0)
# MAGIC
# MAGIC **Target** (`merge_ex9_target`): 5 rows - standard orders schema (no discount_pct column)
# MAGIC
# MAGIC **Expected Output**: 6 rows. Column `discount_pct` exists.
# MAGIC ORD-001 discount_pct=10.0. ORD-101 discount_pct=5.0. Other rows discount_pct=null.
# MAGIC
# MAGIC **Requirements**:
# MAGIC 1. Enable schema evolution in the MERGE statement itself (serverless compute, including Free Edition, does not let you set `spark.databricks.delta.schema.autoMerge.enabled`)
# MAGIC 2. MERGE source into target on `order_id`
# MAGIC 3. Update matched, insert not matched
# MAGIC 4. Target schema should gain the `discount_pct` column

# COMMAND ----------

# EXERCISE_KEY: merge_ex9
# TODO: Write your MERGE with schema evolution

# Your code here


# COMMAND ----------

# Validate Exercise 9
result = spark.table(f"{CATALOG}.{SCHEMA}.merge_ex9_target")

assert result.count() == 6, f"Expected 6 rows, got {result.count()}"
assert "discount_pct" in result.columns, \
    "Target should have 'discount_pct' column after schema evolution"
assert result.filter("order_id = 'ORD-001'").select("discount_pct").collect()[0][0] == 10.0, \
    "ORD-001 should have discount_pct = 10.0"
assert result.filter("order_id = 'ORD-101'").select("discount_pct").collect()[0][0] == 5.0, \
    "ORD-101 should have discount_pct = 5.0"
assert result.filter("order_id = 'ORD-002'").select("discount_pct").collect()[0][0] is None, \
    "ORD-002 should have null discount_pct (not in source)"

print("Exercise 9 passed!")
