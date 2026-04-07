# Databricks notebook source
# COMMAND ----------
# MAGIC %md
# MAGIC # Time Travel & Restore
# MAGIC **Topic**: Delta Lake | **Exercises**: 7 | **Checkpoints**: 1 | **Total Time**: ~90 min
# MAGIC
# MAGIC Practice Delta Lake versioning: query historical versions by number and timestamp,
# MAGIC inspect table history, restore tables, diff changes, audit operations, and use
# MAGIC time travel for selective undo and reproducible reporting.
# MAGIC
# MAGIC **Solutions**: If stuck, see `solutions/time-travel-solutions.py` for hints and answers.
# MAGIC
# MAGIC **Tables used** (from `00_Setup.py`):
# MAGIC - `db_code.delta_lake.orders` - order records
# MAGIC
# MAGIC **Schema** (`orders`):
# MAGIC | Column | Type | Notes |
# MAGIC |--------|------|-------|
# MAGIC | order_id | STRING | Primary key |
# MAGIC | customer_id | STRING | FK to customers (some nulls) |
# MAGIC | product_id | STRING | FK to products |
# MAGIC | amount | DOUBLE | Order total in USD (includes $0) |
# MAGIC | status | STRING | completed, pending, shipped, cancelled |
# MAGIC | order_date | DATE | Order placement date |
# MAGIC | updated_at | TIMESTAMP | Last modification time |

# COMMAND ----------

# MAGIC %run ./00_Setup

# COMMAND ----------

# MAGIC %run ./setup/time-travel-setup

# COMMAND ----------
# MAGIC %md
# MAGIC **Setup complete.** Exercise tables are in `{CATALOG}.{SCHEMA}` (time_travel schema).
# MAGIC Base tables (orders, customers) are in `{CATALOG}.{BASE_SCHEMA}` (delta_lake schema).
# MAGIC Each exercise has its own table with pre-built version history:
# MAGIC - Ex 1 (easy): `tt_ex1_orders` - operations: CREATE, UPDATE 2 rows, DELETE 1 row
# MAGIC - Ex 2 (easy): `tt_ex2_orders` - operations: CREATE, UPDATE ORD-003, DELETE ORD-003
# MAGIC - Ex 3 (easy): `tt_ex3_orders` - operations: CREATE, UPDATE, DELETE, INSERT
# MAGIC - Ex 4 (medium): `tt_ex4_orders` - operations: CREATE (good data), bad UPDATE (all amounts=0)
# MAGIC - Ex 5 (medium): `tt_ex5_orders` - operations: CREATE, UPDATE ORD-001, DELETE ORD-005, INSERT ORD-101
# MAGIC - Ex 6 (medium): `tt_ex6_orders` - operations: CREATE, normal UPDATE, **suspicious INSERT**, normal UPDATE
# MAGIC - Ex 7 (hard): `tt_ex7_orders` - operations: CREATE (good data), bad UPDATE (cancelled low-amount orders)
# MAGIC - Ex 8 (hard): `tt_ex8_orders` - operations: CREATE, INSERT 3 orders, **bad UPDATE** (amounts x100)
# MAGIC
# MAGIC **Note**: On serverless, auto-OPTIMIZE may insert extra versions between your DML operations.
# MAGIC The setup records actual version numbers as variables (e.g., `TT_EX2_UPDATE_V`) so
# MAGIC assertions work regardless of auto-OPTIMIZE behavior. Version 0 (CTAS) is always safe.

# COMMAND ----------
# MAGIC %md
# MAGIC ## Exercise 1: Query by Version Number
# MAGIC **Difficulty**: Easy | **Time**: ~5 min
# MAGIC
# MAGIC Query a historical version of a Delta table using `VERSION AS OF`.
# MAGIC
# MAGIC **Table** (`tt_ex1_orders`): currently 4 rows. Operations performed after creation:
# MAGIC - v0 (CREATE): 5 rows (ORD-001 to ORD-005 from base)
# MAGIC - UPDATE: ORD-001 and ORD-002 updated (status='shipped', amount+10)
# MAGIC - DELETE: ORD-005 deleted
# MAGIC
# MAGIC **Expected Output**: Write version 0 data to `tt_ex1_result`. 5 rows. ORD-005 should exist.
# MAGIC
# MAGIC **Requirements**:
# MAGIC 1. Query `tt_ex1_orders` at version 0 (the initial CREATE is always version 0)
# MAGIC 2. Save the result as table `tt_ex1_result`

# COMMAND ----------

# EXERCISE_KEY: tt_ex1
# TODO: Query the table at version 0 and save to tt_ex1_result

# Your code here


# COMMAND ----------

# Validate Exercise 1
result = spark.table(f"{CATALOG}.{SCHEMA}.tt_ex1_result")

assert result.count() == 5, f"Expected 5 rows at version 0, got {result.count()}"
assert result.filter("order_id = 'ORD-005'").count() == 1, \
    "ORD-005 should exist at version 0 (deleted in a later version)"

print("Exercise 1 passed!")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Exercise 2: Query by Timestamp
# MAGIC **Difficulty**: Easy | **Time**: ~10 min
# MAGIC
# MAGIC Query a historical state using `TIMESTAMP AS OF` instead of a version number.
# MAGIC You'll need to look up the timestamp first using `DESCRIBE HISTORY`.
# MAGIC
# MAGIC **Table** (`tt_ex2_orders`): currently 4 rows. Operations performed:
# MAGIC - CREATE: 5 rows (ORD-001 to ORD-005 from base)
# MAGIC - UPDATE: ORD-003 updated (status='shipped', amount=200.00)
# MAGIC - DELETE: ORD-003 deleted
# MAGIC
# MAGIC **Expected Output**: Write the state as of the UPDATE operation to `tt_ex2_result`.
# MAGIC 5 rows. ORD-003 exists with amount=200.00.
# MAGIC
# MAGIC **Requirements**:
# MAGIC 1. Use `DESCRIBE HISTORY` to find the UPDATE operation and its timestamp
# MAGIC 2. Query `tt_ex2_orders` using `TIMESTAMP AS OF` with that timestamp
# MAGIC 3. Save the result as table `tt_ex2_result`

# COMMAND ----------

# EXERCISE_KEY: tt_ex2
# TODO: Look up the UPDATE operation's timestamp via DESCRIBE HISTORY, then query with TIMESTAMP AS OF and save to tt_ex2_result

# Your code here


# COMMAND ----------

# Validate Exercise 2
result = spark.table(f"{CATALOG}.{SCHEMA}.tt_ex2_result")

assert result.count() == 5, f"Expected 5 rows at the UPDATE version, got {result.count()}"
assert result.filter("order_id = 'ORD-003'").count() == 1, \
    "ORD-003 should exist at the UPDATE version (deleted later)"
assert result.filter("order_id = 'ORD-003'").select("amount").collect()[0][0] == 200.00, \
    "ORD-003 amount should be 200.00 at the UPDATE version"

print("Exercise 2 passed!")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Checkpoint 3: DESCRIBE HISTORY
# MAGIC **Time**: ~5 min
# MAGIC
# MAGIC Inspect the operation history of a Delta table using `DESCRIBE HISTORY`.
# MAGIC This command returns one row per version, showing what operation was performed.
# MAGIC
# MAGIC **Table** (`tt_ex3_orders`): has multiple versions. DML operations performed:
# MAGIC - CREATE TABLE AS SELECT (5 rows)
# MAGIC - UPDATE on ORD-001
# MAGIC - DELETE of ORD-005
# MAGIC - INSERT of ORD-101
# MAGIC
# MAGIC **Note**: On serverless, auto-OPTIMIZE may add extra versions between DML operations,
# MAGIC so total version count may be higher than 4.
# MAGIC
# MAGIC **Requirements**:
# MAGIC 1. Run `DESCRIBE HISTORY` on `tt_ex3_orders`
# MAGIC 2. Read the `operation` column for each version
# MAGIC 3. Fill in the placeholder values below

# COMMAND ----------

# EXERCISE_KEY: tt_ex3
# TODO: Run DESCRIBE HISTORY on tt_ex3_orders, then fill in what you observe

total_versions = 0        # Replace: how many versions (rows) does the history show?
last_dml_operation = ""   # Replace: what is the operation type of the LATEST version? (e.g., "UPDATE", "DELETE", "WRITE", "OPTIMIZE")
num_dml_operations = 0    # Replace: how many DML operations (non-OPTIMIZE) are in the history?

spark.sql(f"""
    CREATE OR REPLACE TABLE {CATALOG}.{SCHEMA}.tt_ex3_history AS
    SELECT CAST({total_versions} AS INT) AS total_versions,
           '{last_dml_operation}' AS last_dml_operation,
           CAST({num_dml_operations} AS INT) AS num_dml_operations
""")

# COMMAND ----------

# Validate Exercise 3
row = spark.table(f"{CATALOG}.{SCHEMA}.tt_ex3_history").collect()[0]

assert row.total_versions == TT_EX3_TOTAL_VERSIONS, \
    f"Expected {TT_EX3_TOTAL_VERSIONS} total versions, got {row.total_versions}"
assert row.last_dml_operation == TT_EX3_LAST_OPERATION, \
    f"Latest operation should be '{TT_EX3_LAST_OPERATION}', got '{row.last_dml_operation}'"
assert row.num_dml_operations == TT_EX3_DML_COUNT, \
    f"Expected {TT_EX3_DML_COUNT} DML operations (non-OPTIMIZE), got {row.num_dml_operations}"

print("Exercise 3 passed!")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Exercise 4: RESTORE to Previous Version
# MAGIC **Difficulty**: Medium | **Time**: ~10 min
# MAGIC
# MAGIC A bad UPDATE set all amounts to 0.00. Restore the table to its original state.
# MAGIC
# MAGIC **Table** (`tt_ex4_orders`): currently 5 rows, ALL amounts = 0.00 (a bad UPDATE corrupted them).
# MAGIC Version 0 has the original good data with positive amounts.
# MAGIC
# MAGIC **Expected Output**: After RESTORE, `tt_ex4_orders` has 5 rows with original amounts (all > 0).
# MAGIC RESTORE modifies the table in place (no separate output table needed).
# MAGIC
# MAGIC **Requirements**:
# MAGIC 1. RESTORE the table to version 0
# MAGIC
# MAGIC **Constraints**:
# MAGIC - RESTORE creates a new version (history is preserved, not deleted)

# COMMAND ----------

# EXERCISE_KEY: tt_ex4
# TODO: Restore the table to version 0

# Your code here


# COMMAND ----------

# Validate Exercise 4
result = spark.table(f"{CATALOG}.{SCHEMA}.tt_ex4_orders")

assert result.count() == 5, f"Expected 5 rows, got {result.count()}"
assert result.filter("amount = 0.00").count() == 0, \
    "No amounts should be 0.00 after restoring to version 0"
assert result.filter("amount > 0").count() == 5, \
    "All 5 rows should have positive amounts after restore"

print("Exercise 4 passed!")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Exercise 5: Diff Two Versions
# MAGIC **Difficulty**: Medium | **Time**: ~15 min
# MAGIC
# MAGIC Find all order_ids that were added, removed, or modified between version 0 and
# MAGIC the current version.
# MAGIC
# MAGIC **Table** (`tt_ex5_orders`): currently 5 rows. Operations performed:
# MAGIC - v0 (CREATE): 5 rows (ORD-001 to ORD-005)
# MAGIC - UPDATE: ORD-001 updated (amount+50, status='shipped')
# MAGIC - DELETE: ORD-005 deleted
# MAGIC - INSERT: ORD-101 inserted
# MAGIC
# MAGIC **Expected Output**: Write distinct changed order_ids to `tt_ex5_changes` with column `order_id`.
# MAGIC 3 rows: ORD-001 (modified), ORD-005 (deleted), ORD-101 (added).
# MAGIC
# MAGIC **Requirements**:
# MAGIC 1. Compare version 0 with the current version
# MAGIC 2. Find order_ids that were added, removed, or modified (any column value differs)
# MAGIC 3. Save distinct order_ids to `tt_ex5_changes`
# MAGIC
# MAGIC **Approach**: Think of it as finding rows that exist in one version but not the other.
# MAGIC If you compare full rows (not just order_ids), a modified row will appear as "missing"
# MAGIC from both sides since its values changed. Check both directions to catch all three cases.
# MAGIC
# MAGIC **Constraints**:
# MAGIC - ORD-002, ORD-003, ORD-004 are unchanged and should NOT appear

# COMMAND ----------

# EXERCISE_KEY: tt_ex5
# TODO: Find order_ids that changed between version 0 and current

# Your code here


# COMMAND ----------

# Validate Exercise 5
result = spark.table(f"{CATALOG}.{SCHEMA}.tt_ex5_changes")

assert result.count() == 3, f"Expected 3 changed order_ids, got {result.count()}"
changed_ids = set(row.order_id for row in result.collect())
assert "ORD-001" in changed_ids, "ORD-001 was modified and should be in changes"
assert "ORD-005" in changed_ids, "ORD-005 was deleted and should be in changes"
assert "ORD-101" in changed_ids, "ORD-101 was added and should be in changes"

print("Exercise 5 passed!")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Exercise 6: Audit Trail
# MAGIC **Difficulty**: Medium | **Time**: ~15 min
# MAGIC
# MAGIC A fraudulent order with amount > 1000 appeared in the table. Use time travel to find
# MAGIC which version introduced it.
# MAGIC
# MAGIC **Table** (`tt_ex6_orders`): currently 6 rows. DML operations performed:
# MAGIC - CREATE: 5 rows (all amounts < 500)
# MAGIC - UPDATE: normal (ORD-001 status changed)
# MAGIC - INSERT: suspicious insert
# MAGIC - UPDATE: normal (ORD-002 status changed)
# MAGIC
# MAGIC **Note**: On serverless, auto-OPTIMIZE may add extra versions. Use `DESCRIBE HISTORY`
# MAGIC to see actual version numbers, and iterate through all versions.
# MAGIC
# MAGIC **Expected Output**: Write a single-row table `tt_ex6_audit` with columns:
# MAGIC - `bad_version` (INT): the version that introduced the bad data
# MAGIC - `bad_order_id` (STRING): the order_id of the fraudulent order
# MAGIC
# MAGIC **Requirements**:
# MAGIC 1. Query the current table to find the row with amount > 1000 (note the order_id)
# MAGIC 2. Use `DESCRIBE HISTORY` to get all version numbers
# MAGIC 3. Check each version to find when that order_id first appeared
# MAGIC 4. Fill in the placeholder values below

# COMMAND ----------

# EXERCISE_KEY: tt_ex6
# TODO: Investigate versions to find who introduced the bad data, then fill in values

bad_version = 0        # Replace: the version number that introduced the fraudulent order
bad_order_id = ""      # Replace: the order_id of the fraudulent order

spark.sql(f"""
    CREATE OR REPLACE TABLE {CATALOG}.{SCHEMA}.tt_ex6_audit AS
    SELECT CAST({bad_version} AS INT) AS bad_version, '{bad_order_id}' AS bad_order_id
""")

# COMMAND ----------

# Validate Exercise 6
row = spark.table(f"{CATALOG}.{SCHEMA}.tt_ex6_audit").collect()[0]

assert row.bad_version == TT_EX6_BAD_VERSION, \
    f"Bad data was introduced in version {TT_EX6_BAD_VERSION}, got {row.bad_version}"
assert row.bad_order_id == "ORD-999", f"Bad order_id is ORD-999, got {row.bad_order_id}"

print("Exercise 6 passed!")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Exercise 7: Selective Undo via MERGE + Time Travel
# MAGIC **Difficulty**: Hard | **Time**: ~20 min
# MAGIC
# MAGIC A bad UPDATE accidentally set status='cancelled' for all orders with amount < 100.
# MAGIC Use MERGE with time travel to restore ONLY the affected rows without reverting
# MAGIC everything.
# MAGIC
# MAGIC **Table** (`tt_ex7_orders`): currently 5 rows. Operations performed:
# MAGIC - v0 (CREATE): 5 rows (original good data)
# MAGIC - UPDATE: bad update set status='cancelled' WHERE amount < 100
# MAGIC
# MAGIC **Expected Output**: After your MERGE, orders with amount < 100 have their original
# MAGIC status from version 0 restored. Orders with amount >= 100 are unchanged.
# MAGIC
# MAGIC **Requirements**:
# MAGIC 1. Use the version 0 state as source for a MERGE into the current table
# MAGIC 2. Only update rows where status was incorrectly set to 'cancelled'
# MAGIC 3. Do NOT use RESTORE (that reverts all changes, not just the bad one)
# MAGIC
# MAGIC **Constraints**:
# MAGIC - RESTORE is not allowed (it's too coarse for selective undo)
# MAGIC - Only the incorrectly cancelled rows should change

# COMMAND ----------

# EXERCISE_KEY: tt_ex7
# TODO: Use MERGE with time travel to selectively undo the bad update

# Your code here


# COMMAND ----------

# Validate Exercise 7
result = spark.table(f"{CATALOG}.{SCHEMA}.tt_ex7_orders")
v0 = spark.sql(f"SELECT * FROM {CATALOG}.{SCHEMA}.tt_ex7_orders VERSION AS OF 0")

assert result.count() == 5, f"Expected 5 rows, got {result.count()}"
# Orders with amount < 100 should have their v0 status restored
for row in result.filter("amount < 100").collect():
    v0_status = v0.filter(f"order_id = '{row.order_id}'").select("status").collect()[0][0]
    assert row.status == v0_status, \
        f"{row.order_id} should have status '{v0_status}' (from v0), got '{row.status}'"
# No incorrectly cancelled rows remaining
bad_rows = result.filter("status = 'cancelled' AND amount < 100").count()
assert bad_rows == 0, f"Found {bad_rows} incorrectly cancelled rows (amount < 100)"

print("Exercise 7 passed!")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Exercise 8: Reproducible Reporting with Time Travel
# MAGIC **Difficulty**: Hard | **Time**: ~15 min
# MAGIC
# MAGIC A bulk UPDATE corrupted some order amounts (multiplied by 100).
# MAGIC Re-create the daily revenue report from the last known-good state - the version
# MAGIC where the INSERT (WRITE) happened, before the bad UPDATE.
# MAGIC
# MAGIC **Table** (`tt_ex8_orders`): currently 8 rows. DML operations performed:
# MAGIC - CREATE: 5 rows from base
# MAGIC - INSERT: 3 orders added (ORD-101: $250, ORD-102: $175, ORD-103: $75) - total 8 rows
# MAGIC - UPDATE: bad bulk update multiplied amounts by 100 for ORD-101 and ORD-102
# MAGIC
# MAGIC The setup stored the INSERT version number in `TT_EX8_INSERT_V`.
# MAGIC
# MAGIC **Expected Output**: Write a single-row table `tt_ex8_report` with columns:
# MAGIC - `order_count` (LONG): total number of orders at the INSERT version
# MAGIC - `total_amount` (DOUBLE): sum of all order amounts at the INSERT version
# MAGIC
# MAGIC **Requirements**:
# MAGIC 1. Query `tt_ex8_orders` at the INSERT version (`TT_EX8_INSERT_V`) - the last clean state
# MAGIC 2. Compute total order count and sum of amounts
# MAGIC 3. Save as table `tt_ex8_report`
# MAGIC
# MAGIC **Constraints**:
# MAGIC - Do NOT query the current version (it has corrupted amounts)

# COMMAND ----------

# EXERCISE_KEY: tt_ex8
# TODO: Create the revenue report from the INSERT version (TT_EX8_INSERT_V)

# Your code here


# COMMAND ----------

# Validate Exercise 8
result = spark.table(f"{CATALOG}.{SCHEMA}.tt_ex8_report")

assert result.count() == 1, f"Expected 1 summary row, got {result.count()}"
row = result.collect()[0]
assert row.order_count == 8, f"INSERT version should have 8 orders, got {row.order_count}"
# Verify total matches independently calculated INSERT version total
expected_total = spark.sql(
    f"SELECT SUM(amount) FROM {CATALOG}.{SCHEMA}.tt_ex8_orders VERSION AS OF {TT_EX8_INSERT_V}"
).collect()[0][0]
assert abs(row.total_amount - expected_total) < 0.01, \
    f"Total amount should be {expected_total}, got {row.total_amount}"
# Verify it's different from current (corrupted) total
current_total = spark.sql(
    f"SELECT SUM(amount) FROM {CATALOG}.{SCHEMA}.tt_ex8_orders"
).collect()[0][0]
assert row.total_amount < current_total, \
    "Report total should be less than current total (which has corrupted 100x amounts)"

print("Exercise 8 passed!")
