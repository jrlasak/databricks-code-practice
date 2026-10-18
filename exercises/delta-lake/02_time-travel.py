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

# MAGIC %run ./setup/time-travel-setup

# COMMAND ----------
# MAGIC %md
# MAGIC **Setup complete.** Every exercise table is in `db_code.time_travel`. Write your output tables there too, by full name (for example `db_code.time_travel.tt_ex1_result`). In Python cells, the variables `CATALOG` and `SCHEMA` hold `db_code` and `time_travel`. The base table (`orders`) is in `db_code.delta_lake`.
# MAGIC
# MAGIC Keep the `# EXERCISE_KEY: ...` line as the first line of each answer cell.
# MAGIC
# MAGIC Every `tt_ex*_orders` table has the `orders` schema above and its own version history:
# MAGIC - Ex 1: `tt_ex1_orders` - CREATE (5 rows), UPDATE of ORD-001 and ORD-002, DELETE of ORD-005
# MAGIC - Ex 2: `tt_ex2_orders` - CREATE (5 rows), UPDATE of ORD-003, DELETE of ORD-003
# MAGIC - Checkpoint 3: `tt_ex3_orders` - CREATE (5 rows), UPDATE, DELETE, INSERT
# MAGIC - Ex 4: `tt_ex4_orders` - CREATE (5 rows), UPDATE that set every amount to 0.00
# MAGIC - Ex 5: `tt_ex5_orders` - CREATE (5 rows), UPDATE of ORD-001, DELETE of ORD-005, INSERT of ORD-101
# MAGIC - Ex 6: `tt_ex6_orders` - CREATE (5 rows), UPDATE of ORD-001, INSERT of one order, UPDATE of ORD-002
# MAGIC - Ex 7: `tt_ex7_orders` - CREATE (5 rows), UPDATE that cancelled every order with amount < 100, UPDATE of ORD-001
# MAGIC - Ex 8: `tt_ex8_orders` - CREATE (5 rows), INSERT of 3 orders, UPDATE that multiplied two amounts by 100
# MAGIC
# MAGIC **Note**: Auto compaction can add an `OPTIMIZE` version after an UPDATE or DELETE, so version numbers can shift. Version 0 is always the CREATE; never hardcode any other version number. Where you need a later version, the setup stores it in a variable: `TT_EX8_INSERT_V` in Exercise 8.

# COMMAND ----------
# MAGIC %md
# MAGIC ## Exercise 1: Query by Version Number
# MAGIC **Difficulty**: Easy | **Time**: ~5 min
# MAGIC
# MAGIC Query a historical version of a Delta table using `VERSION AS OF`.
# MAGIC
# MAGIC **Table** (`tt_ex1_orders`): currently 4 rows. Operations performed:
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
# MAGIC **Table** (`tt_ex3_orders`): has multiple versions. Operations performed:
# MAGIC - CREATE TABLE AS SELECT (5 rows)
# MAGIC - UPDATE on ORD-001
# MAGIC - DELETE of ORD-005
# MAGIC - INSERT of ORD-101
# MAGIC
# MAGIC **Note**: Auto compaction can add `OPTIMIZE` versions between these operations, so the history may show more than 4 rows.
# MAGIC
# MAGIC **Requirements**:
# MAGIC 1. Run `DESCRIBE HISTORY` on `tt_ex3_orders`
# MAGIC 2. Read the `operation` column for each version
# MAGIC 3. Fill in the placeholder values below

# COMMAND ----------

# EXERCISE_KEY: tt_ex3
# TODO: Run DESCRIBE HISTORY on tt_ex3_orders, then fill in what you observe

total_versions = 0        # Replace: how many versions (rows) does the history show?
latest_operation = ""     # Replace: the `operation` value of the highest version in DESCRIBE HISTORY
num_dml_operations = 0    # Replace: how many history rows have an operation other than OPTIMIZE? (the CREATE TABLE AS SELECT row counts)

spark.sql(f"""
    CREATE OR REPLACE TABLE {CATALOG}.{SCHEMA}.tt_ex3_history AS
    SELECT CAST({total_versions} AS INT) AS total_versions,
           '{latest_operation}' AS latest_operation,
           CAST({num_dml_operations} AS INT) AS num_dml_operations
""")

# COMMAND ----------

# Validate Checkpoint 3
row = spark.table(f"{CATALOG}.{SCHEMA}.tt_ex3_history").collect()[0]

assert row.total_versions == TT_EX3_TOTAL_VERSIONS, \
    f"total_versions = {row.total_versions} does not match the number of rows DESCRIBE HISTORY returns for tt_ex3_orders"
assert row.latest_operation == TT_EX3_LATEST_OPERATION, \
    f"'{row.latest_operation}' is not the operation of the highest version in DESCRIBE HISTORY"
assert row.num_dml_operations == TT_EX3_DML_COUNT, \
    f"num_dml_operations = {row.num_dml_operations} does not match the number of history rows whose operation is not OPTIMIZE"

print("Checkpoint 3 passed!")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Exercise 4: RESTORE to Previous Version
# MAGIC **Difficulty**: Medium | **Time**: ~10 min
# MAGIC
# MAGIC A bad UPDATE set every amount to 0.00. Restore the table to its original state.
# MAGIC
# MAGIC **Table** (`tt_ex4_orders`): currently 5 rows, all with amount = 0.00. Version 0 holds the original amounts, all positive.
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
_ops = [r.operation for r in spark.sql(f"DESCRIBE HISTORY {CATALOG}.{SCHEMA}.tt_ex4_orders").orderBy("version", ascending=False).collect() if r.operation != "OPTIMIZE"]
assert _ops[0] == "RESTORE", f"The latest change to tt_ex4_orders is {_ops[0]}, not RESTORE"

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
# MAGIC **Approach**: A modified row keeps its `order_id` but changes other values. Your comparison has to catch that as well as rows that were added or removed.
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
# MAGIC **Table** (`tt_ex6_orders`): currently 6 rows. Operations performed:
# MAGIC - CREATE: 5 rows (all amounts < 500)
# MAGIC - UPDATE: ORD-001 status changed
# MAGIC - INSERT: one order added
# MAGIC - UPDATE: ORD-002 status changed
# MAGIC
# MAGIC **Expected Output**: Write a single-row table `tt_ex6_audit` with columns:
# MAGIC - `bad_version` (INT): the version that introduced the bad data
# MAGIC - `bad_order_id` (STRING): the order_id of the fraudulent order
# MAGIC
# MAGIC **Requirements**:
# MAGIC 1. Query the current table to find the row with amount > 1000 (note the order_id)
# MAGIC 2. Use `DESCRIBE HISTORY` to list the version numbers (auto compaction can add `OPTIMIZE` versions between the others)
# MAGIC 3. Check each version to find when that order_id first appeared
# MAGIC 4. Fill in the placeholder values below

# COMMAND ----------

# EXERCISE_KEY: tt_ex6
# TODO: Find the version that introduced the order with amount > 1000, then fill in the values below

bad_version = 0        # Replace: the version number that introduced the fraudulent order
bad_order_id = ""      # Replace: the order_id of the fraudulent order

spark.sql(f"""
    CREATE OR REPLACE TABLE {CATALOG}.{SCHEMA}.tt_ex6_audit AS
    SELECT CAST({bad_version} AS INT) AS bad_version, '{bad_order_id}' AS bad_order_id
""")

# COMMAND ----------

# Validate Exercise 6
row = spark.table(f"{CATALOG}.{SCHEMA}.tt_ex6_audit").collect()[0]

expected_order_id = spark.sql(
    f"SELECT order_id FROM {CATALOG}.{SCHEMA}.tt_ex6_orders WHERE amount > 1000"
).collect()[0][0]
assert row.bad_order_id == expected_order_id, \
    f"'{row.bad_order_id}' is not the order with amount > 1000 in the current table"
assert row.bad_version == TT_EX6_BAD_VERSION, \
    f"Version {row.bad_version} is not the first version that contains that order. Query each version from DESCRIBE HISTORY with VERSION AS OF."

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
# MAGIC - UPDATE: valid update set ORD-001 status='shipped' (keep this change)
# MAGIC
# MAGIC **Expected Output**: ORD-003 and ORD-005 are back to their version 0 status, `pending`. ORD-001 stays `shipped`. ORD-002 and ORD-004 are unchanged (ORD-004 was already `cancelled` at version 0).
# MAGIC
# MAGIC **Requirements**:
# MAGIC 1. Use the version 0 state as source for a MERGE into the current table
# MAGIC 2. Only update rows where status was incorrectly set to 'cancelled'
# MAGIC
# MAGIC **Constraints**:
# MAGIC - Do not use RESTORE: it would also undo ORD-001's valid update
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
assert result.filter("order_id = 'ORD-001'").collect()[0].status == "shipped", \
    "ORD-001's later valid update to 'shipped' must survive: restore only the rows the bad UPDATE cancelled"

print("Exercise 7 passed!")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Exercise 8: Reproducible Reporting with Time Travel
# MAGIC **Difficulty**: Medium | **Time**: ~10 min
# MAGIC
# MAGIC A bulk UPDATE corrupted some order amounts (multiplied by 100).
# MAGIC Re-create the revenue report from the last known-good state: the version where the INSERT (WRITE) happened, before the bad UPDATE.
# MAGIC
# MAGIC **Table** (`tt_ex8_orders`): currently 8 rows. Operations performed:
# MAGIC - CREATE: 5 rows from base
# MAGIC - INSERT: 3 orders added (ORD-101: $250, ORD-102: $175, ORD-103: $75) - total 8 rows
# MAGIC - UPDATE: bad bulk update multiplied amounts by 100 for ORD-101 and ORD-102
# MAGIC
# MAGIC The setup stored the INSERT version number in `TT_EX8_INSERT_V`.
# MAGIC
# MAGIC **Expected Output**: Write a single-row table `tt_ex8_report` with columns:
# MAGIC - `order_count` (LONG): number of orders at the INSERT version (8)
# MAGIC - `total_amount` (DOUBLE): sum of `amount` at the INSERT version (1239.49; the corrupted current total is 43314.49)
# MAGIC
# MAGIC **Requirements**:
# MAGIC 1. Query `tt_ex8_orders` at version `TT_EX8_INSERT_V`
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
    f"total_amount = {row.total_amount} does not match SUM(amount) at version TT_EX8_INSERT_V"
# Verify it's different from current (corrupted) total
current_total = spark.sql(
    f"SELECT SUM(amount) FROM {CATALOG}.{SCHEMA}.tt_ex8_orders"
).collect()[0][0]
assert row.total_amount < current_total, \
    "Report total should be less than current total (which has corrupted 100x amounts)"

print("Exercise 8 passed!")
