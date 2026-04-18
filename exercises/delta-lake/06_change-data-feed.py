# Databricks notebook source
# COMMAND ----------
# MAGIC %md
# MAGIC # Change Data Feed (CDF)
# MAGIC **Topic**: Delta Lake | **Exercises**: 7 | **Total Time**: ~80 min
# MAGIC
# MAGIC Practice Delta Lake Change Data Feed: enable CDF, read change records after INSERT,
# MAGIC UPDATE, DELETE, and MERGE operations, filter by change type, and propagate changes
# MAGIC to downstream tables.
# MAGIC
# MAGIC **Solutions**: If stuck, see `solutions/change-data-feed-solutions.py` for hints and answers.
# MAGIC
# MAGIC **Tables used** (from `00_Setup.py`):
# MAGIC - `db_code.delta_lake.orders` - order records
# MAGIC
# MAGIC **Key concept**: When CDF is enabled, Delta Lake records row-level changes. Read them
# MAGIC with `table_changes('catalog.schema.table', start_version)`. Each row includes:
# MAGIC - All original columns
# MAGIC - `_change_type`: `insert`, `update_preimage`, `update_postimage`, or `delete`
# MAGIC - `_commit_version`: version number of the change
# MAGIC - `_commit_timestamp`: when the change happened

# COMMAND ----------

# MAGIC %run ./00_Setup

# COMMAND ----------

# MAGIC %run ./setup/change-data-feed-setup

# COMMAND ----------
# MAGIC %md
# MAGIC **Setup complete.** Exercise tables are in `{CATALOG}.{SCHEMA}` (change_data_feed schema).
# MAGIC Base tables (orders) are in `{CATALOG}.{BASE_SCHEMA}` (delta_lake schema).
# MAGIC Exercise tables:
# MAGIC - Ex 1 (easy): `cdf_ex1_orders` - CDF **not** enabled (you enable it)
# MAGIC - Ex 2 (easy): `cdf_ex2_orders` - CDF enabled, then 2 rows INSERTed at v1
# MAGIC - Ex 3 (medium): `cdf_ex3_orders` - CDF enabled, then ORD-001 UPDATEd at v1
# MAGIC - Ex 4 (medium): `cdf_ex4_orders` - CDF enabled, then ORD-005 DELETEd at v1
# MAGIC - Ex 5 (medium): `cdf_ex5_orders` - CDF enabled, then INSERT (v1) + UPDATE (v2) + DELETE (v3)
# MAGIC - Ex 6 (hard): `cdf_ex6_source` (CDF, 4 versions of changes) + `cdf_ex6_target` (copy of v0, out of sync)
# MAGIC - Ex 7 (hard): `cdf_ex7_orders` - CDF enabled, then MERGE at v1 (1 update + 1 insert)

# COMMAND ----------
# MAGIC %md
# MAGIC ## Exercise 1: Enable CDF on an Existing Table
# MAGIC **Difficulty**: Easy | **Time**: ~5 min
# MAGIC
# MAGIC Enable Change Data Feed on a table so Delta Lake starts recording row-level changes.
# MAGIC
# MAGIC **Table** (`cdf_ex1_orders`): 5 rows. CDF is currently **disabled**.
# MAGIC
# MAGIC **Expected Output**: CDF is enabled. The table property `delta.enableChangeDataFeed` = `true`.
# MAGIC
# MAGIC **Requirements**:
# MAGIC 1. Use `ALTER TABLE SET TBLPROPERTIES` to enable CDF

# COMMAND ----------

# EXERCISE_KEY: cdf_ex1
# TODO: Enable Change Data Feed on cdf_ex1_orders

# Your code here


# COMMAND ----------

# Validate Exercise 1
props = spark.sql(f"SHOW TBLPROPERTIES {CATALOG}.{SCHEMA}.cdf_ex1_orders")
cdf_rows = props.filter("key = 'delta.enableChangeDataFeed'").collect()

assert len(cdf_rows) > 0, "Should have delta.enableChangeDataFeed property"
assert cdf_rows[0].value == "true", f"CDF should be enabled, got '{cdf_rows[0].value}'"

print("Exercise 1 passed!")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Exercise 2: Read Changes After INSERT
# MAGIC **Difficulty**: Easy | **Time**: ~5 min
# MAGIC
# MAGIC The setup INSERTed 2 rows (ORD-101, ORD-102) at version 1. Use `table_changes()`
# MAGIC to read those INSERT changes.
# MAGIC
# MAGIC **Table** (`cdf_ex2_orders`): 7 rows total. CDF enabled. v0: 5 base rows, v1: 2 INSERTed.
# MAGIC
# MAGIC **Expected Output**: Save the version 1 changes to `cdf_ex2_changes`. 2 rows,
# MAGIC both with `_change_type = 'insert'`.
# MAGIC
# MAGIC **Requirements**:
# MAGIC 1. Use `table_changes()` to read changes from version 1
# MAGIC 2. Save the result as `cdf_ex2_changes`

# COMMAND ----------

# EXERCISE_KEY: cdf_ex2
# TODO: Read CDF changes from version 1 and save to cdf_ex2_changes

# Your code here


# COMMAND ----------

# Validate Exercise 2
result = spark.table(f"{CATALOG}.{SCHEMA}.cdf_ex2_changes")

assert result.count() == 2, f"Expected 2 INSERT changes, got {result.count()}"
assert result.filter("_change_type = 'insert'").count() == 2, \
    "All changes should be inserts"
assert result.filter("order_id = 'ORD-101'").count() == 1, "ORD-101 should be in changes"

print("Exercise 2 passed!")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Exercise 3: Read Changes After UPDATE
# MAGIC **Difficulty**: Medium | **Time**: ~10 min
# MAGIC
# MAGIC The setup UPDATEd ORD-001 (status='shipped', amount=120.00) at version 1.
# MAGIC UPDATEs produce TWO CDF records per row: the old values (`update_preimage`)
# MAGIC and the new values (`update_postimage`).
# MAGIC
# MAGIC **Table** (`cdf_ex3_orders`): 5 rows. CDF enabled. v1: ORD-001 updated.
# MAGIC
# MAGIC **Expected Output**: Save the version 1 changes to `cdf_ex3_changes`. 2 rows:
# MAGIC one `update_preimage` (old ORD-001) and one `update_postimage` (new ORD-001).
# MAGIC
# MAGIC **Requirements**:
# MAGIC 1. Use `table_changes()` to read changes from version 1
# MAGIC 2. Save the result as `cdf_ex3_changes`

# COMMAND ----------

# EXERCISE_KEY: cdf_ex3
# TODO: Read CDF changes from the UPDATE and save to cdf_ex3_changes

# Your code here


# COMMAND ----------

# Validate Exercise 3
result = spark.table(f"{CATALOG}.{SCHEMA}.cdf_ex3_changes")

assert result.count() == 2, f"Expected 2 change rows (pre + post), got {result.count()}"
assert result.filter("_change_type = 'update_preimage'").count() == 1, \
    "Should have 1 update_preimage row"
assert result.filter("_change_type = 'update_postimage'").count() == 1, \
    "Should have 1 update_postimage row"
post = result.filter("_change_type = 'update_postimage'").collect()[0]
assert post.amount == 120.00, f"Updated amount should be 120.00, got {post.amount}"

print("Exercise 3 passed!")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Exercise 4: Read Changes After DELETE
# MAGIC **Difficulty**: Medium | **Time**: ~10 min
# MAGIC
# MAGIC The setup DELETEd ORD-005 at version 1. Deletes produce CDF records with
# MAGIC `_change_type = 'delete'` containing the deleted row's values.
# MAGIC
# MAGIC **Table** (`cdf_ex4_orders`): 4 rows (was 5). CDF enabled. v1: ORD-005 deleted.
# MAGIC
# MAGIC **Expected Output**: Save the version 1 changes to `cdf_ex4_changes`. 1 row
# MAGIC with `_change_type = 'delete'` and `order_id = 'ORD-005'`.
# MAGIC
# MAGIC **Requirements**:
# MAGIC 1. Use `table_changes()` to read changes from version 1
# MAGIC 2. Save the result as `cdf_ex4_changes`

# COMMAND ----------

# EXERCISE_KEY: cdf_ex4
# TODO: Read CDF changes from the DELETE and save to cdf_ex4_changes

# Your code here


# COMMAND ----------

# Validate Exercise 4
result = spark.table(f"{CATALOG}.{SCHEMA}.cdf_ex4_changes")

assert result.count() == 1, f"Expected 1 delete change, got {result.count()}"
row = result.collect()[0]
assert row._change_type == "delete", f"Should be delete, got '{row._change_type}'"
assert row.order_id == "ORD-005", f"Deleted order should be ORD-005, got '{row.order_id}'"

print("Exercise 4 passed!")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Exercise 5: Filter CDF by Operation Type
# MAGIC **Difficulty**: Medium | **Time**: ~10 min
# MAGIC
# MAGIC The table went through INSERT (v1), UPDATE (v2), and DELETE (v3). Read ALL changes
# MAGIC across all versions, but filter to only the INSERT operations.
# MAGIC
# MAGIC **Table** (`cdf_ex5_orders`): CDF enabled. v1: INSERT ORD-101, v2: UPDATE ORD-001,
# MAGIC v3: DELETE ORD-005.
# MAGIC
# MAGIC **Expected Output**: Save only the INSERT changes to `cdf_ex5_inserts`. 1 row
# MAGIC (ORD-101) with `_change_type = 'insert'`.
# MAGIC
# MAGIC **Requirements**:
# MAGIC 1. Use `table_changes()` to read changes from version 1 through current
# MAGIC 2. Filter for `_change_type = 'insert'` only
# MAGIC 3. Save as `cdf_ex5_inserts`

# COMMAND ----------

# EXERCISE_KEY: cdf_ex5
# TODO: Read all CDF changes, filter for inserts only, save to cdf_ex5_inserts

# Your code here


# COMMAND ----------

# Validate Exercise 5
result = spark.table(f"{CATALOG}.{SCHEMA}.cdf_ex5_inserts")

assert result.count() == 1, f"Expected 1 insert, got {result.count()}"
assert result.filter("_change_type = 'insert'").count() == result.count(), \
    "All rows should have _change_type = 'insert'"
assert result.collect()[0].order_id == "ORD-101", "Insert should be ORD-101"

print("Exercise 5 passed!")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Exercise 6: Propagate Changes to Downstream Table
# MAGIC **Difficulty**: Hard | **Time**: ~20 min
# MAGIC
# MAGIC The target table is a copy of the source's initial state (v0) and is now out of sync.
# MAGIC The source went through: UPDATE ORD-001 (v1), DELETE ORD-005 (v2), INSERT ORD-101 (v3).
# MAGIC Use CDF to propagate all changes to the target.
# MAGIC
# MAGIC **Source** (`cdf_ex6_source`): CDF enabled, 5 rows at v0, then 3 changes (v1-v3). Currently 5 rows.
# MAGIC
# MAGIC **Target** (`cdf_ex6_target`): copy of source v0 (5 original rows, stale).
# MAGIC
# MAGIC **Expected Output**: `cdf_ex6_target` matches `cdf_ex6_source` current state:
# MAGIC 5 rows, ORD-001 updated (amount=120.00), ORD-005 deleted, ORD-101 inserted.
# MAGIC
# MAGIC **Requirements**:
# MAGIC 1. Read CDF from source (versions 1 through current)
# MAGIC 2. Use MERGE to apply changes to target: delete deletes, update postimages, insert inserts
# MAGIC 3. Exclude `update_preimage` rows (they're the old values, not needed for sync)
# MAGIC
# MAGIC **Approach**: MERGE INTO target USING (filtered CDF) ON order_id. Use multiple WHEN
# MAGIC MATCHED clauses to handle deletes vs updates. WHEN NOT MATCHED for inserts.

# COMMAND ----------

# EXERCISE_KEY: cdf_ex6
# TODO: Read CDF from source, MERGE into target to sync

# Your code here


# COMMAND ----------

# Validate Exercise 6
target = spark.table(f"{CATALOG}.{SCHEMA}.cdf_ex6_target")
source = spark.table(f"{CATALOG}.{SCHEMA}.cdf_ex6_source")

assert target.count() == source.count(), \
    f"Target ({target.count()}) should match source ({source.count()}) row count"
assert target.filter("order_id = 'ORD-001'").select("amount").collect()[0][0] == 120.00, \
    "ORD-001 should be updated to 120.00"
assert target.filter("order_id = 'ORD-005'").count() == 0, \
    "ORD-005 should be deleted"
assert target.filter("order_id = 'ORD-101'").count() == 1, \
    "ORD-101 should be inserted"

print("Exercise 6 passed!")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Exercise 7: Read CDF from a MERGE Operation
# MAGIC **Difficulty**: Hard | **Time**: ~15 min
# MAGIC
# MAGIC The setup performed a MERGE at version 1 that updated ORD-001 and inserted ORD-101.
# MAGIC Read the CDF to see exactly what the MERGE did, and count the operations.
# MAGIC
# MAGIC **Table** (`cdf_ex7_orders`): CDF enabled. v1: MERGE (1 update + 1 insert).
# MAGIC
# MAGIC **Expected Output**: Save the version 1 changes to `cdf_ex7_changes`.
# MAGIC Should contain: 1 `update_preimage`, 1 `update_postimage`, 1 `insert` = 3 rows total.
# MAGIC
# MAGIC **Requirements**:
# MAGIC 1. Use `table_changes()` to read changes from version 1
# MAGIC 2. Save the result as `cdf_ex7_changes`

# COMMAND ----------

# EXERCISE_KEY: cdf_ex7
# TODO: Read CDF from the MERGE operation and save to cdf_ex7_changes

# Your code here


# COMMAND ----------

# Validate Exercise 7
result = spark.table(f"{CATALOG}.{SCHEMA}.cdf_ex7_changes")

assert result.count() == 3, f"Expected 3 CDF rows (pre + post + insert), got {result.count()}"
assert result.filter("_change_type = 'update_preimage'").count() == 1, \
    "Should have 1 update_preimage (old ORD-001)"
assert result.filter("_change_type = 'update_postimage'").count() == 1, \
    "Should have 1 update_postimage (new ORD-001)"
assert result.filter("_change_type = 'insert'").count() == 1, \
    "Should have 1 insert (ORD-101)"

print("Exercise 7 passed!")
