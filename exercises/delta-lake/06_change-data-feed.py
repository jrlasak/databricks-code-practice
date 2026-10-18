# Databricks notebook source
# COMMAND ----------
# MAGIC %md
# MAGIC # Change Data Feed (CDF)
# MAGIC **Topic**: Delta Lake | **Exercises**: 7 | **Total Time**: ~60 min
# MAGIC
# MAGIC Practice Delta Lake Change Data Feed: enable CDF, read change records after INSERT,
# MAGIC UPDATE, DELETE, and MERGE operations, filter by change type, and propagate changes
# MAGIC to downstream tables.
# MAGIC
# MAGIC **Solutions**: If stuck, see `solutions/change-data-feed-solutions.py` for hints and answers.
# MAGIC
# MAGIC **Tables used** (from `00_Setup.py`):
# MAGIC - `db_code.delta_lake.orders`: `order_id`, `customer_id`, `product_id`, `amount`, `status`, `order_date`, `updated_at`. Each exercise table below starts as a 5-row copy of `ORD-001` to `ORD-005` with these columns, so `order_id` is unique in every exercise table.
# MAGIC
# MAGIC **Key concept**: When CDF is enabled, Delta Lake records row-level changes. Read them
# MAGIC with `table_changes('catalog.schema.table', start_version)`. Each row includes:
# MAGIC - All original columns
# MAGIC - `_change_type`: `insert`, `update_preimage`, `update_postimage`, or `delete`
# MAGIC - `_commit_version`: version number of the change
# MAGIC - `_commit_timestamp`: when the change happened
# MAGIC
# MAGIC This notebook uses the table-property change data feed (`delta.enableChangeDataFeed`). It records changes only from the moment it is turned on, never earlier ones. Databricks now recommends automatic change data feed where your workspace supports it: Unity Catalog tables with row tracking get their changes computed at query time, with no property to set, and you read them with the same `table_changes()`. A table uses one or the other, never both.

# COMMAND ----------

# MAGIC %run ./00_Setup

# COMMAND ----------

# MAGIC %run ./setup/change-data-feed-setup

# COMMAND ----------
# MAGIC %md
# MAGIC **Setup complete.** Every exercise table is in `db_code.change_data_feed`. Write your output tables there too, by full name (for example `db_code.change_data_feed.cdf_ex2_changes`). In Python cells, the variables `CATALOG` and `SCHEMA` hold `db_code` and `change_data_feed`. The base table (`orders`) is in `db_code.delta_lake`.
# MAGIC
# MAGIC Keep the `# EXERCISE_KEY: ...` line as the first line of each answer cell.
# MAGIC
# MAGIC Exercise tables:
# MAGIC - Ex 1: `cdf_ex1_orders`: 5 rows, CDF **not** enabled
# MAGIC - Ex 2: `cdf_ex2_orders`: CDF enabled, 2 rows inserted at v1
# MAGIC - Ex 3: `cdf_ex3_orders`: CDF enabled, ORD-001 updated at v1
# MAGIC - Ex 4: `cdf_ex4_orders`: CDF enabled, ORD-005 deleted at v1
# MAGIC - Ex 5: `cdf_ex5_orders`: CDF enabled, insert (v1), update (v2), delete (v3)
# MAGIC - Ex 6: `cdf_ex6_source` (CDF enabled, v0 plus 3 changes at v1-v3) and `cdf_ex6_target` (copy of v0, out of sync)
# MAGIC - Ex 7: `cdf_ex7_orders`: CDF enabled, MERGE at v1 (1 update + 1 insert)

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
# MAGIC Setup inserted 2 rows (ORD-101, ORD-102) at version 1. Use `table_changes()`
# MAGIC to read those INSERT changes.
# MAGIC
# MAGIC **Table** (`cdf_ex2_orders`): 7 rows total. CDF enabled. v0: 5 base rows, v1: 2 inserted.
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
# MAGIC **Difficulty**: Easy | **Time**: ~5 min
# MAGIC
# MAGIC Setup updated ORD-001 (status='shipped', amount=120.00) at version 1.
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
# MAGIC **Difficulty**: Easy | **Time**: ~5 min
# MAGIC
# MAGIC Setup deleted ORD-005 at version 1. Deletes produce CDF records with
# MAGIC `_change_type = 'delete'` containing the deleted row's values.
# MAGIC
# MAGIC **Table** (`cdf_ex4_orders`): 4 rows. CDF enabled. v1: ORD-005 deleted.
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
# MAGIC The table went through INSERT (v1), UPDATE (v2), and DELETE (v3). Read every change from version 1 onward, then keep only the inserts.
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
# TODO: Read CDF changes from version 1, keep only inserts, save to cdf_ex5_inserts

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
# MAGIC 2. Exclude `update_preimage` rows (they're the old values, not needed for sync)
# MAGIC 3. MERGE the changes into `cdf_ex6_target` on `order_id`: a `delete` row deletes the matching target row, an `update_postimage` row updates it, and a row whose key the target does not have is inserted unless it is a `delete`. Write only the seven order columns into the target: the CDF columns `_change_type`, `_commit_version` and `_commit_timestamp` are for the MERGE logic and do not belong in `cdf_ex6_target`. (Here each `order_id` changes once; in production, also keep only the latest `_commit_version` per key before the MERGE.)

# COMMAND ----------

# EXERCISE_KEY: cdf_ex6
# TODO: Read CDF from source, MERGE into target to sync

# Your code here


# COMMAND ----------

# Validate Exercise 6
target = spark.table(f"{CATALOG}.{SCHEMA}.cdf_ex6_target")
source = spark.table(f"{CATALOG}.{SCHEMA}.cdf_ex6_source")

target_ids = sorted(r.order_id for r in target.select("order_id").collect())
source_ids = sorted(r.order_id for r in source.select("order_id").collect())
assert target_ids == source_ids, \
    f"Target holds order_ids {target_ids}: it should hold the same order_ids as the current source"
assert len(target.columns) == 7, \
    f"cdf_ex6_target should have only the seven order columns, without _change_type, _commit_version or _commit_timestamp; got {target.columns}"
assert target.filter("order_id = 'ORD-001'").select("amount").collect()[0][0] == 120.00, \
    "ORD-001 should be updated to 120.00"
assert target.filter("order_id = 'ORD-005'").count() == 0, \
    "ORD-005 should be deleted"
assert target.filter("order_id = 'ORD-101'").count() == 1, \
    "ORD-101 should be inserted"

print("Exercise 6 passed!")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Exercise 7: Count the Changes a MERGE Wrote
# MAGIC **Difficulty**: Medium | **Time**: ~10 min
# MAGIC
# MAGIC Setup ran a MERGE at version 1 that updated ORD-001 and inserted ORD-101. Read the CDF and count the change rows the MERGE wrote, per change type.
# MAGIC
# MAGIC **Table** (`cdf_ex7_orders`): CDF enabled. v1: MERGE (1 update + 1 insert).
# MAGIC
# MAGIC **Expected Output**: Save one row per `_change_type` to `cdf_ex7_counts`, with columns `_change_type` and `row_count`. 3 rows: `insert` = 1, `update_preimage` = 1, `update_postimage` = 1.
# MAGIC
# MAGIC **Requirements**:
# MAGIC 1. Use `table_changes()` to read changes from version 1
# MAGIC 2. Group by `_change_type` and count the rows as `row_count`
# MAGIC 3. Save the result as `cdf_ex7_counts`

# COMMAND ----------

# EXERCISE_KEY: cdf_ex7
# TODO: Count the CDF rows per _change_type from the MERGE and save to cdf_ex7_counts

# Your code here


# COMMAND ----------

# Validate Exercise 7
result = spark.table(f"{CATALOG}.{SCHEMA}.cdf_ex7_counts")

assert {"_change_type", "row_count"} <= set(result.columns), \
    f"cdf_ex7_counts should have columns _change_type and row_count, got {result.columns}"
counts = {r["_change_type"]: r["row_count"] for r in result.collect()}
assert result.count() == 3, f"Expected one row per change type, got {result.count()} rows"
assert set(counts) == {"insert", "update_preimage", "update_postimage"}, \
    f"Unexpected change types: {sorted(counts)}"
for change_type in ["insert", "update_preimage", "update_postimage"]:
    assert counts[change_type] == 1, f"Wrong row_count for {change_type}: got {counts[change_type]}"

print("Exercise 7 passed!")
