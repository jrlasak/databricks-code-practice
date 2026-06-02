# Databricks notebook source
# COMMAND ----------
# MAGIC %md
# MAGIC # Stream-Static Joins
# MAGIC **Topic**: Streaming | **Exercises**: 7 | **Total Time**: ~95 min
# MAGIC
# MAGIC Practice the most common enrichment pattern: joining a streaming events DataFrame with
# MAGIC a static Delta dimension table. Covers inner and left-outer joins, broadcast hints,
# MAGIC predicate pushdown on the static side, what happens when the static table changes
# MAGIC mid-run, and why right-outer joins on the streaming side are unsupported.
# MAGIC
# MAGIC **Solutions**: If stuck, see `solutions/stream-static-joins-solutions.py` for hints and answers.
# MAGIC
# MAGIC **Key concepts**:
# MAGIC - Stream-static joins use a regular DataFrame `.join(static_df, ...)` - no special API
# MAGIC - Every micro-batch re-reads the static side as of its current Delta version (snapshot per batch)
# MAGIC - Left outer with the stream on the left preserves unmatched events; right outer on the streaming side is unsupported
# MAGIC - Broadcast hints (`df.hint("broadcast")`) avoid shuffling the static side on every batch
# MAGIC - Filtering the static side before the join reduces what each batch scans
# MAGIC - All streams use `.trigger(availableNow=True)` + `.awaitTermination()` so they finish and stop

# COMMAND ----------

# MAGIC %run ./00_Setup

# COMMAND ----------

# MAGIC %run ./setup/stream-static-joins-setup

# COMMAND ----------
# MAGIC %md
# MAGIC **Setup complete.** Exercise tables and the static dimension are in `db_code.stream_static_joins`.
# MAGIC
# MAGIC **Static dimension** (`customers_static`, 20 rows):
# MAGIC | Column | Type | Notes |
# MAGIC |--------|------|-------|
# MAGIC | customer_id | STRING | C-001 .. C-020 |
# MAGIC | name | STRING | Customer name |
# MAGIC | region | STRING | us-east, us-west, eu, apac |
# MAGIC | tier | STRING | gold, silver, bronze |
# MAGIC | is_active | BOOLEAN | 14 active, 6 inactive |
# MAGIC
# MAGIC **Event sources** (`ex1_source`-`ex5_source` and `ex7_source`, 50 rows each; ex6 uses two separate run tables):
# MAGIC | Column | Type | Notes |
# MAGIC |--------|------|-------|
# MAGIC | event_id | STRING | Unique event id |
# MAGIC | event_type | STRING | purchase / view / click |
# MAGIC | customer_id | STRING | 42 matched (C-001..C-015), 3 unmatched (C-991/C-992), 5 NULL |
# MAGIC | amount | DOUBLE | 0.00 for non-purchases |
# MAGIC | event_ts | TIMESTAMP | Event timestamp |
# MAGIC
# MAGIC **Exercise 6 sources**: `ex6_source_run1` (3 rows, all customer_id='C-999') and `ex6_source_run2` (2 rows, all customer_id='C-999').
# MAGIC
# MAGIC **Pattern**: Each exercise reads from its own `exN_source` via `spark.readStream.table(...)`,
# MAGIC joins with `customers_static`, and writes to `exN_target` using `trigger(availableNow=True)`.
# MAGIC Checkpoints live under `CHECKPOINT_BASE` (one subdir per exercise).

# COMMAND ----------
# MAGIC %md
# MAGIC ## Exercise 1: Inner Stream-Static Join
# MAGIC **Difficulty**: Easy | **Time**: ~10 min
# MAGIC
# MAGIC The foundational pattern: read events as a stream, join with the static customer dimension,
# MAGIC write enriched events. Inner join drops events whose `customer_id` does not match the dimension.
# MAGIC
# MAGIC **Source stream** (`ex1_source`): 50 events. 42 have matching customer_ids (C-001..C-015),
# MAGIC 3 reference unmatched ids (C-991/C-992), 5 have NULL customer_id.
# MAGIC
# MAGIC **Static side** (`customers_static`): 20 customers (C-001..C-020).
# MAGIC
# MAGIC **Target**: Write to `db_code.stream_static_joins.ex1_target`. Expected 42 rows
# MAGIC (inner join drops the 3 unmatched + 5 NULL customer_ids).
# MAGIC
# MAGIC **Requirements**:
# MAGIC 1. Read `ex1_source` as a stream with `spark.readStream.table(...)`
# MAGIC 2. Read `customers_static` as a regular (non-streaming) DataFrame with `spark.read.table(...)`
# MAGIC 3. Inner join on `customer_id`
# MAGIC 4. Write the result as Delta to `ex1_target` with `trigger(availableNow=True)` and `.awaitTermination()`
# MAGIC
# MAGIC **Constraints**:
# MAGIC - Use `trigger(availableNow=True)`, NOT `trigger(once=True)` (deprecated)
# MAGIC - Checkpoint path must be in a Volume under `CHECKPOINT_BASE`
# MAGIC - The static side is read with `spark.read`, NOT `spark.readStream`

# COMMAND ----------

# EXERCISE_KEY: stream_static_ex1
# TODO: Inner join the events stream with the static customer dimension and write to ex1_target

# Your code here


# COMMAND ----------

# Validate Exercise 1
result = spark.table(f"{CATALOG}.{SCHEMA}.ex1_target")

assert result.count() == 42, f"Expected 42 rows (inner join drops 3 unmatched + 5 NULL), got {result.count()}"
assert "name" in result.columns, "Output should have 'name' column from the static side"
assert "region" in result.columns, "Output should have 'region' column from the static side"
# No unmatched / NULL customer_ids should survive an inner join
assert result.filter("customer_id IS NULL").count() == 0, "Inner join must drop NULL customer_ids"
assert result.filter("customer_id IN ('C-991', 'C-992')").count() == 0, \
    "Inner join must drop unmatched customer_ids (C-991, C-992)"
# Specific value check: EV-001 belongs to C-001 (Alice Chen, us-east)
ev001 = result.filter("event_id = 'EV-001'").select("name", "region").collect()[0]
assert ev001["name"] == "Alice Chen", f"EV-001 should join to Alice Chen, got {ev001['name']}"
assert ev001["region"] == "us-east", f"EV-001 should join to region us-east, got {ev001['region']}"

print("Exercise 1 passed!")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Exercise 2: Project Specific Enriched Columns
# MAGIC **Difficulty**: Easy | **Time**: ~10 min
# MAGIC
# MAGIC In production you rarely want every column from the dimension. Project only what downstream
# MAGIC consumers need: `name`, `region`, `tier`. Keep all event columns.
# MAGIC
# MAGIC **Source stream** (`ex2_source`): 50 events (same schema as ex1).
# MAGIC
# MAGIC **Target**: Write to `db_code.stream_static_joins.ex2_target`. Expected 42 rows.
# MAGIC Output schema must contain exactly these columns (in any order):
# MAGIC `event_id`, `event_type`, `customer_id`, `amount`, `event_ts`, `name`, `region`, `tier`.
# MAGIC The `is_active` column from the dimension must NOT appear.
# MAGIC
# MAGIC **Requirements**:
# MAGIC 1. Inner join `ex2_source` with `customers_static` on `customer_id`
# MAGIC 2. Project the 5 event columns + only `name`, `region`, `tier` from the static side
# MAGIC 3. Drop / do not select `is_active`
# MAGIC 4. Write to `ex2_target` with `trigger(availableNow=True)`
# MAGIC
# MAGIC **Constraints**:
# MAGIC - Do NOT include `is_active` in the output
# MAGIC - Use `select(...)` after the join (not `drop("is_active")` only - explicit projection is clearer)

# COMMAND ----------

# EXERCISE_KEY: stream_static_ex2
# TODO: Inner join and project only name, region, tier from the static side

# Your code here


# COMMAND ----------

# Validate Exercise 2
result = spark.table(f"{CATALOG}.{SCHEMA}.ex2_target")

assert result.count() == 42, f"Expected 42 rows, got {result.count()}"
expected_cols = {"event_id", "event_type", "customer_id", "amount", "event_ts", "name", "region", "tier"}
actual_cols = set(result.columns)
assert actual_cols == expected_cols, f"Column mismatch.\n  Expected: {sorted(expected_cols)}\n  Got:      {sorted(actual_cols)}"
assert "is_active" not in result.columns, "is_active should NOT be projected"
# Specific value check: EV-003 belongs to C-003 (Carla Diaz, eu, gold)
ev003 = result.filter("event_id = 'EV-003'").select("name", "region", "tier").collect()[0]
assert ev003["name"] == "Carla Diaz", f"EV-003 name expected Carla Diaz, got {ev003['name']}"
assert ev003["tier"] == "gold", f"EV-003 tier expected gold, got {ev003['tier']}"

print("Exercise 2 passed!")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Exercise 3: Left Outer Join (Stream on the Left)
# MAGIC **Difficulty**: Medium | **Time**: ~15 min
# MAGIC
# MAGIC Inner join silently drops events whose `customer_id` does not match the dimension. In production
# MAGIC you usually want to KEEP those events with nulls in the dimension columns, so downstream
# MAGIC consumers can route or count unmatched traffic. Stream-static left-outer with the stream on
# MAGIC the left is supported and is the default pattern for "enrich but never lose".
# MAGIC
# MAGIC **Source stream** (`ex3_source`): 50 events. 42 matched + 3 unmatched (C-991/C-992) + 5 NULL = 50 rows expected.
# MAGIC
# MAGIC **Target**: Write to `db_code.stream_static_joins.ex3_target`. Expected 50 rows.
# MAGIC - 42 rows with `name` populated
# MAGIC - 8 rows with `name IS NULL` (3 unmatched + 5 NULL customer_id)
# MAGIC
# MAGIC **Requirements**:
# MAGIC 1. Left outer join `ex3_source` (left) with `customers_static` (right) on `customer_id`
# MAGIC 2. Stream must be on the LEFT side of the join
# MAGIC 3. All 50 events must appear in the output
# MAGIC 4. Unmatched / NULL events should have NULL for `name`, `region`, `tier`, `is_active`
# MAGIC 5. Write to `ex3_target` with `trigger(availableNow=True)`
# MAGIC
# MAGIC **Constraints**:
# MAGIC - The join type string is `"left"` or `"left_outer"`
# MAGIC - Do NOT swap sides - stream MUST be on the left

# COMMAND ----------

# EXERCISE_KEY: stream_static_ex3
# TODO: Left outer join with stream on the left, write to ex3_target

# Your code here


# COMMAND ----------

# Validate Exercise 3
result = spark.table(f"{CATALOG}.{SCHEMA}.ex3_target")

assert result.count() == 50, f"Expected 50 rows (left outer keeps all events), got {result.count()}"
# Matched rows: name populated
matched = result.filter("name IS NOT NULL").count()
assert matched == 42, f"Expected 42 matched rows (name populated), got {matched}"
# Unmatched rows: name null
unmatched = result.filter("name IS NULL").count()
assert unmatched == 8, f"Expected 8 unmatched rows (3 unmatched id + 5 NULL customer_id), got {unmatched}"
# Specific value check: EV-036 (C-991) is unmatched -> name is null
ev036 = result.filter("event_id = 'EV-036'").select("name", "customer_id").collect()[0]
assert ev036["customer_id"] == "C-991", f"EV-036 customer_id should be C-991"
assert ev036["name"] is None, f"EV-036 name should be NULL (C-991 unmatched), got {ev036['name']}"
# EV-039 has NULL customer_id -> name is null
ev039 = result.filter("event_id = 'EV-039'").select("name", "customer_id").collect()[0]
assert ev039["customer_id"] is None and ev039["name"] is None, \
    f"EV-039 should have NULL customer_id and NULL name, got customer_id={ev039['customer_id']}, name={ev039['name']}"

print("Exercise 3 passed!")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Exercise 4: Filter the Static Side Before Joining
# MAGIC **Difficulty**: Medium | **Time**: ~10 min
# MAGIC
# MAGIC The dimension is read on every micro-batch. Filtering it BEFORE the join reduces what each
# MAGIC batch scans and what each task carries through the join. Here, restrict the static side to
# MAGIC active customers only - any event whose customer is inactive becomes "no match".
# MAGIC
# MAGIC **Source stream** (`ex4_source`): 50 events.
# MAGIC
# MAGIC **Static side**: `customers_static` filtered to `is_active = TRUE` (14 customers).
# MAGIC Inactive customers in C-001..C-015 are: C-004, C-007, C-011, C-014 (4 inactive in the matched-id range).
# MAGIC
# MAGIC **Target**: Write to `db_code.stream_static_joins.ex4_target`. Inner join + active-only filter.
# MAGIC - Events whose `customer_id` matches an INACTIVE customer become unmatched and are dropped (inner join)
# MAGIC - Expected row count: 42 (matched) minus the events for inactive customers.
# MAGIC - In the source, events for inactive customers (C-004, C-007, C-011, C-014) total 10 rows.
# MAGIC - So expected = 42 - 10 = 32 rows.
# MAGIC
# MAGIC **Requirements**:
# MAGIC 1. Filter `customers_static` to `is_active = TRUE` BEFORE the join
# MAGIC 2. Inner join the events stream with the filtered dimension on `customer_id`
# MAGIC 3. Write to `ex4_target` with `trigger(availableNow=True)`
# MAGIC
# MAGIC **Constraints**:
# MAGIC - Apply the filter on the static DataFrame, NOT in a WHERE on the joined output (the goal is to scan less)
# MAGIC - Inner join (not left outer) - inactive matches should be dropped

# COMMAND ----------

# EXERCISE_KEY: stream_static_ex4
# TODO: Filter customers_static to active, then inner join with the events stream

# Your code here


# COMMAND ----------

# Validate Exercise 4
result = spark.table(f"{CATALOG}.{SCHEMA}.ex4_target")

assert result.count() == 32, f"Expected 32 rows (42 matched - 10 inactive-customer events), got {result.count()}"
# No inactive customers should appear in the output
inactive_present = result.filter("customer_id IN ('C-004', 'C-007', 'C-011', 'C-014')").count()
assert inactive_present == 0, f"Expected 0 events for inactive customers, got {inactive_present}"
# Active customer C-001 should still appear
active_present = result.filter("customer_id = 'C-001'").count()
assert active_present > 0, f"Active customer C-001 should have events in the output"
# is_active should not be FALSE anywhere (we joined against active-only)
false_active = result.filter("is_active = FALSE").count()
assert false_active == 0, f"Joined output should never contain is_active=FALSE, got {false_active}"

print("Exercise 4 passed!")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Exercise 5: Broadcast the Static Dimension
# MAGIC **Difficulty**: Medium | **Time**: ~15 min
# MAGIC
# MAGIC When the static side is small (fits in driver/executor memory), broadcast it instead of
# MAGIC shuffling the stream by `customer_id` on every micro-batch. On Free Edition serverless
# MAGIC `spark.conf.set` is not available, so apply the hint on the DataFrame itself:
# MAGIC `customers_df.hint("broadcast")`.
# MAGIC
# MAGIC **Source stream** (`ex5_source`): 50 events.
# MAGIC
# MAGIC **Target**: Write to `db_code.stream_static_joins.ex5_target`. Expected 42 rows (inner join).
# MAGIC
# MAGIC **Requirements**:
# MAGIC 1. Apply `.hint("broadcast")` to the static `customers_static` DataFrame
# MAGIC 2. Inner join the events stream with the hinted static side on `customer_id`
# MAGIC 3. Capture the join's explain plan as a STRING and store one row in the `ex5_explain` table
# MAGIC    with column `plan STRING`. The captured plan must contain `BroadcastHashJoin`.
# MAGIC 4. Write the streaming join result to `ex5_target` with `trigger(availableNow=True)`
# MAGIC
# MAGIC **Note**: streaming queries materialize plans per micro-batch, so capturing the plan of
# MAGIC the streaming DataFrame directly is unreliable. Build the equivalent BATCH join (read
# MAGIC `ex5_source` with `spark.read`, apply the same broadcast-hinted join) and capture its
# MAGIC `.explain(extended=False)` output. The physical join operator is identical.
# MAGIC
# MAGIC **Constraints**:
# MAGIC - Use DataFrame `.hint("broadcast")` (NOT `spark.conf.set` - blocked on serverless)
# MAGIC - The broadcast hint must be on the small (static) side, not the stream
# MAGIC - The explain plan saved to `ex5_explain` must contain the substring "BroadcastHashJoin"

# COMMAND ----------

# EXERCISE_KEY: stream_static_ex5
# TODO: Broadcast-hint the static side, join, and capture the explain plan to ex5_explain

# Your code here


# COMMAND ----------

# Validate Exercise 5
result = spark.table(f"{CATALOG}.{SCHEMA}.ex5_target")
explain = spark.table(f"{CATALOG}.{SCHEMA}.ex5_explain")

assert result.count() == 42, f"Expected 42 rows from inner broadcast join, got {result.count()}"
assert explain.count() == 1, f"Expected 1 row in ex5_explain (the plan string), got {explain.count()}"
plan_str = explain.collect()[0]["plan"]
assert plan_str is not None and len(plan_str) > 0, "ex5_explain.plan should be non-empty"
assert "BroadcastHashJoin" in plan_str, \
    f"Expected 'BroadcastHashJoin' in explain plan (proves the hint took effect). Got plan:\n{plan_str[:500]}"
# Sanity: standard inner-join row check
ev001 = result.filter("event_id = 'EV-001'").select("name").collect()[0]["name"]
assert ev001 == "Alice Chen", f"EV-001 should join to Alice Chen, got {ev001}"

print("Exercise 5 passed!")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Exercise 6: Picking Up a Static-Side Refresh Between Stream Runs
# MAGIC **Difficulty**: Hard | **Time**: ~20 min
# MAGIC
# MAGIC Stream-static joins do NOT pin the static side - each micro-batch (and each new `availableNow`
# MAGIC run) re-reads the dimension at its current Delta version, so a refresh that lands between
# MAGIC batches/runs is automatically visible. You will run two `availableNow` streams separated by an
# MAGIC INSERT and observe the second one seeing rows the first couldn't.
# MAGIC
# MAGIC The static dimension (`customers_static`) currently has NO customer `C-999`.
# MAGIC Both source batches in this exercise contain ONLY events for `C-999`.
# MAGIC
# MAGIC **Source streams**:
# MAGIC - `ex6_source_run1`: 3 events, all `customer_id = 'C-999'`
# MAGIC - `ex6_source_run2`: 2 events, all `customer_id = 'C-999'`
# MAGIC
# MAGIC **Targets**:
# MAGIC - `ex6_target_run1`: inner-join result of run 1 (BEFORE adding C-999) -> expected 0 rows
# MAGIC - `ex6_target_run2`: inner-join result of run 2 (AFTER adding C-999) -> expected 2 rows
# MAGIC
# MAGIC **Requirements**:
# MAGIC 1. **Run 1**: Inner join `ex6_source_run1` with `customers_static` on `customer_id`. Write
# MAGIC    to `ex6_target_run1` with `trigger(availableNow=True)` and `.awaitTermination()`.
# MAGIC    Result will be empty (C-999 doesn't exist yet).
# MAGIC 2. **Refresh the dimension**: After run 1 finishes, INSERT a new row into `customers_static`:
# MAGIC    `('C-999', 'New Customer', 'us-east', 'gold', TRUE)`.
# MAGIC 3. **Run 2**: Inner join `ex6_source_run2` with `customers_static` (now refreshed) on
# MAGIC    `customer_id`. Write to `ex6_target_run2` with `trigger(availableNow=True)`.
# MAGIC    Result will contain 2 enriched rows.
# MAGIC
# MAGIC **Constraints**:
# MAGIC - Use a DIFFERENT checkpoint subdirectory for each run (run1 and run2 are separate queries)
# MAGIC - The two source batches are separate tables; do NOT combine them into one stream
# MAGIC - The INSERT into `customers_static` is regular SQL, run between the two streams

# COMMAND ----------

# EXERCISE_KEY: stream_static_ex6
# TODO: Run two stream-static joins separated by an INSERT into customers_static

# Your code here


# COMMAND ----------

# Validate Exercise 6
result1 = spark.table(f"{CATALOG}.{SCHEMA}.ex6_target_run1")
result2 = spark.table(f"{CATALOG}.{SCHEMA}.ex6_target_run2")

# Run 1: C-999 does not exist yet -> inner join produces 0 rows
assert result1.count() == 0, f"Expected 0 rows in ex6_target_run1 (C-999 not in dim yet), got {result1.count()}"

# Refresh actually happened: customers_static now contains C-999
c999 = spark.table(f"{CATALOG}.{SCHEMA}.customers_static").filter("customer_id = 'C-999'")
assert c999.count() == 1, f"customers_static should now contain C-999 (the refresh insert). Found {c999.count()}"

# Run 2: After refresh, inner join produces 2 rows
assert result2.count() == 2, f"Expected 2 rows in ex6_target_run2 (after refresh), got {result2.count()}"
# Both rows must have the new customer's name
names = [r["name"] for r in result2.select("name").collect()]
assert all(n == "New Customer" for n in names), f"All ex6_target_run2 rows should have name='New Customer', got {names}"

print("Exercise 6 passed!")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Exercise 7: The Unsupported Right Outer - Diagnose and Choose the Correct Alternative
# MAGIC **Difficulty**: Hard | **Time**: ~15 min
# MAGIC
# MAGIC A right outer join with the **stream on the left** and the **static on the right** is unsupported
# MAGIC in Structured Streaming. The semantics would require emitting one row per static row that has
# MAGIC no match in the stream - but the stream is unbounded, so "no match" is never decidable. Spark
# MAGIC raises `AnalysisException: Right outer/Full outer joins are not supported with streaming
# MAGIC DataFrame/Dataset on the left side.`
# MAGIC
# MAGIC The correct alternative when you want to KEEP all events including unmatched ones is a
# MAGIC LEFT OUTER join with the stream on the left (Exercise 3's pattern). For this exercise, do both:
# MAGIC 1. Attempt the right outer join and catch the exception
# MAGIC 2. Implement the correct alternative (left outer with stream on the left) and write the result
# MAGIC
# MAGIC **Source stream** (`ex7_source`): 50 events (42 matched, 3 unmatched, 5 NULL customer_id).
# MAGIC
# MAGIC **Targets**:
# MAGIC - `ex7_error_log`: 1 row, column `error_msg STRING`. Contains the exception message text from the
# MAGIC   failed right outer join attempt (must contain the substring "Right outer").
# MAGIC - `ex7_target`: Left outer join result (the correct alternative). Expected 50 rows.
# MAGIC
# MAGIC **Requirements**:
# MAGIC 1. Attempt `events_stream.join(customers_static, "customer_id", "right_outer")` and start a
# MAGIC    streaming write. Wrap in `try / except` to catch `AnalysisException` (or any Exception).
# MAGIC    Capture the exception message as a STRING.
# MAGIC 2. Save the captured error message to `ex7_error_log` as a single row with column `error_msg`.
# MAGIC 3. Implement the correct alternative: left outer join with the stream on the left.
# MAGIC 4. Write the alternative result to `ex7_target` with `trigger(availableNow=True)`.
# MAGIC
# MAGIC **Constraints**:
# MAGIC - The exception must be caught at analysis time (when `writeStream...start()` or `.toTable()` is
# MAGIC   called). The error message stored must mention right outer / not supported (the assertion
# MAGIC   checks for "right" or "Right" + "outer" tokens in the message).
# MAGIC - The correct-alternative output (`ex7_target`) must include the 8 unmatched events with NULL dimension columns.

# COMMAND ----------

# EXERCISE_KEY: stream_static_ex7
# TODO: Attempt right outer (catch exception), then implement left outer as the correct alternative

# Your code here


# COMMAND ----------

# Validate Exercise 7
error_log = spark.table(f"{CATALOG}.{SCHEMA}.ex7_error_log")
result = spark.table(f"{CATALOG}.{SCHEMA}.ex7_target")

# Error log: 1 row containing the analysis-exception message
assert error_log.count() == 1, f"Expected 1 row in ex7_error_log, got {error_log.count()}"
err = error_log.collect()[0]["error_msg"]
assert err is not None and len(err) > 0, "ex7_error_log.error_msg should be non-empty"
err_lower = err.lower()
assert "right" in err_lower and "outer" in err_lower, \
    f"Expected 'right' and 'outer' tokens in the caught exception message. Got:\n{err[:500]}"

# Correct alternative: left outer keeps all 50 events
assert result.count() == 50, f"Expected 50 rows from left outer alternative, got {result.count()}"
unmatched = result.filter("name IS NULL").count()
assert unmatched == 8, f"Expected 8 unmatched rows (NULL name), got {unmatched}"
matched = result.filter("name IS NOT NULL").count()
assert matched == 42, f"Expected 42 matched rows (name populated), got {matched}"

print("Exercise 7 passed!")
