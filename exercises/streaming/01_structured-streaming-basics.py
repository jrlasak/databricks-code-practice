# Databricks notebook source
# COMMAND ----------
# MAGIC %md
# MAGIC # Structured Streaming Basics
# MAGIC **Topic**: Streaming | **Exercises**: 8 | **Total Time**: ~90 min
# MAGIC
# MAGIC Practice the Structured Streaming foundation on Databricks: `readStream` and `writeStream`
# MAGIC against Delta tables, choosing triggers (`processingTime` vs `availableNow`), picking the
# MAGIC right output mode (`append` / `complete` / `update`), naming queries and reading
# MAGIC `lastProgress`, and deduplicating streams. Every query uses `trigger(availableNow=True)`
# MAGIC and `awaitTermination()` so the notebook terminates cleanly on Free Edition.
# MAGIC
# MAGIC **Solutions**: If stuck, see `solutions/structured-streaming-basics-solutions.py` for hints and answers.
# MAGIC
# MAGIC **Key concepts**:
# MAGIC - `spark.readStream.table(...)` reads a Delta table as a stream of newly committed rows
# MAGIC - `trigger(availableNow=True)` processes everything available and stops (use this on Free Edition)
# MAGIC - Output modes: `append` (only new rows), `complete` (full result table), `update` (changed rows)
# MAGIC - Streaming queries always need a `checkpointLocation` for state and offset tracking
# MAGIC - `.queryName(...)` makes a stream identifiable; `query.lastProgress` exposes runtime metrics
# MAGIC - `dropDuplicates(["key"])` deduplicates a stream using watermarked state

# COMMAND ----------

# MAGIC %run ./00_Setup

# COMMAND ----------

# MAGIC %run ./setup/structured-streaming-basics-setup

# COMMAND ----------
# MAGIC %md
# MAGIC **Setup complete.** Per-exercise source tables and checkpoint paths are ready under
# MAGIC `db_code.structured_streaming_basics`.
# MAGIC
# MAGIC **Sources** (Delta tables in `db_code.structured_streaming_basics`):
# MAGIC - Each exercise has its own `exN_source` table cloned from `db_code.streaming.events_source`
# MAGIC - Exercise 8 has a custom source with intentional duplicate `event_id`s
# MAGIC
# MAGIC **Targets**: each exercise writes to its own `exN_target` table (Ex 5 writes to
# MAGIC `ex5_target_available_now`; Ex 7 writes both a target table and an `ex7_progress` metrics table).
# MAGIC
# MAGIC **Checkpoints**: use `f"{CHECKPOINT_BASE}/exN"` (Ex 5 uses `/ex5_an` for the `availableNow` query).
# MAGIC
# MAGIC **Pattern**: every streaming query MUST use `.trigger(availableNow=True)` and
# MAGIC `.awaitTermination()` so the notebook does not block.

# COMMAND ----------
# MAGIC %md
# MAGIC ## Exercise 1: Read a Delta Table as a Stream, Write to Another Delta Table
# MAGIC **Difficulty**: Easy | **Time**: ~10 min
# MAGIC
# MAGIC The foundational Structured Streaming pattern: treat an existing Delta table as a stream
# MAGIC source and write the streamed rows to a target Delta table in append mode.
# MAGIC
# MAGIC **Source** (`db_code.structured_streaming_basics.ex1_source`): 35 events with columns
# MAGIC `event_id STRING, event_type STRING, user_id STRING, amount DOUBLE, event_ts TIMESTAMP`.
# MAGIC
# MAGIC **Target**: Write to `db_code.structured_streaming_basics.ex1_target`. Expected 35 rows.
# MAGIC
# MAGIC **Requirements**:
# MAGIC 1. Read `ex1_source` with `spark.readStream` (Delta source)
# MAGIC 2. Write to `ex1_target` as Delta with output mode `append`
# MAGIC 3. Set a `checkpointLocation` under `CHECKPOINT_BASE` (e.g., `f"{CHECKPOINT_BASE}/ex1"`)
# MAGIC 4. Use `.trigger(availableNow=True)` so the stream terminates after one batch
# MAGIC 5. Call `.awaitTermination()` so the next cell runs only after the stream finishes
# MAGIC
# MAGIC **Constraints**:
# MAGIC - Use `trigger(availableNow=True)` (NOT `trigger(once=True)`, NOT `trigger(processingTime=...)`)
# MAGIC - Checkpoint path must be in a Volume

# COMMAND ----------

# EXERCISE_KEY: ss_basics_ex1
# TODO: Read ex1_source as a stream and write all rows to ex1_target

# Your code here


# COMMAND ----------

# Validate Exercise 1
result = spark.table(f"{CATALOG}.{SCHEMA}.ex1_target")

assert result.count() == 35, f"Expected 35 rows, got {result.count()}"
assert "event_id" in result.columns, "Missing event_id column"
assert "amount" in result.columns, "Missing amount column"
assert result.filter("event_id = 'EV-001'").count() == 1, "EV-001 should exist"
assert result.filter("event_id = 'EV-035'").count() == 1, "EV-035 should exist"
ev3_amount = result.filter("event_id = 'EV-003'").select("amount").collect()[0][0]
assert ev3_amount == 120.50, f"EV-003 amount should be 120.50, got {ev3_amount}"

print("Exercise 1 passed!")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Exercise 2: Filter a Streaming DataFrame Before Writing
# MAGIC **Difficulty**: Easy | **Time**: ~10 min
# MAGIC
# MAGIC Transformations on streaming DataFrames work exactly like batch transformations - filters,
# MAGIC projections, and column derivations all compose into the streaming plan. Filter the events
# MAGIC stream to keep only `purchase` events before writing.
# MAGIC
# MAGIC **Source** (`db_code.structured_streaming_basics.ex2_source`): 35 events. 18 rows have
# MAGIC `event_type = 'purchase'`, the rest are `view` or `click`.
# MAGIC
# MAGIC **Target**: Write to `db_code.structured_streaming_basics.ex2_target`. Expected 18 rows,
# MAGIC all with `event_type = 'purchase'`.
# MAGIC
# MAGIC **Requirements**:
# MAGIC 1. Read `ex2_source` with `spark.readStream`
# MAGIC 2. Keep only rows where `event_type = 'purchase'`
# MAGIC 3. Write to `ex2_target` as Delta in append mode
# MAGIC 4. Use `.trigger(availableNow=True)` and `.awaitTermination()`
# MAGIC 5. Checkpoint at `f"{CHECKPOINT_BASE}/ex2"`
# MAGIC
# MAGIC **Constraints**:
# MAGIC - Apply the filter on the streaming DataFrame, not as a post-write SQL query
# MAGIC - Output must contain ONLY purchase events

# COMMAND ----------

# EXERCISE_KEY: ss_basics_ex2
# TODO: Filter the stream to only purchase events and write to ex2_target

# Your code here


# COMMAND ----------

# Validate Exercise 2
result = spark.table(f"{CATALOG}.{SCHEMA}.ex2_target")

assert result.count() == 18, f"Expected 18 rows, got {result.count()}"
non_purchase = result.filter("event_type != 'purchase'").count()
assert non_purchase == 0, f"Expected 0 non-purchase rows, got {non_purchase}"
assert result.filter("event_id = 'EV-001'").count() == 1, "EV-001 (purchase) should be present"
assert result.filter("event_id = 'EV-002'").count() == 0, "EV-002 (view) should be filtered out"

print("Exercise 2 passed!")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Exercise 3: Add a Derived Column to a Streaming DataFrame
# MAGIC **Difficulty**: Easy | **Time**: ~10 min
# MAGIC
# MAGIC Streaming DataFrames support `withColumn` just like batch DataFrames. Add a derived
# MAGIC column `amount_with_tax` equal to `amount * 1.10` (10% tax) before writing.
# MAGIC
# MAGIC **Source** (`db_code.structured_streaming_basics.ex3_source`): 35 events with the standard
# MAGIC schema. `amount` is DOUBLE.
# MAGIC
# MAGIC **Target**: Write to `db_code.structured_streaming_basics.ex3_target`. Expected 35 rows.
# MAGIC The target must include all original columns PLUS an `amount_with_tax` DOUBLE column.
# MAGIC
# MAGIC **Requirements**:
# MAGIC 1. Read `ex3_source` with `spark.readStream`
# MAGIC 2. Add a new column `amount_with_tax = amount * 1.10`
# MAGIC 3. Write to `ex3_target` as Delta in append mode
# MAGIC 4. Use `.trigger(availableNow=True)` and `.awaitTermination()`
# MAGIC 5. Checkpoint at `f"{CHECKPOINT_BASE}/ex3"`
# MAGIC
# MAGIC **Constraints**:
# MAGIC - The new column must be named exactly `amount_with_tax`
# MAGIC - The original `amount` column must still be present

# COMMAND ----------

# EXERCISE_KEY: ss_basics_ex3
# TODO: Add amount_with_tax = amount * 1.10 and write to ex3_target

# Your code here


# COMMAND ----------

# Validate Exercise 3
result = spark.table(f"{CATALOG}.{SCHEMA}.ex3_target")

assert result.count() == 35, f"Expected 35 rows, got {result.count()}"
assert "amount_with_tax" in result.columns, "Missing amount_with_tax column"
assert "amount" in result.columns, "Original amount column must still be present"

# EV-001 has amount=50.00, so amount_with_tax should be 55.00
ev1_tax = result.filter("event_id = 'EV-001'").select("amount_with_tax").collect()[0][0]
assert abs(ev1_tax - 55.00) < 1e-6, f"EV-001 amount_with_tax should be 55.00, got {ev1_tax}"
# EV-003 has amount=120.50, so amount_with_tax should be 132.55
ev3_tax = result.filter("event_id = 'EV-003'").select("amount_with_tax").collect()[0][0]
assert abs(ev3_tax - 132.55) < 1e-6, f"EV-003 amount_with_tax should be 132.55, got {ev3_tax}"

print("Exercise 3 passed!")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Exercise 4: Append-Only Ingestion - Pick the Right Output Mode
# MAGIC **Difficulty**: Medium | **Time**: ~10 min
# MAGIC
# MAGIC Output mode is one of `append`, `complete`, or `update`. Each is only legal for certain
# MAGIC query shapes. For an ingestion sink that simply copies streamed events into a target,
# MAGIC only ONE mode is correct - the other two fail. This exercise has you write the pipeline
# MAGIC AND record which mode is correct (and which two would fail and why).
# MAGIC
# MAGIC **Source** (`db_code.structured_streaming_basics.ex4_source`): 35 events.
# MAGIC
# MAGIC **Target**: Write to `db_code.structured_streaming_basics.ex4_target`. Expected 35 rows.
# MAGIC
# MAGIC **Requirements**:
# MAGIC 1. Read `ex4_source` and write it unchanged to `ex4_target`
# MAGIC 2. Pass the correct output mode to `outputMode(...)` on the writeStream
# MAGIC 3. Use `.trigger(availableNow=True)` and `.awaitTermination()`
# MAGIC 4. Checkpoint at `f"{CHECKPOINT_BASE}/ex4"`
# MAGIC 5. Set a Python variable `correct_mode` to the string of the mode you used (one of
# MAGIC    `"append"`, `"complete"`, `"update"`)
# MAGIC
# MAGIC **Constraints**:
# MAGIC - The pipeline has no aggregation, so `complete` and `update` are illegal here
# MAGIC - You must call `outputMode(...)` explicitly even if the default would work

# COMMAND ----------

# EXERCISE_KEY: ss_basics_ex4
# TODO: Stream from ex4_source to ex4_target with the correct output mode
# Set `correct_mode` to one of "append", "complete", "update".

# Your code here


# COMMAND ----------

# Validate Exercise 4
result = spark.table(f"{CATALOG}.{SCHEMA}.ex4_target")

assert result.count() == 35, f"Expected 35 rows, got {result.count()}"
assert "correct_mode" in dir(), "Set a variable `correct_mode` to your chosen output mode"
assert correct_mode == "append", \
    f"For an append-only ingestion sink (no aggregation), the correct mode is 'append'. Got '{correct_mode}'"
# Verify a specific value to catch wrong data
ev5_amount = result.filter("event_id = 'EV-005'").select("amount").collect()[0][0]
assert ev5_amount == 75.25, f"EV-005 amount should be 75.25, got {ev5_amount}"

print("Exercise 4 passed!")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Exercise 5: Trigger Modes - Understanding the Cost/Latency Tradeoff
# MAGIC **Difficulty**: Medium | **Time**: ~10 min
# MAGIC
# MAGIC Triggers control when a micro-batch fires. Two modes you must know:
# MAGIC - `processingTime='30 seconds'`: long-running query, fires every 30 seconds, keeps compute
# MAGIC   warm. Lower latency, higher cost.
# MAGIC - `availableNow=True`: processes everything currently available, then stops. Compute spins
# MAGIC   down between scheduled runs. Higher latency, lower cost.
# MAGIC
# MAGIC **Free Edition note**: `Trigger.ProcessingTime` is NOT supported on Free Edition (which is
# MAGIC serverless-only). Per the Databricks docs: "On serverless compute, only `Trigger.AvailableNow()`
# MAGIC and `Trigger.Once()` are supported." You would only see processingTime in production on
# MAGIC classic compute. In this exercise you write the `availableNow` query and identify which
# MAGIC trigger wins on latency and which wins on cost.
# MAGIC
# MAGIC **Source** (`db_code.structured_streaming_basics.ex5_source`): 35 events.
# MAGIC
# MAGIC **Target** (`ex5_target_available_now`): 35 rows written by the `availableNow` query.
# MAGIC
# MAGIC **Requirements**:
# MAGIC 1. Write a streaming query with `.trigger(availableNow=True)`, sink to
# MAGIC    `ex5_target_available_now`, checkpoint `f"{CHECKPOINT_BASE}/ex5_an"`, then
# MAGIC    `.awaitTermination()`.
# MAGIC 2. Set a Python variable `latency_winner` to the trigger string that gives LOWER LATENCY
# MAGIC    (one of `"processingTime"` or `"availableNow"`).
# MAGIC 3. Set a Python variable `cost_winner` to the trigger string that minimizes COST on Free
# MAGIC    Edition / scheduled jobs (the one where compute spins down between runs).
# MAGIC
# MAGIC **Constraints**:
# MAGIC - Do NOT use `.trigger(processingTime=...)` here - it would fail on Free Edition serverless.
# MAGIC - Use a checkpoint location in a Volume.

# COMMAND ----------

# EXERCISE_KEY: ss_basics_ex5
# TODO: Write the availableNow streaming query, then set `latency_winner` and `cost_winner`.

# Your code here


# COMMAND ----------

# Validate Exercise 5
an = spark.table(f"{CATALOG}.{SCHEMA}.ex5_target_available_now")

assert an.count() == 35, f"availableNow target should have 35 rows, got {an.count()}"

assert "latency_winner" in dir(), "Set `latency_winner` to 'processingTime' or 'availableNow'"
assert "cost_winner" in dir(), "Set `cost_winner` to 'processingTime' or 'availableNow'"
assert latency_winner == "processingTime", \
    f"processingTime gives lower latency (continuous compute). Got '{latency_winner}'"
assert cost_winner == "availableNow", \
    f"availableNow minimizes cost (compute spins down between runs). Got '{cost_winner}'"

print("Exercise 5 passed!")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Exercise 6: Streaming Aggregation in Complete Output Mode
# MAGIC **Difficulty**: Medium | **Time**: ~10 min
# MAGIC
# MAGIC Stateful aggregations (groupBy without windowing) require `complete` output mode when
# MAGIC writing to a Delta sink without a watermark - the engine emits the FULL result table after
# MAGIC every micro-batch, overwriting the previous result. This is fine for small grouping
# MAGIC dimensions (handful of event types). For large key spaces, you would add a watermark and
# MAGIC switch to `update`.
# MAGIC
# MAGIC **Source** (`db_code.structured_streaming_basics.ex6_source`): 35 events with 3 distinct
# MAGIC `event_type` values: `purchase` (18), `view` (9), `click` (8).
# MAGIC
# MAGIC **Target**: Write to `db_code.structured_streaming_basics.ex6_target`. Expected 3 rows,
# MAGIC one per `event_type`, with a column `cnt` holding the count.
# MAGIC
# MAGIC **Requirements**:
# MAGIC 1. Read `ex6_source` with `spark.readStream`
# MAGIC 2. Group by `event_type` and compute `count(*) AS cnt`
# MAGIC 3. Write to `ex6_target` with `outputMode("complete")`
# MAGIC 4. Use `.trigger(availableNow=True)` and `.awaitTermination()`
# MAGIC 5. Checkpoint at `f"{CHECKPOINT_BASE}/ex6"`
# MAGIC
# MAGIC **Constraints**:
# MAGIC - The count column MUST be named `cnt` (the validator checks this name)
# MAGIC - Use `complete` mode - `append` would fail because there is no watermark, and the result
# MAGIC   table is being overwritten on every batch

# COMMAND ----------

# EXERCISE_KEY: ss_basics_ex6
# TODO: Count events per event_type as a streaming aggregation in complete mode.

# Your code here


# COMMAND ----------

# Validate Exercise 6
result = spark.table(f"{CATALOG}.{SCHEMA}.ex6_target")

assert result.count() == 3, f"Expected 3 rows (one per event_type), got {result.count()}"
assert "event_type" in result.columns, "Missing event_type column"
assert "cnt" in result.columns, "Count column must be named 'cnt'"

purchase_cnt = result.filter("event_type = 'purchase'").select("cnt").collect()[0][0]
view_cnt = result.filter("event_type = 'view'").select("cnt").collect()[0][0]
click_cnt = result.filter("event_type = 'click'").select("cnt").collect()[0][0]

assert purchase_cnt == 18, f"purchase count should be 18, got {purchase_cnt}"
assert view_cnt == 9, f"view count should be 9, got {view_cnt}"
assert click_cnt == 8, f"click count should be 8, got {click_cnt}"

print("Exercise 6 passed!")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Exercise 7: Name a Query and Inspect lastProgress
# MAGIC **Difficulty**: Medium | **Time**: ~10 min
# MAGIC
# MAGIC Production streams need observability. `.queryName(...)` gives a stream a human-readable
# MAGIC identifier; `query.lastProgress` exposes the most recent batch's metrics
# MAGIC (`numInputRows`, `processedRowsPerSecond`, `inputRowsPerSecond`, batch durations, etc.).
# MAGIC In this exercise you start a named streaming write, wait for it to finish, then read
# MAGIC `lastProgress` to capture metrics and store them in a Delta metrics table.
# MAGIC
# MAGIC **Source** (`db_code.structured_streaming_basics.ex7_source`): 35 events.
# MAGIC
# MAGIC **Targets**:
# MAGIC - `ex7_target`: written by the stream (35 rows)
# MAGIC - `ex7_progress`: 1 row with columns `query_name STRING`, `num_input_rows BIGINT`
# MAGIC
# MAGIC **Requirements**:
# MAGIC 1. Start a streaming write from `ex7_source` to `ex7_target` with `.queryName("ss_basics_ex7")`
# MAGIC 2. Use `.trigger(availableNow=True)` and `.awaitTermination()`
# MAGIC 3. Checkpoint at `f"{CHECKPOINT_BASE}/ex7"`
# MAGIC 4. After the stream terminates, read `query.lastProgress` and extract the query name and
# MAGIC    `numInputRows`. Write these as a 1-row Delta table `ex7_progress` with columns
# MAGIC    `query_name STRING`, `num_input_rows BIGINT`.
# MAGIC
# MAGIC **Constraints**:
# MAGIC - The query name string MUST be `ss_basics_ex7`
# MAGIC - Use `lastProgress`, not `recentProgress` (the validator checks the single-batch row)

# COMMAND ----------

# EXERCISE_KEY: ss_basics_ex7
# TODO: Start a named stream, then write its lastProgress metrics to ex7_progress.

# Your code here


# COMMAND ----------

# Validate Exercise 7
target = spark.table(f"{CATALOG}.{SCHEMA}.ex7_target")
progress = spark.table(f"{CATALOG}.{SCHEMA}.ex7_progress")

assert target.count() == 35, f"ex7_target should have 35 rows, got {target.count()}"
assert progress.count() == 1, f"ex7_progress should have exactly 1 row, got {progress.count()}"

row = progress.collect()[0]
assert row["query_name"] == "ss_basics_ex7", \
    f"query_name should be 'ss_basics_ex7', got '{row['query_name']}'"
# Total numInputRows across the single availableNow batch must equal source row count
assert row["num_input_rows"] == 35, \
    f"num_input_rows should be 35 (rows in source), got {row['num_input_rows']}"

print("Exercise 7 passed!")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Exercise 8: Deduplicate a Stream with dropDuplicates
# MAGIC **Difficulty**: Hard | **Time**: ~20 min
# MAGIC
# MAGIC Exactly-once delivery is a common requirement: even if the source delivers an event
# MAGIC multiple times (retries, replay, upstream bug), the target must contain each event_id
# MAGIC exactly once. `dropDuplicates(["event_id"])` on a streaming DataFrame stores seen keys in
# MAGIC state and drops duplicates on subsequent batches.
# MAGIC
# MAGIC **Source** (`db_code.structured_streaming_basics.ex8_source`): 15 rows with deliberate
# MAGIC duplicates -
# MAGIC - EV-101 appears 3 times
# MAGIC - EV-102, EV-104, EV-107 each appear 2 times
# MAGIC - The remaining 6 event_ids appear once
# MAGIC - **Total unique event_ids**: 10
# MAGIC
# MAGIC **Target**: Write to `db_code.structured_streaming_basics.ex8_target`. Expected 10 rows,
# MAGIC one per unique `event_id`. No duplicate `event_id` may appear.
# MAGIC
# MAGIC **Requirements**:
# MAGIC 1. Read `ex8_source` with `spark.readStream`
# MAGIC 2. Apply `dropDuplicates(["event_id"])` on the streaming DataFrame
# MAGIC 3. Write to `ex8_target` as Delta in append mode
# MAGIC 4. Use `.trigger(availableNow=True)` and `.awaitTermination()`
# MAGIC 5. Checkpoint at `f"{CHECKPOINT_BASE}/ex8"`
# MAGIC
# MAGIC **Constraints**:
# MAGIC - Use `dropDuplicates` on the STREAMING DataFrame (not a post-write `SELECT DISTINCT`)
# MAGIC - Dedup key is `event_id` only

# COMMAND ----------

# EXERCISE_KEY: ss_basics_ex8
# TODO: Dedup the stream on event_id and write to ex8_target.

# Your code here


# COMMAND ----------

# Validate Exercise 8
result = spark.table(f"{CATALOG}.{SCHEMA}.ex8_target")

assert result.count() == 10, f"Expected 10 unique rows, got {result.count()}"
# Uniqueness check
distinct_ids = result.select("event_id").distinct().count()
assert distinct_ids == 10, f"Expected 10 distinct event_ids, got {distinct_ids}"
# No duplicate event_id allowed
assert result.count() == distinct_ids, \
    f"Row count ({result.count()}) does not match distinct event_id count ({distinct_ids})"
# Spot checks - duplicated source rows must appear exactly once
assert result.filter("event_id = 'EV-101'").count() == 1, "EV-101 should appear exactly once after dedup"
assert result.filter("event_id = 'EV-107'").count() == 1, "EV-107 should appear exactly once after dedup"
assert result.filter("event_id = 'EV-110'").count() == 1, "EV-110 should appear exactly once"

print("Exercise 8 passed!")
