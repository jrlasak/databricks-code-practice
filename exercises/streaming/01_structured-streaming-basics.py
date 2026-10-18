# Databricks notebook source
# COMMAND ----------
# MAGIC %md
# MAGIC # Structured Streaming Basics
# MAGIC **Topic**: Streaming | **Exercises**: 8 | **Total Time**: ~80 min
# MAGIC
# MAGIC Practice the Structured Streaming foundation on Databricks: `readStream` and `writeStream`
# MAGIC against Delta tables, choosing triggers (`processingTime` vs `availableNow`), picking the
# MAGIC right output mode (`append` / `complete` / `update`), naming queries and reading
# MAGIC `lastProgress`, and deduplicating streams.
# MAGIC
# MAGIC **Solutions**: If stuck, see `solutions/structured-streaming-basics-solutions.py` for hints and answers.
# MAGIC
# MAGIC **Key concepts**:
# MAGIC - `spark.readStream.table(...)` reads a Delta table as a stream: the rows already in the table first, then each new commit
# MAGIC - `trigger(availableNow=True)` processes everything available and stops (use this on Free Edition)
# MAGIC - Output modes: `append` (only new rows), `complete` (full result table), `update` (changed rows)
# MAGIC - Streaming queries always need a `checkpointLocation` for state and offset tracking
# MAGIC - `.queryName(...)` makes a stream identifiable; `query.lastProgress` exposes runtime metrics
# MAGIC - Deduplicating a stream on a key keeps every seen key in state unless a watermark bounds it

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
# MAGIC - Exercises 1-7 each have their own `exN_source` table, a copy of `db_code.streaming.events_source` (35 events)
# MAGIC - Exercise 8 has a custom source with intentional duplicate `event_id`s
# MAGIC
# MAGIC **Targets**: each exercise writes to its own `exN_target` table (Ex 5 writes to
# MAGIC `ex5_target_available_now`; Ex 7 writes both a target table and an `ex7_progress` metrics table).
# MAGIC
# MAGIC **Python variables**: the setup defines three variables you can use in your code: `CATALOG = "db_code"`, `SCHEMA = "structured_streaming_basics"` and `CHECKPOINT_BASE = "/Volumes/db_code/structured_streaming_basics/checkpoints"`. Write tables by their full name (`db_code.structured_streaming_basics.exN_target`).
# MAGIC
# MAGIC **Checkpoints**: each exercise checkpoints to its own subdirectory, `f"{CHECKPOINT_BASE}/exN"` (Ex 5 uses `/ex5_an` for the `availableNow` query).
# MAGIC
# MAGIC **Pattern**: every query in this notebook ends with `.trigger(availableNow=True)` and `.awaitTermination()`, so the stream processes what is available, stops, and the next cell sees the finished table.
# MAGIC
# MAGIC **Retrying an exercise**: a checkpoint remembers which rows its stream already read, so running a fixed TODO cell again reads nothing new. Before a retry, reset that exercise: `dbutils.fs.rm(f"{CHECKPOINT_BASE}/ex2", True)` and `spark.sql("DROP TABLE IF EXISTS db_code.structured_streaming_basics.ex2_target")` (use your exercise's number; for Exercise 5 the names are `ex5_an` and `ex5_target_available_now`; for Exercise 7 also drop `db_code.structured_streaming_basics.ex7_progress`), or re-run the `%run ./setup/structured-streaming-basics-setup` cell, which drops every target and checkpoint of this notebook.

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
# MAGIC 3. Set `checkpointLocation` to `f"{CHECKPOINT_BASE}/ex1"`
# MAGIC 4. Use `.trigger(availableNow=True)` so the stream processes every available row and then stops
# MAGIC 5. Call `.awaitTermination()` so the next cell runs only after the stream finishes
# MAGIC
# MAGIC **Constraints**:
# MAGIC - Use `availableNow`, not `trigger(once=True)` (deprecated) or `processingTime` (not supported on serverless)
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
# MAGIC 4. Checkpoint to `f"{CHECKPOINT_BASE}/ex2"`, end with `.trigger(availableNow=True)` and call `.awaitTermination()`
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
# MAGIC For example, `EV-003` (amount 120.50) gets `amount_with_tax` 132.55.
# MAGIC
# MAGIC **Requirements**:
# MAGIC 1. Read `ex3_source` with `spark.readStream`
# MAGIC 2. Add a new column `amount_with_tax = amount * 1.10`
# MAGIC 3. Write to `ex3_target` as Delta in append mode
# MAGIC 4. Checkpoint to `f"{CHECKPOINT_BASE}/ex3"`, end with `.trigger(availableNow=True)` and call `.awaitTermination()`
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
# MAGIC ## Exercise 4: Pick the Right Output Mode for an Ingestion Stream
# MAGIC **Difficulty**: Medium | **Time**: ~10 min
# MAGIC
# MAGIC Output mode is one of `append`, `complete` or `update`, and each works only for certain query shapes and sinks. This query copies events into a Delta table with no aggregation: exactly one mode works, and the other two raise an error when the stream starts. Write the pipeline, then record the mode you used in `correct_mode`.
# MAGIC
# MAGIC **Source** (`db_code.structured_streaming_basics.ex4_source`): 35 events.
# MAGIC
# MAGIC **Target**: Write to `db_code.structured_streaming_basics.ex4_target`. Expected 35 rows.
# MAGIC
# MAGIC **Requirements**:
# MAGIC 1. Read `ex4_source` and write it unchanged to `ex4_target`
# MAGIC 2. Call `outputMode(...)` explicitly on the writeStream with the mode you chose (do not rely on the default)
# MAGIC 3. Checkpoint to `f"{CHECKPOINT_BASE}/ex4"`, end with `.trigger(availableNow=True)` and call `.awaitTermination()`
# MAGIC 4. Set a Python variable `correct_mode` to the string of the mode you used (one of
# MAGIC    `"append"`, `"complete"`, `"update"`)

# COMMAND ----------

# EXERCISE_KEY: ss_basics_ex4
# TODO: Stream from ex4_source to ex4_target with the correct output mode
# Set `correct_mode` to one of "append", "complete", "update".

# Your code here


# COMMAND ----------

# Validate Exercise 4
import hashlib

result = spark.table(f"{CATALOG}.{SCHEMA}.ex4_target")

assert result.count() == 35, f"Expected 35 rows, got {result.count()}"
assert "correct_mode" in dir(), "Set a variable `correct_mode` to your chosen output mode"
assert hashlib.sha256(correct_mode.encode()).hexdigest() == "ae524b54d9b5acbe3bbf0a4f4825bae82bca8b0a474878c2713394ea284ef929", \
    f"'{correct_mode}' is not the mode that works for a plain copy into a Delta sink. Check which modes need an aggregation and which modes the Delta sink accepts."
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
# MAGIC - `processingTime='30 seconds'`: a long-running query that checks for new data every 30 seconds and runs a micro-batch when there is some; the query and its compute stay up.
# MAGIC - `availableNow=True`: processes everything currently available, then stops; a scheduled job starts it again later, and compute shuts down in between.
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
# MAGIC 3. Set a Python variable `cost_winner` to the trigger string that gives LOWER COST (one of `"processingTime"` or `"availableNow"`).
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
import hashlib

an = spark.table(f"{CATALOG}.{SCHEMA}.ex5_target_available_now")

assert an.count() == 35, f"availableNow target should have 35 rows, got {an.count()}"

assert "latency_winner" in dir(), "Set `latency_winner` to 'processingTime' or 'availableNow'"
assert "cost_winner" in dir(), "Set `cost_winner` to 'processingTime' or 'availableNow'"
assert hashlib.sha256(latency_winner.encode()).hexdigest() == "96e8d9790f722b3903b5c94684dd31fffdc7545064f9c4966c388e531ba0b368", \
    f"'{latency_winner}' is not the lower-latency trigger. Which one keeps the query running between batches?"
assert hashlib.sha256(cost_winner.encode()).hexdigest() == "5a43126b98f5b7a5460bfe7a907807bcf3c1eccf8098f3fa6a8f30312b59069f", \
    f"'{cost_winner}' is not the lower-cost trigger. Which one lets compute shut down between runs?"

print("Exercise 5 passed!")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Exercise 6: Streaming Aggregation in Complete Output Mode
# MAGIC **Difficulty**: Medium | **Time**: ~10 min
# MAGIC
# MAGIC Stateful aggregations grouped by a plain column (here `event_type`, no event-time window) need `complete` output mode when
# MAGIC writing to a Delta sink: `append` works on an aggregation only when the grouping includes an event-time window or column
# MAGIC that has a watermark. In `complete` mode the engine emits the FULL result table after
# MAGIC every micro-batch, overwriting the previous result. This is fine for small grouping
# MAGIC dimensions (handful of event types). For large key spaces, you would write the aggregate
# MAGIC with `foreachBatch` and a MERGE instead of rewriting the full table.
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
# MAGIC 4. Checkpoint to `f"{CHECKPOINT_BASE}/ex6"`, end with `.trigger(availableNow=True)` and call `.awaitTermination()`
# MAGIC
# MAGIC **Constraints**:
# MAGIC - The count column MUST be named `cnt` (the validation cell reads that column)
# MAGIC - Use `complete` mode: `append` fails here because the grouping has no event-time column with a watermark

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
# MAGIC 1. Start a streaming write from `ex7_source` to `ex7_target` with `.queryName("ss_basics_ex7")`, checkpoint `f"{CHECKPOINT_BASE}/ex7"` and `.trigger(availableNow=True)`
# MAGIC 2. Keep the `StreamingQuery` that `.toTable(...)` returns in a variable `query`, then call `query.awaitTermination()` as a separate statement (chained onto `.toTable(...)`, it returns `None` and you lose the query). Then read `query.lastProgress["name"]` and `query.lastProgress["numInputRows"]`, and write them with `mode("overwrite")` as a 1-row Delta table `ex7_progress` with columns `query_name STRING`, `num_input_rows BIGINT`.
# MAGIC
# MAGIC **Constraints**:
# MAGIC - The query name string MUST be `ss_basics_ex7`
# MAGIC - Use `lastProgress`, not `recentProgress` (the validation cell expects one row from the last batch)

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
# MAGIC ## Exercise 8: Deduplicate a Stream by event_id
# MAGIC **Difficulty**: Medium | **Time**: ~10 min
# MAGIC
# MAGIC Duplicate events are a fact of life (retries, replays, upstream bugs), and the target must still contain each `event_id` once. Remove the repeats on the streaming DataFrame itself, so the dedup is part of the streaming plan and also catches a repeat that arrives in a later micro-batch.
# MAGIC
# MAGIC **Source** (`db_code.structured_streaming_basics.ex8_source`): 15 rows with deliberate duplicates:
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
# MAGIC 2. Keep one row per `event_id` in the stream
# MAGIC 3. Write to `ex8_target` as Delta in append mode
# MAGIC 4. Checkpoint to `f"{CHECKPOINT_BASE}/ex8"`, end with `.trigger(availableNow=True)` and call `.awaitTermination()`
# MAGIC
# MAGIC **Constraints**:
# MAGIC - Deduplicate the STREAMING DataFrame (not a post-write `SELECT DISTINCT`)
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
