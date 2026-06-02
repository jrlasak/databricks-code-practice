# Databricks notebook source
# COMMAND ----------
# MAGIC %md
# MAGIC # foreachBatch Patterns
# MAGIC **Topic**: Streaming | **Exercises**: 7 | **Total Time**: ~95 min
# MAGIC
# MAGIC Practice `foreachBatch`, the escape hatch that lets you apply arbitrary batch DataFrame
# MAGIC logic to each streaming microbatch. Covers basic appends, MERGE in streaming, multi-sink
# MAGIC writes, `batch_id` for idempotency, quality routing to a quarantine table, running
# MAGIC microbatch metrics, and materializing `batch_df` via `collect()` when it must be read more than once.
# MAGIC
# MAGIC **Solutions**: If stuck, see `solutions/foreachbatch-patterns-solutions.py` for hints and answers.
# MAGIC
# MAGIC **Key concepts**:
# MAGIC - `foreachBatch(fn)` calls `fn(batch_df, batch_id)` once per microbatch with a static DataFrame
# MAGIC - Inside the function you can use any batch API: MERGE, multi-sink writes, joins, materialization via `collect()`
# MAGIC - Every stream uses `.trigger(availableNow=True)` + `.awaitTermination()` so the assertion cell runs only after the stream finishes
# MAGIC - Sources are Delta tables (Free Edition has no Kafka)
# MAGIC - To make the function idempotent across retries, you must dedupe on `batch_id` or rely on a transactional sink (MERGE on a primary key is transactional, plain append is not)
# MAGIC
# MAGIC **Free Edition note: caching workaround inside foreachBatch**
# MAGIC
# MAGIC Several exercises below (Ex 3, 5, 6, 7) read `batch_df` more than once inside the
# MAGIC `foreachBatch` function. On classic compute the standard fix is `batch_df.cache()` +
# MAGIC `unpersist()` so both reads share one materialization. On Databricks Free Edition
# MAGIC (serverless), `cache()` / `persist()` is rejected with `[NOT_SUPPORTED_WITH_SERVERLESS]`.
# MAGIC The serverless-safe equivalent is to materialize the batch once via `collect()` and rebuild
# MAGIC a local DataFrame from the rows:
# MAGIC
# MAGIC ```python
# MAGIC rows = batch_df.collect()
# MAGIC materialized = batch_df.sparkSession.createDataFrame(rows, batch_df.schema)
# MAGIC # then read `materialized` as many times as you need
# MAGIC ```
# MAGIC
# MAGIC Use this pattern in every multi-read foreachBatch on Free Edition. Each affected exercise
# MAGIC below points back to this note.

# COMMAND ----------

# MAGIC %run ./00_Setup

# COMMAND ----------

# MAGIC %run ./setup/foreachbatch-patterns-setup

# COMMAND ----------
# MAGIC %md
# MAGIC **Setup complete.** All exercise tables are in `db_code.foreachbatch_patterns`.
# MAGIC
# MAGIC **Source tables**: `ex1_source` ... `ex7_source` are identical copies of a 40-row event stream:
# MAGIC - 36 "good" rows (`event_id` EV-301..EV-336, valid `amount`, valid `user_id`)
# MAGIC - 4 "bad" rows (EV-901/902 have negative `amount`; EV-903/904 have NULL `user_id`)
# MAGIC
# MAGIC **Schema** (`exN_source`):
# MAGIC | Column     | Type      | Notes |
# MAGIC |------------|-----------|-------|
# MAGIC | event_id   | STRING    | Unique within source |
# MAGIC | event_type | STRING    | purchase / view / click |
# MAGIC | user_id    | STRING    | Nullable; 2 rows have NULL |
# MAGIC | amount     | DOUBLE    | Nullable; 2 rows are negative |
# MAGIC | event_ts   | TIMESTAMP | Event timestamp |
# MAGIC
# MAGIC **Checkpoints**: Use the `CHECKPOINT_BASE` variable (`/Volumes/db_code/foreachbatch_patterns/checkpoints/`).
# MAGIC Each exercise must use its own subdirectory (e.g., `f"{CHECKPOINT_BASE}/ex1"`).
# MAGIC
# MAGIC **Pattern**: Each exercise reads from `db_code.foreachbatch_patterns.exN_source` as a stream and writes via a `foreachBatch` function to `exN_target` (or, for Exercise 3, to both `ex3_target_raw` and `ex3_target_clean`).

# COMMAND ----------
# MAGIC %md
# MAGIC ## Exercise 1: Basic foreachBatch Append
# MAGIC **Difficulty**: Easy | **Time**: ~10 min
# MAGIC
# MAGIC The foundational `foreachBatch` pattern: read a Delta source as a stream, and inside
# MAGIC the foreachBatch function, append the batch to a Delta target. This is functionally
# MAGIC equivalent to `.writeStream.format("delta").toTable(...)` but exposes the batch DataFrame
# MAGIC and `batch_id` so you can apply arbitrary batch logic in later exercises.
# MAGIC
# MAGIC **Source** (`ex1_source`): 40 rows (36 good + 4 bad).
# MAGIC
# MAGIC **Target** (`ex1_target`): empty. After the stream completes, expect 40 rows.
# MAGIC
# MAGIC **Requirements**:
# MAGIC 1. Read `ex1_source` as a stream with `spark.readStream.table(...)` or `.format("delta").table(...)`
# MAGIC 2. Define `def foreach_batch_function(batch_df, batch_id):` that writes `batch_df` to `ex1_target` with mode `"append"` and format `"delta"`
# MAGIC 3. Apply it via `.writeStream.foreachBatch(foreach_batch_function)`
# MAGIC 4. Use `.option("checkpointLocation", f"{CHECKPOINT_BASE}/ex1")`
# MAGIC 5. Use `.trigger(availableNow=True)` and `.awaitTermination()`
# MAGIC
# MAGIC **Constraints**:
# MAGIC - Do NOT use `.toTable(...)` on the writeStream - you must go through `foreachBatch`
# MAGIC - All 40 rows (good + bad) end up in the target - no filtering in this exercise

# COMMAND ----------

# EXERCISE_KEY: foreach_batch_ex1
# TODO: Read ex1_source as a stream and append each microbatch to ex1_target via foreachBatch.

def foreach_batch_function(batch_df, batch_id):
    # Your code here
    pass

# Your code here (build and start the streaming query)


# COMMAND ----------

# Validate Exercise 1
result = spark.table(f"{CATALOG}.{SCHEMA}.ex1_target")

assert result.count() == 40, f"Expected 40 rows, got {result.count()}"
assert result.filter("event_id = 'EV-301'").count() == 1, "EV-301 should exist"
assert result.filter("event_id = 'EV-336'").count() == 1, "EV-336 (last good row) should exist"
assert result.filter("event_id = 'EV-901'").count() == 1, \
    "EV-901 (bad amount) should still be appended - this exercise does no filtering"
amt_303 = result.filter("event_id = 'EV-303'").select("amount").collect()[0][0]
assert amt_303 == 120.50, f"EV-303 amount should be 120.50, got {amt_303}"

print("Exercise 1 passed!")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Exercise 2: foreachBatch + MERGE INTO for Streaming Upsert
# MAGIC **Difficulty**: Medium | **Time**: ~15 min
# MAGIC
# MAGIC The most common production use of `foreachBatch`: turn a streaming source into a MERGE
# MAGIC upsert against a Delta target. MERGE only works on static DataFrames, so you cannot call
# MAGIC it directly on a streaming DataFrame - `foreachBatch` is the bridge.
# MAGIC
# MAGIC **Source** (`ex2_source`): 40 rows (36 good + 4 bad).
# MAGIC
# MAGIC **Target** (`ex2_target`): 10 pre-existing rows for `event_id` EV-301..EV-310, all with
# MAGIC placeholder values (`event_type='OLD'`, `amount=0.01`).
# MAGIC
# MAGIC **Expected Output**: After the stream completes, `ex2_target` should have 40 rows.
# MAGIC - Rows EV-301..EV-310: UPDATED with values from the source (e.g., EV-303 has `amount=120.50`, `event_type='purchase'`)
# MAGIC - Rows EV-311..EV-336 and EV-901..EV-904: INSERTED with source values
# MAGIC
# MAGIC **Requirements**:
# MAGIC 1. Read `ex2_source` as a stream
# MAGIC 2. Define `foreach_batch_function(batch_df, batch_id)` that:
# MAGIC    - Registers `batch_df` as a temp view (e.g., `fb_ex2_batch`)
# MAGIC    - Runs `MERGE INTO ex2_target USING fb_ex2_batch ON event_id WHEN MATCHED THEN UPDATE SET * WHEN NOT MATCHED THEN INSERT *`
# MAGIC 3. Wire the function via `.writeStream.foreachBatch(...)` with the ex2 checkpoint and `availableNow=True`
# MAGIC
# MAGIC **Constraints**:
# MAGIC - Use SQL `MERGE INTO`, not `DeltaTable.merge(...)` (either works, but the assertions only care about row state)
# MAGIC - Do NOT overwrite `ex2_target` - MERGE only

# COMMAND ----------

# EXERCISE_KEY: foreach_batch_ex2
# TODO: foreachBatch + MERGE INTO ex2_target on event_id (UPDATE SET * / INSERT *)

def foreach_batch_function(batch_df, batch_id):
    # Your code here
    pass

# Your code here (build and start the streaming query)


# COMMAND ----------

# Validate Exercise 2
result = spark.table(f"{CATALOG}.{SCHEMA}.ex2_target")

assert result.count() == 40, f"Expected 40 rows after upsert, got {result.count()}"
# Updated row (was OLD/0.01, should now match source)
amt_303 = result.filter("event_id = 'EV-303'").select("amount").collect()[0][0]
assert amt_303 == 120.50, f"EV-303 amount should be UPDATED to 120.50, got {amt_303}"
type_303 = result.filter("event_id = 'EV-303'").select("event_type").collect()[0][0]
assert type_303 == "purchase", f"EV-303 event_type should be 'purchase' (updated), got {type_303}"
# Confirm no row is still in the OLD placeholder state
old_rows = result.filter("event_type = 'OLD'").count()
assert old_rows == 0, f"All 10 pre-existing rows should be UPDATED; {old_rows} still have event_type='OLD'"
# Inserted new row
assert result.filter("event_id = 'EV-336'").count() == 1, "EV-336 should be inserted"
# Bad rows are upserted too (no filtering in this exercise)
assert result.filter("event_id = 'EV-901'").count() == 1, "EV-901 should be inserted (no quality filter here)"

print("Exercise 2 passed!")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Exercise 3: Multi-Sink Write (Two Delta Tables per Batch)
# MAGIC **Difficulty**: Medium | **Time**: ~15 min
# MAGIC
# MAGIC Inside `foreachBatch` you can write the same batch to multiple sinks. The catch:
# MAGIC if you read `batch_df` twice (once per sink), Spark may recompute the upstream
# MAGIC plan each time. Use the materialize-via-collect pattern from the notebook header
# MAGIC (cache() is not supported on serverless).
# MAGIC
# MAGIC **Source** (`ex3_source`): 40 rows (36 good + 4 bad).
# MAGIC
# MAGIC **Targets**:
# MAGIC - `ex3_target_raw` - every row in the batch (all 40 rows expected at the end)
# MAGIC - `ex3_target_clean` - only `event_type = 'purchase' AND amount > 0`
# MAGIC
# MAGIC **Expected Output**:
# MAGIC - `ex3_target_raw`: 40 rows
# MAGIC - `ex3_target_clean`: 20 rows (19 good purchase rows + EV-903, which is a purchase with amount=50.00 even though its user_id is NULL - this filter does not check user_id. EV-901 and EV-902 are purchases but have negative amounts so they are excluded; EV-904 is a view so it is excluded.)
# MAGIC
# MAGIC **Requirements**:
# MAGIC 1. Inside `foreach_batch_function`, materialize the batch once:
# MAGIC    `rows = batch_df.collect()`, then `materialized = batch_df.sparkSession.createDataFrame(rows, batch_df.schema)`
# MAGIC 2. Append `materialized` to `ex3_target_raw`
# MAGIC 3. Filter `materialized` for `event_type = 'purchase' AND amount > 0` and append the result to `ex3_target_clean`
# MAGIC 4. Wire with `availableNow=True` and the ex3 checkpoint
# MAGIC
# MAGIC **Constraints**:
# MAGIC - Use `.write.format("delta").mode("append").saveAsTable(...)` for both sinks
# MAGIC - Do NOT start two separate `writeStream`s reading the source twice - that's a different pattern
# MAGIC - See the "Free Edition note: caching workaround" in the notebook header for the required pattern.

# COMMAND ----------

# EXERCISE_KEY: foreach_batch_ex3
# TODO: Multi-sink foreachBatch. Materialize batch_df once via collect()+createDataFrame,
#       write raw to ex3_target_raw, write filtered clean rows (purchase AND amount > 0) to ex3_target_clean.

def foreach_batch_function(batch_df, batch_id):
    # Your code here
    pass

# Your code here (build and start the streaming query)


# COMMAND ----------

# Validate Exercise 3
raw = spark.table(f"{CATALOG}.{SCHEMA}.ex3_target_raw")
clean = spark.table(f"{CATALOG}.{SCHEMA}.ex3_target_clean")

assert raw.count() == 40, f"Expected 40 rows in ex3_target_raw, got {raw.count()}"
# Clean = event_type='purchase' AND amount > 0
# Good purchase rows: EV-301, 303, 305, 307, 309, 311, 313, 315, 317, 319, 321, 323, 325, 327, 329, 331, 333, 335, 336 (19)
# Bad rows with event_type='purchase': EV-901 (amount=-10), EV-902 (amount=-25.5), EV-903 (amount=50, user NULL)
# Excluded by amount>0: EV-901, EV-902
# Included: 19 good purchases + EV-903 = 20
assert clean.count() == 20, f"Expected 20 clean rows (purchase AND amount > 0), got {clean.count()}"
# Spot-check: EV-301 (clean), EV-901 (excluded - negative amount), EV-302 (excluded - view)
assert clean.filter("event_id = 'EV-301'").count() == 1, "EV-301 (purchase, amount=50.00) should be in clean"
assert clean.filter("event_id = 'EV-901'").count() == 0, "EV-901 (amount=-10) must be filtered out of clean"
assert clean.filter("event_id = 'EV-302'").count() == 0, "EV-302 (view) must be filtered out of clean"
assert clean.filter("amount <= 0").count() == 0, "No row in clean should have amount <= 0"
# Raw should contain everything, good and bad
assert raw.filter("event_id = 'EV-901'").count() == 1, "EV-901 must be in raw target"

print("Exercise 3 passed!")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Exercise 4: Idempotent foreachBatch via batch_id Dedup Key
# MAGIC **Difficulty**: Medium | **Time**: ~15 min
# MAGIC
# MAGIC `foreachBatch` runs at least once per microbatch. On retry, Spark may call the same
# MAGIC `batch_id` again. Plain appends produce duplicates; MERGE on the primary key is
# MAGIC transactionally safe. Another defense, useful when the sink is non-transactional or
# MAGIC when you want explicit safety: tag each row with the `batch_id`, and inside the
# MAGIC MERGE, ensure rows with an already-applied `batch_id` are not re-inserted.
# MAGIC
# MAGIC In this exercise you implement the pattern and then verify it by calling the
# MAGIC `foreach_batch_function` a second time with the same `batch_id` - the target must not change.
# MAGIC
# MAGIC **Source** (`ex4_source`): 40 rows.
# MAGIC
# MAGIC **Target** (`ex4_target`): empty. Schema includes a `batch_id LONG` column.
# MAGIC
# MAGIC **Expected Output**: After the stream completes, `ex4_target` should have 40 rows, all
# MAGIC tagged with the same `batch_id` (since `availableNow=True` processes everything in
# MAGIC one microbatch). Calling the function a second time with the same `batch_id` must
# MAGIC leave the target unchanged.
# MAGIC
# MAGIC **Requirements**:
# MAGIC 1. Inside `foreach_batch_function(batch_df, batch_id)`:
# MAGIC    - Add a literal `batch_id` column to `batch_df` (`F.lit(batch_id).cast("long")`)
# MAGIC    - Register the tagged DataFrame as a temp view (e.g., `fb_ex4_batch`)
# MAGIC    - Run a MERGE: `MERGE INTO ex4_target t USING fb_ex4_batch s ON t.event_id = s.event_id AND t.batch_id = s.batch_id WHEN NOT MATCHED THEN INSERT *`
# MAGIC 2. After the stream completes, manually invoke `foreach_batch_function(spark.table(f"{CATALOG}.{SCHEMA}.ex4_source"), <same_batch_id>)` to simulate a replay.
# MAGIC    Under `availableNow` the single microbatch has `batch_id=0`. Pass `0` to your replay call (or read the applied batch_id from the target table).
# MAGIC 3. Use the ex4 checkpoint and `availableNow=True`
# MAGIC
# MAGIC **Constraints**:
# MAGIC - The MERGE join key must include `batch_id`, otherwise a replay would update rows (not be a true no-op)
# MAGIC - Do NOT include `WHEN MATCHED THEN UPDATE` - this pattern is insert-only
# MAGIC - On replay (manual call with the same `batch_id`), the row count must NOT change

# COMMAND ----------

# EXERCISE_KEY: foreach_batch_ex4
# TODO: Build an idempotent foreachBatch that tags rows with batch_id and MERGEs
#       only WHEN NOT MATCHED on (event_id, batch_id). Then manually re-invoke the
#       function with the same batch_id to prove idempotency.

from pyspark.sql import functions as F

def foreach_batch_function(batch_df, batch_id):
    # Your code here
    pass

# Your code here (build and start the streaming query, then manually replay)


# COMMAND ----------

# Validate Exercise 4
result = spark.table(f"{CATALOG}.{SCHEMA}.ex4_target")

assert result.count() == 40, f"Expected 40 rows after stream + replay, got {result.count()}"
# All rows should have the same batch_id (single microbatch under availableNow)
distinct_batches = result.select("batch_id").distinct().count()
assert distinct_batches == 1, f"Expected 1 distinct batch_id, got {distinct_batches} (replay caused new batch_id or stream split)"
# No duplicates - dedup key (event_id, batch_id) must hold
dup_rows = result.count() - result.select("event_id", "batch_id").distinct().count()
assert dup_rows == 0, f"Expected 0 duplicate (event_id, batch_id) rows, got {dup_rows} - replay was not idempotent"
# Spot-check tagging
bid_301 = result.filter("event_id = 'EV-301'").select("batch_id").collect()[0][0]
assert bid_301 is not None, "EV-301 should have a non-null batch_id"

print("Exercise 4 passed!")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Exercise 5: Quality Routing - Good Rows to Target, Bad Rows to Quarantine
# MAGIC **Difficulty**: Hard | **Time**: ~15 min
# MAGIC
# MAGIC In production, batches contain rows that fail quality checks. Dropping them silently
# MAGIC is the worst possible answer - you lose evidence of the upstream problem. The standard
# MAGIC pattern is to route good rows to the main target and bad rows to a quarantine table
# MAGIC with a `reason` column explaining the rejection.
# MAGIC
# MAGIC **Source** (`ex5_source`): 40 rows. "Bad" rows:
# MAGIC - EV-901: amount = -10.00 (negative)
# MAGIC - EV-902: amount = -25.50 (negative)
# MAGIC - EV-903: user_id IS NULL
# MAGIC - EV-904: user_id IS NULL
# MAGIC
# MAGIC **Targets**:
# MAGIC - `ex5_target` (empty): receives rows where `amount >= 0 AND user_id IS NOT NULL`
# MAGIC - `ex5_quarantine` (empty): receives rejects with an extra `reason` STRING column
# MAGIC
# MAGIC **Expected Output**:
# MAGIC - `ex5_target`: 36 rows (the 36 good rows)
# MAGIC - `ex5_quarantine`: 4 rows total
# MAGIC   - 2 rows with `reason = 'negative_amount'` (EV-901, EV-902)
# MAGIC   - 2 rows with `reason = 'null_user_id'` (EV-903, EV-904)
# MAGIC
# MAGIC **Requirements**:
# MAGIC 1. Inside `foreach_batch_function`, materialize the batch once via
# MAGIC    `rows = batch_df.collect()` + `materialized = batch_df.sparkSession.createDataFrame(rows, batch_df.schema)` (you will read the materialized DataFrame multiple times: at least once for good rows and at least once more to classify the bad rows)
# MAGIC 2. Append good rows (`amount >= 0 AND user_id IS NOT NULL`) to `ex5_target`
# MAGIC 3. Append rows with `amount < 0` to `ex5_quarantine` with `reason = 'negative_amount'`
# MAGIC 4. Append rows with `user_id IS NULL` to `ex5_quarantine` with `reason = 'null_user_id'`
# MAGIC
# MAGIC **Constraints**:
# MAGIC - The quarantine table has 5 base columns + `reason` STRING - use `withColumn` to add `reason` before writing
# MAGIC - Order of writes does not matter
# MAGIC - A row that fails both checks (e.g., negative amount AND null user) is not present in the source - each bad row fails exactly one rule
# MAGIC - See the "Free Edition note: caching workaround" in the notebook header for the required pattern.

# COMMAND ----------

# EXERCISE_KEY: foreach_batch_ex5
# TODO: Route good rows to ex5_target and bad rows to ex5_quarantine with a reason.

from pyspark.sql import functions as F

def foreach_batch_function(batch_df, batch_id):
    # Your code here
    pass

# Your code here (build and start the streaming query)


# COMMAND ----------

# Validate Exercise 5
good = spark.table(f"{CATALOG}.{SCHEMA}.ex5_target")
quarantine = spark.table(f"{CATALOG}.{SCHEMA}.ex5_quarantine")

assert good.count() == 36, f"Expected 36 good rows in ex5_target, got {good.count()}"
assert quarantine.count() == 4, f"Expected 4 quarantined rows, got {quarantine.count()}"
# Specific reason assignments
neg = quarantine.filter("reason = 'negative_amount'")
nul = quarantine.filter("reason = 'null_user_id'")
assert neg.count() == 2, f"Expected 2 'negative_amount' quarantine rows, got {neg.count()}"
assert nul.count() == 2, f"Expected 2 'null_user_id' quarantine rows, got {nul.count()}"
assert neg.filter("event_id = 'EV-901'").count() == 1, "EV-901 must be in quarantine with reason='negative_amount'"
assert nul.filter("event_id = 'EV-903'").count() == 1, "EV-903 must be in quarantine with reason='null_user_id'"
# No good row leaked to quarantine or vice versa
assert good.filter("amount < 0").count() == 0, "ex5_target must not contain rows with amount < 0"
assert good.filter("user_id IS NULL").count() == 0, "ex5_target must not contain rows with NULL user_id"

print("Exercise 5 passed!")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Exercise 6: Running Batch Metrics
# MAGIC **Difficulty**: Hard | **Time**: ~10 min
# MAGIC
# MAGIC Operationally you need to know what happened in each microbatch: how many rows came
# MAGIC in, how many were quarantined, when it finished. The standard pattern is to compute
# MAGIC those counts inside `foreachBatch` and append one row to a metrics table per batch_id.
# MAGIC
# MAGIC **Source** (`ex6_source`): 40 rows (36 good + 4 bad).
# MAGIC
# MAGIC **Targets**:
# MAGIC - `ex6_target` (empty): clean rows go here
# MAGIC - `ex6_metrics` (empty): one row per microbatch with `(batch_id, rows_in, rows_good, rows_quarantined, processed_at)`
# MAGIC
# MAGIC **Expected Output**:
# MAGIC - `ex6_target`: 36 rows
# MAGIC - `ex6_metrics`: 1 row (single microbatch under `availableNow`) with `rows_in=40, rows_good=36, rows_quarantined=4`
# MAGIC
# MAGIC **Requirements**:
# MAGIC 1. Inside `foreach_batch_function`, materialize the batch once via
# MAGIC    `rows = batch_df.collect()` + `materialized = batch_df.sparkSession.createDataFrame(rows, batch_df.schema)` (you read it for counts and for the write)
# MAGIC 2. Compute `rows_in = materialized.count()`, `rows_good = materialized.filter("amount >= 0 AND user_id IS NOT NULL").count()`, `rows_quarantined = rows_in - rows_good`
# MAGIC 3. Append clean rows (good filter) to `ex6_target`
# MAGIC 4. Append exactly one row to `ex6_metrics` with `(batch_id, rows_in, rows_good, rows_quarantined, current_timestamp())`. Use `spark.createDataFrame([(batch_id, ...)], ...)` and `saveAsTable("...", mode="append")`
# MAGIC
# MAGIC **Constraints**:
# MAGIC - The metrics row's `batch_id` must equal the `batch_id` argument
# MAGIC - Do NOT compute the metrics by reading `ex6_target` after writing - they must come from the in-memory batch DataFrame
# MAGIC - See the "Free Edition note: caching workaround" in the notebook header for the required pattern.

# COMMAND ----------

# EXERCISE_KEY: foreach_batch_ex6
# TODO: foreachBatch that writes clean rows to ex6_target and appends a metrics row to ex6_metrics per batch.

from pyspark.sql import functions as F

def foreach_batch_function(batch_df, batch_id):
    # Your code here
    pass

# Your code here (build and start the streaming query)


# COMMAND ----------

# Validate Exercise 6
target = spark.table(f"{CATALOG}.{SCHEMA}.ex6_target")
metrics = spark.table(f"{CATALOG}.{SCHEMA}.ex6_metrics")

assert target.count() == 36, f"Expected 36 clean rows in ex6_target, got {target.count()}"
assert metrics.count() == 1, f"Expected exactly 1 metrics row (single microbatch under availableNow), got {metrics.count()}"

m = metrics.collect()[0]
assert m["rows_in"] == 40, f"Expected rows_in=40, got {m['rows_in']}"
assert m["rows_good"] == 36, f"Expected rows_good=36, got {m['rows_good']}"
assert m["rows_quarantined"] == 4, f"Expected rows_quarantined=4, got {m['rows_quarantined']}"
assert m["batch_id"] is not None, "batch_id column must be populated"
assert m["processed_at"] is not None, "processed_at timestamp must be populated"

print("Exercise 6 passed!")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Exercise 7: Materialized batch_df for MERGE + Quality Routing in One Pass
# MAGIC **Difficulty**: Hard | **Time**: ~15 min
# MAGIC
# MAGIC The production payoff: combine MERGE upsert with quality routing in a single
# MAGIC `foreachBatch`. The batch DataFrame is read twice - once for the MERGE (good rows)
# MAGIC and once for the quarantine append (bad rows). Use the materialize-via-collect pattern
# MAGIC from the notebook header (cache() is not supported on serverless).
# MAGIC
# MAGIC **Source** (`ex7_source`): 40 rows (36 good + 4 bad).
# MAGIC
# MAGIC **Targets**:
# MAGIC - `ex7_target`: 3 pre-existing rows (EV-301, EV-302, EV-303, all `event_type='OLD'`, `amount=0.01`). MERGE on `event_id` with good rows only.
# MAGIC - `ex7_quarantine`: empty. Receives bad rows (`amount < 0 OR user_id IS NULL`).
# MAGIC
# MAGIC **Expected Output**:
# MAGIC - `ex7_target`: 36 rows (3 pre-existing get UPDATED, 33 new good rows INSERTED)
# MAGIC - `ex7_quarantine`: 4 rows (EV-901, EV-902, EV-903, EV-904)
# MAGIC
# MAGIC **Requirements**:
# MAGIC 1. Inside `foreach_batch_function`:
# MAGIC    - Materialize the batch once: `rows = batch_df.collect()`, then `materialized = batch_df.sparkSession.createDataFrame(rows, batch_df.schema)`
# MAGIC    - Split: `good_df = materialized.filter(...)`, `bad_df = materialized.filter(...)`
# MAGIC    - Register `good_df` as a temp view and MERGE into `ex7_target` (UPDATE SET * WHEN MATCHED, INSERT * WHEN NOT MATCHED)
# MAGIC    - Append `bad_df` to `ex7_quarantine`
# MAGIC 2. Wire with `availableNow=True` and the ex7 checkpoint
# MAGIC
# MAGIC **Constraints**:
# MAGIC - Materialize via `collect()` once on `batch_df`, not on `good_df`/`bad_df` independently - both filters must derive from the same in-memory copy
# MAGIC - MERGE only against good rows - bad rows do NOT touch the main target
# MAGIC - The 3 pre-existing rows must end up with the SOURCE values, not the OLD placeholders
# MAGIC - See the "Free Edition note: caching workaround" in the notebook header for the required pattern.

# COMMAND ----------

# EXERCISE_KEY: foreach_batch_ex7
# TODO: Materialize batch_df once via collect()+createDataFrame, MERGE good rows into ex7_target, append bad rows to ex7_quarantine.

from pyspark.sql import functions as F

def foreach_batch_function(batch_df, batch_id):
    # Your code here
    pass

# Your code here (build and start the streaming query)


# COMMAND ----------

# Validate Exercise 7
target = spark.table(f"{CATALOG}.{SCHEMA}.ex7_target")
quarantine = spark.table(f"{CATALOG}.{SCHEMA}.ex7_quarantine")

assert target.count() == 36, f"Expected 36 rows in ex7_target after MERGE, got {target.count()}"
assert quarantine.count() == 4, f"Expected 4 bad rows in ex7_quarantine, got {quarantine.count()}"
# Pre-existing rows should be UPDATED, no OLD placeholders left
old_rows = target.filter("event_type = 'OLD'").count()
assert old_rows == 0, f"All pre-existing rows should be UPDATED; {old_rows} still have event_type='OLD'"
# Spot-check an updated row
amt_303 = target.filter("event_id = 'EV-303'").select("amount").collect()[0][0]
assert amt_303 == 120.50, f"EV-303 amount should be UPDATED to 120.50, got {amt_303}"
# Bad rows must NOT be in the main target
assert target.filter("event_id = 'EV-901'").count() == 0, "EV-901 (bad amount) must not be in ex7_target"
assert target.filter("event_id = 'EV-903'").count() == 0, "EV-903 (null user_id) must not be in ex7_target"
# Quarantine has the expected event_ids
q_ids = {r["event_id"] for r in quarantine.select("event_id").collect()}
assert q_ids == {"EV-901", "EV-902", "EV-903", "EV-904"}, f"Quarantine should contain exactly EV-901..EV-904, got {q_ids}"

print("Exercise 7 passed!")
