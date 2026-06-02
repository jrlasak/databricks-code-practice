# Databricks notebook source
# COMMAND ----------
# MAGIC %md
# MAGIC # Checkpointing & Recovery
# MAGIC **Topic**: Streaming | **Exercises**: 7 | **Total Time**: ~95 min
# MAGIC
# MAGIC Practice the operational layer of Structured Streaming: checkpoint locations, restart
# MAGIC behavior, and what changes when you lose, share, or move a checkpoint. Covers the
# MAGIC difference between resuming a stream and replaying it, and why this controls exactly-once
# MAGIC semantics on Delta sinks.
# MAGIC
# MAGIC **Solutions**: If stuck, see `solutions/checkpointing-recovery-solutions.py` for hints and answers.
# MAGIC
# MAGIC **Tables used**:
# MAGIC - `db_code.checkpointing_recovery.exN_source` - per-exercise Delta source (clone of `db_code.streaming.events_source`)
# MAGIC - `db_code.checkpointing_recovery.exN_target` - per-exercise Delta sink the stream writes to
# MAGIC
# MAGIC **Source schema** (`exN_source`):
# MAGIC | Column | Type | Notes |
# MAGIC |--------|------|-------|
# MAGIC | event_id | STRING | Primary key (`EV-001` ... `EV-030`) |
# MAGIC | event_type | STRING | purchase, view, click |
# MAGIC | user_id | STRING | U-1 ... U-5 |
# MAGIC | amount | DOUBLE | 0.0 for non-purchase events |
# MAGIC | event_ts | TIMESTAMP | Event time, ~1 min apart starting 2026-04-01 10:00:00 |
# MAGIC
# MAGIC **Checkpoint convention**: every exercise uses `CHECKPOINT_BASE/exN/` (defined in setup).
# MAGIC The checkpoint directory is what Structured Streaming uses to remember which source files
# MAGIC and offsets have been processed and what state (e.g., per-window counts) has been
# MAGIC accumulated. Losing it forces a full replay. Sharing it across queries causes corruption.
# MAGIC
# MAGIC **Free Edition constraints**:
# MAGIC - Every stream uses `.trigger(availableNow=True)` + `.awaitTermination()` so it terminates.
# MAGIC - Sources are Delta tables (Kafka is not available on Free Edition).
# MAGIC - No `spark.conf.set()` - serverless does not allow runtime Spark config changes.

# COMMAND ----------

# MAGIC %run ./00_Setup

# COMMAND ----------

# MAGIC %run ./setup/checkpointing-recovery-setup

# COMMAND ----------
# MAGIC %md
# MAGIC **Setup complete.** Per-exercise tables are in `db_code.checkpointing_recovery`.
# MAGIC
# MAGIC **Pattern**: Each exercise reads from `exN_source` (a Delta table) with
# MAGIC `spark.readStream.table(...)`, writes to `exN_target` with `.trigger(availableNow=True)`
# MAGIC and a checkpoint location of `{CHECKPOINT_BASE}/exN/`. Every TODO cell runs the full
# MAGIC demonstration sequence (initial stream, in-between mutation, second stream) and produces
# MAGIC a final state that the validate cell asserts.
# MAGIC
# MAGIC **Why one cell per exercise**: Databricks notebook cells execute once per invocation, but
# MAGIC each TODO cell is self-contained - it spans multiple `.start()` / `.awaitTermination()`
# MAGIC calls separated by `dbutils.fs.rm`, `INSERT INTO`, or a second `.start()` with a different
# MAGIC checkpoint. The whole sequence runs from one click.

# COMMAND ----------
# MAGIC %md
# MAGIC ## Exercise 1: Resume from Checkpoint, No Reprocessing
# MAGIC **Difficulty**: Easy | **Time**: ~10 min
# MAGIC
# MAGIC When a streaming query restarts with the same `checkpointLocation`, it must NOT
# MAGIC reprocess files it has already committed. This is the basis of exactly-once semantics on
# MAGIC Delta sinks.
# MAGIC
# MAGIC **Source** (`ex1_source`): 30 rows. **Target** (`ex1_target`): empty Delta table.
# MAGIC **Checkpoint**: `{CHECKPOINT_BASE}/ex1/`
# MAGIC
# MAGIC **What you will do AND observe**:
# MAGIC 1. Start a streaming read from `ex1_source`, write to `ex1_target` with
# MAGIC    `trigger(availableNow=True)` and a checkpoint at `{CHECKPOINT_BASE}/ex1/`. Wait for
# MAGIC    it to terminate. Target now has 30 rows.
# MAGIC 2. Start the SAME query again (same source, same target, same checkpoint).
# MAGIC 3. Observe: the second run completes immediately and processes 0 new rows because the
# MAGIC    checkpoint already records that all 30 source rows were committed. The target row
# MAGIC    count is unchanged at 30, not 60.
# MAGIC
# MAGIC **Requirements**:
# MAGIC 1. Both runs must use the SAME `checkpointLocation`
# MAGIC 2. Both runs must use `trigger(availableNow=True)` and `.awaitTermination()`
# MAGIC 3. Do NOT truncate or overwrite `ex1_target` between runs
# MAGIC
# MAGIC **Constraints**:
# MAGIC - Source is a Delta table - use `spark.readStream.table(...)`
# MAGIC - `outputMode("append")` is the only supported mode for a non-aggregating Delta sink

# COMMAND ----------

# EXERCISE_KEY: checkpoint_ex1
# TODO: Run the stream twice with the SAME checkpoint and verify the second run adds 0 rows

# Your code here


# COMMAND ----------

# Validate Exercise 1
result = spark.table(f"{CATALOG}.{SCHEMA}.ex1_target")

assert result.count() == 30, \
    f"Expected 30 rows after two runs with same checkpoint, got {result.count()}. " \
    f"If you see 60, the second run reprocessed everything - likely a different checkpoint path."
# Spot-check: every source event_id appears exactly once
unique_ids = result.select("event_id").distinct().count()
assert unique_ids == 30, f"Expected 30 distinct event_ids, got {unique_ids}"
assert result.filter("event_id = 'EV-001'").count() == 1, "EV-001 should appear exactly once"
assert result.filter("event_id = 'EV-030'").count() == 1, "EV-030 should appear exactly once"

print("Exercise 1 passed!")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Exercise 2: Inspect the Checkpoint Directory Structure
# MAGIC **Difficulty**: Easy | **Time**: ~10 min
# MAGIC
# MAGIC The checkpoint directory is not a black box. After a streaming query commits, it writes a
# MAGIC well-defined set of subdirectories that you can list with `dbutils.fs.ls`. Knowing what
# MAGIC each one does is the difference between guessing and diagnosing a recovery problem.
# MAGIC
# MAGIC **Source** (`ex2_source`): 30 rows. **Checkpoint**: `{CHECKPOINT_BASE}/ex2/`
# MAGIC
# MAGIC **What you will do AND observe**:
# MAGIC 1. Run a streaming read of `ex2_source` and write it to `ex2_target` once
# MAGIC    with checkpoint `{CHECKPOINT_BASE}/ex2/` (so the directory gets populated).
# MAGIC 2. List the checkpoint directory with `dbutils.fs.ls(f"{CHECKPOINT_BASE}/ex2/")`.
# MAGIC 3. Observe the four standard subdirectories Structured Streaming creates:
# MAGIC    - **offsets/**: per-batch source offsets (what the stream is about to read)
# MAGIC    - **commits/**: per-batch commit markers (proof the batch finished)
# MAGIC    - **sources/**: source-specific bookkeeping (for file sources: which files were seen)
# MAGIC    - **state/**: state store snapshots for aggregating queries; directory is created at query start (may be empty for non-stateful queries). If `dbutils.fs.ls` doesn't show `state/` on your runtime, write `"state"` anyway - it's the canonical name and the assertion accepts it.
# MAGIC
# MAGIC    You will also see a `metadata` file at the top level (query ID + stable settings).
# MAGIC    Databricks docs sometimes mention `metadata` and omit `sources` (or vice versa). Both
# MAGIC    exist: `metadata` is a small file, `sources` is a directory that only appears for
# MAGIC    file/Delta sources.
# MAGIC 4. Fill in the four subdirectory names below.
# MAGIC
# MAGIC **Expected Output**: `ex2_findings` table with 1 row and columns
# MAGIC `(offsets_dir, commits_dir, sources_dir, state_dir)`, all populated with the four standard
# MAGIC subdirectory names (one per column).
# MAGIC
# MAGIC **Requirements**:
# MAGIC 1. Run the stream first (with `availableNow=True`) so the checkpoint dir exists
# MAGIC 2. List the checkpoint dir and visually identify the four subdirectories
# MAGIC 3. Fill in the four placeholder strings in the pre-built INSERT below
# MAGIC
# MAGIC **Constraints**:
# MAGIC - The four values must be the lowercase directory names exactly as they appear on disk

# COMMAND ----------

# EXERCISE_KEY: checkpoint_ex2
# TODO: Run the stream once, then list the checkpoint dir and fill in the four subdir names

# 1. Run the stream once so the checkpoint directory is populated.
# Your code here for the streaming write:


# 2. List the checkpoint directory and inspect the subdirectory names:
# Your code here:
# dbutils.fs.ls(f"{CHECKPOINT_BASE}/ex2/")


# 3. Fill in the four standard checkpoint subdirectory names you observed above:
offsets_dir = ""   # Replace: name of the subdir holding per-batch offsets
commits_dir = ""   # Replace: name of the subdir holding per-batch commit markers
sources_dir = ""   # Replace: name of the subdir holding source-specific bookkeeping
state_dir = ""     # Replace: name of the subdir holding state store snapshots

spark.sql(f"""
    CREATE OR REPLACE TABLE {CATALOG}.{SCHEMA}.ex2_findings AS
    SELECT
        '{offsets_dir}' AS offsets_dir,
        '{commits_dir}' AS commits_dir,
        '{sources_dir}' AS sources_dir,
        '{state_dir}'   AS state_dir
""")

# COMMAND ----------

# Validate Exercise 2
row = spark.table(f"{CATALOG}.{SCHEMA}.ex2_findings").collect()[0]

assert row.offsets_dir == "offsets", \
    f"offsets_dir should be 'offsets', got '{row.offsets_dir}'"
assert row.commits_dir == "commits", \
    f"commits_dir should be 'commits', got '{row.commits_dir}'"
assert row.sources_dir == "sources", \
    f"sources_dir should be 'sources', got '{row.sources_dir}'"
assert row.state_dir == "state", \
    f"state_dir should be 'state', got '{row.state_dir}'"

# Also sanity-check the stream actually ran
target_count = spark.table(f"{CATALOG}.{SCHEMA}.ex2_target").count()
assert target_count == 30, \
    f"ex2_target should have 30 rows (stream must run before listing the dir), got {target_count}"

print("Exercise 2 passed!")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Exercise 3: Deleted Checkpoint Causes Full Reprocessing
# MAGIC **Difficulty**: Medium | **Time**: ~10 min
# MAGIC
# MAGIC The checkpoint is what makes a stream exactly-once. Without it, the stream has no memory
# MAGIC of what it has already processed and replays the source from the beginning. With a
# MAGIC Delta sink that has no idempotency key, this produces duplicates.
# MAGIC
# MAGIC **Source** (`ex3_source`): 30 rows. **Target** (`ex3_target`): empty Delta table.
# MAGIC **Checkpoint**: `{CHECKPOINT_BASE}/ex3/`
# MAGIC
# MAGIC **What you will do AND observe**:
# MAGIC 1. Run the stream once. Target has 30 rows.
# MAGIC 2. Delete the entire checkpoint directory: `dbutils.fs.rm(f"{CHECKPOINT_BASE}/ex3/", recurse=True)`.
# MAGIC 3. Run the SAME stream again (same source, same target, same path, but the checkpoint
# MAGIC    is now empty). The stream has no memory, so it reprocesses all 30 source rows and
# MAGIC    appends them again.
# MAGIC 4. Observe: the target now has 60 rows. Each source event_id appears twice. This is
# MAGIC    why losing a checkpoint is a data-quality incident, not just a restart problem.
# MAGIC
# MAGIC **Expected Output**: `ex3_target` has 60 rows total (each event_id appears exactly 2x).
# MAGIC
# MAGIC **Requirements**:
# MAGIC 1. Both runs must use the SAME checkpoint path string
# MAGIC 2. Between the runs, delete the checkpoint directory recursively
# MAGIC 3. Both runs use `trigger(availableNow=True)` and `.awaitTermination()`
# MAGIC
# MAGIC **Constraints**:
# MAGIC - Do NOT truncate `ex3_target` between runs - the duplicate rows are the point

# COMMAND ----------

# EXERCISE_KEY: checkpoint_ex3
# TODO: Run stream, delete the checkpoint dir, run again, and verify the target doubles

# Your code here


# COMMAND ----------

# Validate Exercise 3
result = spark.table(f"{CATALOG}.{SCHEMA}.ex3_target")

assert result.count() == 60, \
    f"Expected 60 rows after deleting checkpoint and re-running (full replay), got {result.count()}. " \
    f"If you got 30, the checkpoint was not actually deleted between runs."
# Every source event_id should appear exactly twice
dup_check = result.groupBy("event_id").count().filter("count != 2").count()
assert dup_check == 0, \
    f"Every event_id should appear exactly 2x after full replay, but {dup_check} event_ids do not."
ev001_count = result.filter("event_id = 'EV-001'").count()
assert ev001_count == 2, f"EV-001 should appear exactly 2x, got {ev001_count}"

print("Exercise 3 passed!")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Exercise 4: Different Checkpoint Location → Duplicates
# MAGIC **Difficulty**: Medium | **Time**: ~10 min
# MAGIC
# MAGIC A checkpoint identifies a streaming query. If you change the checkpoint path between two
# MAGIC runs, Structured Streaming treats them as TWO INDEPENDENT QUERIES that happen to share
# MAGIC a sink. Neither knows what the other committed. Both replay the source. Result: duplicates.
# MAGIC
# MAGIC This is a classic production bug: someone copy-pastes a stream into a new notebook,
# MAGIC forgets to keep the checkpoint path, and silently doubles every row.
# MAGIC
# MAGIC **Source** (`ex4_source`): 30 rows. **Target** (`ex4_target`): empty Delta table.
# MAGIC **Checkpoints**: `{CHECKPOINT_BASE}/ex4_a/` and `{CHECKPOINT_BASE}/ex4_b/`
# MAGIC
# MAGIC **What you will do AND observe**:
# MAGIC 1. Run the stream with checkpoint `{CHECKPOINT_BASE}/ex4_a/`. Target has 30 rows.
# MAGIC 2. Run the SAME stream (same source, same target) but with checkpoint
# MAGIC    `{CHECKPOINT_BASE}/ex4_b/` - a different path with no prior state.
# MAGIC 3. Observe: the second query has no record of what the first committed, so it replays
# MAGIC    all 30 rows. Target now has 60 rows.
# MAGIC
# MAGIC **Expected Output**: `ex4_target` has 60 rows total (each event_id appears exactly 2x).
# MAGIC
# MAGIC **Requirements**:
# MAGIC 1. First run uses `{CHECKPOINT_BASE}/ex4_a/`
# MAGIC 2. Second run uses `{CHECKPOINT_BASE}/ex4_b/`
# MAGIC 3. Both runs write to the same `ex4_target` with `trigger(availableNow=True)`
# MAGIC
# MAGIC **Constraints**:
# MAGIC - Do NOT delete or copy any files between the two checkpoint paths
# MAGIC - Both checkpoint directories should exist after the cell finishes (proof both ran)

# COMMAND ----------

# EXERCISE_KEY: checkpoint_ex4
# TODO: Run the stream twice into the same target with DIFFERENT checkpoint paths

# Your code here


# COMMAND ----------

# Validate Exercise 4
result = spark.table(f"{CATALOG}.{SCHEMA}.ex4_target")

assert result.count() == 60, \
    f"Expected 60 rows (each event_id duplicated), got {result.count()}. " \
    f"If you got 30, both runs may have shared the same checkpoint path."
dup_check = result.groupBy("event_id").count().filter("count != 2").count()
assert dup_check == 0, \
    f"Every event_id should appear exactly 2x, but {dup_check} event_ids do not."
# Both checkpoint dirs must exist
ckpt_a = dbutils.fs.ls(f"{CHECKPOINT_BASE}/ex4_a/")
ckpt_b = dbutils.fs.ls(f"{CHECKPOINT_BASE}/ex4_b/")
assert len(ckpt_a) > 0, "ex4_a checkpoint directory should be populated"
assert len(ckpt_b) > 0, "ex4_b checkpoint directory should be populated"

print("Exercise 4 passed!")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Exercise 5: Append New Rows, Resume Without Reprocessing
# MAGIC **Difficulty**: Medium | **Time**: ~15 min
# MAGIC
# MAGIC The normal production pattern: a stream catches up, you append more rows to the source,
# MAGIC and the next run picks up ONLY the new rows. This is what makes a checkpoint useful -
# MAGIC the stream remembers where it left off in the source and resumes from that offset.
# MAGIC
# MAGIC **Source** (`ex5_source`): starts with 20 rows (`EV-001` ... `EV-020`).
# MAGIC **Target** (`ex5_target`): empty Delta table.
# MAGIC **Checkpoint**: `{CHECKPOINT_BASE}/ex5/`
# MAGIC
# MAGIC **What you will do AND observe**:
# MAGIC 1. Run the stream once. Target has 20 rows (`EV-001` ... `EV-020`).
# MAGIC 2. `INSERT INTO ex5_source` 10 more rows by selecting `EV-021` ... `EV-030` from the
# MAGIC    base table (or use the exact `INSERT` provided below).
# MAGIC 3. Run the SAME stream again with the SAME checkpoint.
# MAGIC 4. Observe: the second run processes only the 10 new rows (the checkpoint records that
# MAGIC    `EV-001` ... `EV-020` were already committed). Target now has 30 rows total.
# MAGIC
# MAGIC **Helper INSERT** for step 2 (copy this into your cell, after the first stream run):
# MAGIC ```python
# MAGIC spark.sql(f"""
# MAGIC     INSERT INTO {CATALOG}.{SCHEMA}.ex5_source
# MAGIC     SELECT * FROM {CATALOG}.{BASE_SCHEMA}.events_source
# MAGIC     WHERE event_id NOT IN (SELECT event_id FROM {CATALOG}.{SCHEMA}.ex5_source)
# MAGIC       AND event_id <= 'EV-030'
# MAGIC """)
# MAGIC ```
# MAGIC
# MAGIC **Expected Output**: `ex5_target` has 30 rows total. Each event_id appears exactly once.
# MAGIC
# MAGIC **Requirements**:
# MAGIC 1. First stream run before any inserts → 20 rows in target
# MAGIC 2. Insert 10 new rows into `ex5_source` between the runs
# MAGIC 3. Second stream run with same checkpoint → only the 10 new rows are appended
# MAGIC
# MAGIC **Constraints**:
# MAGIC - Both runs use the same checkpoint path
# MAGIC - Do NOT use `dropDuplicates` to fake the assertion - the dedup must come from the checkpoint

# COMMAND ----------

# EXERCISE_KEY: checkpoint_ex5
# TODO: Run stream (20 rows), INSERT 10 new rows into source, run again, verify target = 30

# Your code here


# COMMAND ----------

# Validate Exercise 5
result = spark.table(f"{CATALOG}.{SCHEMA}.ex5_target")
src = spark.table(f"{CATALOG}.{SCHEMA}.ex5_source")

assert src.count() == 30, \
    f"ex5_source should have 30 rows after your insert, got {src.count()}"
assert result.count() == 30, \
    f"Expected 30 rows in target (20 initial + 10 incremental), got {result.count()}. " \
    f"60 means the second run replayed everything (wrong checkpoint?)."
unique_ids = result.select("event_id").distinct().count()
assert unique_ids == 30, f"Expected 30 distinct event_ids, got {unique_ids}"
# The first 20 should be there, AND the 10 incremental rows should be there
assert result.filter("event_id = 'EV-001'").count() == 1, "EV-001 from first batch should exist"
assert result.filter("event_id = 'EV-020'").count() == 1, "EV-020 from first batch should exist"
assert result.filter("event_id = 'EV-021'").count() == 1, "EV-021 (incremental) should exist"
assert result.filter("event_id = 'EV-030'").count() == 1, "EV-030 (incremental) should exist"

print("Exercise 5 passed!")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Exercise 6: Concurrent Streams Need Distinct Checkpoints
# MAGIC **Difficulty**: Hard | **Time**: ~20 min
# MAGIC
# MAGIC Two streaming queries that share BOTH a sink AND a checkpoint location collide.
# MAGIC Structured Streaming uses the checkpoint as the identity of a query - two queries with
# MAGIC the same checkpoint path are not "two queries" to the engine, they are "the same query
# MAGIC running twice." Starting the second one while the first still holds the checkpoint
# MAGIC typically raises an error (commonly: `ConcurrentModificationException` or a similar
# MAGIC checkpoint-lock error). See the timing caveat below.
# MAGIC
# MAGIC **Source** (`ex6_source`): 30 rows. **Target** (`ex6_target`): empty Delta table.
# MAGIC **Checkpoints**:
# MAGIC - `{CHECKPOINT_BASE}/ex6_shared/` (the bad case - both queries try to use it)
# MAGIC - `{CHECKPOINT_BASE}/ex6_a/` and `{CHECKPOINT_BASE}/ex6_b/` (the good case)
# MAGIC
# MAGIC **What you will do AND observe**:
# MAGIC 1. **Bad case**: Try to start two concurrent streams from `ex6_source` to `ex6_target`
# MAGIC    sharing `{CHECKPOINT_BASE}/ex6_shared/`. Wrap each `.start()` call in `try/except` so
# MAGIC    the cell doesn't abort. Record whether starting the second one raised an exception.
# MAGIC 2. **Good case**: Start two concurrent streams again, each writing to `ex6_target`, but
# MAGIC    with DISTINCT checkpoint paths `ex6_a` and `ex6_b`. Both succeed. Both append 30 rows
# MAGIC    each into the target, giving 60 total.
# MAGIC 3. Record both observations in `ex6_observation`:
# MAGIC    - `shared_checkpoint_errored` (BOOLEAN): did the shared-checkpoint case error?
# MAGIC    - `distinct_checkpoints_target_count` (BIGINT): row count in `ex6_target` after both
# MAGIC      distinct-checkpoint runs (expected 60).
# MAGIC
# MAGIC **Note**: `availableNow=True` means each `.start()` returns quickly after processing
# MAGIC its available data. The collision is timing-dependent - if q1 finishes before q2 begins,
# MAGIC no error fires. Record whether the collision was observed; the assertion only requires
# MAGIC that the DISTINCT-checkpoint case succeeds. In production with a continuous trigger the
# MAGIC collision is reliably reproduced, but on Free Edition serverless with `availableNow` it's
# MAGIC best-effort.
# MAGIC
# MAGIC **Expected Output**: `ex6_observation` has 1 row. `shared_checkpoint_errored` may be
# MAGIC True or False (timing-dependent). `distinct_checkpoints_target_count` must equal 60.
# MAGIC `ex6_target` ends up with at least 60 rows.
# MAGIC
# MAGIC **Requirements**:
# MAGIC 1. Try the shared-checkpoint case first; catch any exception and record whether one was raised (True or False).
# MAGIC 2. Run the distinct-checkpoint case next; both `.awaitTermination()` calls succeed
# MAGIC 3. Insert one row into `ex6_observation` with the observed values
# MAGIC
# MAGIC **Constraints**:
# MAGIC - Do NOT truncate the target between cases - the test reads its final state
# MAGIC - Use `try / except Exception as e` around the second `.start()` in the bad case so the
# MAGIC   cell continues to the good case
# MAGIC - `.toTable(...)` already starts the query and returns a `StreamingQuery` - do NOT chain
# MAGIC   `.start()` after it. If your helper builds a write with `.toTable(...)`, just call
# MAGIC   `.awaitTermination()` directly on its return value.

# COMMAND ----------

# EXERCISE_KEY: checkpoint_ex6
# TODO: Demonstrate shared-checkpoint collision, then distinct checkpoints succeeding

# Your code here


# COMMAND ----------

# Validate Exercise 6
obs = spark.table(f"{CATALOG}.{SCHEMA}.ex6_observation").collect()[0]
target = spark.table(f"{CATALOG}.{SCHEMA}.ex6_target")

# Whether the shared-checkpoint case raises is timing-dependent under availableNow;
# we only require that the user RECORDED a boolean observation (the value itself is fine
# either way - the production lesson is in the markdown, not in forcing a runtime collision).
assert obs.shared_checkpoint_errored in (True, False), \
    "Record `shared_checkpoint_errored` as a BOOLEAN (True if the shared-checkpoint case " \
    "raised, False if it didn't)."
assert obs.distinct_checkpoints_target_count == 60, \
    f"Two streams with distinct checkpoints should each append 30 rows " \
    f"(total 60), got {obs.distinct_checkpoints_target_count}"
# Final target should still reflect those 60 rows (plus possibly partial writes from the bad case)
assert target.count() >= 60, \
    f"ex6_target should have at least 60 rows from the two distinct-checkpoint runs, " \
    f"got {target.count()}"

print("Exercise 6 passed!")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Exercise 7: Recover a Windowed Aggregation from the State Store
# MAGIC **Difficulty**: Hard | **Time**: ~20 min
# MAGIC
# MAGIC For STATEFUL streams (windowed aggregations, dropDuplicates with watermark, stream-stream
# MAGIC joins), the checkpoint holds more than offsets - it holds the STATE STORE (per-window
# MAGIC counts, dedup hashes, etc.). When the stream restarts, it RESTORES that state instead of
# MAGIC recomputing from the beginning. This is what makes long-running aggregations practical
# MAGIC even though the underlying source data may be much shorter than the run.
# MAGIC
# MAGIC **Source** (`ex7_source`): starts with 30 rows. Event timestamps span 10:00 to 10:29
# MAGIC (30 minutes), so a 5-minute tumbling window produces 6 windows.
# MAGIC **Target** (`ex7_target`): a `(window_start, window_end, event_count)` table written in
# MAGIC `complete` output mode (windowed aggregation rewrites the full result each batch).
# MAGIC **Checkpoint**: `{CHECKPOINT_BASE}/ex7/`
# MAGIC
# MAGIC **What you will do AND observe**:
# MAGIC 1. Build a streaming query that counts events per 5-minute tumbling window on `event_ts`.
# MAGIC    Use a watermark of `0 seconds` so all input is in scope. Write to `ex7_target` with
# MAGIC    `outputMode("complete")`, `trigger(availableNow=True)`, and checkpoint `{CHECKPOINT_BASE}/ex7/`.
# MAGIC    Run once. The target has 6 windows with counts summing to 30.
# MAGIC 2. Append 5 more rows to `ex7_source` that fall into the `10:25–10:30` window. After the
# MAGIC    append, the source has 35 rows total. The window `10:25–10:30` originally held 5
# MAGIC    events (`EV-026` ... `EV-030`); after the append it holds 10.
# MAGIC 3. Run the SAME aggregation again with the SAME checkpoint.
# MAGIC 4. Observe: the second run does NOT recompute counts for the first five windows from
# MAGIC    scratch - the state store restored them. Only the affected window's count grows.
# MAGIC    The new totals: 5+5+5+5+5+10 = 35.
# MAGIC
# MAGIC **Helper INSERT** for step 2:
# MAGIC ```python
# MAGIC spark.sql(f"""
# MAGIC     INSERT INTO {CATALOG}.{SCHEMA}.ex7_source VALUES
# MAGIC         ('EV-201', 'purchase', 'U-1', 10.00, TIMESTAMP '2026-04-01 10:25:30'),
# MAGIC         ('EV-202', 'view',     'U-2', 0.00,  TIMESTAMP '2026-04-01 10:26:00'),
# MAGIC         ('EV-203', 'purchase', 'U-3', 20.00, TIMESTAMP '2026-04-01 10:27:00'),
# MAGIC         ('EV-204', 'click',    'U-4', 0.00,  TIMESTAMP '2026-04-01 10:28:00'),
# MAGIC         ('EV-205', 'purchase', 'U-5', 30.00, TIMESTAMP '2026-04-01 10:29:30')
# MAGIC """)
# MAGIC ```
# MAGIC
# MAGIC **Expected Output**: `ex7_target` has 6 windows. Total `event_count` across all windows
# MAGIC = 35. The window starting at `2026-04-01 10:25:00` has `event_count` = 10.
# MAGIC
# MAGIC **Requirements**:
# MAGIC 1. Use `window(event_ts, "5 minutes")` and `groupBy(window).count()`
# MAGIC 2. Apply `.withWatermark("event_ts", "0 seconds")` before the groupBy
# MAGIC 3. `outputMode("complete")` and `format("delta")` writing to `ex7_target`
# MAGIC 4. Same checkpoint on both runs - state must be restored, not recomputed
# MAGIC 5. Both runs use `trigger(availableNow=True)` and `.awaitTermination()`
# MAGIC
# MAGIC **Constraints**:
# MAGIC - `outputMode("complete")` overwrites the sink each batch - that's expected for windowed aggs
# MAGIC - Do NOT recompute counts via a batch query - the test verifies the streaming aggregation

# COMMAND ----------

# EXERCISE_KEY: checkpoint_ex7
# TODO: Run a windowed aggregation stream, append more rows in the same window, run again

# Your code here


# COMMAND ----------

# Validate Exercise 7
result = spark.table(f"{CATALOG}.{SCHEMA}.ex7_target")

assert result.count() == 6, \
    f"Expected 6 tumbling windows (30 min source / 5 min windows), got {result.count()}"
total_events = result.agg({"event_count": "sum"}).collect()[0][0]
assert total_events == 35, \
    f"Sum of event_count across windows should be 35 (30 original + 5 appended), got {total_events}"
# The affected window (starts 10:25:00) should have count = 10
target_window = result.filter("window_start = TIMESTAMP '2026-04-01 10:25:00'").collect()
assert len(target_window) == 1, \
    "There should be exactly one window starting at 2026-04-01 10:25:00"
assert target_window[0].event_count == 10, \
    f"Window 10:25-10:30 should have count = 10 (5 original + 5 appended), got {target_window[0].event_count}"
# Source must have grown
src_count = spark.table(f"{CATALOG}.{SCHEMA}.ex7_source").count()
assert src_count == 35, f"ex7_source should have 35 rows after the append, got {src_count}"

print("Exercise 7 passed!")
