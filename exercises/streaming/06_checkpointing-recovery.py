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
# MAGIC - `db_code.checkpointing_recovery.exN_source` - per-exercise Delta source: the first 30 rows (`EV-001` ... `EV-030`) of `db_code.streaming.events_source`; `ex5_source` starts with the first 20, and Exercise 6 splits the 30 rows into `ex6_source_a` and `ex6_source_b`
# MAGIC - `db_code.checkpointing_recovery.exN_target` - per-exercise Delta sink the stream writes to
# MAGIC
# MAGIC **Source schema** (`exN_source`):
# MAGIC | Column | Type | Notes |
# MAGIC |--------|------|-------|
# MAGIC | event_id | STRING | Primary key (`EV-001` ... `EV-030`) |
# MAGIC | event_type | STRING | purchase, view, click |
# MAGIC | user_id | STRING | U-1 ... U-5 |
# MAGIC | amount | DOUBLE | 0.0 for non-purchase events |
# MAGIC | event_ts | TIMESTAMP | Event time, one per minute: `EV-001` at 2026-04-01 10:00:00 to `EV-030` at 10:29:00 |
# MAGIC
# MAGIC **Checkpoint convention**: every exercise uses `/Volumes/db_code/checkpointing_recovery/checkpoints/exN/`. The setup cell stores the base in the Python variable `CHECKPOINT_BASE`, so in code write `f"{CHECKPOINT_BASE}/exN/"`.
# MAGIC The checkpoint directory is what Structured Streaming uses to remember which source files
# MAGIC and offsets have been processed and what state (e.g., per-window counts) has been
# MAGIC accumulated. Losing it on a Delta source like the ones here means the next run replays the whole table. A Kafka source would start from the latest offsets instead. Two queries must never share one checkpoint.
# MAGIC
# MAGIC **Free Edition constraints**:
# MAGIC - Every stream uses `.trigger(availableNow=True)` + `.awaitTermination()` so it terminates.
# MAGIC - Sources are Delta tables (Kafka is not available on Free Edition).
# MAGIC - No Spark config is needed: serverless lets you set only a short list of properties, and none of these exercises uses one.

# COMMAND ----------

# MAGIC %run ./00_Setup

# COMMAND ----------

# MAGIC %run ./setup/checkpointing-recovery-setup

# COMMAND ----------
# MAGIC %md
# MAGIC **Setup complete.** Per-exercise tables are in `db_code.checkpointing_recovery`.
# MAGIC
# MAGIC **Python variables**: the setup defines `CATALOG = "db_code"`, `SCHEMA = "checkpointing_recovery"`, `BASE_SCHEMA = "streaming"` and `CHECKPOINT_BASE = "/Volumes/db_code/checkpointing_recovery/checkpoints"`. Write tables by their full name (`db_code.checkpointing_recovery.exN_target`).
# MAGIC
# MAGIC **Pattern**: each exercise streams from `exN_source` with `spark.readStream.table(...)` into `exN_target`, using `.trigger(availableNow=True)`, `.awaitTermination()` and the checkpoint `/Volumes/db_code/checkpointing_recovery/checkpoints/exN/`. Put the exercise's whole sequence (its stream runs and any change between them) in the exercise's TODO cell so it runs in one click. The validation cell below it checks the final tables.
# MAGIC
# MAGIC **Redo an exercise**: re-run the `%run ./setup/checkpointing-recovery-setup` cell first. It recreates every source and target table of this notebook and deletes every checkpoint, so the exercise starts from the state its text describes.

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
# MAGIC **Checkpoint**: `/Volumes/db_code/checkpointing_recovery/checkpoints/ex1/`
# MAGIC
# MAGIC **What you will do AND observe**:
# MAGIC 1. Start a streaming read from `ex1_source`, write to `ex1_target` with
# MAGIC    `trigger(availableNow=True)` and a checkpoint at `/Volumes/db_code/checkpointing_recovery/checkpoints/ex1/`. Wait for
# MAGIC    it to terminate. Target now has 30 rows.
# MAGIC 2. Start the SAME query again (same source, same target, same checkpoint).
# MAGIC 3. Observe: the second run completes immediately and processes 0 new rows because the
# MAGIC    checkpoint already records that all 30 source rows were committed. The target row
# MAGIC    count is unchanged at 30, not 60.
# MAGIC
# MAGIC **Expected Output**: `ex1_target` has 30 rows, each event_id (`EV-001` ... `EV-030`) exactly once.
# MAGIC
# MAGIC **Requirements**:
# MAGIC 1. Both runs must use the SAME `checkpointLocation`
# MAGIC 2. Do NOT truncate or overwrite `ex1_target` between runs
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
# MAGIC well-defined set of entries that you can list with `dbutils.fs.ls`. Knowing what
# MAGIC each one does is the difference between guessing and diagnosing a recovery problem.
# MAGIC
# MAGIC **Source** (`ex2_source`): 30 rows. **Checkpoint**: `/Volumes/db_code/checkpointing_recovery/checkpoints/ex2/`
# MAGIC
# MAGIC **What you will do AND observe**:
# MAGIC 1. Run a streaming read of `ex2_source` and write it to `ex2_target` once
# MAGIC    with checkpoint `/Volumes/db_code/checkpointing_recovery/checkpoints/ex2/` (so the directory gets populated).
# MAGIC 2. List the checkpoint directory with `dbutils.fs.ls(f"{CHECKPOINT_BASE}/ex2/")`.
# MAGIC 3. Match each entry in the listing to its role:
# MAGIC    - the directory recording, for each batch, which source data the batch will read (written BEFORE the batch runs)
# MAGIC    - the directory recording, for each batch, that its output reached the sink (written AFTER the batch finishes)
# MAGIC    - the top-level file holding the query's unique ID
# MAGIC 4. Record those three names.
# MAGIC
# MAGIC A checkpoint can hold two more entries. A `state/` directory appears only for stateful queries (aggregations, deduplication, stream-stream joins); this query is stateless, and you will find `state/` in Exercise 7's checkpoint. A `sources/` directory holds source bookkeeping when the source keeps any (a file source lists the files it has seen), so your listing may or may not show it.
# MAGIC
# MAGIC **Expected Output**: `ex2_findings` has 1 row with columns `planned_batches_dir`, `finished_batches_dir` and `query_id_file`, each holding a name from your listing. `ex2_target` has 30 rows.
# MAGIC
# MAGIC **Requirements**:
# MAGIC 1. Run the stream first so the checkpoint dir exists
# MAGIC 2. List the checkpoint dir and match its entries to the three roles above
# MAGIC 3. Fill in the three placeholder strings above the pre-built `CREATE OR REPLACE TABLE ... ex2_findings` statement
# MAGIC
# MAGIC **Constraints**:
# MAGIC - The values are the lowercase names without the trailing `/` that `dbutils.fs.ls` prints

# COMMAND ----------

# EXERCISE_KEY: checkpoint_ex2
# TODO: Run the stream once, then list the checkpoint dir and fill in the three names

# 1. Run the stream once so the checkpoint directory is populated.
# Your code here for the streaming write:


# 2. List the checkpoint directory and inspect the names:
# Your code here:
# dbutils.fs.ls(f"{CHECKPOINT_BASE}/ex2/")


# 3. Fill in the names you observed above:
planned_batches_dir = ""   # Replace: the directory written BEFORE each batch runs
finished_batches_dir = ""  # Replace: the directory written AFTER each batch finishes
query_id_file = ""         # Replace: the top-level file holding the query ID

spark.sql(f"""
    CREATE OR REPLACE TABLE {CATALOG}.{SCHEMA}.ex2_findings AS
    SELECT
        '{planned_batches_dir}'  AS planned_batches_dir,
        '{finished_batches_dir}' AS finished_batches_dir,
        '{query_id_file}'        AS query_id_file
""")

# COMMAND ----------

# Validate Exercise 2
findings = spark.table(f"{CATALOG}.{SCHEMA}.ex2_findings")
assert findings.count() == 1, f"ex2_findings should have exactly 1 row, got {findings.count()}"
row = findings.collect()[0]

ckpt = f"{CHECKPOINT_BASE}/ex2"
entries = dbutils.fs.ls(f"{ckpt}/")
listing = [f.name.rstrip("/") for f in entries]
dirs = [f.name.rstrip("/") for f in entries if f.name.endswith("/")]
for column in ["planned_batches_dir", "finished_batches_dir", "query_id_file"]:
    assert row[column] in listing, \
        f"{column} = '{row[column]}' is not an entry of the checkpoint listing {sorted(listing)}"
assert row.planned_batches_dir in dirs and row.finished_batches_dir in dirs, \
    "planned_batches_dir and finished_batches_dir must both be directories"
assert row.query_id_file not in dirs, "query_id_file must be a file, not a directory"

# Check each role by what the entry holds: the record written before batch 0 ran, the record written after it finished, the query ID
assert "batchTimestampMs" in dbutils.fs.head(f"{ckpt}/{row.planned_batches_dir}/0"), \
    f"'{row.planned_batches_dir}/0' is not the record written BEFORE batch 0 ran"
assert "nextBatchWatermarkMs" in dbutils.fs.head(f"{ckpt}/{row.finished_batches_dir}/0"), \
    f"'{row.finished_batches_dir}/0' is not the record written AFTER batch 0 finished"
assert '"id"' in dbutils.fs.head(f"{ckpt}/{row.query_id_file}"), \
    f"'{row.query_id_file}' does not hold the query ID"

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
# MAGIC The checkpoint is what makes a stream exactly-once. Without it, a stream on a Delta source has no memory
# MAGIC of what it already processed and replays the whole table. With a
# MAGIC Delta sink that has no idempotency key, this produces duplicates.
# MAGIC
# MAGIC **Source** (`ex3_source`): 30 rows. **Target** (`ex3_target`): empty Delta table.
# MAGIC **Checkpoint**: `/Volumes/db_code/checkpointing_recovery/checkpoints/ex3/`
# MAGIC
# MAGIC **What you will do AND observe**:
# MAGIC 1. Run the stream once. Target has 30 rows.
# MAGIC 2. Delete the entire checkpoint directory: `dbutils.fs.rm(f"{CHECKPOINT_BASE}/ex3/", recurse=True)`.
# MAGIC 3. Run the SAME stream again (same source, same target, same checkpoint path, but the directory is gone).
# MAGIC    The stream has no memory, so it reprocesses all 30 source rows and
# MAGIC    appends them again.
# MAGIC 4. Observe: the target now has 60 rows. Each source event_id appears twice. This is
# MAGIC    why losing a checkpoint is a data-quality incident, not just a restart problem.
# MAGIC
# MAGIC **Expected Output**: `ex3_target` has 60 rows total (each event_id appears exactly 2x).
# MAGIC
# MAGIC **Requirements**:
# MAGIC 1. Both runs must use the SAME checkpoint path string
# MAGIC 2. Between the runs, delete the checkpoint directory recursively
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
# MAGIC **Checkpoints**: `/Volumes/db_code/checkpointing_recovery/checkpoints/ex4_a/` and `/Volumes/db_code/checkpointing_recovery/checkpoints/ex4_b/`
# MAGIC
# MAGIC **What you will do AND observe**:
# MAGIC 1. Run the stream with checkpoint `/Volumes/db_code/checkpointing_recovery/checkpoints/ex4_a/`. Target has 30 rows.
# MAGIC 2. Run the SAME stream (same source, same target) but with checkpoint
# MAGIC    `/Volumes/db_code/checkpointing_recovery/checkpoints/ex4_b/` - a different path with no prior state.
# MAGIC 3. Observe: the second query has no record of what the first committed, so it replays
# MAGIC    all 30 rows. Target now has 60 rows.
# MAGIC
# MAGIC **Expected Output**: `ex4_target` has 60 rows total (each event_id appears exactly 2x).
# MAGIC
# MAGIC **Requirements**:
# MAGIC 1. First run uses `f"{CHECKPOINT_BASE}/ex4_a/"`
# MAGIC 2. Second run uses `f"{CHECKPOINT_BASE}/ex4_b/"`
# MAGIC 3. Both runs write to the same `ex4_target`
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
# MAGIC **Checkpoint**: `/Volumes/db_code/checkpointing_recovery/checkpoints/ex5/`
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
# MAGIC - Do NOT add `dropDuplicates`: the checkpoint alone keeps the 20 committed rows from being processed again

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
    f"50 means the second run replayed all 30 source rows (wrong checkpoint path?)."
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
# MAGIC Two streaming queries must never share a checkpoint location: the checkpoint is the query's identity. On one cluster Spark will not run two copies side by side. When a second run starts on a checkpoint that an active run is using, Spark stops the first run and keeps the second (`spark.sql.streaming.stopActiveRunOnRestart`, on by default). Two runs on different clusters, such as two jobs pointing at the same path, race on the same offset log and can fail with `ConcurrentModificationException` or corrupt it.
# MAGIC
# MAGIC Two concurrent streams can write to one table safely when each query has its own checkpoint and its own input.
# MAGIC
# MAGIC **Sources**: `ex6_source_a` (15 rows, `EV-001` ... `EV-015`) and `ex6_source_b` (15 rows, `EV-016` ... `EV-030`).
# MAGIC **Targets**: `ex6_shared_target` (bad case) and `ex6_target` (good case), both empty Delta tables.
# MAGIC **Checkpoints**:
# MAGIC - `/Volumes/db_code/checkpointing_recovery/checkpoints/ex6_shared/` (the bad case - both queries use it)
# MAGIC - `/Volumes/db_code/checkpointing_recovery/checkpoints/ex6_a/` and `/Volumes/db_code/checkpointing_recovery/checkpoints/ex6_b/` (the good case)
# MAGIC
# MAGIC **What you will do AND observe**:
# MAGIC 1. **Bad case**: Start query 1 from `ex6_source_a` into `ex6_shared_target` with `.toTable(...)` on the `ex6_shared/` checkpoint, then start query 2 (the same source, target and checkpoint) inside `try / except Exception`, and call `.awaitTermination()` on both inside `try` blocks. Set `shared_checkpoint_errored` to True if any of these raised.
# MAGIC 2. **Good case**: Start two concurrent streams into `ex6_target`, one reading `ex6_source_a` with checkpoint `ex6_a/`, one reading `ex6_source_b` with checkpoint `ex6_b/`. Both succeed and `ex6_target` ends with 30 rows, each event_id once.
# MAGIC 3. Create `db_code.checkpointing_recovery.ex6_observation` with one row holding the two observed values (`CREATE OR REPLACE TABLE ... AS SELECT ...`):
# MAGIC    - `shared_checkpoint_errored` (BOOLEAN): did anything in the bad case raise?
# MAGIC    - `distinct_checkpoints_target_count` (BIGINT): the `ex6_target` row count after both distinct-checkpoint runs, measured with `spark.table(...).count()`.
# MAGIC
# MAGIC **Note**: Both shared-checkpoint queries run on one cluster here, so starting the second one usually does not raise. Spark stops the first run instead, or the first run has already finished because `availableNow` returns quickly.
# MAGIC
# MAGIC **Expected Output**: `ex6_observation` has 1 row. `shared_checkpoint_errored` is usually False (see the note above); either value passes. `ex6_shared_target` holds at least 15 rows, which shows the bad case ran. `distinct_checkpoints_target_count` is 30, and `ex6_target` has 30 rows with 30 distinct event_ids.
# MAGIC
# MAGIC **Requirements**:
# MAGIC 1. Try the shared-checkpoint case first; catch any exception and record whether one was raised (True or False).
# MAGIC 2. Run the distinct-checkpoint case next; both `.awaitTermination()` calls succeed
# MAGIC 3. Count `ex6_target` and create `ex6_observation` with the two values
# MAGIC
# MAGIC **Constraints**:
# MAGIC - Do NOT truncate or overwrite `ex6_target`: the validation cell reads its final row count
# MAGIC - `.toTable(...)` already starts the query and returns a `StreamingQuery` - do NOT chain
# MAGIC   `.start()` after it. If your helper builds a write with `.toTable(...)`, just call
# MAGIC   `.awaitTermination()` directly on its return value.

# COMMAND ----------

# EXERCISE_KEY: checkpoint_ex6
# TODO: Run two queries on one shared checkpoint, then two queries with distinct checkpoints, and record what you observed

# Your code here


# COMMAND ----------

# Validate Exercise 6
obs_rows = spark.table(f"{CATALOG}.{SCHEMA}.ex6_observation").collect()
assert len(obs_rows) == 1, f"ex6_observation should have exactly 1 row, got {len(obs_rows)}"
obs = obs_rows[0]
target = spark.table(f"{CATALOG}.{SCHEMA}.ex6_target")

# On one cluster Spark stops the first shared-checkpoint run instead of raising, so either boolean passes.
assert obs.shared_checkpoint_errored in (True, False), \
    "Record `shared_checkpoint_errored` as a BOOLEAN (True if the shared-checkpoint case " \
    "raised, False if it didn't)."
shared_rows = spark.table(f"{CATALOG}.{SCHEMA}.ex6_shared_target").count()
assert shared_rows >= 15, \
    f"ex6_shared_target should hold at least the 15 rows of ex6_source_a from the bad case, got {shared_rows}. " \
    "Run the bad case too: two queries from ex6_source_a into ex6_shared_target on the ex6_shared/ checkpoint."
assert target.count() == 30, \
    f"ex6_target should have 30 rows (15 from each source, each through its own checkpoint), got {target.count()}"
distinct_ids = target.select("event_id").distinct().count()
assert distinct_ids == 30, \
    f"Every event_id should appear once in ex6_target, got {distinct_ids} distinct ids in {target.count()} rows"
assert obs.distinct_checkpoints_target_count == target.count(), \
    f"distinct_checkpoints_target_count should be the ex6_target row count ({target.count()}), " \
    f"got {obs.distinct_checkpoints_target_count}"

print("Exercise 6 passed!")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Exercise 7: Recover a Windowed Aggregation from the State Store
# MAGIC **Difficulty**: Hard | **Time**: ~20 min
# MAGIC
# MAGIC For STATEFUL streams (windowed aggregations, dropDuplicates with watermark, stream-stream
# MAGIC joins), the checkpoint holds more than offsets - it holds the STATE STORE (per-window
# MAGIC counts, dedup hashes, etc.). When the stream restarts, it RESTORES that state instead of
# MAGIC recomputing from the beginning. This is what lets a long-running aggregation restart
# MAGIC without re-reading its whole source history.
# MAGIC
# MAGIC **Source** (`ex7_source`): starts with 30 rows. Event timestamps span 10:00 to 10:29
# MAGIC (30 minutes), so a 5-minute tumbling window produces 6 windows.
# MAGIC **Target** (`ex7_target`): a `(window_start, window_end, event_count)` table written in
# MAGIC `complete` output mode (windowed aggregation rewrites the full result each batch).
# MAGIC **Checkpoint**: `/Volumes/db_code/checkpointing_recovery/checkpoints/ex7/`
# MAGIC
# MAGIC **What you will do AND observe**:
# MAGIC 1. Build a streaming query that counts events per 5-minute tumbling window on `event_ts`.
# MAGIC    Add a watermark of `0 seconds`. `complete` mode keeps every window's state whatever the watermark, so the appended rows still land in their window even though most of them are behind it. Write to `ex7_target` with
# MAGIC    `outputMode("complete")`, `trigger(availableNow=True)`, and checkpoint `/Volumes/db_code/checkpointing_recovery/checkpoints/ex7/`.
# MAGIC    Run once. The target has 6 windows with counts summing to 30.
# MAGIC 2. Append 5 more rows to `ex7_source` that fall into the `10:25-10:30` window. After the
# MAGIC    append, the source has 35 rows total. The window `10:25-10:30` originally held 5
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
# MAGIC 1. Count events with `.groupBy(window("event_ts", "5 minutes")).count()`, then select `window.start AS window_start`, `window.end AS window_end`, `count AS event_count` to match `ex7_target`
# MAGIC 2. Apply `.withWatermark("event_ts", "0 seconds")` before the groupBy
# MAGIC 3. `outputMode("complete")` and `format("delta")` writing to `ex7_target`
# MAGIC 4. Same checkpoint on both runs - state must be restored, not recomputed
# MAGIC
# MAGIC **Constraints**:
# MAGIC - `outputMode("complete")` overwrites the sink each batch - that's expected for windowed aggs
# MAGIC - Write it as a streaming query: a batch `groupBy` gives the same six rows but never reads or writes the state store in the checkpoint

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
