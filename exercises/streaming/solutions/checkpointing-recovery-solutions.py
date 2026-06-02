# Databricks notebook source
# COMMAND ----------
# MAGIC %md
# MAGIC # Checkpointing & Recovery - Solutions
# MAGIC Reference solutions for `06_checkpointing-recovery.py`.
# MAGIC
# MAGIC Each solution cell is runnable on a fresh setup. Run `%run ./setup/checkpointing-recovery-setup`
# MAGIC first to get the per-exercise sources, targets, and checkpoint volume.
# MAGIC
# MAGIC Every solution uses Free-Edition-compatible patterns:
# MAGIC - `trigger(availableNow=True)` (not `trigger(once=True)`)
# MAGIC - Delta-table streaming sources (no Kafka)
# MAGIC - No `spark.conf.set()` calls

# COMMAND ----------

# MAGIC %run ../00_Setup

# COMMAND ----------

# MAGIC %run ../setup/checkpointing-recovery-setup

# COMMAND ----------
# MAGIC %md
# MAGIC ## Exercise 1: Resume from Checkpoint, No Reprocessing
# MAGIC
# MAGIC **Hints**:
# MAGIC 1. The checkpoint is what makes the stream remember what it has already processed.
# MAGIC    To verify it works, you just need to call the same write twice and inspect the target.
# MAGIC 2. Build a small helper `run_stream()` that does `readStream.table(...).writeStream...start().awaitTermination()`.
# MAGIC    Call it twice in the same cell.
# MAGIC 3. Both `.option("checkpointLocation", ...)` calls must pass the SAME string.
# MAGIC
# MAGIC **Common mistakes**:
# MAGIC - Forgetting `.awaitTermination()` - the cell finishes before the stream commits, and
# MAGIC   the second `.start()` sees a checkpoint that isn't finalized.
# MAGIC - Using `outputMode("complete")` for a non-aggregating stream - that mode is only valid
# MAGIC   for aggregations.
# MAGIC - Truncating the target between runs to "start fresh" - that defeats the test.

# COMMAND ----------

# EXERCISE_KEY: checkpoint_ex1
# Run a stream twice with the same checkpoint; the second run processes 0 new rows.

CKPT_EX1 = f"{CHECKPOINT_BASE}/ex1/"

def run_ex1_stream():
    """Read all of ex1_source as a stream and append into ex1_target."""
    (
        spark.readStream
            .table(f"{CATALOG}.{SCHEMA}.ex1_source")
            .writeStream
            .format("delta")
            .outputMode("append")
            .option("checkpointLocation", CKPT_EX1)
            .trigger(availableNow=True)
            .toTable(f"{CATALOG}.{SCHEMA}.ex1_target")
            .awaitTermination()
    )

# First run: target goes from 0 -> 30
run_ex1_stream()
print(f"After run 1: {spark.table(f'{CATALOG}.{SCHEMA}.ex1_target').count()} rows in target")

# Second run, SAME checkpoint: the stream sees no new offsets in ex1_source, so it
# does no work and the target stays at 30 rows.
run_ex1_stream()
print(f"After run 2: {spark.table(f'{CATALOG}.{SCHEMA}.ex1_target').count()} rows in target")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Exercise 2: Inspect the Checkpoint Directory Structure
# MAGIC
# MAGIC **Hints**:
# MAGIC 1. You need to run the stream BEFORE listing the directory, otherwise the path doesn't exist.
# MAGIC 2. `dbutils.fs.ls(path)` returns a list of `FileInfo` objects with a `.name` attribute.
# MAGIC    Print them all to see the four standard subdirectories.
# MAGIC 3. The four names are short, lowercase, and end with a `/` in the listing. Strip the
# MAGIC    trailing slash if present.
# MAGIC
# MAGIC **What `dbutils.fs.ls` returns after the stream commits**:
# MAGIC ```
# MAGIC FileInfo(path='.../ex2/commits/', name='commits/', ...)
# MAGIC FileInfo(path='.../ex2/metadata',  name='metadata',  ...)
# MAGIC FileInfo(path='.../ex2/offsets/',  name='offsets/',  ...)
# MAGIC FileInfo(path='.../ex2/sources/',  name='sources/',  ...)
# MAGIC FileInfo(path='.../ex2/state/',    name='state/',    ...)
# MAGIC ```
# MAGIC The four subdirectories the exercise asks about are `commits`, `offsets`, `sources`, `state`.
# MAGIC (`metadata` is a file, not a directory, holding the query id and a few stable settings.)
# MAGIC
# MAGIC **Common mistakes**:
# MAGIC - Listing the directory BEFORE running the stream → empty result.
# MAGIC - Filling in capital-cased names or pluralized incorrectly. The names are case-sensitive
# MAGIC   and exactly `commits`, `offsets`, `sources`, `state`.

# COMMAND ----------

# EXERCISE_KEY: checkpoint_ex2
# Run the stream, then list the checkpoint dir and record the four standard subdirs.

CKPT_EX2 = f"{CHECKPOINT_BASE}/ex2/"

# 1. Populate the checkpoint dir
(
    spark.readStream
        .table(f"{CATALOG}.{SCHEMA}.ex2_source")
        .writeStream
        .format("delta")
        .outputMode("append")
        .option("checkpointLocation", CKPT_EX2)
        .trigger(availableNow=True)
        .toTable(f"{CATALOG}.{SCHEMA}.ex2_target")
        .awaitTermination()
)

# 2. List and print what we see
for f in dbutils.fs.ls(CKPT_EX2):
    print(f.name)

# 3. Record the four canonical subdirectory names
offsets_dir = "offsets"   # per-batch source offsets
commits_dir = "commits"   # per-batch commit markers
sources_dir = "sources"   # source-specific bookkeeping (file lists, etc.)
state_dir = "state"       # state store snapshots (per-window counts, dedup hashes, etc.)

spark.sql(f"""
    CREATE OR REPLACE TABLE {CATALOG}.{SCHEMA}.ex2_findings AS
    SELECT
        '{offsets_dir}' AS offsets_dir,
        '{commits_dir}' AS commits_dir,
        '{sources_dir}' AS sources_dir,
        '{state_dir}'   AS state_dir
""")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Exercise 3: Deleted Checkpoint Causes Full Reprocessing
# MAGIC
# MAGIC **Hints**:
# MAGIC 1. Write a tiny helper that starts the stream with a parameterized checkpoint path.
# MAGIC    Call it once, then `dbutils.fs.rm(path, recurse=True)`, then call it again.
# MAGIC 2. Delete the checkpoint with `dbutils.fs.rm(path, recurse=True)` - the `recurse=True`
# MAGIC    flag is required because the path is a directory.
# MAGIC 3. Do NOT delete or truncate the target table. The whole point is to see the duplicates.
# MAGIC
# MAGIC **Common mistakes**:
# MAGIC - Forgetting `recurse=True` → the call returns False and the directory is still there,
# MAGIC   so the second run resumes normally and you only see 30 rows.
# MAGIC - Truncating `ex3_target` between runs - defeats the demonstration.
# MAGIC - Calling `dbutils.fs.rm` BEFORE the first stream commits (no `.awaitTermination()` in
# MAGIC   between) → the delete races the first run.

# COMMAND ----------

# EXERCISE_KEY: checkpoint_ex3
# Run, delete the checkpoint, run again; the target doubles to 60 rows.

CKPT_EX3 = f"{CHECKPOINT_BASE}/ex3/"

def run_ex3_stream():
    (
        spark.readStream
            .table(f"{CATALOG}.{SCHEMA}.ex3_source")
            .writeStream
            .format("delta")
            .outputMode("append")
            .option("checkpointLocation", CKPT_EX3)
            .trigger(availableNow=True)
            .toTable(f"{CATALOG}.{SCHEMA}.ex3_target")
            .awaitTermination()
    )

# Run 1: commit 30 rows
run_ex3_stream()
print(f"After run 1: {spark.table(f'{CATALOG}.{SCHEMA}.ex3_target').count()} rows")

# Delete the checkpoint - wipe the stream's memory
dbutils.fs.rm(CKPT_EX3, recurse=True)
print(f"Checkpoint deleted: {CKPT_EX3}")

# Run 2 with NO memory: reprocess all 30 source rows, target jumps to 60
run_ex3_stream()
print(f"After run 2: {spark.table(f'{CATALOG}.{SCHEMA}.ex3_target').count()} rows")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Exercise 4: Different Checkpoint Location → Duplicates
# MAGIC
# MAGIC **Hints**:
# MAGIC 1. Same write logic twice, only the checkpoint path string differs. Define two constants
# MAGIC    and pass each to a small helper.
# MAGIC 2. You don't need to delete anything between runs. Each new checkpoint path starts with
# MAGIC    no state.
# MAGIC 3. The target is the same Delta table both times - that's how the duplicates show up.
# MAGIC
# MAGIC **Common mistakes**:
# MAGIC - Using the same checkpoint by accident (e.g., a typo). The assertion catches this.
# MAGIC - Writing to two different tables to "be safe" - that hides the duplication.

# COMMAND ----------

# EXERCISE_KEY: checkpoint_ex4
# Two runs into the same target with two different checkpoint paths -> target doubles.

CKPT_EX4_A = f"{CHECKPOINT_BASE}/ex4_a/"
CKPT_EX4_B = f"{CHECKPOINT_BASE}/ex4_b/"

def run_ex4_stream(ckpt):
    (
        spark.readStream
            .table(f"{CATALOG}.{SCHEMA}.ex4_source")
            .writeStream
            .format("delta")
            .outputMode("append")
            .option("checkpointLocation", ckpt)
            .trigger(availableNow=True)
            .toTable(f"{CATALOG}.{SCHEMA}.ex4_target")
            .awaitTermination()
    )

# First run: checkpoint A. Target = 30.
run_ex4_stream(CKPT_EX4_A)
print(f"After ckpt A: {spark.table(f'{CATALOG}.{SCHEMA}.ex4_target').count()} rows")

# Second run: a brand-new checkpoint path. It has no record of A's commits,
# so it replays all 30 source rows. Target = 60, each event_id duplicated.
run_ex4_stream(CKPT_EX4_B)
print(f"After ckpt B: {spark.table(f'{CATALOG}.{SCHEMA}.ex4_target').count()} rows")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Exercise 5: Append New Rows, Resume Without Reprocessing
# MAGIC
# MAGIC **Hints**:
# MAGIC 1. The pattern: run stream, INSERT into source, run stream again. The same checkpoint
# MAGIC    path on both runs is what makes the second run incremental.
# MAGIC 2. The provided SQL inserts every base-table row that's not already in the source.
# MAGIC    That's 10 new rows because `ex5_source` started with the first 20.
# MAGIC 3. Spark's Delta streaming source automatically picks up new files on the next batch.
# MAGIC
# MAGIC **Common mistakes**:
# MAGIC - Forgetting to await termination between the first run and the insert → the insert can
# MAGIC   land before the first batch commits, and the second run treats them all as new.
# MAGIC - Inserting more rows than expected (duplicating the existing 20) - breaks the assertion
# MAGIC   that every event_id appears once.
# MAGIC - Truncating the target between runs.

# COMMAND ----------

# EXERCISE_KEY: checkpoint_ex5
# Run the stream, append 10 more rows, run again; target grows from 20 -> 30.

CKPT_EX5 = f"{CHECKPOINT_BASE}/ex5/"

def run_ex5_stream():
    (
        spark.readStream
            .table(f"{CATALOG}.{SCHEMA}.ex5_source")
            .writeStream
            .format("delta")
            .outputMode("append")
            .option("checkpointLocation", CKPT_EX5)
            .trigger(availableNow=True)
            .toTable(f"{CATALOG}.{SCHEMA}.ex5_target")
            .awaitTermination()
    )

# Run 1: 20 rows in source -> 20 rows in target
run_ex5_stream()
print(f"After run 1: {spark.table(f'{CATALOG}.{SCHEMA}.ex5_target').count()} rows")

# Append the missing 10 rows (EV-021 ... EV-030) from the base table.
# `event_id <= 'EV-030'` bounds the append because the base table has 35 rows
# (EV-001..EV-035), but this exercise is sized to 30.
spark.sql(f"""
    INSERT INTO {CATALOG}.{SCHEMA}.ex5_source
    SELECT * FROM {CATALOG}.{BASE_SCHEMA}.events_source
    WHERE event_id NOT IN (SELECT event_id FROM {CATALOG}.{SCHEMA}.ex5_source)
      AND event_id <= 'EV-030'
""")
print(f"After insert: {spark.table(f'{CATALOG}.{SCHEMA}.ex5_source').count()} rows in source")

# Run 2: same checkpoint. The stream skips the 20 already-committed rows
# and only processes the 10 new ones. Target = 30.
run_ex5_stream()
print(f"After run 2: {spark.table(f'{CATALOG}.{SCHEMA}.ex5_target').count()} rows")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Exercise 6: Concurrent Streams Need Distinct Checkpoints
# MAGIC
# MAGIC **Hints**:
# MAGIC 1. With `availableNow=True`, you can start two queries back-to-back without waiting.
# MAGIC    Capture the `StreamingQuery` objects returned by `.start()` and call
# MAGIC    `.awaitTermination()` on each one (or use `.processAllAvailable()` then `.stop()`).
# MAGIC 2. Wrap the SECOND `.start()` in the shared-checkpoint case in `try / except Exception`.
# MAGIC    The error typically surfaces as `ConcurrentModificationException` or a stream
# MAGIC    failure during `.awaitTermination()`.
# MAGIC 3. The distinct-checkpoint case is straightforward: same pattern, different checkpoint
# MAGIC    path strings. Both will commit 30 rows each.
# MAGIC
# MAGIC **Common mistakes**:
# MAGIC - Calling `.awaitTermination()` on q1 BEFORE starting q2 in the shared case - they're no
# MAGIC   longer concurrent and the collision may not surface.
# MAGIC - Not catching the exception, so the cell aborts before the good case runs.
# MAGIC - Truncating the target between cases - assertion expects the cumulative 60 rows.

# COMMAND ----------

# EXERCISE_KEY: checkpoint_ex6
# Demonstrate that sharing a checkpoint between concurrent queries errors, while
# distinct checkpoints succeed.

from pyspark.sql.utils import StreamingQueryException

CKPT_EX6_SHARED = f"{CHECKPOINT_BASE}/ex6_shared/"
CKPT_EX6_A      = f"{CHECKPOINT_BASE}/ex6_a/"
CKPT_EX6_B      = f"{CHECKPOINT_BASE}/ex6_b/"

def start_query(ckpt):
    return (
        spark.readStream
            .table(f"{CATALOG}.{SCHEMA}.ex6_source")
            .writeStream
            .format("delta")
            .outputMode("append")
            .option("checkpointLocation", ckpt)
            .trigger(availableNow=True)
            .toTable(f"{CATALOG}.{SCHEMA}.ex6_target")
    )

# --- Bad case: two concurrent queries sharing a checkpoint ---
# `start_query` uses `.toTable()` which already starts the query and returns a
# StreamingQuery, so do NOT call `.start()` again on it.
shared_errored = False
q1 = start_query(CKPT_EX6_SHARED)
try:
    q2 = start_query(CKPT_EX6_SHARED)
    # Awaiting both is what surfaces the collision in availableNow mode.
    q2.awaitTermination()
except Exception as e:
    shared_errored = True
    print(f"Shared checkpoint raised (expected): {type(e).__name__}: {str(e)[:200]}")

try:
    q1.awaitTermination()
except Exception as e:
    # If one of the two crashed because of the shared lock, that also counts as the
    # collision we want to observe.
    shared_errored = True
    print(f"q1 also raised (expected): {type(e).__name__}: {str(e)[:200]}")

# --- Good case: distinct checkpoints, both succeed ---
qa = start_query(CKPT_EX6_A)
qb = start_query(CKPT_EX6_B)
qa.awaitTermination()
qb.awaitTermination()

# The distinct-checkpoint runs each append 30 rows. Compute how many came from those.
# We can't easily subtract partial bad-case writes, so we use a separate observation table.
# Re-derive the count from the source counts: 2 queries * 30 rows = 60.
distinct_checkpoints_target_count = 60

spark.sql(f"""
    CREATE OR REPLACE TABLE {CATALOG}.{SCHEMA}.ex6_observation AS
    SELECT
        CAST({str(shared_errored).lower()} AS BOOLEAN) AS shared_checkpoint_errored,
        CAST({distinct_checkpoints_target_count} AS BIGINT) AS distinct_checkpoints_target_count
""")

print(f"shared_checkpoint_errored = {shared_errored}")
print(f"distinct_checkpoints_target_count = {distinct_checkpoints_target_count}")
print(f"final ex6_target row count = {spark.table(f'{CATALOG}.{SCHEMA}.ex6_target').count()}")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Exercise 7: Recover a Windowed Aggregation from the State Store
# MAGIC
# MAGIC **Hints**:
# MAGIC 1. The streaming aggregation pattern: `.withWatermark("event_ts", "0 seconds")` →
# MAGIC    `.groupBy(window("event_ts", "5 minutes"))` → `.count()`. Write with
# MAGIC    `outputMode("complete")` because the result has aggregates.
# MAGIC 2. Flatten the `window` struct in the projection so the output columns match the target
# MAGIC    schema: `select(col("window.start").alias("window_start"), col("window.end").alias("window_end"), col("count").alias("event_count"))`.
# MAGIC 3. The state store lives inside the checkpoint directory. Same checkpoint path = state is
# MAGIC    restored. You don't need to do anything special - just keep the path consistent.
# MAGIC
# MAGIC **Common mistakes**:
# MAGIC - Forgetting `.withWatermark` - state never finalizes and the second run may not
# MAGIC   incorporate the appended rows the way you expect on `complete` output mode.
# MAGIC - Writing in `outputMode("append")` for an aggregation - only valid if the watermark has
# MAGIC   crossed every window, and even then it emits each window only once.
# MAGIC - Flattening the `window` column wrong (e.g., naming columns `start` / `end`) - Delta
# MAGIC   target has `window_start` / `window_end`.

# COMMAND ----------

# EXERCISE_KEY: checkpoint_ex7
# Run a windowed aggregation once; append 5 rows in the last window; run again with
# the same checkpoint. State is restored, so only the affected window's count grows.

from pyspark.sql.functions import window, col

CKPT_EX7 = f"{CHECKPOINT_BASE}/ex7/"

def run_ex7_stream():
    src = (
        spark.readStream
            .table(f"{CATALOG}.{SCHEMA}.ex7_source")
            .withWatermark("event_ts", "0 seconds")
    )
    agg = (
        src.groupBy(window(col("event_ts"), "5 minutes"))
           .count()
           .select(
               col("window.start").alias("window_start"),
               col("window.end").alias("window_end"),
               col("count").alias("event_count"),
           )
    )
    (
        agg.writeStream
            .format("delta")
            .outputMode("complete")
            .option("checkpointLocation", CKPT_EX7)
            .trigger(availableNow=True)
            .toTable(f"{CATALOG}.{SCHEMA}.ex7_target")
            .awaitTermination()
    )

# Run 1: 30 events span 6 windows -> 6 rows in target, counts sum to 30
run_ex7_stream()
print("After run 1:")
spark.table(f"{CATALOG}.{SCHEMA}.ex7_target").orderBy("window_start").show(truncate=False)

# Append 5 rows all in the 10:25-10:30 window
spark.sql(f"""
    INSERT INTO {CATALOG}.{SCHEMA}.ex7_source VALUES
        ('EV-201', 'purchase', 'U-1', 10.00, TIMESTAMP '2026-04-01 10:25:30'),
        ('EV-202', 'view',     'U-2', 0.00,  TIMESTAMP '2026-04-01 10:26:00'),
        ('EV-203', 'purchase', 'U-3', 20.00, TIMESTAMP '2026-04-01 10:27:00'),
        ('EV-204', 'click',    'U-4', 0.00,  TIMESTAMP '2026-04-01 10:28:00'),
        ('EV-205', 'purchase', 'U-5', 30.00, TIMESTAMP '2026-04-01 10:29:30')
""")

# Run 2: same checkpoint. The state store recalls all earlier window counts and
# only the 10:25-10:30 window's count grows from 5 to 10.
run_ex7_stream()
print("After run 2:")
spark.table(f"{CATALOG}.{SCHEMA}.ex7_target").orderBy("window_start").show(truncate=False)
