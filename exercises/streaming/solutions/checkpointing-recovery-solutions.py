# Databricks notebook source
# COMMAND ----------
# MAGIC %md
# MAGIC # Checkpointing & Recovery - Solutions
# MAGIC Reference solutions for `06_checkpointing-recovery.py`.
# MAGIC
# MAGIC The next two cells run `00_Setup` and the topic setup, so every solution cell below starts from a fresh setup.
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
# MAGIC 2. Build a small helper `run_stream()` that does `spark.readStream.table(...).writeStream...toTable(...).awaitTermination()`.
# MAGIC    Call it twice in the same cell.
# MAGIC 3. Both `.option("checkpointLocation", ...)` calls must pass the SAME string.
# MAGIC
# MAGIC **Common mistakes**:
# MAGIC - Forgetting `.awaitTermination()`: the second run starts while the first is still active on the same checkpoint, Spark stops the first run, and the validation cell can read the target before 30 rows have committed.
# MAGIC - Using `outputMode("complete")` for a non-aggregating stream - that mode is only valid
# MAGIC   for aggregations.
# MAGIC - Truncating the target between runs to "start fresh": the second run adds 0 rows, so the target ends empty instead of at 30.

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
# MAGIC    Print them all and match each name to a role.
# MAGIC 3. Names in the listing are lowercase, and directories end with `/`. Strip the slash.
# MAGIC
# MAGIC **What `dbutils.fs.ls` returns after the stream commits**: `commits/`, `offsets/` and the `metadata` file, plus `sources/` when the source keeps bookkeeping there. `state/` appears only for stateful queries, such as the windowed count in Exercise 7. The three names to record are `offsets` (per-batch source offsets), `commits` (per-batch commit markers) and `metadata` (the file holding the query ID).
# MAGIC
# MAGIC **Common mistakes**:
# MAGIC - Listing the directory BEFORE running the stream: `ex2/` does not exist yet, so `dbutils.fs.ls` raises a file-not-found error.
# MAGIC - Capitalizing a name or changing its plural (`Offsets`, `commit`): the names are case-sensitive and exactly `offsets`, `commits`, `metadata`.

# COMMAND ----------

# EXERCISE_KEY: checkpoint_ex2
# Run the stream, then list the checkpoint dir and record the three names by role.

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

# 2. List and print what the checkpoint holds
for f in dbutils.fs.ls(CKPT_EX2):
    print(f.name)

# 3. Record the names that hold each role, as read from the listing
planned_batches_dir = "offsets"   # offset log: written BEFORE each batch runs
finished_batches_dir = "commits"  # commit log: written AFTER each batch finishes
query_id_file = "metadata"        # top-level file holding the query's unique ID

spark.sql(f"""
    CREATE OR REPLACE TABLE {CATALOG}.{SCHEMA}.ex2_findings AS
    SELECT
        '{planned_batches_dir}'  AS planned_batches_dir,
        '{finished_batches_dir}' AS finished_batches_dir,
        '{query_id_file}'        AS query_id_file
""")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Exercise 3: Deleted Checkpoint Causes Full Reprocessing
# MAGIC
# MAGIC **Hints**:
# MAGIC 1. The stream's memory is the checkpoint directory. Run once, remove that memory, run again.
# MAGIC 2. Delete it with `dbutils.fs.rm(path, recurse=True)`: `recurse=True` is required because the path is a directory.
# MAGIC 3. Do NOT delete or truncate the target table. The whole point is to see the duplicates.
# MAGIC
# MAGIC **Common mistakes**:
# MAGIC - Forgetting `recurse=True`: the checkpoint is a non-empty directory, so `dbutils.fs.rm` raises an error and the cell stops before the second run.
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
# MAGIC 2. The provided SQL inserts the base-table rows up to `EV-030` that are not already in the source: 10 rows (`EV-021` ... `EV-030`).
# MAGIC 3. Spark's Delta streaming source automatically picks up new files on the next batch.
# MAGIC
# MAGIC **Common mistakes**:
# MAGIC - Forgetting `.awaitTermination()` after the first run: the second start on the same checkpoint stops the first run, and the counts you print no longer show 20, then 30.
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
# MAGIC    Capture the `StreamingQuery` objects returned by `.toTable(...)` and call
# MAGIC    `.awaitTermination()` on each one.
# MAGIC 2. Wrap the shared-checkpoint queries in `try / except Exception` and record whether anything raised. On one cluster nothing usually does: Spark stops the first run when the second starts on the same checkpoint (`spark.sql.streaming.stopActiveRunOnRestart`).
# MAGIC 3. The distinct-checkpoint case: each query reads its own source (`ex6_source_a`, `ex6_source_b`) with its own checkpoint path, and both append into `ex6_target`. Count `ex6_target` after both finish: 15 + 15 = 30 rows.
# MAGIC
# MAGIC **Common mistakes**:
# MAGIC - Not catching the exception, so the cell aborts before the good case runs.
# MAGIC - Pointing both good-case queries at the same source: two checkpoints on one source each replay all of it, so every event_id lands in `ex6_target` twice (the Exercise 4 bug).
# MAGIC - Truncating `ex6_target` between runs or before counting: `distinct_checkpoints_target_count` must be the final row count the validation cell reads.

# COMMAND ----------

# EXERCISE_KEY: checkpoint_ex6
# Start two queries on one shared checkpoint (Spark stops the first), then two on distinct
# checkpoints, each reading its own 15-row source into ex6_target.

CKPT_EX6_SHARED = f"{CHECKPOINT_BASE}/ex6_shared/"
CKPT_EX6_A      = f"{CHECKPOINT_BASE}/ex6_a/"
CKPT_EX6_B      = f"{CHECKPOINT_BASE}/ex6_b/"

def start_query(source_table, target_table, ckpt):
    return (
        spark.readStream
            .table(f"{CATALOG}.{SCHEMA}.{source_table}")
            .writeStream
            .format("delta")
            .outputMode("append")
            .option("checkpointLocation", ckpt)
            .trigger(availableNow=True)
            .toTable(f"{CATALOG}.{SCHEMA}.{target_table}")
    )

# Bad case: two queries sharing one checkpoint.
# `start_query` uses `.toTable()` which already starts the query and returns a
# StreamingQuery, so do NOT call `.start()` again on it.
shared_errored = False
q1 = start_query("ex6_source_a", "ex6_shared_target", CKPT_EX6_SHARED)
try:
    # On one cluster, starting q2 on the same checkpoint stops q1 instead of raising.
    q2 = start_query("ex6_source_a", "ex6_shared_target", CKPT_EX6_SHARED)
    q2.awaitTermination()
except Exception as e:
    shared_errored = True
    print(f"Shared checkpoint raised: {type(e).__name__}: {str(e)[:200]}")

try:
    q1.awaitTermination()
except Exception as e:
    # q1 was stopped by q2's start, which ends it without an exception; anything raised here still counts.
    shared_errored = True
    print(f"q1 raised: {type(e).__name__}: {str(e)[:200]}")

# Good case: distinct checkpoints and disjoint sources, both succeed
qa = start_query("ex6_source_a", "ex6_target", CKPT_EX6_A)
qb = start_query("ex6_source_b", "ex6_target", CKPT_EX6_B)
qa.awaitTermination()
qb.awaitTermination()

# Measure the rows the two distinct-checkpoint runs left in ex6_target
distinct_checkpoints_target_count = spark.table(f"{CATALOG}.{SCHEMA}.ex6_target").count()

spark.sql(f"""
    CREATE OR REPLACE TABLE {CATALOG}.{SCHEMA}.ex6_observation AS
    SELECT
        CAST({str(shared_errored).lower()} AS BOOLEAN) AS shared_checkpoint_errored,
        CAST({distinct_checkpoints_target_count} AS BIGINT) AS distinct_checkpoints_target_count
""")

print(f"shared_checkpoint_errored = {shared_errored}")
print(f"distinct_checkpoints_target_count = {distinct_checkpoints_target_count}")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Exercise 7: Recover a Windowed Aggregation from the State Store
# MAGIC
# MAGIC **Hints**:
# MAGIC 1. The streaming aggregation pattern: `.withWatermark("event_ts", "0 seconds")` →
# MAGIC    `.groupBy(window("event_ts", "5 minutes"))` → `.count()`. Write with
# MAGIC    `outputMode("complete")` so every run rewrites all windows, including the one that is still open.
# MAGIC 2. Flatten the `window` struct in the projection so the output columns match the target
# MAGIC    schema: `select(col("window.start").alias("window_start"), col("window.end").alias("window_end"), col("count").alias("event_count"))`.
# MAGIC 3. The state store lives inside the checkpoint directory. Same checkpoint path = state is
# MAGIC    restored. You don't need to do anything special - just keep the path consistent.
# MAGIC 4. The watermark does not change this result: `complete` mode keeps every window in state. A watermark only cleans state in `append` and `update` mode.
# MAGIC
# MAGIC **Common mistakes**:
# MAGIC - Writing in `outputMode("append")`: append emits a window only after the watermark passes its end. No row after 10:30 ever moves the watermark past the 10:25-10:30 window, so the window this exercise grows is never written.
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
