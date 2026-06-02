# Databricks notebook source
# COMMAND ----------
# MAGIC %md
# MAGIC # Stream-Static Joins - Solutions
# MAGIC **Topic**: Streaming | **Exercises**: 7
# MAGIC
# MAGIC Reference solutions, hints, and common mistakes for each exercise.
# MAGIC Try solving the exercises first before looking here.

# COMMAND ----------

CATALOG = "db_code"
SCHEMA = "stream_static_joins"
BASE_SCHEMA = "streaming"
CHECKPOINT_BASE = f"/Volumes/{CATALOG}/{SCHEMA}/checkpoints"

# COMMAND ----------
# MAGIC %md
# MAGIC ## Exercise 1: Inner Stream-Static Join
# MAGIC
# MAGIC **Hints**:
# MAGIC 1. Stream-static joins use the regular `DataFrame.join()` API - there is no special streaming-join method.
# MAGIC    The "magic" is that one side is a streaming DataFrame and the other is a batch DataFrame.
# MAGIC 2. Read the stream with `spark.readStream.table(...)` and the static side with `spark.read.table(...)`.
# MAGIC    Each micro-batch re-reads the static side at its current Delta version (snapshot semantics).
# MAGIC 3. Use `.writeStream.toTable(...).trigger(availableNow=True).awaitTermination()` so the stream
# MAGIC    finishes deterministically on Free Edition.
# MAGIC
# MAGIC **Common mistakes**:
# MAGIC - Reading the static side with `spark.readStream` - that turns it into a stream-stream join (different semantics, requires watermarks).
# MAGIC - Forgetting `.awaitTermination()` - the streaming write starts asynchronously, and the next cell runs before the data lands.
# MAGIC - Using `trigger(once=True)` - deprecated; use `trigger(availableNow=True)`.
# MAGIC - Putting the checkpoint outside a Volume - Free Edition cannot write checkpoints to DBFS root paths reliably.

# COMMAND ----------

# EXERCISE_KEY: stream_static_ex1
events_stream = spark.readStream.table(f"{CATALOG}.{SCHEMA}.ex1_source")
customers = spark.read.table(f"{CATALOG}.{SCHEMA}.customers_static")

(events_stream
    .join(customers, "customer_id", "inner")
    .writeStream
    .format("delta")
    .option("checkpointLocation", f"{CHECKPOINT_BASE}/ex1")
    .trigger(availableNow=True)
    .toTable(f"{CATALOG}.{SCHEMA}.ex1_target")
    .awaitTermination()
)

# COMMAND ----------
# MAGIC %md
# MAGIC ## Exercise 2: Project Specific Enriched Columns
# MAGIC
# MAGIC **Hints**:
# MAGIC 1. After the join, chain a `.select(...)` to keep only the columns you want.
# MAGIC 2. List the event columns explicitly: `event_id`, `event_type`, `customer_id`, `amount`, `event_ts`.
# MAGIC 3. From the static side, project only `name`, `region`, `tier`. Omit `is_active`.
# MAGIC
# MAGIC **Common mistakes**:
# MAGIC - Selecting from the static side BEFORE the join - this drops the `customer_id` you need to join on
# MAGIC   unless you keep it explicitly.
# MAGIC - Using `customers.drop("is_active")` instead of an explicit select - works, but explicit projection is clearer
# MAGIC   and protects against future dimension columns being added.
# MAGIC - Forgetting that `select` returns a new DataFrame - chain it directly into `.writeStream`.

# COMMAND ----------

# EXERCISE_KEY: stream_static_ex2
from pyspark.sql import functions as F

events_stream = spark.readStream.table(f"{CATALOG}.{SCHEMA}.ex2_source")
customers = spark.read.table(f"{CATALOG}.{SCHEMA}.customers_static")

(events_stream
    .join(customers, "customer_id", "inner")
    .select(
        F.col("event_id"),
        F.col("event_type"),
        F.col("customer_id"),
        F.col("amount"),
        F.col("event_ts"),
        F.col("name"),
        F.col("region"),
        F.col("tier"),
    )
    .writeStream
    .format("delta")
    .option("checkpointLocation", f"{CHECKPOINT_BASE}/ex2")
    .trigger(availableNow=True)
    .toTable(f"{CATALOG}.{SCHEMA}.ex2_target")
    .awaitTermination()
)

# COMMAND ----------
# MAGIC %md
# MAGIC ## Exercise 3: Left Outer Join (Stream on the Left)
# MAGIC
# MAGIC **Hints**:
# MAGIC 1. Pass `"left_outer"` (or `"left"`) as the third argument to `.join(...)`.
# MAGIC 2. The stream goes on the LEFT (the side you don't want to lose rows from).
# MAGIC 3. Unmatched events get NULL for every column projected from the static side.
# MAGIC
# MAGIC **Common mistakes**:
# MAGIC - Putting the stream on the right (`customers.join(events_stream, ...)`) - flips semantics and
# MAGIC   sometimes hits the unsupported right-outer error.
# MAGIC - Using `"inner"` and being surprised that 8 rows disappear.
# MAGIC - Joining on `customer_id` using `events.customer_id == customers.customer_id` and ending up with
# MAGIC   two `customer_id` columns - prefer the single-column shorthand `.join(customers, "customer_id", ...)`.

# COMMAND ----------

# EXERCISE_KEY: stream_static_ex3
events_stream = spark.readStream.table(f"{CATALOG}.{SCHEMA}.ex3_source")
customers = spark.read.table(f"{CATALOG}.{SCHEMA}.customers_static")

(events_stream
    .join(customers, "customer_id", "left_outer")
    .writeStream
    .format("delta")
    .option("checkpointLocation", f"{CHECKPOINT_BASE}/ex3")
    .trigger(availableNow=True)
    .toTable(f"{CATALOG}.{SCHEMA}.ex3_target")
    .awaitTermination()
)

# COMMAND ----------
# MAGIC %md
# MAGIC ## Exercise 4: Filter the Static Side Before Joining
# MAGIC
# MAGIC **Hints**:
# MAGIC 1. Apply `.filter("is_active = TRUE")` on the static DataFrame BEFORE passing it to `.join(...)`.
# MAGIC 2. Spark's Catalyst optimizer pushes the predicate down so the join probes a smaller hash table
# MAGIC    on every micro-batch. The DataFrame definition is what tells Spark "only active rows matter."
# MAGIC 3. Use inner join - inactive matches should disappear from the output entirely.
# MAGIC
# MAGIC **Common mistakes**:
# MAGIC - Filtering AFTER the join (`joined.filter("is_active = TRUE")`) - the static side is still read in
# MAGIC   full on every batch. The output looks the same but the work is wasted.
# MAGIC - Using left outer here - the spec says drop inactive matches, not keep them with NULL.
# MAGIC - Forgetting that `is_active` is BOOLEAN, not STRING - use `= TRUE`, not `= 'TRUE'`.

# COMMAND ----------

# EXERCISE_KEY: stream_static_ex4
events_stream = spark.readStream.table(f"{CATALOG}.{SCHEMA}.ex4_source")
active_customers = spark.read.table(f"{CATALOG}.{SCHEMA}.customers_static").filter("is_active = TRUE")

(events_stream
    .join(active_customers, "customer_id", "inner")
    .writeStream
    .format("delta")
    .option("checkpointLocation", f"{CHECKPOINT_BASE}/ex4")
    .trigger(availableNow=True)
    .toTable(f"{CATALOG}.{SCHEMA}.ex4_target")
    .awaitTermination()
)

# COMMAND ----------
# MAGIC %md
# MAGIC ## Exercise 5: Broadcast the Static Dimension
# MAGIC
# MAGIC **Hints**:
# MAGIC 1. On serverless you can't set `spark.conf` - find a DataFrame method that tells Spark which
# MAGIC    side to broadcast.
# MAGIC 2. The method is `.hint(...)` on a DataFrame. Think about which side of the join is small
# MAGIC    enough to ship to every executor.
# MAGIC 3. `customers_df.hint("broadcast")` on the static DataFrame, then join as usual. To verify
# MAGIC    the hint took effect, capture the executed plan as a string and save it to a table;
# MAGIC    look for "BroadcastHashJoin".
# MAGIC
# MAGIC **Common mistakes**:
# MAGIC - Hinting the stream side - Spark refuses to broadcast a streaming DataFrame.
# MAGIC - Using `spark.conf.set("spark.sql.autoBroadcastJoinThreshold", ...)` - blocked on serverless.
# MAGIC - Calling `.explain()` and letting it print to the notebook - to capture it as a string, redirect
# MAGIC   stdout with `io.StringIO` + `contextlib.redirect_stdout` (see solution below).
# MAGIC - Reading the explain plan of the WRITESTREAM - the plan we want is on the joined batch DataFrame
# MAGIC   that Catalyst materializes per micro-batch.

# COMMAND ----------

# EXERCISE_KEY: stream_static_ex5
import io, contextlib

events_stream = spark.readStream.table(f"{CATALOG}.{SCHEMA}.ex5_source")
customers_broadcast = spark.read.table(f"{CATALOG}.{SCHEMA}.customers_static").hint("broadcast")

# Capture the explain plan for the EQUIVALENT batch join (same join, same hint, but events read
# as batch instead of stream). The physical operator chosen by Catalyst is the same and will
# include BroadcastHashJoin when the hint takes effect. Capturing .explain() output on a
# streaming DataFrame directly is unreliable - streaming queries materialize per-batch plans.
events_batch = spark.read.table(f"{CATALOG}.{SCHEMA}.ex5_source")
batch_joined_for_plan = events_batch.join(customers_broadcast, "customer_id", "inner")

buf = io.StringIO()
with contextlib.redirect_stdout(buf):
    batch_joined_for_plan.explain(extended=False)
plan_str = buf.getvalue()

spark.createDataFrame([(plan_str,)], "plan STRING").write.mode("overwrite").saveAsTable(
    f"{CATALOG}.{SCHEMA}.ex5_explain"
)

# Now run the actual streaming write with the broadcast-hinted static side
(events_stream
    .join(customers_broadcast, "customer_id", "inner")
    .writeStream
    .format("delta")
    .option("checkpointLocation", f"{CHECKPOINT_BASE}/ex5")
    .trigger(availableNow=True)
    .toTable(f"{CATALOG}.{SCHEMA}.ex5_target")
    .awaitTermination()
)

# COMMAND ----------
# MAGIC %md
# MAGIC ## Exercise 6: Picking Up a Static-Side Refresh Between Stream Runs
# MAGIC
# MAGIC **Hints**:
# MAGIC 1. The two runs are two separate streaming queries. Each has its OWN source, its OWN target table,
# MAGIC    and MUST have its OWN checkpoint subdirectory. Sharing a checkpoint would tie the queries together.
# MAGIC 2. The static side is read fresh on every micro-batch. After run 1 finishes, any INSERT into
# MAGIC    `customers_static` is visible to run 2 with no special action.
# MAGIC 3. Order matters: run 1 (empty result) -> INSERT C-999 -> run 2 (2 enriched rows).
# MAGIC
# MAGIC **Common mistakes**:
# MAGIC - Sharing the same checkpoint for both runs - the second run resumes the first's state and writes nothing new.
# MAGIC - INSERTing C-999 BEFORE run 1 - now run 1 emits rows too and the assertion fails.
# MAGIC - Caching `customers_static` (`.cache()`) - the cached snapshot doesn't see the INSERT.
# MAGIC - Forgetting `.awaitTermination()` after run 1 - the INSERT then executes before run 1's batch
# MAGIC   commits, and run 1's row count may flake.

# COMMAND ----------

# EXERCISE_KEY: stream_static_ex6
# --- Run 1: C-999 does NOT exist yet -> inner join emits 0 rows ---
events_stream_1 = spark.readStream.table(f"{CATALOG}.{SCHEMA}.ex6_source_run1")
customers_1 = spark.read.table(f"{CATALOG}.{SCHEMA}.customers_static")

(events_stream_1
    .join(customers_1, "customer_id", "inner")
    .writeStream
    .format("delta")
    .option("checkpointLocation", f"{CHECKPOINT_BASE}/ex6_run1")
    .trigger(availableNow=True)
    .toTable(f"{CATALOG}.{SCHEMA}.ex6_target_run1")
    .awaitTermination()
)

# --- Refresh the static dimension: insert C-999 ---
spark.sql(f"""
    INSERT INTO {CATALOG}.{SCHEMA}.customers_static VALUES
        ('C-999', 'New Customer', 'us-east', 'gold', TRUE)
""")

# --- Run 2: C-999 now exists -> inner join emits 2 rows ---
events_stream_2 = spark.readStream.table(f"{CATALOG}.{SCHEMA}.ex6_source_run2")
customers_2 = spark.read.table(f"{CATALOG}.{SCHEMA}.customers_static")

(events_stream_2
    .join(customers_2, "customer_id", "inner")
    .writeStream
    .format("delta")
    .option("checkpointLocation", f"{CHECKPOINT_BASE}/ex6_run2")
    .trigger(availableNow=True)
    .toTable(f"{CATALOG}.{SCHEMA}.ex6_target_run2")
    .awaitTermination()
)

# COMMAND ----------
# MAGIC %md
# MAGIC ## Exercise 7: The Unsupported Right Outer - Diagnose and Choose the Correct Alternative
# MAGIC
# MAGIC **Hints**:
# MAGIC 1. Spark raises `AnalysisException` at plan time when you ask for a right outer (or full outer)
# MAGIC    with a streaming DataFrame on the left. Catch a broad `Exception` and read `str(e)`.
# MAGIC 2. The exception fires when you call `.start()` / `.toTable()` on the writeStream - not when you
# MAGIC    define the join. Wrap the writeStream call in `try / except`.
# MAGIC 3. The correct alternative for "keep every event even if no match" is `left_outer` with the
# MAGIC    stream on the LEFT (see Exercise 3).
# MAGIC
# MAGIC **Common mistakes**:
# MAGIC - Catching the exception when defining the join - the join expression builds fine; the failure is at start.
# MAGIC - Saving the exception's `type` instead of its `str(...)` - assertion looks for the message text.
# MAGIC - Swapping sides to put the stream on the right (`customers.join(events_stream, "customer_id", "left_outer")`)
# MAGIC   - this is equivalent to a right outer with the stream on the left and raises the same error.
# MAGIC   Keep the stream on the LEFT.
# MAGIC - Trying full outer instead - same unsupported error.

# COMMAND ----------

# EXERCISE_KEY: stream_static_ex7
events_stream = spark.readStream.table(f"{CATALOG}.{SCHEMA}.ex7_source")
customers = spark.read.table(f"{CATALOG}.{SCHEMA}.customers_static")

# --- Attempt right outer; catch the analysis exception ---
error_msg = None
try:
    (events_stream
        .join(customers, "customer_id", "right_outer")
        .writeStream
        .format("delta")
        .option("checkpointLocation", f"{CHECKPOINT_BASE}/ex7_attempt")
        .trigger(availableNow=True)
        .toTable(f"{CATALOG}.{SCHEMA}.ex7_target_attempt")
        .awaitTermination()
    )
except Exception as e:
    error_msg = str(e)

assert error_msg is not None, "Right outer with stream on the left MUST raise an exception"

spark.createDataFrame([(error_msg,)], "error_msg STRING").write.mode("overwrite").saveAsTable(
    f"{CATALOG}.{SCHEMA}.ex7_error_log"
)

# --- Correct alternative: left outer with stream on the left ---
(events_stream
    .join(customers, "customer_id", "left_outer")
    .writeStream
    .format("delta")
    .option("checkpointLocation", f"{CHECKPOINT_BASE}/ex7")
    .trigger(availableNow=True)
    .toTable(f"{CATALOG}.{SCHEMA}.ex7_target")
    .awaitTermination()
)
