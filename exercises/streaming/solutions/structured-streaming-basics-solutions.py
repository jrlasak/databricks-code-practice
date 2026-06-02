# Databricks notebook source
# COMMAND ----------
# MAGIC %md
# MAGIC # Structured Streaming Basics - Solutions
# MAGIC **Topic**: Streaming | **Exercises**: 8
# MAGIC
# MAGIC Reference solutions, hints, and common mistakes for each exercise.
# MAGIC Try solving the exercises first before looking here.

# COMMAND ----------

CATALOG = "db_code"
SCHEMA = "structured_streaming_basics"
BASE_SCHEMA = "streaming"
CHECKPOINT_BASE = f"/Volumes/{CATALOG}/{SCHEMA}/checkpoints"

# COMMAND ----------
# MAGIC %md
# MAGIC ## Exercise 1: Read a Delta Table as a Stream, Write to Another Delta Table
# MAGIC
# MAGIC **Hints**:
# MAGIC 1. `spark.readStream.table("...")` treats an existing Delta table as a streaming source -
# MAGIC    every new commit produces new streamed rows
# MAGIC 2. The write side needs `.format("delta")`, a `checkpointLocation`, and `.toTable(...)`
# MAGIC    (or `.format("delta").outputMode("append").start()` against a path)
# MAGIC 3. `trigger(availableNow=True)` plus `.awaitTermination()` is the canonical "run once and
# MAGIC    stop" pattern for batch-style streaming on Databricks
# MAGIC
# MAGIC **Common mistakes**:
# MAGIC - Using `spark.read` instead of `spark.readStream` (no offsets, won't track progress)
# MAGIC - Using `trigger(once=True)` (deprecated since DBR 11.3; `availableNow=True` is correct)
# MAGIC - Forgetting `.awaitTermination()` (cell returns before stream finishes, assertions fail)
# MAGIC - Putting the checkpoint somewhere outside a Volume (works in some environments, fails on
# MAGIC   Free Edition where DBFS is restricted)

# COMMAND ----------

# EXERCISE_KEY: ss_basics_ex1
(spark.readStream
    .table(f"{CATALOG}.{SCHEMA}.ex1_source")
    .writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", f"{CHECKPOINT_BASE}/ex1")
    .trigger(availableNow=True)
    .toTable(f"{CATALOG}.{SCHEMA}.ex1_target")
    .awaitTermination()
)

# COMMAND ----------
# MAGIC %md
# MAGIC ## Exercise 2: Filter a Streaming DataFrame Before Writing
# MAGIC
# MAGIC **Hints**:
# MAGIC 1. Streaming DataFrames support `.filter(...)` and `.where(...)` exactly like batch DataFrames
# MAGIC 2. Compose the filter directly on the result of `spark.readStream.table(...)` before
# MAGIC    starting the writeStream
# MAGIC 3. Filtering on the streaming DF is what we want - it gets pushed into the streaming plan
# MAGIC    instead of running as a separate post-process
# MAGIC
# MAGIC **Common mistakes**:
# MAGIC - Running the filter as a post-write SQL query against the target (defeats the purpose -
# MAGIC   non-purchase rows still land in the target)
# MAGIC - Filtering on the wrong column or wrong value (assertion count mismatch)
# MAGIC - Mixing PySpark column syntax with SQL string syntax: use `F.col("event_type") == "purchase"`
# MAGIC   in Python OR `"event_type = 'purchase'"` as a SQL string - not both

# COMMAND ----------

# EXERCISE_KEY: ss_basics_ex2
(spark.readStream
    .table(f"{CATALOG}.{SCHEMA}.ex2_source")
    .filter("event_type = 'purchase'")
    .writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", f"{CHECKPOINT_BASE}/ex2")
    .trigger(availableNow=True)
    .toTable(f"{CATALOG}.{SCHEMA}.ex2_target")
    .awaitTermination()
)

# COMMAND ----------
# MAGIC %md
# MAGIC ## Exercise 3: Add a Derived Column to a Streaming DataFrame
# MAGIC
# MAGIC **Hints**:
# MAGIC 1. Use `.withColumn("amount_with_tax", F.col("amount") * 1.10)` on the streaming DF
# MAGIC 2. Import `pyspark.sql.functions as F` once at the top of the cell
# MAGIC 3. Streaming `withColumn` runs on each micro-batch; the projection becomes part of the
# MAGIC    streaming plan
# MAGIC
# MAGIC **Common mistakes**:
# MAGIC - Referring to the column by bare Python name (e.g., `amount * 1.10` with no import) -
# MAGIC    use `F.col("amount") * 1.10` or a SQL expression in `selectExpr`/`expr`.
# MAGIC - Using `selectExpr` and dropping other columns - assertions check that `amount` is still present
# MAGIC - Naming the column something other than `amount_with_tax` (validator is strict)

# COMMAND ----------

# EXERCISE_KEY: ss_basics_ex3
from pyspark.sql import functions as F

(spark.readStream
    .table(f"{CATALOG}.{SCHEMA}.ex3_source")
    .withColumn("amount_with_tax", F.col("amount") * 1.10)
    .writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", f"{CHECKPOINT_BASE}/ex3")
    .trigger(availableNow=True)
    .toTable(f"{CATALOG}.{SCHEMA}.ex3_target")
    .awaitTermination()
)

# COMMAND ----------
# MAGIC %md
# MAGIC ## Exercise 4: Append-Only Ingestion - Pick the Right Output Mode
# MAGIC
# MAGIC **Hints**:
# MAGIC 1. Output mode rules: `append` works for any non-aggregating query and for aggregations
# MAGIC    with a watermark that emits closed windows; `complete` requires an aggregation and
# MAGIC    emits the FULL result table every batch; `update` requires an aggregation and emits
# MAGIC    only changed rows
# MAGIC 2. This query has no aggregation - only `complete` and `update` would fail; only `append`
# MAGIC    is legal
# MAGIC 3. Always state output mode explicitly in production code so readers see your intent
# MAGIC
# MAGIC **Common mistakes**:
# MAGIC - Picking `complete` because "it sounds safer" - it's illegal without an aggregation and
# MAGIC    Spark throws `AnalysisException` immediately
# MAGIC - Picking `update` thinking it means "update the target table" - it actually means
# MAGIC    "emit only updated aggregate rows", and requires an aggregation
# MAGIC - Relying on the default mode without stating it - reviewer cannot tell whether you knew

# COMMAND ----------

# EXERCISE_KEY: ss_basics_ex4
correct_mode = "append"

(spark.readStream
    .table(f"{CATALOG}.{SCHEMA}.ex4_source")
    .writeStream
    .format("delta")
    .outputMode(correct_mode)
    .option("checkpointLocation", f"{CHECKPOINT_BASE}/ex4")
    .trigger(availableNow=True)
    .toTable(f"{CATALOG}.{SCHEMA}.ex4_target")
    .awaitTermination()
)

# COMMAND ----------
# MAGIC %md
# MAGIC ## Exercise 5: Trigger Modes - Understanding the Cost/Latency Tradeoff
# MAGIC
# MAGIC **Hints**:
# MAGIC 1. `trigger(availableNow=True)` is single-shot - one batch, then the query terminates and
# MAGIC    `awaitTermination()` returns. This is the only trigger supported on Free Edition serverless.
# MAGIC 2. The cost/latency tradeoff is conceptual: `processingTime` keeps compute warm for lower
# MAGIC    latency; `availableNow` spins compute down between runs for lower cost.
# MAGIC 3. In production on classic compute you could use `.trigger(processingTime='30 seconds')`,
# MAGIC    but never in a Free Edition notebook - serverless rejects `Trigger.ProcessingTime` outright.
# MAGIC
# MAGIC **Common mistakes**:
# MAGIC - Using `.trigger(processingTime=...)` on Free Edition serverless - the API is rejected,
# MAGIC    even with a `processAllAvailable() + stop()` mitigation.
# MAGIC - Confusing the two winners: lower LATENCY comes from continuous trigger; lower COST
# MAGIC    comes from `availableNow` because compute spins down between scheduled runs.

# COMMAND ----------

# EXERCISE_KEY: ss_basics_ex5
# availableNow - runs once and terminates by itself (only trigger Free Edition supports)
(spark.readStream
    .table(f"{CATALOG}.{SCHEMA}.ex5_source")
    .writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", f"{CHECKPOINT_BASE}/ex5_an")
    .trigger(availableNow=True)
    .toTable(f"{CATALOG}.{SCHEMA}.ex5_target_available_now")
    .awaitTermination()
)

latency_winner = "processingTime"  # continuous compute -> lower per-batch latency
cost_winner = "availableNow"        # compute spins down between runs -> lower cost

# COMMAND ----------
# MAGIC %md
# MAGIC ## Exercise 6: Streaming Aggregation in Complete Output Mode
# MAGIC
# MAGIC **Hints**:
# MAGIC 1. `groupBy("event_type").count()` returns a DataFrame with columns `event_type` and
# MAGIC    `count`. Rename `count` to `cnt` with `.withColumnRenamed("count", "cnt")` or use
# MAGIC    `.agg(F.count("*").alias("cnt"))` for clarity
# MAGIC 2. Output mode MUST be `complete` for a non-windowed streaming aggregation - the engine
# MAGIC    overwrites the full result table on every micro-batch
# MAGIC 3. `availableNow` plus `awaitTermination()` still applies - the cell must block until the
# MAGIC    batch is done so assertions read the final result table
# MAGIC
# MAGIC **Common mistakes**:
# MAGIC - Trying `append` mode - throws
# MAGIC    `Append output mode not supported when there are streaming aggregations on streaming DataFrames/DataSets without watermark`
# MAGIC - Using `count()` and letting the column name stay `count` - assertions look for `cnt`
# MAGIC - Forgetting that `complete` mode rewrites the target every batch - this is desirable
# MAGIC    here (small key space), but for large key spaces you'd add a watermark and use `update`

# COMMAND ----------

# EXERCISE_KEY: ss_basics_ex6
from pyspark.sql import functions as F

(spark.readStream
    .table(f"{CATALOG}.{SCHEMA}.ex6_source")
    .groupBy("event_type")
    .agg(F.count("*").alias("cnt"))
    .writeStream
    .format("delta")
    .outputMode("complete")
    .option("checkpointLocation", f"{CHECKPOINT_BASE}/ex6")
    .trigger(availableNow=True)
    .toTable(f"{CATALOG}.{SCHEMA}.ex6_target")
    .awaitTermination()
)

# COMMAND ----------
# MAGIC %md
# MAGIC ## Exercise 7: Name a Query and Inspect lastProgress
# MAGIC
# MAGIC **Hints**:
# MAGIC 1. `.queryName("ss_basics_ex7")` must be called on the writeStream BEFORE `.toTable(...)`
# MAGIC 2. Capture the StreamingQuery: `query = (spark.readStream.... .toTable(...))` then call
# MAGIC    `query.awaitTermination()`. After it returns, `query.lastProgress` is a dict-like
# MAGIC    object with keys `name`, `numInputRows`, `processedRowsPerSecond`, etc.
# MAGIC 3. To persist, build a 1-row DataFrame with `spark.createDataFrame([(...)], schema)` and
# MAGIC    `.write.format("delta").mode("overwrite").saveAsTable(...)` - this is a batch write of
# MAGIC    a captured metric, not a streaming write
# MAGIC
# MAGIC **Common mistakes**:
# MAGIC - Reading `lastProgress` BEFORE the stream terminates - it returns `None` because no
# MAGIC    batch has completed yet
# MAGIC - Using `recentProgress` (a list) and forgetting it's a list, not a dict
# MAGIC - Calling `.queryName(...)` after `.toTable(...)` - by then the query is already started
# MAGIC    and the name is set to the auto-generated UUID

# COMMAND ----------

# EXERCISE_KEY: ss_basics_ex7
from pyspark.sql.types import StructType, StructField, StringType, LongType

query = (spark.readStream
    .table(f"{CATALOG}.{SCHEMA}.ex7_source")
    .writeStream
    .queryName("ss_basics_ex7")
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", f"{CHECKPOINT_BASE}/ex7")
    .trigger(availableNow=True)
    .toTable(f"{CATALOG}.{SCHEMA}.ex7_target")
)
query.awaitTermination()

progress = query.lastProgress
metrics_schema = StructType([
    StructField("query_name", StringType()),
    StructField("num_input_rows", LongType()),
])
metrics_df = spark.createDataFrame(
    [(progress["name"], int(progress["numInputRows"]))],
    schema=metrics_schema,
)
metrics_df.write.format("delta").mode("overwrite").saveAsTable(
    f"{CATALOG}.{SCHEMA}.ex7_progress"
)

# COMMAND ----------
# MAGIC %md
# MAGIC ## Exercise 8: Deduplicate a Stream with dropDuplicates
# MAGIC
# MAGIC **Hints**:
# MAGIC 1. `dropDuplicates(["event_id"])` keeps the first occurrence of each `event_id` seen in
# MAGIC    the stream and stores keys in state so future batches drop subsequent duplicates
# MAGIC 2. For exactly-once semantics in production you'd pair this with `withWatermark(...)` so
# MAGIC    state doesn't grow forever; on a one-shot `availableNow` run with a bounded source the
# MAGIC    plain dropDuplicates is sufficient
# MAGIC 3. Apply dropDuplicates on the streaming DataFrame BEFORE the writeStream - it has to be
# MAGIC    part of the streaming plan
# MAGIC
# MAGIC **Common mistakes**:
# MAGIC - Running a post-write `SELECT DISTINCT * FROM target` - works for a single run but not
# MAGIC    incremental: subsequent batches would re-introduce duplicates
# MAGIC - Calling `dropDuplicates()` with no arguments - dedups on ALL columns, which fails the
# MAGIC    moment one duplicate has slightly different non-key columns
# MAGIC - Forgetting that dropDuplicates is stateful - on a long-running stream without a
# MAGIC    watermark the state grows unboundedly. For this exercise's bounded source it's fine

# COMMAND ----------

# EXERCISE_KEY: ss_basics_ex8
(spark.readStream
    .table(f"{CATALOG}.{SCHEMA}.ex8_source")
    .dropDuplicates(["event_id"])
    .writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", f"{CHECKPOINT_BASE}/ex8")
    .trigger(availableNow=True)
    .toTable(f"{CATALOG}.{SCHEMA}.ex8_target")
    .awaitTermination()
)
