# Databricks notebook source
# COMMAND ----------
# MAGIC %md
# MAGIC # foreachBatch Patterns - Solutions
# MAGIC **Topic**: Streaming | **Exercises**: 7
# MAGIC
# MAGIC Reference solutions, hints, and common mistakes for each exercise.
# MAGIC Try solving the exercises first before looking here.

# COMMAND ----------

CATALOG = "db_code"
SCHEMA = "foreachbatch_patterns"
CHECKPOINT_BASE = f"/Volumes/{CATALOG}/{SCHEMA}/checkpoints"

# COMMAND ----------
# MAGIC %md
# MAGIC ## Exercise 1: Basic foreachBatch Append
# MAGIC
# MAGIC **Hints**:
# MAGIC 1. `foreachBatch(fn)` calls `fn(batch_df, batch_id)` once per microbatch with a static (batch) DataFrame
# MAGIC 2. Inside the function, write `batch_df` to Delta with `.format("delta").mode("append").saveAsTable(...)`
# MAGIC 3. The writeStream wiring is `.writeStream.foreachBatch(fn).option("checkpointLocation", ...).trigger(availableNow=True).start().awaitTermination()`
# MAGIC
# MAGIC **Common mistakes**:
# MAGIC - Calling `batch_df.writeStream...` inside the function (batch_df is a static DataFrame, not streaming)
# MAGIC - Forgetting `.awaitTermination()` - the assertion cell runs before the stream finishes and sees an empty table
# MAGIC - Using `mode("overwrite")` and getting only the last batch (this 40-row source fits in one batch, so it accidentally passes, but it would silently break with multiple batches)
# MAGIC - Wiring `foreachBatch` AND `format("delta").toTable(...)` together - they are mutually exclusive sinks

# COMMAND ----------

# EXERCISE_KEY: foreach_batch_ex1
def foreach_batch_function(batch_df, batch_id):
    (batch_df.write
        .format("delta")
        .mode("append")
        .saveAsTable(f"{CATALOG}.{SCHEMA}.ex1_target"))

(spark.readStream
    .format("delta")
    .table(f"{CATALOG}.{SCHEMA}.ex1_source")
    .writeStream
    .foreachBatch(foreach_batch_function)
    .option("checkpointLocation", f"{CHECKPOINT_BASE}/ex1")
    .trigger(availableNow=True)
    .start()
    .awaitTermination()
)

# COMMAND ----------
# MAGIC %md
# MAGIC ## Exercise 2: foreachBatch + MERGE INTO for Streaming Upsert
# MAGIC
# MAGIC **Hints**:
# MAGIC 1. MERGE INTO works only on static DataFrames, so it must run inside `foreachBatch`, not on the streaming DataFrame
# MAGIC 2. Inside the function, register `batch_df` as a temp view (`batch_df.createOrReplaceTempView(...)`), then run the MERGE with `batch_df.sparkSession.sql("MERGE INTO ... USING <view> ...")`. `spark.sql(...)` runs in a different session and cannot see the view
# MAGIC 3. The MERGE matches on `event_id`, does `WHEN MATCHED THEN UPDATE SET *` and `WHEN NOT MATCHED THEN INSERT *`
# MAGIC
# MAGIC **Common mistakes**:
# MAGIC - Trying `streaming_df.writeStream.format("delta").mode("merge")...` - MERGE is not a writeStream mode
# MAGIC - Using `.toTable(target)` instead of `.foreachBatch(fn)` - this appends, never updates
# MAGIC - Not registering the temp view - the MERGE SQL can't reference the batch DataFrame by variable name
# MAGIC - Forgetting `.awaitTermination()` so the MERGE has not finished when the assertion cell runs
# MAGIC - Reusing the Exercise 1 checkpoint - checkpoints are stream-specific; each exercise needs its own subdirectory

# COMMAND ----------

# EXERCISE_KEY: foreach_batch_ex2
def foreach_batch_function(batch_df, batch_id):
    batch_df.createOrReplaceTempView("fb_ex2_batch")
    batch_df.sparkSession.sql(f"""
        MERGE INTO {CATALOG}.{SCHEMA}.ex2_target t
        USING fb_ex2_batch s
        ON t.event_id = s.event_id
        WHEN MATCHED THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
    """)

(spark.readStream
    .format("delta")
    .table(f"{CATALOG}.{SCHEMA}.ex2_source")
    .writeStream
    .foreachBatch(foreach_batch_function)
    .option("checkpointLocation", f"{CHECKPOINT_BASE}/ex2")
    .trigger(availableNow=True)
    .start()
    .awaitTermination()
)

# COMMAND ----------
# MAGIC %md
# MAGIC ## Exercise 3: Multi-Sink Write (Two Delta Tables per Batch)
# MAGIC
# MAGIC **Hints**:
# MAGIC 1. Without materialization, each `.write` on `batch_df` recomputes the upstream plan - that means the source Delta scan runs twice
# MAGIC 2. On classic compute the idiomatic fix is `batch_df.cache()` / `batch_df.unpersist()`. On Databricks Free Edition (serverless) `cache()`/`persist()` is rejected with `[NOT_SUPPORTED_WITH_SERVERLESS]`, so use the serverless-safe substitute: `rows = batch_df.collect()` + `materialized = batch_df.sparkSession.createDataFrame(rows, batch_df.schema)`
# MAGIC 3. The collected rows live in the driver, so this pattern is safe only for small batches like these 40-row sources. For huge batches on classic compute you would prefer `cache()`
# MAGIC
# MAGIC **Common mistakes**:
# MAGIC - Calling `batch_df.cache()` on Free Edition - raises `[NOT_SUPPORTED_WITH_SERVERLESS] PERSIST TABLE is not supported on serverless compute`
# MAGIC - Calling `cache()` on the streaming DataFrame outside `foreachBatch` - streaming DataFrames cannot be cached at all
# MAGIC - Skipping the materialize step on big batches - every read re-scans the source, which doubles the cost on real loads
# MAGIC - Writing the second sink with `mode("overwrite")` - it would erase the previous batch's data

# COMMAND ----------

# EXERCISE_KEY: foreach_batch_ex3
def foreach_batch_function(batch_df, batch_id):
    # NOTE: Databricks Free Edition (serverless) does NOT support batch_df.cache() /
    # .persist() inside foreachBatch (raises [NOT_SUPPORTED_WITH_SERVERLESS]).
    # Production pattern on classic compute: batch_df.cache() ... batch_df.unpersist().
    # Serverless-safe equivalent: materialize the batch via collect() and rebuild a
    # local DataFrame so subsequent reads do not re-scan the source.
    rows = batch_df.collect()
    schema = batch_df.schema
    materialized = batch_df.sparkSession.createDataFrame(rows, schema)

    # Sink 1: raw - every row in the batch
    (materialized.write
        .format("delta")
        .mode("append")
        .saveAsTable(f"{CATALOG}.{SCHEMA}.ex3_target_raw"))

    # Sink 2: clean - only purchase events with positive amount
    (materialized.filter("event_type = 'purchase' AND amount > 0")
        .write
        .format("delta")
        .mode("append")
        .saveAsTable(f"{CATALOG}.{SCHEMA}.ex3_target_clean"))

(spark.readStream
    .format("delta")
    .table(f"{CATALOG}.{SCHEMA}.ex3_source")
    .writeStream
    .foreachBatch(foreach_batch_function)
    .option("checkpointLocation", f"{CHECKPOINT_BASE}/ex3")
    .trigger(availableNow=True)
    .start()
    .awaitTermination()
)

# COMMAND ----------
# MAGIC %md
# MAGIC ## Exercise 4: Idempotent foreachBatch via batch_id Dedup Key
# MAGIC
# MAGIC **Hints**:
# MAGIC 1. Tag every row in the batch with the `batch_id` argument using `withColumn("batch_id", F.lit(batch_id).cast("long"))` (the cast matches the target's `LONG` column)
# MAGIC 2. Use a MERGE with the join key `(event_id, batch_id)` and ONLY `WHEN NOT MATCHED THEN INSERT *`. No `WHEN MATCHED` clause - a replay must be a no-op, not an update. An `event_id`-only key with no `WHEN MATCHED` would also skip the replay, but it skips any later batch that re-sends an `event_id` too.
# MAGIC 3. Calling `foreach_batch_function(spark.table(ex4_source), <same_batch_id>)` a second time should perform a MERGE that finds existing rows and does nothing
# MAGIC
# MAGIC **Common mistakes**:
# MAGIC - Including `WHEN MATCHED THEN UPDATE SET *` - replays now update rows; the row count stays the same but you have lost idempotency for any downstream consumer reading the change feed
# MAGIC - Tagging with `F.current_timestamp()` instead of `F.lit(batch_id)` - now every replay has a different "batch identifier" and dedupe fails

# COMMAND ----------

# EXERCISE_KEY: foreach_batch_ex4
from pyspark.sql import functions as F

def foreach_batch_function(batch_df, batch_id):
    tagged = batch_df.withColumn("batch_id", F.lit(batch_id).cast("long"))
    tagged.createOrReplaceTempView("fb_ex4_batch")
    tagged.sparkSession.sql(f"""
        MERGE INTO {CATALOG}.{SCHEMA}.ex4_target t
        USING fb_ex4_batch s
        ON t.event_id = s.event_id AND t.batch_id = s.batch_id
        WHEN NOT MATCHED THEN INSERT *
    """)

# Run the stream once (writes 40 rows tagged with batch_id from availableNow)
(spark.readStream
    .format("delta")
    .table(f"{CATALOG}.{SCHEMA}.ex4_source")
    .writeStream
    .foreachBatch(foreach_batch_function)
    .option("checkpointLocation", f"{CHECKPOINT_BASE}/ex4")
    .trigger(availableNow=True)
    .start()
    .awaitTermination()
)

# Manually replay with the same batch_id as the streamed batch.
# This 40-row source fits in one microbatch, so a fresh checkpoint gives it batch_id=0.
applied_batch_id = (
    spark.table(f"{CATALOG}.{SCHEMA}.ex4_target")
    .select("batch_id")
    .distinct()
    .collect()[0][0]
)
foreach_batch_function(
    spark.table(f"{CATALOG}.{SCHEMA}.ex4_source"),
    applied_batch_id,
)

# COMMAND ----------
# MAGIC %md
# MAGIC ## Exercise 5: Quality Routing - Good Rows to Target, Bad Rows to Quarantine
# MAGIC
# MAGIC **Hints**:
# MAGIC 1. Materialize `batch_df` once via `collect()` + `createDataFrame(rows, schema)` - you will read it three times (good filter, negative-amount filter, null-user filter). On Free Edition serverless `batch_df.cache()` is rejected, so this is the serverless-safe substitute
# MAGIC 2. Use `.filter()` and `.withColumn("reason", F.lit("..."))` to tag the rejects before appending to quarantine
# MAGIC 3. The quarantine table has the 5 base columns plus `reason`. `saveAsTable` in append mode matches columns by name, so adding `reason` with `withColumn` is all the append needs
# MAGIC
# MAGIC **Common mistakes**:
# MAGIC - Calling `batch_df.cache()` on Free Edition - raises `[NOT_SUPPORTED_WITH_SERVERLESS]`
# MAGIC - Writing rejects without a `reason` column - the append still succeeds (Delta fills the missing column with NULL), so every quarantined row loses the reason it was rejected
# MAGIC - Using `.union()` to combine the two reject sets without first adding the `reason` column - both reason values end up null
# MAGIC - Forgetting that the good filter is `amount >= 0 AND user_id IS NOT NULL` - using just one of the predicates leaks bad rows into the main target
# MAGIC - Writing to quarantine with `mode("overwrite")` - subsequent microbatches erase earlier rejects

# COMMAND ----------

# EXERCISE_KEY: foreach_batch_ex5
from pyspark.sql import functions as F

def foreach_batch_function(batch_df, batch_id):
    # Materialize once: the good filter and both reject filters read this copy
    rows = batch_df.collect()
    materialized = batch_df.sparkSession.createDataFrame(rows, batch_df.schema)

    # Good rows -> main target
    good = materialized.filter("amount >= 0 AND user_id IS NOT NULL")
    (good.write
        .format("delta")
        .mode("append")
        .saveAsTable(f"{CATALOG}.{SCHEMA}.ex5_target"))

    # Negative amount rejects
    neg = (materialized.filter("amount < 0")
        .withColumn("reason", F.lit("negative_amount")))
    (neg.write
        .format("delta")
        .mode("append")
        .saveAsTable(f"{CATALOG}.{SCHEMA}.ex5_quarantine"))

    # NULL user_id rejects
    null_user = (materialized.filter("user_id IS NULL")
        .withColumn("reason", F.lit("null_user_id")))
    (null_user.write
        .format("delta")
        .mode("append")
        .saveAsTable(f"{CATALOG}.{SCHEMA}.ex5_quarantine"))

(spark.readStream
    .format("delta")
    .table(f"{CATALOG}.{SCHEMA}.ex5_source")
    .writeStream
    .foreachBatch(foreach_batch_function)
    .option("checkpointLocation", f"{CHECKPOINT_BASE}/ex5")
    .trigger(availableNow=True)
    .start()
    .awaitTermination()
)

# COMMAND ----------
# MAGIC %md
# MAGIC ## Exercise 6: Running Batch Metrics
# MAGIC
# MAGIC **Hints**:
# MAGIC 1. Materialize `batch_df` once via `collect()` + `createDataFrame(rows, schema)` - you read it for `count()`, for the filtered `count()`, and again for the write. On Free Edition serverless `batch_df.cache()` is rejected, so this is the serverless-safe substitute
# MAGIC 2. Build the metrics row as a one-row DataFrame from a Python tuple `(batch_id, rows_in, rows_good, rows_quarantined, datetime.now())`, then append it to `ex6_metrics`
# MAGIC 3. `batch_df.sparkSession.createDataFrame([(batch_id, rows_in, rows_good, rows_quarantined, datetime.now())], schema)` with a schema of `LongType()` for the first four fields and `TimestampType()` for `processed_at` matches the `ex6_metrics` columns exactly
# MAGIC
# MAGIC **Common mistakes**:
# MAGIC - Calling `batch_df.cache()` on Free Edition - raises `[NOT_SUPPORTED_WITH_SERVERLESS]`
# MAGIC - Computing `rows_good` by reading `ex6_target` after the write - on a replayed batch the table already holds the earlier attempt's rows, so the count comes out too high
# MAGIC - Skipping the materialize step - the three `.count()` / `.filter()` calls each re-read the source from Delta
# MAGIC - Writing the metrics row to `ex6_target` by accident (schema mismatch, runtime error)

# COMMAND ----------

# EXERCISE_KEY: foreach_batch_ex6
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, LongType, TimestampType
from datetime import datetime

def foreach_batch_function(batch_df, batch_id):
    # Materialize once: count, filter+count and the write read this copy
    rows = batch_df.collect()
    materialized = batch_df.sparkSession.createDataFrame(rows, batch_df.schema)

    # Compute metrics from the in-memory batch
    rows_in = materialized.count()
    good_df = materialized.filter("amount >= 0 AND user_id IS NOT NULL")
    rows_good = good_df.count()
    rows_quarantined = rows_in - rows_good

    # Write clean rows to main target
    (good_df.write
        .format("delta")
        .mode("append")
        .saveAsTable(f"{CATALOG}.{SCHEMA}.ex6_target"))

    # Append one metrics row
    metrics_schema = StructType([
        StructField("batch_id", LongType()),
        StructField("rows_in", LongType()),
        StructField("rows_good", LongType()),
        StructField("rows_quarantined", LongType()),
        StructField("processed_at", TimestampType()),
    ])
    metrics_row = [(
        int(batch_id),
        int(rows_in),
        int(rows_good),
        int(rows_quarantined),
        datetime.now(),
    )]
    (batch_df.sparkSession.createDataFrame(metrics_row, schema=metrics_schema)
        .write
        .format("delta")
        .mode("append")
        .saveAsTable(f"{CATALOG}.{SCHEMA}.ex6_metrics"))

(spark.readStream
    .format("delta")
    .table(f"{CATALOG}.{SCHEMA}.ex6_source")
    .writeStream
    .foreachBatch(foreach_batch_function)
    .option("checkpointLocation", f"{CHECKPOINT_BASE}/ex6")
    .trigger(availableNow=True)
    .start()
    .awaitTermination()
)

# COMMAND ----------
# MAGIC %md
# MAGIC ## Exercise 7: Materialized batch_df for MERGE + Quality Routing in One Pass
# MAGIC
# MAGIC **Hints**:
# MAGIC 1. The production pattern (classic compute): `batch_df.cache()` -> split into `good_df` / `bad_df` -> MERGE good into main target -> append bad to quarantine -> `unpersist()`
# MAGIC 2. On Free Edition serverless, swap `cache()` for the materialize pattern: `rows = batch_df.collect()` + `materialized = batch_df.sparkSession.createDataFrame(rows, batch_df.schema)`, then split `materialized` into `good_df` / `bad_df`
# MAGIC 3. MERGE needs a static source, so run it inside the foreachBatch function: register `good_df` as a temp view and call `batch_df.sparkSession.sql(...)`, or pass `good_df` to `DeltaTable.merge(...)`
# MAGIC 4. The materialize step must run on `batch_df` itself (the full batch) so both downstream filters share the same in-memory copy
# MAGIC
# MAGIC **Common mistakes**:
# MAGIC - Calling `batch_df.cache()` on Free Edition - raises `[NOT_SUPPORTED_WITH_SERVERLESS]`
# MAGIC - Materializing `good_df` and `bad_df` separately instead of `batch_df` - the upstream source is still scanned twice (once per filter)
# MAGIC - MERGE'ing the full batch including bad rows - then negative amounts and NULL user_ids land in the main target
# MAGIC - Using `cache()` outside the function (on the streaming DataFrame) - that is not supported

# COMMAND ----------

# EXERCISE_KEY: foreach_batch_ex7
from pyspark.sql import functions as F

def foreach_batch_function(batch_df, batch_id):
    # Materialize once: the MERGE and the quarantine append read this copy
    rows = batch_df.collect()
    materialized = batch_df.sparkSession.createDataFrame(rows, batch_df.schema)

    good_df = materialized.filter("amount >= 0 AND user_id IS NOT NULL")
    bad_df = materialized.filter("amount < 0 OR user_id IS NULL")

    # MERGE good rows into the main target
    good_df.createOrReplaceTempView("fb_ex7_good_batch")
    materialized.sparkSession.sql(f"""
        MERGE INTO {CATALOG}.{SCHEMA}.ex7_target t
        USING fb_ex7_good_batch s
        ON t.event_id = s.event_id
        WHEN MATCHED THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
    """)

    # Append rejects to quarantine
    (bad_df.write
        .format("delta")
        .mode("append")
        .saveAsTable(f"{CATALOG}.{SCHEMA}.ex7_quarantine"))

(spark.readStream
    .format("delta")
    .table(f"{CATALOG}.{SCHEMA}.ex7_source")
    .writeStream
    .foreachBatch(foreach_batch_function)
    .option("checkpointLocation", f"{CHECKPOINT_BASE}/ex7")
    .trigger(availableNow=True)
    .start()
    .awaitTermination()
)
