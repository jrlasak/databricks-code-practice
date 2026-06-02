# Databricks notebook source
# COMMAND ----------
# MAGIC %md
# MAGIC # Stream-Stream Joins - Solutions
# MAGIC **Topic**: Streaming | **Exercises**: 7
# MAGIC
# MAGIC Reference solutions, hints, and common mistakes for each exercise.
# MAGIC Try solving the exercises first before looking here.

# COMMAND ----------

CATALOG = "db_code"
SCHEMA = "stream_stream_joins"
BASE_SCHEMA = "streaming"
CHECKPOINT_BASE = f"/Volumes/{CATALOG}/{SCHEMA}/checkpoints"

# COMMAND ----------
# MAGIC %md
# MAGIC ## Exercise 1: Inner Join with No Watermark, No Time Bound
# MAGIC
# MAGIC **Hints**:
# MAGIC 1. Read each side with `spark.readStream.table(...)`. Once both are streaming DataFrames,
# MAGIC    the `.join(...)` call on streaming DFs is a stream-stream join automatically.
# MAGIC 2. The join condition is just `orders.order_id == shipments.order_id` (default join type
# MAGIC    is inner).
# MAGIC 3. The write side needs `.format("delta")`, a checkpoint location, `trigger(availableNow=True)`,
# MAGIC    and `.toTable(...)`.
# MAGIC
# MAGIC **Common mistakes**:
# MAGIC - Using `spark.read.table` instead of `spark.readStream.table` - that's a static read, not a stream.
# MAGIC - Forgetting `.awaitTermination()` - the next cell runs before the stream completes and the
# MAGIC   target table is empty when assertions fire.
# MAGIC - Using `trigger(once=True)` - deprecated. Use `trigger(availableNow=True)`.
# MAGIC - Forgetting to drop the duplicated `order_id` column. The join produces two `order_id`
# MAGIC   columns by default; select explicitly or use the `on=` form so the column is coalesced.

# COMMAND ----------

# EXERCISE_KEY: stream_stream_ex1
from pyspark.sql.functions import col

orders = spark.readStream.table(f"{CATALOG}.{SCHEMA}.orders_stream")
shipments = spark.readStream.table(f"{CATALOG}.{SCHEMA}.shipments_stream")

joined = orders.join(shipments, on="order_id", how="inner").select(
    "order_id",
    "customer_id",
    "amount",
    "order_ts",
    "shipment_id",
    "carrier",
    "shipment_ts",
)

(joined.writeStream
    .format("delta")
    .option("checkpointLocation", f"{CHECKPOINT_BASE}/ex1")
    .trigger(availableNow=True)
    .toTable(f"{CATALOG}.{SCHEMA}.ex1_target")
    .awaitTermination()
)

# COMMAND ----------
# MAGIC %md
# MAGIC ## Exercise 2: Inner Join with Watermarks on Both Sides
# MAGIC
# MAGIC **Hints**:
# MAGIC 1. Apply `.withWatermark("<event_time_col>", "<delay>")` to each streaming DataFrame BEFORE
# MAGIC    joining.
# MAGIC 2. The watermark delay is a string like `"1 minute"` or `"10 minutes"`.
# MAGIC 3. The join condition is still just `order_id` equality - no time-range predicate yet.
# MAGIC
# MAGIC **Common mistakes**:
# MAGIC - Applying watermark AFTER the join - Spark needs the watermark on the source DataFrames
# MAGIC   so it can track event-time progress per side.
# MAGIC - Wrong unit spelling: `"1 minutes"` works, but `"1 min"` does NOT.
# MAGIC - Thinking watermark alone bounds state. It does not - without a time-range predicate,
# MAGIC   Spark cannot drop state because any future row could still match.

# COMMAND ----------

# EXERCISE_KEY: stream_stream_ex2
orders_wm = (
    spark.readStream.table(f"{CATALOG}.{SCHEMA}.orders_stream")
    .withWatermark("order_ts", "1 minute")
)
shipments_wm = (
    spark.readStream.table(f"{CATALOG}.{SCHEMA}.shipments_stream")
    .withWatermark("shipment_ts", "1 minute")
)

joined = orders_wm.join(shipments_wm, on="order_id", how="inner").select(
    "order_id",
    "customer_id",
    "amount",
    "order_ts",
    "shipment_id",
    "carrier",
    "shipment_ts",
)

(joined.writeStream
    .format("delta")
    .option("checkpointLocation", f"{CHECKPOINT_BASE}/ex2")
    .trigger(availableNow=True)
    .toTable(f"{CATALOG}.{SCHEMA}.ex2_target")
    .awaitTermination()
)

# COMMAND ----------
# MAGIC %md
# MAGIC ## Exercise 3: Inner Join with Watermark + 5-Minute Time-Range Constraint
# MAGIC
# MAGIC **Hints**:
# MAGIC 1. The time-range predicate goes inside the join condition, alongside the `order_id`
# MAGIC    equality. Use `&` to combine in PySpark.
# MAGIC 2. Reference both timestamp columns via the source DataFrames (e.g.,
# MAGIC    `shipments_wm.shipment_ts`) so Spark can statically detect the time bound.
# MAGIC 3. The SQL interval syntax `INTERVAL 5 MINUTES` is available via `expr(...)` or in raw
# MAGIC    SQL; in PySpark you can also write `orders_wm.order_ts + expr("INTERVAL 5 MINUTES")`.
# MAGIC
# MAGIC **Common mistakes**:
# MAGIC - Putting the time-range predicate in a `.filter(...)` AFTER the join. The output rows are
# MAGIC   the same, but state stays unbounded because Spark cannot infer the bound.
# MAGIC - Using a one-sided bound (only `shipment_ts >= order_ts`) - that doesn't cap how far apart
# MAGIC   matches can be in event time, so state still grows.
# MAGIC - Forgetting `withWatermark` on one side. Spark may run, but the bound on that side is not
# MAGIC   enforced and the state for that side grows unboundedly.

# COMMAND ----------

# EXERCISE_KEY: stream_stream_ex3
from pyspark.sql.functions import expr

orders_wm = (
    spark.readStream.table(f"{CATALOG}.{SCHEMA}.orders_stream")
    .withWatermark("order_ts", "1 minute")
)
shipments_wm = (
    spark.readStream.table(f"{CATALOG}.{SCHEMA}.shipments_stream")
    .withWatermark("shipment_ts", "1 minute")
)

join_cond = (
    (orders_wm.order_id == shipments_wm.order_id)
    & (shipments_wm.shipment_ts >= orders_wm.order_ts)
    & (shipments_wm.shipment_ts <= orders_wm.order_ts + expr("INTERVAL 5 MINUTES"))
)

joined = orders_wm.join(shipments_wm, on=join_cond, how="inner").select(
    orders_wm.order_id,
    orders_wm.customer_id,
    orders_wm.amount,
    orders_wm.order_ts,
    shipments_wm.shipment_id,
    shipments_wm.carrier,
    shipments_wm.shipment_ts,
)

(joined.writeStream
    .format("delta")
    .option("checkpointLocation", f"{CHECKPOINT_BASE}/ex3")
    .trigger(availableNow=True)
    .toTable(f"{CATALOG}.{SCHEMA}.ex3_target")
    .awaitTermination()
)

# COMMAND ----------
# MAGIC %md
# MAGIC ## Exercise 4: Left Outer Join with Watermark + Time Bound
# MAGIC
# MAGIC **Hints**:
# MAGIC 1. Switch the join type to `"leftOuter"` (or `"left_outer"`). Everything else from Exercise
# MAGIC    3 stays the same.
# MAGIC 2. NULL rows for unmatched left orders are emitted when the watermark passes the order's
# MAGIC    window end (`order_ts + 5 minutes`). The setup includes two late "advancement" orders
# MAGIC    so the watermark gets there.
# MAGIC 3. The output schema should still have all 7 columns. Unmatched-left rows have NULL in
# MAGIC    `shipment_id`, `carrier`, and `shipment_ts`.
# MAGIC
# MAGIC **Common mistakes**:
# MAGIC - Forgetting that outer joins REQUIRE both `withWatermark` AND a time-range predicate.
# MAGIC   Spark raises `AnalysisException` if either is missing.
# MAGIC - Expecting unmatched rows to appear immediately. They only emit AFTER the watermark passes
# MAGIC   the upper bound of the window. With `availableNow`, the watermark advances based on event
# MAGIC   time across the data - late rows that push it forward let earlier unmatched rows be emitted.
# MAGIC - Passing `on="order_id"` (a string column name) instead of a full join condition
# MAGIC   expression. For stream-stream outer joins with a time predicate you need the expression
# MAGIC   form so the predicate is part of the join.

# COMMAND ----------

# EXERCISE_KEY: stream_stream_ex4
from pyspark.sql.functions import expr

orders_wm = (
    spark.readStream.table(f"{CATALOG}.{SCHEMA}.orders_stream")
    .withWatermark("order_ts", "1 minute")
)
shipments_wm = (
    spark.readStream.table(f"{CATALOG}.{SCHEMA}.shipments_stream")
    .withWatermark("shipment_ts", "1 minute")
)

join_cond = (
    (orders_wm.order_id == shipments_wm.order_id)
    & (shipments_wm.shipment_ts >= orders_wm.order_ts)
    & (shipments_wm.shipment_ts <= orders_wm.order_ts + expr("INTERVAL 5 MINUTES"))
)

joined = orders_wm.join(shipments_wm, on=join_cond, how="leftOuter").select(
    orders_wm.order_id,
    orders_wm.customer_id,
    orders_wm.amount,
    orders_wm.order_ts,
    shipments_wm.shipment_id,
    shipments_wm.carrier,
    shipments_wm.shipment_ts,
)

(joined.writeStream
    .format("delta")
    .option("checkpointLocation", f"{CHECKPOINT_BASE}/ex4")
    .trigger(availableNow=True)
    .toTable(f"{CATALOG}.{SCHEMA}.ex4_target")
    .awaitTermination()
)

# COMMAND ----------
# MAGIC %md
# MAGIC ## Exercise 5: Right Outer Join with Watermark + Time Bound
# MAGIC
# MAGIC **Hints**:
# MAGIC 1. Same join condition as Exercise 4. Only `how` changes - use `"rightOuter"`.
# MAGIC 2. Now `shipment_id` is the "always present" column. Unmatched rows have NULL `order_id`,
# MAGIC    `customer_id`, `amount`, `order_ts`.
# MAGIC 3. Spark requires both watermark and time bound for outer joins - same as left outer.
# MAGIC
# MAGIC **Common mistakes**:
# MAGIC - Confusing which side gets NULLed. In `leftOuter`, the RIGHT side gets nulls when no match.
# MAGIC   In `rightOuter`, the LEFT side gets nulls.
# MAGIC - Filtering the output for `order_id IS NOT NULL` and wondering why unmatched shipments
# MAGIC   disappear - that filter removes exactly the rows the outer join produced.
# MAGIC - Treating right-outer state as equivalent to left-outer state. The watermark math is
# MAGIC   symmetric here, but in general the choice of outer side affects which rows linger and
# MAGIC   for how long.

# COMMAND ----------

# EXERCISE_KEY: stream_stream_ex5
from pyspark.sql.functions import expr

orders_wm = (
    spark.readStream.table(f"{CATALOG}.{SCHEMA}.orders_stream")
    .withWatermark("order_ts", "1 minute")
)
shipments_wm = (
    spark.readStream.table(f"{CATALOG}.{SCHEMA}.shipments_stream")
    .withWatermark("shipment_ts", "1 minute")
)

join_cond = (
    (orders_wm.order_id == shipments_wm.order_id)
    & (shipments_wm.shipment_ts >= orders_wm.order_ts)
    & (shipments_wm.shipment_ts <= orders_wm.order_ts + expr("INTERVAL 5 MINUTES"))
)

joined = orders_wm.join(shipments_wm, on=join_cond, how="rightOuter").select(
    orders_wm.order_id,
    orders_wm.customer_id,
    orders_wm.amount,
    orders_wm.order_ts,
    shipments_wm.shipment_id,
    shipments_wm.carrier,
    shipments_wm.shipment_ts,
)

(joined.writeStream
    .format("delta")
    .option("checkpointLocation", f"{CHECKPOINT_BASE}/ex5")
    .trigger(availableNow=True)
    .toTable(f"{CATALOG}.{SCHEMA}.ex5_target")
    .awaitTermination()
)

# COMMAND ----------
# MAGIC %md
# MAGIC ## Exercise 6: Diagnose Unbounded State (No Watermark, No Time Bound)
# MAGIC
# MAGIC **Hints**:
# MAGIC 1. The code is intentionally the same as Exercise 1. The point is recognizing this pattern
# MAGIC    in code review.
# MAGIC 2. Under `availableNow`, both streams terminate after one or two micro-batches, so state
# MAGIC    "leaks" don't accumulate visibly - but in a continuous job they would.
# MAGIC 3. To verify state growth in production: query the StreamingQueryProgress
# MAGIC    (`spark.streams.active[i].lastProgress`) and watch `stateOperators[*].numRowsTotal` grow
# MAGIC    unboundedly across micro-batches.
# MAGIC
# MAGIC **Common mistakes** (the kind that put this pattern in production):
# MAGIC - Adding a watermark and assuming state is bounded - it isn't, you also need a time-range
# MAGIC   predicate.
# MAGIC - Adding a time-range predicate as a `.filter(...)` after the join - Spark won't recognize
# MAGIC   it as a join time bound and won't drop state.
# MAGIC - Adding a one-sided predicate (`shipment_ts >= order_ts`) - this is a bound on order but
# MAGIC   not a bound on lateness; state still grows on the orders side.
# MAGIC - "We don't see state growth in dev" - dev runs are short. Production state growth shows up
# MAGIC   as ballooning checkpoints, slow micro-batches after hours/days, and eventually OOMs.

# COMMAND ----------

# EXERCISE_KEY: stream_stream_ex6
# Intentionally unbounded: no withWatermark, no time-range predicate.
# This is the broken pattern. The fix is in Exercise 3.
orders = spark.readStream.table(f"{CATALOG}.{SCHEMA}.orders_stream")
shipments = spark.readStream.table(f"{CATALOG}.{SCHEMA}.shipments_stream")

joined = orders.join(shipments, on="order_id", how="inner").select(
    "order_id",
    "customer_id",
    "amount",
    "order_ts",
    "shipment_id",
    "carrier",
    "shipment_ts",
)

(joined.writeStream
    .format("delta")
    .option("checkpointLocation", f"{CHECKPOINT_BASE}/ex6")
    .trigger(availableNow=True)
    .toTable(f"{CATALOG}.{SCHEMA}.ex6_target")
    .awaitTermination()
)

# COMMAND ----------
# MAGIC %md
# MAGIC ## Exercise 7: Verify That Out-of-Window Pairs Are Dropped
# MAGIC
# MAGIC **Hints**:
# MAGIC 1. The code is the same as Exercise 3. The new skill is reading the OUTPUT and proving the
# MAGIC    bound is in effect: every surviving row satisfies `shipment_ts - order_ts <= 5 minutes`,
# MAGIC    AND all known out-of-window pairs are absent.
# MAGIC 2. To verify the bound is part of the JOIN (not a post-filter), inspect
# MAGIC    `query.lastProgress["stateOperators"]` and look for the join state operator with a
# MAGIC    bounded `watermarkPredicate`.
# MAGIC 3. In production, the assertion to add to your test suite is:
# MAGIC    `assert spark.table(target).filter("shipment_ts > order_ts + INTERVAL 5 MINUTES").count() == 0`.
# MAGIC
# MAGIC **Common mistakes**:
# MAGIC - Writing the same logical query but with the time predicate as a `.filter(...)` after the
# MAGIC   join. The output assertions still pass, but state is unbounded. The way to catch this in
# MAGIC   review is to look for the time predicate INSIDE the join condition, not in a downstream
# MAGIC   transform.
# MAGIC - Testing only that the right rows ARE present, not that wrong rows are ABSENT. Out-of-
# MAGIC   window pairs are an easy regression - add explicit "must-not-appear" assertions.
# MAGIC - Assuming a >5-minute pair in production "just won't happen". Late-arriving data is the
# MAGIC   norm, not the exception. Pick a time bound that matches your business latency budget
# MAGIC   and TEST that pairs beyond it are dropped.

# COMMAND ----------

# EXERCISE_KEY: stream_stream_ex7
from pyspark.sql.functions import expr

orders_wm = (
    spark.readStream.table(f"{CATALOG}.{SCHEMA}.orders_stream")
    .withWatermark("order_ts", "1 minute")
)
shipments_wm = (
    spark.readStream.table(f"{CATALOG}.{SCHEMA}.shipments_stream")
    .withWatermark("shipment_ts", "1 minute")
)

join_cond = (
    (orders_wm.order_id == shipments_wm.order_id)
    & (shipments_wm.shipment_ts >= orders_wm.order_ts)
    & (shipments_wm.shipment_ts <= orders_wm.order_ts + expr("INTERVAL 5 MINUTES"))
)

joined = orders_wm.join(shipments_wm, on=join_cond, how="inner").select(
    orders_wm.order_id,
    orders_wm.customer_id,
    orders_wm.amount,
    orders_wm.order_ts,
    shipments_wm.shipment_id,
    shipments_wm.carrier,
    shipments_wm.shipment_ts,
)

(joined.writeStream
    .format("delta")
    .option("checkpointLocation", f"{CHECKPOINT_BASE}/ex7")
    .trigger(availableNow=True)
    .toTable(f"{CATALOG}.{SCHEMA}.ex7_target")
    .awaitTermination()
)
