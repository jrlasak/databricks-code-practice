# Databricks notebook source
# COMMAND ----------
# MAGIC %md
# MAGIC # Stream-Stream Joins
# MAGIC **Topic**: Streaming | **Exercises**: 7 | **Total Time**: ~110 min
# MAGIC
# MAGIC Practice joining two streaming DataFrames in Structured Streaming. Covers inner and outer
# MAGIC semantics, the role of watermarks on both sides, and how a time-range constraint together
# MAGIC with a watermark bounds the state Spark must keep. Also covers the diagnostic skill of
# MAGIC recognizing unbounded state growth and adding the fix.
# MAGIC
# MAGIC **Solutions**: If stuck, see `solutions/stream-stream-joins-solutions.py` for hints and answers.
# MAGIC
# MAGIC **Key concepts**:
# MAGIC - A stream-stream join needs both sides to be streaming DataFrames (`spark.readStream.table(...)`).
# MAGIC - Inner joins work without watermarks, but state grows unboundedly without a time bound.
# MAGIC - Watermark on both sides + a time-range predicate together cap the join state.
# MAGIC - Outer joins (left/right) REQUIRE watermark + time bound. Unmatched-side NULL rows are
# MAGIC   emitted only AFTER the watermark passes the join window.
# MAGIC - On Free Edition serverless, all streams must use `.trigger(availableNow=True)` followed
# MAGIC   by `.awaitTermination()`. The sources are Delta tables (Kafka is unavailable).
# MAGIC
# MAGIC **Tables used** (created by the setup notebook):
# MAGIC - `db_code.stream_stream_joins.orders_stream` (32 rows): `order_id`, `customer_id`, `amount`, `order_ts`
# MAGIC - `db_code.stream_stream_joins.shipments_stream` (32 rows): `shipment_id`, `order_id`, `carrier`, `shipment_ts`
# MAGIC
# MAGIC **Pair design** (reference table for the exercises below):
# MAGIC | Range | Count | Time gap (shipment_ts - order_ts) | In 5-min window? |
# MAGIC |---|---|---|---|
# MAGIC | ORD-001..ORD-020 paired with SHP-001..SHP-020 | 20 | +2 min | YES |
# MAGIC | ORD-021..ORD-025 paired with SHP-021..SHP-025 | 5 | +10 min | NO |
# MAGIC | ORD-026..ORD-030 (no shipment) | 5 | n/a | unmatched left |
# MAGIC | ORD-200..ORD-201 (late, no shipment, push watermark) | 2 | n/a | unmatched left |
# MAGIC | SHP-026..SHP-030 reference ORD-901..ORD-905 (no order) | 5 | n/a | unmatched right |
# MAGIC | SHP-200..SHP-201 reference ORD-906..ORD-907 (late, no order) | 2 | n/a | unmatched right |

# COMMAND ----------

# MAGIC %run ./00_Setup

# COMMAND ----------

# MAGIC %run ./setup/stream-stream-joins-setup

# COMMAND ----------
# MAGIC %md
# MAGIC **Setup complete.** Two streaming source tables and the `stream_stream_joins` schema are ready.
# MAGIC
# MAGIC Each exercise writes to its own target table (`db_code.stream_stream_joins.exN_target`) and
# MAGIC uses its own checkpoint at `/Volumes/db_code/stream_stream_joins/checkpoints/exN/`.
# MAGIC
# MAGIC **Free Edition rules** (apply to every exercise):
# MAGIC - Read each side with `spark.readStream.table("db_code.stream_stream_joins.<source>")`.
# MAGIC - End every query with `.trigger(availableNow=True)` and `.awaitTermination()`.
# MAGIC - Do NOT call `spark.conf.set(...)` - serverless rejects it.

# COMMAND ----------
# MAGIC %md
# MAGIC ## Exercise 1: Inner Join with No Watermark, No Time Bound
# MAGIC **Difficulty**: Easy | **Time**: ~10 min
# MAGIC
# MAGIC Inner-join the two streams on `order_id`. No watermark, no time-range predicate. Verify
# MAGIC that every order with a matching shipment appears in the output, regardless of how far
# MAGIC apart their timestamps are.
# MAGIC
# MAGIC **Source 1** (`db_code.stream_stream_joins.orders_stream`): 32 rows of orders.
# MAGIC **Source 2** (`db_code.stream_stream_joins.shipments_stream`): 32 rows of shipments.
# MAGIC
# MAGIC **Target**: Write the joined stream to `db_code.stream_stream_joins.ex1_target` as Delta.
# MAGIC Columns: `order_id`, `customer_id`, `amount`, `order_ts`, `shipment_id`, `carrier`, `shipment_ts`.
# MAGIC
# MAGIC **Expected Output**: 25 rows. Every `order_id` that exists in BOTH streams. Specifically
# MAGIC ORD-001..ORD-025 each appear once. ORD-026..ORD-030 (no shipment), ORD-200..ORD-201 (no
# MAGIC shipment), ORD-901..ORD-907 (no order, only orphan shipments) do NOT appear.
# MAGIC
# MAGIC **Requirements**:
# MAGIC 1. Read both tables with `spark.readStream.table(...)`.
# MAGIC 2. Inner-join on `order_id`.
# MAGIC 3. Write to the target Delta table with `trigger(availableNow=True)` and `.awaitTermination()`.
# MAGIC 4. Set `checkpointLocation` to `f"{CHECKPOINT_BASE}/ex1"`.
# MAGIC
# MAGIC **Constraints**:
# MAGIC - Do NOT add `withWatermark` or a time-range predicate (those come in later exercises).
# MAGIC - Use `.trigger(availableNow=True)`, never `trigger(once=True)`.

# COMMAND ----------

# EXERCISE_KEY: stream_stream_ex1
# TODO: Inner join orders_stream and shipments_stream on order_id, write to ex1_target

# Your code here


# COMMAND ----------

# Validate Exercise 1
result = spark.table(f"{CATALOG}.{SCHEMA}.ex1_target")

assert result.count() == 25, f"Expected 25 joined rows, got {result.count()}"
assert "shipment_id" in result.columns, "Output must include shipment_id from the right side"
assert "order_ts" in result.columns, "Output must include order_ts from the left side"

# Specific in-window pair (ORD-001 paired with SHP-001)
ord1 = result.filter("order_id = 'ORD-001'").collect()
assert len(ord1) == 1, f"Expected 1 row for ORD-001, got {len(ord1)}"
assert ord1[0]["shipment_id"] == "SHP-001", f"ORD-001 should pair with SHP-001, got {ord1[0]['shipment_id']}"

# Specific out-of-window pair (still matched here because no time bound)
ord21 = result.filter("order_id = 'ORD-021'").collect()
assert len(ord21) == 1, "ORD-021 should be matched (no time bound in this exercise)"
assert ord21[0]["shipment_id"] == "SHP-021", "ORD-021 should pair with SHP-021"

# Unmatched orders must not appear
assert result.filter("order_id = 'ORD-026'").count() == 0, "ORD-026 has no shipment, should not appear"
assert result.filter("order_id = 'ORD-200'").count() == 0, "ORD-200 has no shipment, should not appear"
# Orphan shipments must not appear
assert result.filter("order_id = 'ORD-901'").count() == 0, "ORD-901 only exists in shipments, should not appear"

print("Exercise 1 passed!")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Exercise 2: Inner Join with Watermarks on Both Sides
# MAGIC **Difficulty**: Medium | **Time**: ~15 min
# MAGIC
# MAGIC Add `withWatermark` on both sides, but DO NOT yet add a time-range predicate. For inner
# MAGIC joins, watermarks alone do not change which rows match - they only declare a notion of
# MAGIC event-time progress. Verify the output matches Exercise 1.
# MAGIC
# MAGIC The point of this exercise: watermark by itself is necessary but not sufficient to bound
# MAGIC stream-stream join state. Without a time-range predicate, state can still grow unboundedly
# MAGIC because Spark cannot know how late a match might arrive.
# MAGIC
# MAGIC **Source 1** / **Source 2**: same as Exercise 1.
# MAGIC
# MAGIC **Target**: `db_code.stream_stream_joins.ex2_target`.
# MAGIC
# MAGIC **Expected Output**: 25 rows. Same set as Exercise 1.
# MAGIC
# MAGIC **Requirements**:
# MAGIC 1. Apply `.withWatermark("order_ts", "1 minute")` to the orders stream.
# MAGIC 2. Apply `.withWatermark("shipment_ts", "1 minute")` to the shipments stream.
# MAGIC 3. Inner-join on `order_id`. No additional time-range predicate.
# MAGIC 4. Write to the target with `trigger(availableNow=True)` and `.awaitTermination()`.
# MAGIC 5. Use `checkpointLocation = f"{CHECKPOINT_BASE}/ex2"`.
# MAGIC
# MAGIC **Constraints**:
# MAGIC - Watermark must be applied BEFORE the join (not after).
# MAGIC - Delay string follows the `"<value> <unit>"` format (e.g., `"1 minute"`, `"10 minutes"`).

# COMMAND ----------

# EXERCISE_KEY: stream_stream_ex2
# TODO: Inner join with withWatermark on both sides; no time-range predicate

# Your code here


# COMMAND ----------

# Validate Exercise 2
result = spark.table(f"{CATALOG}.{SCHEMA}.ex2_target")

assert result.count() == 25, f"Expected 25 joined rows, got {result.count()}"

# In-window pair still matched
ord5 = result.filter("order_id = 'ORD-005'").collect()
assert len(ord5) == 1, "ORD-005 should be matched"
assert ord5[0]["shipment_id"] == "SHP-005", "ORD-005 should pair with SHP-005"

# Out-of-window pair still matched (no time-range predicate)
assert result.filter("order_id = 'ORD-022'").count() == 1, "ORD-022 should match SHP-022 (no time bound)"

# Unmatched and orphan rows still absent
assert result.filter("order_id = 'ORD-030'").count() == 0, "ORD-030 has no shipment"
assert result.filter("order_id = 'ORD-905'").count() == 0, "ORD-905 only exists in shipments"

print("Exercise 2 passed!")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Exercise 3: Inner Join with Watermark + 5-Minute Time-Range Constraint
# MAGIC **Difficulty**: Medium | **Time**: ~15 min
# MAGIC
# MAGIC Now add the time-range predicate that bounds the join window:
# MAGIC `shipment_ts BETWEEN order_ts AND order_ts + INTERVAL 5 MINUTES`.
# MAGIC Pairs whose shipment arrives more than 5 minutes after the order are now excluded.
# MAGIC The watermark + the time bound together let Spark drop state past the window.
# MAGIC
# MAGIC **Source 1** / **Source 2**: same as Exercise 1.
# MAGIC
# MAGIC **Target**: `db_code.stream_stream_joins.ex3_target`.
# MAGIC
# MAGIC **Expected Output**: 20 rows. Only ORD-001..ORD-020 paired with SHP-001..SHP-020 (their
# MAGIC shipment_ts is order_ts + 2 minutes, well within the 5-minute bound). ORD-021..ORD-025
# MAGIC are matched on `order_id` but their shipment is 10 minutes later, so the time predicate
# MAGIC excludes them.
# MAGIC
# MAGIC **Requirements**:
# MAGIC 1. Watermark both streams (`"1 minute"` delay on each).
# MAGIC 2. Inner-join on `order_id` AND the time-range predicate
# MAGIC    `shipment_ts BETWEEN order_ts AND order_ts + INTERVAL 5 MINUTES`.
# MAGIC 3. Write to `ex3_target` with `trigger(availableNow=True)` and `.awaitTermination()`.
# MAGIC 4. Use `checkpointLocation = f"{CHECKPOINT_BASE}/ex3"`.
# MAGIC
# MAGIC **Constraints**:
# MAGIC - The join condition must include both `order_id` equality AND the time-range predicate.
# MAGIC - You can express the time predicate either via DataFrame expression or via Spark SQL.
# MAGIC - Do NOT filter the time range AFTER the join - Spark needs the predicate in the join
# MAGIC   condition to bound state.

# COMMAND ----------

# EXERCISE_KEY: stream_stream_ex3
# TODO: Inner join with watermark on both sides AND shipment_ts BETWEEN order_ts AND order_ts + 5 MINUTES

# Your code here


# COMMAND ----------

# Validate Exercise 3
result = spark.table(f"{CATALOG}.{SCHEMA}.ex3_target")

assert result.count() == 20, f"Expected 20 joined rows (in-window only), got {result.count()}"

# In-window pair present
ord10 = result.filter("order_id = 'ORD-010'").collect()
assert len(ord10) == 1, "ORD-010 should be matched (2-min gap, in window)"
assert ord10[0]["shipment_id"] == "SHP-010", "ORD-010 should pair with SHP-010"

# Out-of-window pairs MUST be excluded
assert result.filter("order_id = 'ORD-021'").count() == 0, "ORD-021 pair is 10 min apart, should be excluded"
assert result.filter("order_id = 'ORD-025'").count() == 0, "ORD-025 pair is 10 min apart, should be excluded"

# Sanity: all 20 in-window order_ids present
in_window_ids = [f"ORD-{i:03d}" for i in range(1, 21)]
present_ids = set(r["order_id"] for r in result.select("order_id").collect())
missing = [oid for oid in in_window_ids if oid not in present_ids]
assert not missing, f"Missing in-window order_ids: {missing}"

print("Exercise 3 passed!")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Exercise 4: Left Outer Join with Watermark + Time Bound
# MAGIC **Difficulty**: Medium | **Time**: ~20 min
# MAGIC
# MAGIC Left outer join orders to shipments with the same time-range predicate as Exercise 3.
# MAGIC Orders with no in-window shipment must appear in the output with NULL shipment fields.
# MAGIC
# MAGIC **Important watermark behavior**: in `leftOuter`, an unmatched left row gets emitted with
# MAGIC NULLs once the RIGHT side's watermark passes that row's window upper bound
# MAGIC (`order_ts + 5min`) - at that point no future shipment within the window can arrive, so
# MAGIC Spark commits to the NULL emission. The LEFT side's watermark is needed too, but only to
# MAGIC drop matched-left state (and so the engine tracks event-time progress on both sides). In
# MAGIC practice you watermark BOTH sides; the assertion-driving rule is "right watermark crossed
# MAGIC the window's upper bound." The setup includes two late "watermark
# MAGIC advancement" orders (ORD-200 at 11:00, ORD-201 at 11:01) whose only job is to push the
# MAGIC watermarks far enough that the earlier unmatched orders (ORD-021..ORD-030) become provably
# MAGIC unmatched and their NULL rows can be emitted. The advancement orders themselves stay in
# MAGIC state and are not emitted in this run.
# MAGIC
# MAGIC **Source 1** / **Source 2**: same as Exercise 1.
# MAGIC
# MAGIC **Target**: `db_code.stream_stream_joins.ex4_target`.
# MAGIC
# MAGIC **Expected Output**: 30 rows.
# MAGIC - 20 matched pairs (ORD-001..ORD-020 with their shipments).
# MAGIC - 10 unmatched-left rows with NULL shipment fields:
# MAGIC   - ORD-021..ORD-025 (shipment exists but is out of window)
# MAGIC   - ORD-026..ORD-030 (no shipment at all)
# MAGIC
# MAGIC ORD-200 and ORD-201 are NOT emitted in this run because the watermark has not yet passed
# MAGIC their windows.
# MAGIC
# MAGIC **Requirements**:
# MAGIC 1. Watermark both streams (`"1 minute"` delay).
# MAGIC 2. Use `joinType="leftOuter"` (a.k.a. `"left_outer"`).
# MAGIC 3. Time-range predicate: `shipment_ts BETWEEN order_ts AND order_ts + INTERVAL 5 MINUTES`.
# MAGIC 4. Write to `ex4_target` with `trigger(availableNow=True)` and `.awaitTermination()`.
# MAGIC 5. Use `checkpointLocation = f"{CHECKPOINT_BASE}/ex4"`.
# MAGIC
# MAGIC **Constraints**:
# MAGIC - Outer joins REQUIRE both a watermark and a time-range predicate. Spark will throw if
# MAGIC   either is missing.
# MAGIC - Unmatched-left rows must contain NULL for `shipment_id` and `carrier`.

# COMMAND ----------

# EXERCISE_KEY: stream_stream_ex4
# TODO: Left outer join with watermark + time-range predicate

# Your code here


# COMMAND ----------

# Validate Exercise 4
result = spark.table(f"{CATALOG}.{SCHEMA}.ex4_target")

assert result.count() == 30, f"Expected 30 rows (20 matched + 10 unmatched-left), got {result.count()}"

# Matched rows: 20
matched = result.filter("shipment_id IS NOT NULL")
assert matched.count() == 20, f"Expected 20 matched rows, got {matched.count()}"

# Unmatched-left rows: 10
unmatched = result.filter("shipment_id IS NULL")
assert unmatched.count() == 10, f"Expected 10 unmatched-left rows, got {unmatched.count()}"

# Specific matched row
ord15 = result.filter("order_id = 'ORD-015'").collect()
assert len(ord15) == 1, "ORD-015 should appear once"
assert ord15[0]["shipment_id"] == "SHP-015", "ORD-015 should be matched to SHP-015"

# Specific unmatched-left row: ORD-021's shipment is out of window -> NULL right side
ord21 = result.filter("order_id = 'ORD-021'").collect()
assert len(ord21) == 1, "ORD-021 should appear once (unmatched-left)"
assert ord21[0]["shipment_id"] is None, "ORD-021 must have NULL shipment_id (out of window)"

# ORD-028 has no shipment at all
ord28 = result.filter("order_id = 'ORD-028'").collect()
assert len(ord28) == 1, "ORD-028 should appear once (unmatched-left)"
assert ord28[0]["shipment_id"] is None, "ORD-028 must have NULL shipment_id (no shipment)"

# ORD-200 should NOT be emitted yet (watermark hasn't passed its window)
assert result.filter("order_id = 'ORD-200'").count() == 0, \
    "ORD-200 (late) should still be in state, not yet emitted"

print("Exercise 4 passed!")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Exercise 5: Right Outer Join with Watermark + Time Bound
# MAGIC **Difficulty**: Hard | **Time**: ~20 min
# MAGIC
# MAGIC Right outer join orders to shipments. Shipments with no in-window matching order must
# MAGIC appear with NULL order fields. Same watermark + time bound rules as Exercise 4 - the
# MAGIC requirements for outer joins are symmetric.
# MAGIC
# MAGIC **Source 1** / **Source 2**: same as Exercise 1.
# MAGIC
# MAGIC **Target**: `db_code.stream_stream_joins.ex5_target`.
# MAGIC
# MAGIC **Expected Output**: 30 rows.
# MAGIC - 20 matched pairs (ORD-001..ORD-020 with SHP-001..SHP-020).
# MAGIC - 10 unmatched-right rows with NULL order fields:
# MAGIC   - SHP-021..SHP-025 (matching order exists but the shipment is 10 min after the order,
# MAGIC     outside the 5-min bound)
# MAGIC   - SHP-026..SHP-030 (orphan shipments referencing ORD-901..ORD-905)
# MAGIC
# MAGIC SHP-200 and SHP-201 are NOT emitted in this run (watermark hasn't passed their windows).
# MAGIC
# MAGIC **Requirements**:
# MAGIC 1. Watermark both streams (`"1 minute"`).
# MAGIC 2. Use `joinType="rightOuter"` (a.k.a. `"right_outer"`).
# MAGIC 3. Time-range predicate: `shipment_ts BETWEEN order_ts AND order_ts + INTERVAL 5 MINUTES`.
# MAGIC 4. Write to `ex5_target` with `trigger(availableNow=True)` and `.awaitTermination()`.
# MAGIC 5. Use `checkpointLocation = f"{CHECKPOINT_BASE}/ex5"`.
# MAGIC
# MAGIC **Constraints**:
# MAGIC - Unmatched-right rows must contain NULL for `order_id` and `customer_id` from the orders side.
# MAGIC - Use `shipment_id` (always non-null on the right side) to identify rows, not `order_id`.

# COMMAND ----------

# EXERCISE_KEY: stream_stream_ex5
# TODO: Right outer join with watermark + time-range predicate

# Your code here


# COMMAND ----------

# Validate Exercise 5
from pyspark.sql.functions import col

result = spark.table(f"{CATALOG}.{SCHEMA}.ex5_target")

assert result.count() == 30, f"Expected 30 rows (20 matched + 10 unmatched-right), got {result.count()}"

# Matched rows: 20 (have non-null order_id from the left side)
matched = result.filter("order_id IS NOT NULL")
assert matched.count() == 20, f"Expected 20 matched rows, got {matched.count()}"

# Unmatched-right rows: 10 (NULL order_id, non-null shipment_id)
unmatched = result.filter("order_id IS NULL")
assert unmatched.count() == 10, f"Expected 10 unmatched-right rows, got {unmatched.count()}"

# Specific matched row
shp7 = result.filter("shipment_id = 'SHP-007'").collect()
assert len(shp7) == 1, "SHP-007 should appear once"
assert shp7[0]["order_id"] == "ORD-007", "SHP-007 should be matched to ORD-007"

# Out-of-window pair: SHP-021 - order exists but is too early relative to shipment
shp21 = result.filter("shipment_id = 'SHP-021'").collect()
assert len(shp21) == 1, "SHP-021 should appear once (unmatched-right)"
assert shp21[0]["order_id"] is None, "SHP-021 must have NULL order_id (out of window)"

# Orphan shipment: SHP-027 references ORD-902 which doesn't exist
shp27 = result.filter("shipment_id = 'SHP-027'").collect()
assert len(shp27) == 1, "SHP-027 should appear once (unmatched-right)"
assert shp27[0]["order_id"] is None, "SHP-027 must have NULL order_id (orphan)"

# Late shipment not yet emitted
assert result.filter("shipment_id = 'SHP-200'").count() == 0, \
    "SHP-200 (late) should still be in state, not yet emitted"

print("Exercise 5 passed!")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Exercise 6: Diagnose Unbounded State (No Watermark, No Time Bound)
# MAGIC **Difficulty**: Hard | **Time**: ~15 min
# MAGIC
# MAGIC This exercise is half coding, half diagnosis. You will write an inner join with NO
# MAGIC watermark and NO time-range predicate - exactly like Exercise 1 - and run it with
# MAGIC `availableNow=True`. It will work and emit the 25 matched rows. The teaching point is in
# MAGIC the explanation that follows: this is how unbounded state growth happens in production.
# MAGIC
# MAGIC **Why state grows unboundedly**: Stream-stream joins keep every row received in state until
# MAGIC Spark can prove that no future row could match it. Without a watermark, Spark has no notion
# MAGIC of event-time progress, so it can never drop any row. Without a time-range predicate, even
# MAGIC with a watermark Spark cannot know the maximum lateness of a possible match. Either piece
# MAGIC alone is insufficient: you need BOTH a watermark AND a time-range constraint for the state
# MAGIC to be bounded.
# MAGIC
# MAGIC **What happens in production without the fix**: After hours or days of running, the join
# MAGIC state checkpoint balloons to gigabytes, every micro-batch slows down because state lookups
# MAGIC become expensive, and eventually the job OOMs or the checkpoint becomes too large to commit.
# MAGIC The fix is what you wrote in Exercise 3: `.withWatermark` on both sides plus a time-range
# MAGIC predicate in the join condition.
# MAGIC
# MAGIC In this exercise we run with `availableNow=True` so the stream terminates immediately and
# MAGIC you observe the matched rows. State growth would only become visible on a long-running
# MAGIC continuous stream.
# MAGIC
# MAGIC **Source 1** / **Source 2**: same as Exercise 1.
# MAGIC
# MAGIC **Target**: `db_code.stream_stream_joins.ex6_target`.
# MAGIC
# MAGIC **Expected Output**: 25 rows (same set as Exercise 1).
# MAGIC
# MAGIC **Requirements**:
# MAGIC 1. Inner-join orders_stream and shipments_stream on `order_id`.
# MAGIC 2. Do NOT add `withWatermark`. Do NOT add a time-range predicate.
# MAGIC 3. Write to `ex6_target` with `trigger(availableNow=True)` and `.awaitTermination()`.
# MAGIC 4. Use `checkpointLocation = f"{CHECKPOINT_BASE}/ex6"`.
# MAGIC
# MAGIC **Constraints**:
# MAGIC - The query MUST run to completion under `availableNow` without raising.
# MAGIC - This is intentionally the "broken" pattern. The lesson is the explanation, not the code.

# COMMAND ----------

# EXERCISE_KEY: stream_stream_ex6
# TODO: Inner join with NO watermark and NO time bound (the unbounded-state pattern)

# Your code here


# COMMAND ----------

# Validate Exercise 6
result = spark.table(f"{CATALOG}.{SCHEMA}.ex6_target")

assert result.count() == 25, f"Expected 25 matched rows, got {result.count()}"

# Same matches as Exercise 1 - including out-of-window pairs since no time bound here
assert result.filter("order_id = 'ORD-001'").count() == 1, "ORD-001 must match"
assert result.filter("order_id = 'ORD-024'").count() == 1, "ORD-024 must match (no time bound)"
assert result.filter("order_id = 'ORD-030'").count() == 0, "ORD-030 has no shipment"

# Lesson check: this run succeeded but the pattern is dangerous in production.
print("Exercise 6 passed!")
print("LESSON: this query ran to completion under availableNow, but in a long-running stream the")
print("state would grow without bound. Compare your code to Exercise 3 to see the fix.")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Exercise 7: Verify That Out-of-Window Pairs Are Dropped
# MAGIC **Difficulty**: Hard | **Time**: ~15 min
# MAGIC
# MAGIC The flip-side test of Exercise 3. Same inner join with watermark + 5-minute time-range
# MAGIC predicate, but the assertions focus on **what is NOT in the output**: every out-of-window
# MAGIC pair (ORD-021..ORD-025) must be absent, AND no row in the output may have
# MAGIC `shipment_ts - order_ts > 5 minutes`.
# MAGIC
# MAGIC This is the diagnostic skill: given a stream-stream join, can you prove that the time
# MAGIC bound is doing what you think it is? In production, a misplaced predicate (e.g., applied
# MAGIC as a post-join `.filter` instead of in the join condition) still gives the right OUTPUT
# MAGIC rows but leaves state unbounded. The correct way to verify the bound is in effect is to
# MAGIC check that out-of-bound pairs were dropped at the join, not after.
# MAGIC
# MAGIC **Source 1** / **Source 2**: same as Exercise 1.
# MAGIC
# MAGIC **Target**: `db_code.stream_stream_joins.ex7_target`.
# MAGIC
# MAGIC **Expected Output**: 20 rows. Only ORD-001..ORD-020 paired with SHP-001..SHP-020.
# MAGIC
# MAGIC **Requirements**:
# MAGIC 1. Watermark both streams (`"1 minute"`).
# MAGIC 2. Inner-join on `order_id` AND `shipment_ts BETWEEN order_ts AND order_ts + INTERVAL 5 MINUTES`.
# MAGIC 3. The time-range predicate must be part of the JOIN condition, not a post-join `.filter`.
# MAGIC 4. Write to `ex7_target` with `trigger(availableNow=True)` and `.awaitTermination()`.
# MAGIC 5. Use `checkpointLocation = f"{CHECKPOINT_BASE}/ex7"`.
# MAGIC
# MAGIC **Constraints**:
# MAGIC - Every row in `ex7_target` must satisfy `shipment_ts - order_ts <= INTERVAL 5 MINUTES`.
# MAGIC - No out-of-window pair (ORD-021..ORD-025) may appear in the output.

# COMMAND ----------

# EXERCISE_KEY: stream_stream_ex7
# TODO: Inner join with watermark + time-range predicate in the JOIN condition; assertions
#       verify that out-of-window pairs are dropped

# Your code here


# COMMAND ----------

# Validate Exercise 7
from pyspark.sql.functions import col, expr

result = spark.table(f"{CATALOG}.{SCHEMA}.ex7_target")

assert result.count() == 20, f"Expected 20 in-window rows, got {result.count()}"

# Out-of-window pairs MUST all be dropped
for oid in ["ORD-021", "ORD-022", "ORD-023", "ORD-024", "ORD-025"]:
    assert result.filter(f"order_id = '{oid}'").count() == 0, \
        f"{oid} should be dropped (its shipment is 10 min later, out of 5-min window)"

# Every surviving row must satisfy the time bound
violations = result.filter(
    expr("shipment_ts > order_ts + INTERVAL 5 MINUTES OR shipment_ts < order_ts")
).count()
assert violations == 0, f"Found {violations} rows violating the 5-minute window bound"

# Sanity: every in-window order is present
present = set(r["order_id"] for r in result.select("order_id").collect())
for i in range(1, 21):
    oid = f"ORD-{i:03d}"
    assert oid in present, f"Missing in-window order {oid}"

print("Exercise 7 passed!")
