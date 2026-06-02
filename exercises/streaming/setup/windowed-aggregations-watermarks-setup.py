# Databricks notebook source
# COMMAND ----------
# MAGIC %md
# MAGIC ### Setup: Windowed Aggregations & Watermarks
# MAGIC Creates a deterministic timestamped events source table and the per-exercise checkpoint
# MAGIC volume. Run automatically via `%run` from the exercise notebook.
# MAGIC
# MAGIC **Event timing design** (all events on 2026-03-10, anchored to 10:00:00 UTC):
# MAGIC - Events fall inside 5-min tumbling windows starting at :00, :05, :10, :15, :20, :25, :30
# MAGIC - Boundary events at exactly 10:05:00 and 10:10:00 belong to the LATER window (start-inclusive)
# MAGIC - Late arrival: a single event at 09:30:00 (30 minutes before EVT-001 at 10:00:00) demonstrates
# MAGIC   append-mode closed-window emission - its window [09:30,09:35) closes well before the
# MAGIC   end-of-stream watermark.
# MAGIC - Two regions (NA, EU) and three event_types (click, view, purchase) for grouped aggregations
# MAGIC - Per-user sessions: under Ex 4's 10-min inactivity gap, every user collapses to a single
# MAGIC   session (max consecutive gap per user is < 10 min)

# COMMAND ----------

CATALOG = "db_code"
SCHEMA = "windowed_aggregations"

spark.sql(f"CREATE CATALOG IF NOT EXISTS {CATALOG}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA}")

# Volume for checkpoints (streaming queries need durable checkpoint locations)
spark.sql(f"CREATE VOLUME IF NOT EXISTS {CATALOG}.{SCHEMA}.checkpoints")

CHECKPOINT_BASE = f"/Volumes/{CATALOG}/{SCHEMA}/checkpoints"

# COMMAND ----------

# -- Clean previous runs --
# Remove old checkpoints so streams re-process the source from scratch.
dbutils.fs.rm(CHECKPOINT_BASE, recurse=True)
spark.sql(f"CREATE VOLUME IF NOT EXISTS {CATALOG}.{SCHEMA}.checkpoints")

# Drop exercise output tables
for i in range(1, 10):
    spark.sql(f"DROP TABLE IF EXISTS {CATALOG}.{SCHEMA}.ex{i}_output")

# Drop comparison table for Ex 6 (with/without watermark)
spark.sql(f"DROP TABLE IF EXISTS {CATALOG}.{SCHEMA}.ex6_output_no_watermark")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Source events table
# MAGIC One shared base table `db_code.windowed_aggregations.windows_events`. Every exercise reads it
# MAGIC as a stream via `spark.readStream.table(...)`. 49 rows total (48 main events + 1 late event
# MAGIC at 09:30:00).

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType
from datetime import datetime

# Schema: event_id, event_type, user_id, amount, event_ts, region
events_schema = StructType([
    StructField("event_id", StringType()),
    StructField("event_type", StringType()),
    StructField("user_id", StringType()),
    StructField("amount", DoubleType()),
    StructField("event_ts", TimestampType()),
    StructField("region", StringType()),
])


def ts(h, m, s):
    """Helper: build a 2026-03-10 timestamp."""
    return datetime(2026, 3, 10, h, m, s)


# Anchor day = 2026-03-10.
# Window structure used by exercises (5-min tumbling): [10:00,10:05), [10:05,10:10), [10:10,10:15),
# [10:15,10:20), [10:20,10:25), [10:25,10:30), [10:30,10:35).
#
# Distribution per 5-min window for event_type counts (Ex 1):
#   [10:00,10:05): click=4, view=3, purchase=2  -> sum 9
#   [10:05,10:10): click=3, view=2, purchase=1  -> sum 6
#   [10:10,10:15): click=2, view=4, purchase=2  -> sum 8
#   [10:15,10:20): click=3, view=2, purchase=2  -> sum 7
#   [10:20,10:25): click=2, view=3, purchase=1  -> sum 6
#   [10:25,10:30): click=2, view=2, purchase=2  -> sum 6
#   [10:30,10:35): click=3, view=2, purchase=1  -> sum 6
# Plus 1 late event at 09:30:00. Its window [09:30,09:35) is well below the end-of-stream watermark
# 10:24:00, so it closes and IS emitted under append mode in Ex 5-9.
#
# All event_ts values chosen so:
#  - boundary events sit at the START of their window (e.g., 10:05:00 belongs to [10:05,10:10))
#  - amounts are whole numbers so sums are easy to predict
#  - per-user clusters create predictable session windows

events_data = [
    # ---------- Window [10:00, 10:05) -- 9 events ----------
    # boundary event: 10:00:00 starts this window
    ("EVT-001", "click",    "USER-001",  10.00, ts(10, 0, 0),   "NA"),
    ("EVT-002", "click",    "USER-002",  20.00, ts(10, 0, 30),  "EU"),
    ("EVT-003", "click",    "USER-001",  30.00, ts(10, 1, 0),   "NA"),
    ("EVT-004", "click",    "USER-003",  40.00, ts(10, 2, 0),   "NA"),
    ("EVT-005", "view",     "USER-002",   5.00, ts(10, 2, 30),  "EU"),
    ("EVT-006", "view",     "USER-001",   5.00, ts(10, 3, 0),   "NA"),
    ("EVT-007", "view",     "USER-003",   5.00, ts(10, 3, 30),  "NA"),
    ("EVT-008", "purchase", "USER-001", 100.00, ts(10, 4, 0),   "NA"),
    ("EVT-009", "purchase", "USER-002", 200.00, ts(10, 4, 30),  "EU"),

    # ---------- Window [10:05, 10:10) -- 6 events ----------
    # boundary event: 10:05:00 belongs to THIS window (not the previous)
    ("EVT-010", "click",    "USER-002",  25.00, ts(10, 5, 0),   "EU"),
    ("EVT-011", "click",    "USER-001",  35.00, ts(10, 6, 0),   "NA"),
    ("EVT-012", "click",    "USER-003",  45.00, ts(10, 7, 0),   "NA"),
    ("EVT-013", "view",     "USER-001",   5.00, ts(10, 8, 0),   "NA"),
    ("EVT-014", "view",     "USER-002",   5.00, ts(10, 8, 30),  "EU"),
    ("EVT-015", "purchase", "USER-001", 150.00, ts(10, 9, 0),   "NA"),

    # ---------- Window [10:10, 10:15) -- 8 events ----------
    # boundary event: 10:10:00 belongs to THIS window
    ("EVT-016", "click",    "USER-004",  15.00, ts(10, 10, 0),  "EU"),
    ("EVT-017", "click",    "USER-001",  25.00, ts(10, 11, 0),  "NA"),
    ("EVT-018", "view",     "USER-002",   5.00, ts(10, 11, 30), "EU"),
    ("EVT-019", "view",     "USER-001",   5.00, ts(10, 12, 0),  "NA"),
    ("EVT-020", "view",     "USER-003",   5.00, ts(10, 12, 30), "NA"),
    ("EVT-021", "view",     "USER-004",   5.00, ts(10, 13, 0),  "EU"),
    ("EVT-022", "purchase", "USER-001",  80.00, ts(10, 13, 30), "NA"),
    ("EVT-023", "purchase", "USER-002", 120.00, ts(10, 14, 0),  "EU"),

    # ---------- Window [10:15, 10:20) -- 7 events ----------
    ("EVT-024", "click",    "USER-003",  10.00, ts(10, 15, 30), "NA"),
    ("EVT-025", "click",    "USER-004",  20.00, ts(10, 16, 0),  "EU"),
    ("EVT-026", "click",    "USER-001",  30.00, ts(10, 16, 30), "NA"),
    ("EVT-027", "view",     "USER-002",   5.00, ts(10, 17, 0),  "EU"),
    ("EVT-028", "view",     "USER-003",   5.00, ts(10, 17, 30), "NA"),
    ("EVT-029", "purchase", "USER-001",  90.00, ts(10, 18, 0),  "NA"),
    ("EVT-030", "purchase", "USER-004", 110.00, ts(10, 19, 0),  "EU"),

    # ---------- Window [10:20, 10:25) -- 6 events ----------
    ("EVT-031", "click",    "USER-001",  10.00, ts(10, 20, 30), "NA"),
    ("EVT-032", "click",    "USER-002",  20.00, ts(10, 21, 0),  "EU"),
    ("EVT-033", "view",     "USER-001",   5.00, ts(10, 22, 0),  "NA"),
    ("EVT-034", "view",     "USER-003",   5.00, ts(10, 22, 30), "NA"),
    ("EVT-035", "view",     "USER-004",   5.00, ts(10, 23, 0),  "EU"),
    ("EVT-036", "purchase", "USER-002", 130.00, ts(10, 24, 0),  "EU"),

    # ---------- Window [10:25, 10:30) -- 6 events ----------
    ("EVT-037", "click",    "USER-001",  10.00, ts(10, 25, 30), "NA"),
    ("EVT-038", "click",    "USER-003",  20.00, ts(10, 26, 0),  "NA"),
    ("EVT-039", "view",     "USER-002",   5.00, ts(10, 27, 0),  "EU"),
    ("EVT-040", "view",     "USER-001",   5.00, ts(10, 27, 30), "NA"),
    ("EVT-041", "purchase", "USER-001",  60.00, ts(10, 28, 0),  "NA"),
    ("EVT-042", "purchase", "USER-004",  70.00, ts(10, 29, 0),  "EU"),

    # ---------- Window [10:30, 10:35) -- 6 events ----------
    ("EVT-043", "click",    "USER-002",  10.00, ts(10, 30, 30), "EU"),
    ("EVT-044", "click",    "USER-001",  20.00, ts(10, 31, 0),  "NA"),
    ("EVT-045", "click",    "USER-003",  30.00, ts(10, 32, 0),  "NA"),
    ("EVT-046", "view",     "USER-001",   5.00, ts(10, 33, 0),  "NA"),
    ("EVT-047", "view",     "USER-004",   5.00, ts(10, 33, 30), "EU"),
    ("EVT-048", "purchase", "USER-002",  50.00, ts(10, 34, 0),  "EU"),

    # ---------- Late arrival (used by Ex 5, 6, 7) ----------
    # event_ts is 09:30:00 - 30 min before EVT-001. Its 5-min window [09:30,09:35) is well below
    # (max_event_ts 10:34:00 - 10 min watermark) = 10:24:00, so it closes under append mode and
    # is emitted as its own closed window in the output.
    ("EVT-049", "click",    "USER-005",  99.00, ts(9, 30, 0),   "NA"),
]

# Write all 49 events in a single commit. Under `trigger(availableNow=True)`, the engine reads
# all available data before the watermark advances, so the late event ends up in a closed window
# [09:30,09:35) rather than being dropped. Watermark-aware exercises here demonstrate append-mode
# closed-window emission (open windows held in state), not late-row drop semantics.
all_df = spark.createDataFrame(events_data, schema=events_schema).coalesce(1)

# Drop and recreate the table so setup is idempotent.
spark.sql(f"DROP TABLE IF EXISTS {CATALOG}.{SCHEMA}.windows_events")

(all_df
    .write
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(f"{CATALOG}.{SCHEMA}.windows_events"))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Per-user session timing reference (for Ex 4)
# MAGIC Ex 4 uses a 10-minute inactivity gap and keeps the late event. Every user's main-cluster events
# MAGIC have max consecutive gap < 10 min, so each user collapses to a single session. USER-005's lone
# MAGIC late event at 09:30:00 is naturally isolated (next event > 30 min later) as its own session.
# MAGIC Total expected: 5 sessions.
# MAGIC
# MAGIC - USER-001: 19 events from 10:00:00 -> 10:33:00. Max consecutive gap = 3.5 min (10:22:00 ->
# MAGIC   10:25:30) -> 1 session.
# MAGIC - USER-002: 13 events from 10:00:30 -> 10:34:00. Max consecutive gap = 4 min (10:17:00 ->
# MAGIC   10:21:00) -> 1 session.
# MAGIC - USER-003: 9 events from 10:02:00 -> 10:32:00. Max consecutive gap = 6 min (10:26:00 ->
# MAGIC   10:32:00) -> 1 session.
# MAGIC - USER-004: 7 events from 10:10:00 -> 10:33:30. Max consecutive gap = 6 min (10:23:00 ->
# MAGIC   10:29:00) -> 1 session.
# MAGIC - USER-005: 1 late event at 09:30:00 -> 1 session of size 1 (isolated).

# COMMAND ----------

print("Setup complete for Windowed Aggregations & Watermarks")
print(f"  Catalog: {CATALOG}")
print(f"  Schema: {CATALOG}.{SCHEMA}")
total_rows = all_df.count()
print(f"  Source table: {CATALOG}.{SCHEMA}.windows_events ({total_rows} rows)")
print(f"  Checkpoint base: {CHECKPOINT_BASE}")
print(f"  Max event_ts: 2026-03-10 10:34:00; late row at 2026-03-10 09:30:00 (EVT-049)")
