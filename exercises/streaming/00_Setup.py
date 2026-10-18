# Databricks notebook source
# COMMAND ----------
# MAGIC %md
# MAGIC # 00_Setup: Shared Data Generator (streaming zone)
# MAGIC Every exercise notebook runs this notebook in its second cell, so you never run it by hand. It creates the `db_code` catalog, the `streaming` zone schema, the six per-topic schemas the setups in `setup/` write to, and the shared base table `events_source`.
# MAGIC
# MAGIC **What it creates**:
# MAGIC - Catalog: `db_code`
# MAGIC - Zone schema: `streaming` (holds the base table `events_source`)
# MAGIC - Per-topic schemas:
# MAGIC   - `structured_streaming_basics`
# MAGIC   - `windowed_aggregations`
# MAGIC   - `stream_static_joins`
# MAGIC   - `stream_stream_joins`
# MAGIC   - `foreachbatch_patterns`
# MAGIC   - `checkpointing_recovery`
# MAGIC - Base table:
# MAGIC   - `streaming.events_source` (35 rows, EV-001..EV-035). `structured_streaming_basics` and `checkpointing_recovery` copy it into per-exercise source tables. The other four topics create their own tables in their own schemas.
# MAGIC
# MAGIC **Idempotent**: Safe to re-run. If `events_source` already has >= 35 rows, base
# MAGIC table creation is skipped. Schema/catalog creation is always idempotent via
# MAGIC `IF NOT EXISTS`.
# MAGIC
# MAGIC **Fixed data**: all 35 rows are hardcoded with literal timestamps, so every run builds the same table and the expected values in each exercise always hold.

# COMMAND ----------

# Catalog and schemas

CATALOG = "db_code"

spark.sql(f"CREATE CATALOG IF NOT EXISTS {CATALOG}")
spark.sql(f"USE CATALOG {CATALOG}")

# Zone schema for base tables.
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.streaming")

# Per-topic schemas (exercise tables created by each topic's setup notebook go here).
for schema in [
    "structured_streaming_basics",
    "windowed_aggregations",
    "stream_static_joins",
    "stream_stream_joins",
    "foreachbatch_patterns",
    "checkpointing_recovery",
]:
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{schema}")

# COMMAND ----------

# Idempotency check
# If the events_source base table already exists with the expected row count, skip
# recreation. This keeps re-runs cheap and avoids redundant writes.

_SETUP_SKIP = False
try:
    _events_source_ct = spark.table(f"{CATALOG}.streaming.events_source").count()
    if _events_source_ct >= 35:
        _SETUP_SKIP = True
        print("=" * 60)
        print(f"  Base table already exists with expected row count. Skipping creation.")
        print(f"  streaming.events_source = {_events_source_ct} rows")
        print("=" * 60)
except Exception:
    _SETUP_SKIP = False

# COMMAND ----------

# events_source: the shared streaming events base table
# Schema: event_id STRING, event_type STRING, user_id STRING, amount DOUBLE, event_ts TIMESTAMP
#
# Read by:
#   - structured_streaming_basics: copies all 35 rows (18 purchase, 9 view, 8 click) into the sources of Exercises 1-7; Exercise 8 builds its own 15-row source.
#   - checkpointing_recovery: copies EV-001..EV-030 (ORDER BY event_id LIMIT 30) for Exercises 1-4 and 7, EV-001..EV-020 (ORDER BY event_id LIMIT 20) for Exercise 5, and EV-001..EV-015 / EV-016..EV-030 into two sources for Exercise 6.
# The other four topics create their own tables in their own schemas.
#
# The data:
#   - 35 rows total. Deterministic event_ids EV-001..EV-035, one per minute starting
#     at 2026-04-01 10:00:00 UTC.
#   - Odd positions (EV-001, EV-003, ..., EV-035) are purchases (18); even positions alternate
#     view (EV-002, EV-006, ..., EV-034: 9) and click (EV-004, EV-008, ..., EV-032: 8).
#   - Non-purchase events have amount = 0.00; purchase events have varied positive amounts.
#   - Amount literals are cast to DOUBLE to match the `amount` column.

if not _SETUP_SKIP:
    spark.sql(f"""
    CREATE OR REPLACE TABLE {CATALOG}.streaming.events_source (
        event_id STRING,
        event_type STRING,
        user_id STRING,
        amount DOUBLE,
        event_ts TIMESTAMP
    ) USING DELTA
    """)

    spark.sql(f"""
    INSERT OVERWRITE {CATALOG}.streaming.events_source
    SELECT * FROM VALUES
        ('EV-001', 'purchase', 'U-1', CAST(50.00   AS DOUBLE), TIMESTAMP '2026-04-01 10:00:00'),
        ('EV-002', 'view',     'U-2', CAST(0.00    AS DOUBLE), TIMESTAMP '2026-04-01 10:01:00'),
        ('EV-003', 'purchase', 'U-3', CAST(120.50  AS DOUBLE), TIMESTAMP '2026-04-01 10:02:00'),
        ('EV-004', 'click',    'U-1', CAST(0.00    AS DOUBLE), TIMESTAMP '2026-04-01 10:03:00'),
        ('EV-005', 'purchase', 'U-2', CAST(75.25   AS DOUBLE), TIMESTAMP '2026-04-01 10:04:00'),
        ('EV-006', 'view',     'U-4', CAST(0.00    AS DOUBLE), TIMESTAMP '2026-04-01 10:05:00'),
        ('EV-007', 'purchase', 'U-3', CAST(200.00  AS DOUBLE), TIMESTAMP '2026-04-01 10:06:00'),
        ('EV-008', 'click',    'U-5', CAST(0.00    AS DOUBLE), TIMESTAMP '2026-04-01 10:07:00'),
        ('EV-009', 'purchase', 'U-1', CAST(45.00   AS DOUBLE), TIMESTAMP '2026-04-01 10:08:00'),
        ('EV-010', 'view',     'U-2', CAST(0.00    AS DOUBLE), TIMESTAMP '2026-04-01 10:09:00'),
        ('EV-011', 'purchase', 'U-4', CAST(310.75  AS DOUBLE), TIMESTAMP '2026-04-01 10:10:00'),
        ('EV-012', 'click',    'U-3', CAST(0.00    AS DOUBLE), TIMESTAMP '2026-04-01 10:11:00'),
        ('EV-013', 'purchase', 'U-5', CAST(89.99   AS DOUBLE), TIMESTAMP '2026-04-01 10:12:00'),
        ('EV-014', 'view',     'U-1', CAST(0.00    AS DOUBLE), TIMESTAMP '2026-04-01 10:13:00'),
        ('EV-015', 'purchase', 'U-2', CAST(125.00  AS DOUBLE), TIMESTAMP '2026-04-01 10:14:00'),
        ('EV-016', 'click',    'U-4', CAST(0.00    AS DOUBLE), TIMESTAMP '2026-04-01 10:15:00'),
        ('EV-017', 'purchase', 'U-3', CAST(60.00   AS DOUBLE), TIMESTAMP '2026-04-01 10:16:00'),
        ('EV-018', 'view',     'U-5', CAST(0.00    AS DOUBLE), TIMESTAMP '2026-04-01 10:17:00'),
        ('EV-019', 'purchase', 'U-1', CAST(175.50  AS DOUBLE), TIMESTAMP '2026-04-01 10:18:00'),
        ('EV-020', 'click',    'U-2', CAST(0.00    AS DOUBLE), TIMESTAMP '2026-04-01 10:19:00'),
        ('EV-021', 'purchase', 'U-4', CAST(99.50   AS DOUBLE), TIMESTAMP '2026-04-01 10:20:00'),
        ('EV-022', 'view',     'U-3', CAST(0.00    AS DOUBLE), TIMESTAMP '2026-04-01 10:21:00'),
        ('EV-023', 'purchase', 'U-5', CAST(220.00  AS DOUBLE), TIMESTAMP '2026-04-01 10:22:00'),
        ('EV-024', 'click',    'U-1', CAST(0.00    AS DOUBLE), TIMESTAMP '2026-04-01 10:23:00'),
        ('EV-025', 'purchase', 'U-2', CAST(30.00   AS DOUBLE), TIMESTAMP '2026-04-01 10:24:00'),
        ('EV-026', 'view',     'U-4', CAST(0.00    AS DOUBLE), TIMESTAMP '2026-04-01 10:25:00'),
        ('EV-027', 'purchase', 'U-3', CAST(410.00  AS DOUBLE), TIMESTAMP '2026-04-01 10:26:00'),
        ('EV-028', 'click',    'U-5', CAST(0.00    AS DOUBLE), TIMESTAMP '2026-04-01 10:27:00'),
        ('EV-029', 'purchase', 'U-1', CAST(15.50   AS DOUBLE), TIMESTAMP '2026-04-01 10:28:00'),
        ('EV-030', 'view',     'U-2', CAST(0.00    AS DOUBLE), TIMESTAMP '2026-04-01 10:29:00'),
        ('EV-031', 'purchase', 'U-4', CAST(270.00  AS DOUBLE), TIMESTAMP '2026-04-01 10:30:00'),
        ('EV-032', 'click',    'U-3', CAST(0.00    AS DOUBLE), TIMESTAMP '2026-04-01 10:31:00'),
        ('EV-033', 'purchase', 'U-5', CAST(55.00   AS DOUBLE), TIMESTAMP '2026-04-01 10:32:00'),
        ('EV-034', 'view',     'U-1', CAST(0.00    AS DOUBLE), TIMESTAMP '2026-04-01 10:33:00'),
        ('EV-035', 'purchase', 'U-2', CAST(145.75  AS DOUBLE), TIMESTAMP '2026-04-01 10:34:00')
    AS t(event_id, event_type, user_id, amount, event_ts)
    """)

# COMMAND ----------

# Confirmation
_events_source_count = spark.table(f"{CATALOG}.streaming.events_source").count()

print("=" * 60)
print(f"  00_Setup complete. Catalog: {CATALOG}")
print("=" * 60)
print(f"  Base table:")
print(f"    streaming.events_source       : {_events_source_count} rows")
print("=" * 60)
print(f"  Zone schema   : streaming")
print(f"  Topic schemas : structured_streaming_basics, windowed_aggregations,")
print(f"                  stream_static_joins, stream_stream_joins,")
print(f"                  foreachbatch_patterns, checkpointing_recovery")
print("=" * 60)
print("  Topic tables are created by each notebook's own setup cell.")
print("=" * 60)
