# Databricks notebook source
# COMMAND ----------
# MAGIC %md
# MAGIC ### Setup: Structured Streaming Basics
# MAGIC Creates per-exercise source tables (copies of the shared `db_code.streaming.events_source`
# MAGIC base table) and per-exercise target tables in the `structured_streaming_basics` schema.
# MAGIC Each exercise gets its own isolated source and target so streams cannot interfere with
# MAGIC each other. Checkpoints live under a Unity Catalog volume so streams are restartable.
# MAGIC
# MAGIC Run automatically via `%run` from the exercise notebook.

# COMMAND ----------

CATALOG = "db_code"
SCHEMA = "structured_streaming_basics"
BASE_SCHEMA = "streaming"

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA}")

# Volume for checkpoint locations (one per exercise)
spark.sql(f"CREATE VOLUME IF NOT EXISTS {CATALOG}.{SCHEMA}.checkpoints")

CHECKPOINT_BASE = f"/Volumes/{CATALOG}/{SCHEMA}/checkpoints"

# COMMAND ----------

# Clean previous runs
# Drop per-exercise source and target tables, and wipe checkpoints, so the notebook
# is fully idempotent. The base table in BASE_SCHEMA is left alone.
for i in range(1, 9):
    spark.sql(f"DROP TABLE IF EXISTS {CATALOG}.{SCHEMA}.ex{i}_source")
    spark.sql(f"DROP TABLE IF EXISTS {CATALOG}.{SCHEMA}.ex{i}_target")

# Exercise 5 writes to `ex5_target_available_now`; Exercise 7 also writes `ex7_progress`.
spark.sql(f"DROP TABLE IF EXISTS {CATALOG}.{SCHEMA}.ex5_target_available_now")
spark.sql(f"DROP TABLE IF EXISTS {CATALOG}.{SCHEMA}.ex7_progress")

dbutils.fs.rm(CHECKPOINT_BASE, recurse=True)
spark.sql(f"CREATE VOLUME IF NOT EXISTS {CATALOG}.{SCHEMA}.checkpoints")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Per-exercise source tables
# MAGIC One source table per exercise so streams cannot pollute each other. Exercises 1-7 each get a Delta CTAS copy of `db_code.streaming.events_source`; Exercise 8 gets its own 15-row source with duplicate `event_id`s.

# COMMAND ----------

# Exercises 1, 4, 6: copy of the base events source (35 rows)
for ex in [1, 4, 6]:
    spark.sql(f"""
        CREATE OR REPLACE TABLE {CATALOG}.{SCHEMA}.ex{ex}_source
        USING DELTA
        AS SELECT * FROM {CATALOG}.{BASE_SCHEMA}.events_source
    """)

# COMMAND ----------

# Exercise 2: filter exercise - source has both purchase and non-purchase events.
# Reuses the full base table. Target keeps only event_type='purchase' (18 rows expected).
spark.sql(f"""
    CREATE OR REPLACE TABLE {CATALOG}.{SCHEMA}.ex2_source
    USING DELTA
    AS SELECT * FROM {CATALOG}.{BASE_SCHEMA}.events_source
""")

# COMMAND ----------

# Exercise 3: derived column exercise - amount_with_tax = amount * 1.10.
# Same row set as base; assertions check the new column value.
spark.sql(f"""
    CREATE OR REPLACE TABLE {CATALOG}.{SCHEMA}.ex3_source
    USING DELTA
    AS SELECT * FROM {CATALOG}.{BASE_SCHEMA}.events_source
""")

# COMMAND ----------

# Exercise 5: trigger modes - source for the availableNow query.
spark.sql(f"""
    CREATE OR REPLACE TABLE {CATALOG}.{SCHEMA}.ex5_source
    USING DELTA
    AS SELECT * FROM {CATALOG}.{BASE_SCHEMA}.events_source
""")

# COMMAND ----------

# Exercise 7: lastProgress / named query - same source as ex1.
spark.sql(f"""
    CREATE OR REPLACE TABLE {CATALOG}.{SCHEMA}.ex7_source
    USING DELTA
    AS SELECT * FROM {CATALOG}.{BASE_SCHEMA}.events_source
""")

# COMMAND ----------

# Exercise 8: 15 rows, 10 unique event_ids. EV-101 appears 3 times; EV-102, EV-104 and EV-107 twice each, with identical values.
spark.sql(f"""
    CREATE OR REPLACE TABLE {CATALOG}.{SCHEMA}.ex8_source (
        event_id STRING,
        event_type STRING,
        user_id STRING,
        amount DOUBLE,
        event_ts TIMESTAMP
    ) USING DELTA
""")

spark.sql(f"""
    INSERT INTO {CATALOG}.{SCHEMA}.ex8_source VALUES
        ('EV-101', 'purchase', 'U-1', 50.00, TIMESTAMP '2026-04-02 09:00:00'),
        ('EV-101', 'purchase', 'U-1', 50.00, TIMESTAMP '2026-04-02 09:00:00'),
        ('EV-101', 'purchase', 'U-1', 50.00, TIMESTAMP '2026-04-02 09:00:00'),
        ('EV-102', 'view',     'U-2', 0.00,  TIMESTAMP '2026-04-02 09:01:00'),
        ('EV-102', 'view',     'U-2', 0.00,  TIMESTAMP '2026-04-02 09:01:00'),
        ('EV-103', 'purchase', 'U-3', 120.00, TIMESTAMP '2026-04-02 09:02:00'),
        ('EV-104', 'click',    'U-1', 0.00,  TIMESTAMP '2026-04-02 09:03:00'),
        ('EV-104', 'click',    'U-1', 0.00,  TIMESTAMP '2026-04-02 09:03:00'),
        ('EV-105', 'purchase', 'U-4', 200.00, TIMESTAMP '2026-04-02 09:04:00'),
        ('EV-106', 'view',     'U-5', 0.00,  TIMESTAMP '2026-04-02 09:05:00'),
        ('EV-107', 'purchase', 'U-2', 75.50, TIMESTAMP '2026-04-02 09:06:00'),
        ('EV-107', 'purchase', 'U-2', 75.50, TIMESTAMP '2026-04-02 09:06:00'),
        ('EV-108', 'click',    'U-3', 0.00,  TIMESTAMP '2026-04-02 09:07:00'),
        ('EV-109', 'purchase', 'U-4', 45.00, TIMESTAMP '2026-04-02 09:08:00'),
        ('EV-110', 'view',     'U-1', 0.00,  TIMESTAMP '2026-04-02 09:09:00')
""")

# COMMAND ----------

print(f"Setup complete for Structured Streaming Basics")
print(f"  Schema: {CATALOG}.{SCHEMA}")
print(f"  Base table: {CATALOG}.{BASE_SCHEMA}.events_source (35 rows)")
print(f"  Checkpoints: {CHECKPOINT_BASE}")
print(f"  Per-exercise sources: ex1_source ... ex8_source")
print(f"  Exercise 2 source: 35 events (18 purchase, 9 view, 8 click)")
print(f"  Exercise 8 source: 15 rows total, 10 unique event_ids")
