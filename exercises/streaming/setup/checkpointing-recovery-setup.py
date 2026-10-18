# Databricks notebook source
# COMMAND ----------
# MAGIC %md
# MAGIC ### Setup: Checkpointing & Recovery
# MAGIC Creates per-exercise streaming sources, target tables, and the checkpoint volume for the
# MAGIC `checkpointing_recovery` schema. Each exercise has its own isolated source, target, and
# MAGIC checkpoint directory so demonstrations cannot interfere with each other. Source tables
# MAGIC start with a small initial batch; exercises that append more rows do so inside the TODO cell.
# MAGIC
# MAGIC Run automatically via `%run` from the exercise notebook.

# COMMAND ----------

CATALOG = "db_code"
SCHEMA = "checkpointing_recovery"
BASE_SCHEMA = "streaming"

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA}")

# One volume holds every exercise's checkpoint directory (exN/)
spark.sql(f"CREATE VOLUME IF NOT EXISTS {CATALOG}.{SCHEMA}.checkpoints")

CHECKPOINT_BASE = f"/Volumes/{CATALOG}/{SCHEMA}/checkpoints"

# COMMAND ----------

# Clean previous runs
# Drop per-exercise sources / targets / observation tables, and wipe every per-exercise
# checkpoint dir. The base table in BASE_SCHEMA is left alone.
for i in range(1, 8):
    spark.sql(f"DROP TABLE IF EXISTS {CATALOG}.{SCHEMA}.ex{i}_source")
    spark.sql(f"DROP TABLE IF EXISTS {CATALOG}.{SCHEMA}.ex{i}_target")

# Extra tables used by specific exercises
spark.sql(f"DROP TABLE IF EXISTS {CATALOG}.{SCHEMA}.ex2_findings")
spark.sql(f"DROP TABLE IF EXISTS {CATALOG}.{SCHEMA}.ex6_observation")
for t in ["ex6_source_a", "ex6_source_b", "ex6_shared_target"]:
    spark.sql(f"DROP TABLE IF EXISTS {CATALOG}.{SCHEMA}.{t}")

# Wipe every per-exercise checkpoint dir so a re-run starts clean
dbutils.fs.rm(CHECKPOINT_BASE, recurse=True)
spark.sql(f"CREATE VOLUME IF NOT EXISTS {CATALOG}.{SCHEMA}.checkpoints")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Per-exercise source tables
# MAGIC Each exercise gets its own source: the first 30 rows (`EV-001`..`EV-030`) of `db_code.streaming.events_source`. Exercise 5 starts with the first 20 so its TODO can append the other 10, and Exercise 6 splits the 30 rows into `ex6_source_a` (`EV-001`..`EV-015`) and `ex6_source_b` (`EV-016`..`EV-030`).

# COMMAND ----------

# Exercises 1, 2, 3, 4, 7 use the first 30 rows (EV-001..EV-030, 10:00-10:29),
# so Exercise 7 sees exactly six 5-minute windows.
# Exercise 5 starts with only the first 20 rows so the TODO can append the remaining 10.
for ex in [1, 2, 3, 4, 7]:
    spark.sql(f"""
        CREATE OR REPLACE TABLE {CATALOG}.{SCHEMA}.ex{ex}_source
        USING DELTA
        AS SELECT * FROM {CATALOG}.{BASE_SCHEMA}.events_source
        ORDER BY event_id
        LIMIT 30
    """)

# COMMAND ----------

# Exercise 5: append-then-resume. The source starts with the first 20 events. Between two
# stream runs the helper INSERT in the exercise appends the base rows NOT IN them up to
# EV-030, exactly 10 rows (EV-021..EV-030), to show that only the new rows land.
spark.sql(f"""
    CREATE OR REPLACE TABLE {CATALOG}.{SCHEMA}.ex5_source
    USING DELTA
    AS SELECT * FROM {CATALOG}.{BASE_SCHEMA}.events_source
    ORDER BY event_id
    LIMIT 20
""")

# COMMAND ----------

# Exercise 6: two disjoint sources, one per concurrent query in the good case
spark.sql(f"""
    CREATE OR REPLACE TABLE {CATALOG}.{SCHEMA}.ex6_source_a
    USING DELTA
    AS SELECT * FROM {CATALOG}.{BASE_SCHEMA}.events_source
    WHERE event_id BETWEEN 'EV-001' AND 'EV-015'
""")
spark.sql(f"""
    CREATE OR REPLACE TABLE {CATALOG}.{SCHEMA}.ex6_source_b
    USING DELTA
    AS SELECT * FROM {CATALOG}.{BASE_SCHEMA}.events_source
    WHERE event_id BETWEEN 'EV-016' AND 'EV-030'
""")

# COMMAND ----------

# Pre-create the empty targets with explicit schemas; the streaming writes append into them.
EVENT_TARGET_SCHEMA = """
    event_id STRING,
    event_type STRING,
    user_id STRING,
    amount DOUBLE,
    event_ts TIMESTAMP
"""

for t in ["ex1_target", "ex3_target", "ex4_target", "ex5_target", "ex6_target", "ex6_shared_target"]:
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {CATALOG}.{SCHEMA}.{t} (
            {EVENT_TARGET_SCHEMA}
        ) USING DELTA
    """)

# Exercise 7 target holds the windowed aggregation: window_start, window_end, event_count
spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {CATALOG}.{SCHEMA}.ex7_target (
        window_start TIMESTAMP,
        window_end TIMESTAMP,
        event_count BIGINT
    ) USING DELTA
""")

# COMMAND ----------

print(f"Setup complete for Checkpointing & Recovery")
print(f"  Schema: {CATALOG}.{SCHEMA}")
print(f"  Base table: {CATALOG}.{BASE_SCHEMA}.events_source (35 rows; this topic uses EV-001..EV-030)")
print(f"  Checkpoints base: {CHECKPOINT_BASE}")
print(f"  Per-exercise sources: ex1_source ... ex5_source, ex7_source")
print(f"  Exercise 5 source: 20 rows (first 20 events). TODO appends 10 more.")
print(f"  Exercise 6 sources: ex6_source_a (EV-001..EV-015) and ex6_source_b (EV-016..EV-030).")
print(f"  All other exercise sources: 30 rows (EV-001..EV-030).")
