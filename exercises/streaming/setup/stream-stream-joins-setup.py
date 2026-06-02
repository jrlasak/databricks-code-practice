# Databricks notebook source
# COMMAND ----------
# MAGIC %md
# MAGIC ### Setup: Stream-Stream Joins
# MAGIC Creates two streaming source Delta tables (`orders_stream`, `shipments_stream`) and
# MAGIC per-exercise target tables in the `stream_stream_joins` schema. Timestamps are
# MAGIC carefully designed so each exercise has predictable output:
# MAGIC
# MAGIC - **ORD-001..ORD-020** -> matched by a shipment WITHIN 5 minutes (20 in-window pairs)
# MAGIC - **ORD-021..ORD-025** -> matched by a shipment OUTSIDE 5 minutes (5 out-of-window pairs)
# MAGIC - **ORD-026..ORD-030** -> NO matching shipment (5 unmatched left rows)
# MAGIC - **SHP-026..SHP-030** -> reference unknown orders ORD-901..ORD-905 (5 orphan shipments)
# MAGIC
# MAGIC Run automatically via `%run` from the exercise notebook.

# COMMAND ----------

CATALOG = "db_code"
SCHEMA = "stream_stream_joins"
BASE_SCHEMA = "streaming"

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{BASE_SCHEMA}")

# Volume for checkpoint locations (one per exercise)
spark.sql(f"CREATE VOLUME IF NOT EXISTS {CATALOG}.{SCHEMA}.checkpoints")

CHECKPOINT_BASE = f"/Volumes/{CATALOG}/{SCHEMA}/checkpoints"

# COMMAND ----------

# -- Clean previous runs --
# Drop per-exercise target tables and the two streaming source tables, then wipe
# checkpoints so this setup is fully idempotent.
for i in range(1, 8):
    spark.sql(f"DROP TABLE IF EXISTS {CATALOG}.{SCHEMA}.ex{i}_target")

spark.sql(f"DROP TABLE IF EXISTS {CATALOG}.{SCHEMA}.orders_stream")
spark.sql(f"DROP TABLE IF EXISTS {CATALOG}.{SCHEMA}.shipments_stream")

dbutils.fs.rm(CHECKPOINT_BASE, recurse=True)
spark.sql(f"CREATE VOLUME IF NOT EXISTS {CATALOG}.{SCHEMA}.checkpoints")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Streaming source tables
# MAGIC
# MAGIC Two Delta tables that the exercises will read as streams. We use Delta tables (not Kafka)
# MAGIC because Kafka is not available on Free Edition. Reading a Delta table with
# MAGIC `spark.readStream.table(...)` behaves the same as Kafka for join semantics: each row is a
# MAGIC streaming event with a logical event-time column.

# COMMAND ----------

# Create orders_stream: 32 rows (30 normal + 2 late watermark-advancement)
# Base time 2026-04-01 10:00:00; one order per minute.
spark.sql(f"""
    CREATE OR REPLACE TABLE {CATALOG}.{SCHEMA}.orders_stream (
        order_id STRING,
        customer_id STRING,
        amount DOUBLE,
        order_ts TIMESTAMP
    ) USING DELTA
""")

spark.sql(f"""
    INSERT INTO {CATALOG}.{SCHEMA}.orders_stream VALUES
        ('ORD-001', 'CUST-001',  50.00, TIMESTAMP '2026-04-01 10:00:00'),
        ('ORD-002', 'CUST-002', 120.50, TIMESTAMP '2026-04-01 10:01:00'),
        ('ORD-003', 'CUST-003',  75.25, TIMESTAMP '2026-04-01 10:02:00'),
        ('ORD-004', 'CUST-001', 200.00, TIMESTAMP '2026-04-01 10:03:00'),
        ('ORD-005', 'CUST-004',  45.00, TIMESTAMP '2026-04-01 10:04:00'),
        ('ORD-006', 'CUST-002', 310.75, TIMESTAMP '2026-04-01 10:05:00'),
        ('ORD-007', 'CUST-005',  89.99, TIMESTAMP '2026-04-01 10:06:00'),
        ('ORD-008', 'CUST-003', 125.00, TIMESTAMP '2026-04-01 10:07:00'),
        ('ORD-009', 'CUST-001',  60.00, TIMESTAMP '2026-04-01 10:08:00'),
        ('ORD-010', 'CUST-004', 175.50, TIMESTAMP '2026-04-01 10:09:00'),
        ('ORD-011', 'CUST-002',  30.00, TIMESTAMP '2026-04-01 10:10:00'),
        ('ORD-012', 'CUST-005',  88.50, TIMESTAMP '2026-04-01 10:11:00'),
        ('ORD-013', 'CUST-003', 410.00, TIMESTAMP '2026-04-01 10:12:00'),
        ('ORD-014', 'CUST-001',  15.50, TIMESTAMP '2026-04-01 10:13:00'),
        ('ORD-015', 'CUST-004', 270.00, TIMESTAMP '2026-04-01 10:14:00'),
        ('ORD-016', 'CUST-002',  55.00, TIMESTAMP '2026-04-01 10:15:00'),
        ('ORD-017', 'CUST-005', 145.75, TIMESTAMP '2026-04-01 10:16:00'),
        ('ORD-018', 'CUST-003',  99.50, TIMESTAMP '2026-04-01 10:17:00'),
        ('ORD-019', 'CUST-001', 220.00, TIMESTAMP '2026-04-01 10:18:00'),
        ('ORD-020', 'CUST-004',  35.00, TIMESTAMP '2026-04-01 10:19:00'),
        -- Orders 21-25: shipments arrive OUTSIDE the 5-minute join window
        ('ORD-021', 'CUST-002',  62.00, TIMESTAMP '2026-04-01 10:20:00'),
        ('ORD-022', 'CUST-005',  95.00, TIMESTAMP '2026-04-01 10:21:00'),
        ('ORD-023', 'CUST-003', 180.00, TIMESTAMP '2026-04-01 10:22:00'),
        ('ORD-024', 'CUST-001',  47.50, TIMESTAMP '2026-04-01 10:23:00'),
        ('ORD-025', 'CUST-004', 320.00, TIMESTAMP '2026-04-01 10:24:00'),
        -- Orders 26-30: NO matching shipment at all (unmatched-left for outer joins)
        ('ORD-026', 'CUST-002',  77.00, TIMESTAMP '2026-04-01 10:25:00'),
        ('ORD-027', 'CUST-005', 110.00, TIMESTAMP '2026-04-01 10:26:00'),
        ('ORD-028', 'CUST-003',  25.00, TIMESTAMP '2026-04-01 10:27:00'),
        ('ORD-029', 'CUST-001', 158.00, TIMESTAMP '2026-04-01 10:28:00'),
        ('ORD-030', 'CUST-004',  42.00, TIMESTAMP '2026-04-01 10:29:00'),
        -- Late "watermark advancement" orders: these push the orders watermark well past
        -- the windows of orders 21-30 so that outer joins can emit unmatched NULL rows
        -- under availableNow. They have no shipments, so they are themselves unmatched-left.
        ('ORD-200', 'CUST-001',  10.00, TIMESTAMP '2026-04-01 11:00:00'),
        ('ORD-201', 'CUST-002',  20.00, TIMESTAMP '2026-04-01 11:01:00')
""")

# COMMAND ----------

# Create shipments_stream: 32 rows (30 normal + 2 late watermark-advancement)
# - SHP-001..SHP-020 ship within 5 min of their matching order  (in-window matches)
# - SHP-021..SHP-025 ship 10 min after their order              (out-of-window matches)
# - SHP-026..SHP-030 reference ORD-901..ORD-905 (orphan shipments)
spark.sql(f"""
    CREATE OR REPLACE TABLE {CATALOG}.{SCHEMA}.shipments_stream (
        shipment_id STRING,
        order_id STRING,
        carrier STRING,
        shipment_ts TIMESTAMP
    ) USING DELTA
""")

spark.sql(f"""
    INSERT INTO {CATALOG}.{SCHEMA}.shipments_stream VALUES
        -- In-window matches: shipment_ts is order_ts + 2 minutes (within 5-min bound)
        ('SHP-001', 'ORD-001', 'UPS',    TIMESTAMP '2026-04-01 10:02:00'),
        ('SHP-002', 'ORD-002', 'FedEx',  TIMESTAMP '2026-04-01 10:03:00'),
        ('SHP-003', 'ORD-003', 'USPS',   TIMESTAMP '2026-04-01 10:04:00'),
        ('SHP-004', 'ORD-004', 'UPS',    TIMESTAMP '2026-04-01 10:05:00'),
        ('SHP-005', 'ORD-005', 'FedEx',  TIMESTAMP '2026-04-01 10:06:00'),
        ('SHP-006', 'ORD-006', 'USPS',   TIMESTAMP '2026-04-01 10:07:00'),
        ('SHP-007', 'ORD-007', 'UPS',    TIMESTAMP '2026-04-01 10:08:00'),
        ('SHP-008', 'ORD-008', 'FedEx',  TIMESTAMP '2026-04-01 10:09:00'),
        ('SHP-009', 'ORD-009', 'USPS',   TIMESTAMP '2026-04-01 10:10:00'),
        ('SHP-010', 'ORD-010', 'UPS',    TIMESTAMP '2026-04-01 10:11:00'),
        ('SHP-011', 'ORD-011', 'FedEx',  TIMESTAMP '2026-04-01 10:12:00'),
        ('SHP-012', 'ORD-012', 'USPS',   TIMESTAMP '2026-04-01 10:13:00'),
        ('SHP-013', 'ORD-013', 'UPS',    TIMESTAMP '2026-04-01 10:14:00'),
        ('SHP-014', 'ORD-014', 'FedEx',  TIMESTAMP '2026-04-01 10:15:00'),
        ('SHP-015', 'ORD-015', 'USPS',   TIMESTAMP '2026-04-01 10:16:00'),
        ('SHP-016', 'ORD-016', 'UPS',    TIMESTAMP '2026-04-01 10:17:00'),
        ('SHP-017', 'ORD-017', 'FedEx',  TIMESTAMP '2026-04-01 10:18:00'),
        ('SHP-018', 'ORD-018', 'USPS',   TIMESTAMP '2026-04-01 10:19:00'),
        ('SHP-019', 'ORD-019', 'UPS',    TIMESTAMP '2026-04-01 10:20:00'),
        ('SHP-020', 'ORD-020', 'FedEx',  TIMESTAMP '2026-04-01 10:21:00'),
        -- Out-of-window matches: shipment_ts is order_ts + 10 minutes (>5 min bound)
        ('SHP-021', 'ORD-021', 'USPS',   TIMESTAMP '2026-04-01 10:30:00'),
        ('SHP-022', 'ORD-022', 'UPS',    TIMESTAMP '2026-04-01 10:31:00'),
        ('SHP-023', 'ORD-023', 'FedEx',  TIMESTAMP '2026-04-01 10:32:00'),
        ('SHP-024', 'ORD-024', 'USPS',   TIMESTAMP '2026-04-01 10:33:00'),
        ('SHP-025', 'ORD-025', 'UPS',    TIMESTAMP '2026-04-01 10:34:00'),
        -- Orphan shipments: order_id does not exist in orders_stream (unmatched-right for outer joins)
        ('SHP-026', 'ORD-901', 'FedEx',  TIMESTAMP '2026-04-01 10:35:00'),
        ('SHP-027', 'ORD-902', 'USPS',   TIMESTAMP '2026-04-01 10:36:00'),
        ('SHP-028', 'ORD-903', 'UPS',    TIMESTAMP '2026-04-01 10:37:00'),
        ('SHP-029', 'ORD-904', 'FedEx',  TIMESTAMP '2026-04-01 10:38:00'),
        ('SHP-030', 'ORD-905', 'USPS',   TIMESTAMP '2026-04-01 10:39:00'),
        -- Late "watermark advancement" shipments: push the shipments watermark well past
        -- 11:01 so that left-outer joins can emit unmatched NULL rows for orders 21-30.
        -- They reference unknown orders, so they are themselves unmatched-right.
        ('SHP-200', 'ORD-906', 'UPS',    TIMESTAMP '2026-04-01 12:00:00'),
        ('SHP-201', 'ORD-907', 'FedEx',  TIMESTAMP '2026-04-01 12:01:00')
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Per-exercise checkpoint subdirectories
# MAGIC Each exercise writes its checkpoint to `/Volumes/.../checkpoints/exN/`.
# MAGIC Volumes are created lazily on first write, so we don't pre-create them.

# COMMAND ----------

print("Setup complete for Stream-Stream Joins")
print(f"  Schema: {CATALOG}.{SCHEMA}")
print(f"  Sources: {CATALOG}.{SCHEMA}.orders_stream (32 rows), {CATALOG}.{SCHEMA}.shipments_stream (32 rows)")
print(f"  Checkpoints: {CHECKPOINT_BASE}")
print( "  Pair design:")
print( "    ORD-001..ORD-020 -> matching shipment within 5 minutes  (20 in-window pairs)")
print( "    ORD-021..ORD-025 -> matching shipment 10 minutes later  (5 out-of-window pairs)")
print( "    ORD-026..ORD-030 -> no matching shipment                (5 unmatched orders)")
print( "    ORD-200..ORD-201 -> late orders, no shipment            (2 watermark-advancement rows)")
print( "    SHP-026..SHP-030 -> orphan shipments (ORD-901..ORD-905) (5 unmatched shipments)")
print( "    SHP-200..SHP-201 -> late orphan shipments               (2 watermark-advancement rows)")
