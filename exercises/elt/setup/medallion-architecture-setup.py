# Databricks notebook source
# COMMAND ----------
# MAGIC %md
# MAGIC ### Setup: Medallion Architecture
# MAGIC Creates per-topic schema and independent source tables for each of 8 exercises.
# MAGIC Run automatically via `%run` from the exercise notebook.

# COMMAND ----------

CATALOG = "db_code"
SCHEMA = "medallion_architecture"
BASE_SCHEMA = "elt"

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA}")

# COMMAND ----------

# ============================================================
# Exercise 1: Raw append to bronze preserving all fields
# ============================================================
# 10 raw order rows with messy data: 1 duplicate order_id (ORD-003 twice),
# 1 null customer_id (ORD-007), 1 zero amount (ORD-009), mixed statuses.
# User must append ALL rows to bronze - no dedup, no cleaning.

spark.sql(f"""
CREATE OR REPLACE TABLE {CATALOG}.{SCHEMA}.raw_orders_ex1 (
    order_id STRING,
    customer_id STRING,
    product_id STRING,
    amount DOUBLE,
    status STRING,
    order_date DATE,
    updated_at TIMESTAMP
) USING DELTA
""")

spark.sql(f"""
INSERT OVERWRITE {CATALOG}.{SCHEMA}.raw_orders_ex1 VALUES
    ('ORD-001', 'C-101', 'P-10', 49.99,  'completed',  '2026-03-01', '2026-03-01 08:00:00'),
    ('ORD-002', 'C-102', 'P-20', 129.50, 'pending',    '2026-03-01', '2026-03-01 08:05:00'),
    ('ORD-003', 'C-103', 'P-30', 89.00,  'shipped',    '2026-03-01', '2026-03-01 08:10:00'),
    ('ORD-003', 'C-103', 'P-30', 89.00,  'shipped',    '2026-03-01', '2026-03-01 09:10:00'),
    ('ORD-004', 'C-104', 'P-10', 49.99,  'completed',  '2026-03-02', '2026-03-02 08:00:00'),
    ('ORD-005', 'C-105', 'P-40', 210.00, 'cancelled',  '2026-03-02', '2026-03-02 08:05:00'),
    ('ORD-006', 'C-106', 'P-20', 129.50, 'completed',  '2026-03-02', '2026-03-02 08:10:00'),
    ('ORD-007', NULL,    'P-30', 89.00,  'pending',    '2026-03-03', '2026-03-03 08:00:00'),
    ('ORD-008', 'C-108', 'P-50', 15.00,  'completed',  '2026-03-03', '2026-03-03 08:05:00'),
    ('ORD-009', 'C-109', 'P-10', 0.00,   'completed',  '2026-03-03', '2026-03-03 08:10:00')
""")

MEDAL_EX1_RAW_COUNT = 10

# COMMAND ----------

# ============================================================
# Exercise 2: Bronze with ingestion metadata columns
# ============================================================
# 8 raw order rows. User appends to bronze adding _ingested_at and _source_file.
# No dedup, no cleaning - just add metadata columns.

spark.sql(f"""
CREATE OR REPLACE TABLE {CATALOG}.{SCHEMA}.raw_orders_ex2 (
    order_id STRING,
    customer_id STRING,
    product_id STRING,
    amount DOUBLE,
    status STRING,
    order_date DATE,
    updated_at TIMESTAMP
) USING DELTA
""")

spark.sql(f"""
INSERT OVERWRITE {CATALOG}.{SCHEMA}.raw_orders_ex2 VALUES
    ('ORD-201', 'C-201', 'P-10', 55.00,  'completed',  '2026-03-05', '2026-03-05 08:00:00'),
    ('ORD-202', 'C-202', 'P-20', 140.00, 'pending',    '2026-03-05', '2026-03-05 08:05:00'),
    ('ORD-203', 'C-203', 'P-30', 75.00,  'shipped',    '2026-03-05', '2026-03-05 08:10:00'),
    ('ORD-204', 'C-204', 'P-40', 220.00, 'completed',  '2026-03-06', '2026-03-06 08:00:00'),
    ('ORD-205', 'C-205', 'P-10', 55.00,  'cancelled',  '2026-03-06', '2026-03-06 08:05:00'),
    ('ORD-206', 'C-206', 'P-50', 18.50,  'completed',  '2026-03-06', '2026-03-06 08:10:00'),
    ('ORD-207', NULL,    'P-20', 140.00, 'pending',    '2026-03-07', '2026-03-07 08:00:00'),
    ('ORD-208', 'C-208', 'P-30', 0.00,   'completed',  '2026-03-07', '2026-03-07 08:05:00')
""")

MEDAL_EX2_RAW_COUNT = 8

# COMMAND ----------

# ============================================================
# Exercise 3: Bronze-to-silver dedup using ROW_NUMBER
# ============================================================
# 14 bronze rows with known duplicates: ORD-301 (2x), ORD-305 (2x), ORD-308 (3x).
# 2 rows have NULL customer_id (ORD-304, ORD-310) but are NOT filtered here.
# Dedup only - keep latest _ingested_at per order_id. Expected 10 distinct.

spark.sql(f"""
CREATE OR REPLACE TABLE {CATALOG}.{SCHEMA}.bronze_orders_ex3 (
    order_id STRING,
    customer_id STRING,
    product_id STRING,
    amount DOUBLE,
    status STRING,
    order_date DATE,
    _ingested_at TIMESTAMP,
    _source_file STRING
) USING DELTA
""")

spark.sql(f"""
INSERT OVERWRITE {CATALOG}.{SCHEMA}.bronze_orders_ex3 VALUES
    ('ORD-301', 'C-301', 'P-10', 60.00,  'completed', '2026-03-10', '2026-03-10 09:00:00', 'batch_01.json'),
    ('ORD-301', 'C-301', 'P-10', 60.00,  'completed', '2026-03-10', '2026-03-10 10:00:00', 'batch_02.json'),
    ('ORD-302', 'C-302', 'P-20', 135.00, 'pending',   '2026-03-10', '2026-03-10 09:00:00', 'batch_01.json'),
    ('ORD-303', 'C-303', 'P-30', 90.00,  'shipped',   '2026-03-10', '2026-03-10 09:00:00', 'batch_01.json'),
    ('ORD-304', NULL,    'P-40', 200.00, 'completed', '2026-03-10', '2026-03-10 09:00:00', 'batch_01.json'),
    ('ORD-305', 'C-305', 'P-10', 60.00,  'cancelled', '2026-03-11', '2026-03-11 09:00:00', 'batch_03.json'),
    ('ORD-305', 'C-305', 'P-10', 60.00,  'cancelled', '2026-03-11', '2026-03-11 10:00:00', 'batch_04.json'),
    ('ORD-306', 'C-306', 'P-50', 22.00,  'completed', '2026-03-11', '2026-03-11 09:00:00', 'batch_03.json'),
    ('ORD-307', 'C-307', 'P-20', 135.00, 'pending',   '2026-03-11', '2026-03-11 09:00:00', 'batch_03.json'),
    ('ORD-308', 'C-308', 'P-30', 90.00,  'shipped',   '2026-03-12', '2026-03-12 09:00:00', 'batch_05.json'),
    ('ORD-308', 'C-308', 'P-30', 90.00,  'shipped',   '2026-03-12', '2026-03-12 10:00:00', 'batch_06.json'),
    ('ORD-308', 'C-308', 'P-30', 90.00,  'shipped',   '2026-03-12', '2026-03-12 11:00:00', 'batch_07.json'),
    ('ORD-309', 'C-309', 'P-40', 200.00, 'completed', '2026-03-12', '2026-03-12 09:00:00', 'batch_05.json'),
    ('ORD-310', NULL,    'P-10', 60.00,  'pending',   '2026-03-12', '2026-03-12 09:00:00', 'batch_05.json')
""")

# 14 rows -> 10 distinct order_ids (ORD-301 2x, ORD-305 2x, ORD-308 3x = 4 dups removed)
MEDAL_EX3_EXPECTED_COUNT = 10

# COMMAND ----------

# ============================================================
# Exercise 4: Silver data cleaning - nulls, types, standardization
# ============================================================
# 12 bronze rows with quality issues:
# - 2 null order_ids (rows 4, 9) -> filter out
# - 1 null customer_id (row 7) -> filter out
# - status has whitespace and mixed case (' Completed ', 'PENDING', 'shipped')
# - amount stored as STRING (must cast to DOUBLE)
# After cleaning: 9 rows with standardized data.

spark.sql(f"""
CREATE OR REPLACE TABLE {CATALOG}.{SCHEMA}.bronze_orders_ex4 (
    order_id STRING,
    customer_id STRING,
    product_id STRING,
    amount STRING,
    status STRING,
    order_date STRING,
    _ingested_at TIMESTAMP
) USING DELTA
""")

spark.sql(f"""
INSERT OVERWRITE {CATALOG}.{SCHEMA}.bronze_orders_ex4 VALUES
    ('ORD-401', 'C-401', 'P-10', '65.00',  ' Completed ', '2026-03-15', '2026-03-15 09:00:00'),
    ('ORD-402', 'C-402', 'P-20', '142.50', 'PENDING',     '2026-03-15', '2026-03-15 09:00:00'),
    ('ORD-403', 'C-403', 'P-30', '88.00',  'shipped',     '2026-03-15', '2026-03-15 09:00:00'),
    (NULL,      'C-404', 'P-40', '210.00', 'Completed',   '2026-03-15', '2026-03-15 09:00:00'),
    ('ORD-405', 'C-405', 'P-10', '65.00',  ' cancelled ', '2026-03-16', '2026-03-16 09:00:00'),
    ('ORD-406', 'C-406', 'P-50', '19.99',  'COMPLETED',   '2026-03-16', '2026-03-16 09:00:00'),
    ('ORD-407', NULL,    'P-20', '142.50', 'pending',     '2026-03-16', '2026-03-16 09:00:00'),
    ('ORD-408', 'C-408', 'P-30', '0.00',   'SHIPPED',     '2026-03-16', '2026-03-16 09:00:00'),
    (NULL,      'C-409', 'P-40', '210.00', 'completed',   '2026-03-17', '2026-03-17 09:00:00'),
    ('ORD-410', 'C-410', 'P-10', '65.00',  '  Pending  ', '2026-03-17', '2026-03-17 09:00:00'),
    ('ORD-411', 'C-411', 'P-50', '33.75',  'cancelled',   '2026-03-17', '2026-03-17 09:00:00'),
    ('ORD-412', 'C-412', 'P-20', '142.50', ' SHIPPED',    '2026-03-17', '2026-03-17 09:00:00')
""")

# 12 rows - 2 null order_ids - 1 null customer_id = 9 clean rows
MEDAL_EX4_EXPECTED_COUNT = 9

# COMMAND ----------

# ============================================================
# Exercise 5: Silver-to-gold aggregation (daily/regional summaries)
# ============================================================
# 20 clean silver order rows across 3 dates and 3 regions.
# Aggregate to: order_count, total_revenue, avg_order_amount per (order_date, region).
# 8 distinct (date, region) combos with data.

spark.sql(f"""
CREATE OR REPLACE TABLE {CATALOG}.{SCHEMA}.silver_orders_ex5 (
    order_id STRING,
    customer_id STRING,
    product_id STRING,
    amount DOUBLE,
    status STRING,
    order_date DATE,
    region STRING
) USING DELTA
""")

spark.sql(f"""
INSERT OVERWRITE {CATALOG}.{SCHEMA}.silver_orders_ex5 VALUES
    ('ORD-501', 'C-501', 'P-10', 50.00,  'COMPLETED', '2026-03-20', 'US-East'),
    ('ORD-502', 'C-502', 'P-20', 150.00, 'COMPLETED', '2026-03-20', 'US-East'),
    ('ORD-503', 'C-503', 'P-30', 80.00,  'COMPLETED', '2026-03-20', 'US-East'),
    ('ORD-504', 'C-504', 'P-10', 50.00,  'SHIPPED',   '2026-03-20', 'US-West'),
    ('ORD-505', 'C-505', 'P-40', 200.00, 'COMPLETED', '2026-03-20', 'US-West'),
    ('ORD-506', 'C-506', 'P-50', 25.00,  'COMPLETED', '2026-03-20', 'EU-West'),
    ('ORD-507', 'C-507', 'P-20', 150.00, 'COMPLETED', '2026-03-20', 'EU-West'),
    ('ORD-508', 'C-508', 'P-30', 80.00,  'SHIPPED',   '2026-03-20', 'EU-West'),
    ('ORD-509', 'C-509', 'P-10', 50.00,  'COMPLETED', '2026-03-21', 'US-East'),
    ('ORD-510', 'C-510', 'P-40', 200.00, 'COMPLETED', '2026-03-21', 'US-East'),
    ('ORD-511', 'C-511', 'P-20', 150.00, 'COMPLETED', '2026-03-21', 'US-West'),
    ('ORD-512', 'C-512', 'P-50', 25.00,  'COMPLETED', '2026-03-21', 'US-West'),
    ('ORD-513', 'C-513', 'P-30', 80.00,  'SHIPPED',   '2026-03-21', 'US-West'),
    ('ORD-514', 'C-514', 'P-10', 50.00,  'COMPLETED', '2026-03-21', 'EU-West'),
    ('ORD-515', 'C-515', 'P-40', 200.00, 'COMPLETED', '2026-03-21', 'EU-West'),
    ('ORD-516', 'C-516', 'P-20', 150.00, 'COMPLETED', '2026-03-22', 'US-East'),
    ('ORD-517', 'C-517', 'P-30', 80.00,  'COMPLETED', '2026-03-22', 'US-East'),
    ('ORD-518', 'C-518', 'P-10', 50.00,  'COMPLETED', '2026-03-22', 'EU-West'),
    ('ORD-519', 'C-519', 'P-50', 25.00,  'COMPLETED', '2026-03-22', 'EU-West'),
    ('ORD-520', 'C-520', 'P-40', 200.00, 'COMPLETED', '2026-03-22', 'EU-West')
""")

# 8 combos (no US-West on 2026-03-22):
# 2026-03-20/US-East: 3 orders, revenue=280.00, avg=93.33
# 2026-03-20/US-West: 2 orders, revenue=250.00, avg=125.00
# 2026-03-20/EU-West: 3 orders, revenue=255.00, avg=85.00
# 2026-03-21/US-East: 2 orders, revenue=250.00, avg=125.00
# 2026-03-21/US-West: 3 orders, revenue=255.00, avg=85.00
# 2026-03-21/EU-West: 2 orders, revenue=250.00, avg=125.00
# 2026-03-22/US-East: 2 orders, revenue=230.00, avg=115.00
# 2026-03-22/EU-West: 3 orders, revenue=275.00, avg=91.67
MEDAL_EX5_EXPECTED_COUNT = 8

# COMMAND ----------

# ============================================================
# Exercise 6: Incremental bronze-to-silver with MERGE
# ============================================================
# Existing silver: 6 rows (ORD-601 through ORD-606).
# New bronze batch: 8 rows with _ingested_at:
#   3 updates: ORD-601, ORD-603, ORD-605 with newer updated_at
#   3 new: ORD-607, ORD-608, ORD-609
#   2 dupes within batch: ORD-607 appears 3 times (different _ingested_at)
# After dedup + MERGE: 9 rows.

spark.sql(f"""
CREATE OR REPLACE TABLE {CATALOG}.{SCHEMA}.silver_orders_ex6 (
    order_id STRING,
    customer_id STRING,
    amount DOUBLE,
    status STRING,
    updated_at TIMESTAMP,
    processed_at TIMESTAMP
) USING DELTA
""")

spark.sql(f"""
INSERT OVERWRITE {CATALOG}.{SCHEMA}.silver_orders_ex6 VALUES
    ('ORD-601', 'C-601', 75.00,  'pending',   '2026-03-25 08:00:00', '2026-03-25 09:00:00'),
    ('ORD-602', 'C-602', 160.00, 'completed', '2026-03-25 08:05:00', '2026-03-25 09:00:00'),
    ('ORD-603', 'C-603', 95.00,  'shipped',   '2026-03-25 08:10:00', '2026-03-25 09:00:00'),
    ('ORD-604', 'C-604', 45.00,  'pending',   '2026-03-25 08:15:00', '2026-03-25 09:00:00'),
    ('ORD-605', 'C-605', 210.00, 'completed', '2026-03-25 08:20:00', '2026-03-25 09:00:00'),
    ('ORD-606', 'C-606', 30.00,  'cancelled', '2026-03-25 08:25:00', '2026-03-25 09:00:00')
""")

spark.sql(f"""
CREATE OR REPLACE TABLE {CATALOG}.{SCHEMA}.bronze_batch_ex6 (
    order_id STRING,
    customer_id STRING,
    amount DOUBLE,
    status STRING,
    updated_at TIMESTAMP,
    _ingested_at TIMESTAMP
) USING DELTA
""")

spark.sql(f"""
INSERT OVERWRITE {CATALOG}.{SCHEMA}.bronze_batch_ex6 VALUES
    ('ORD-601', 'C-601', 75.00,  'completed', '2026-03-25 10:00:00', '2026-03-25 11:00:00'),
    ('ORD-603', 'C-603', 95.00,  'delivered', '2026-03-25 10:05:00', '2026-03-25 11:00:00'),
    ('ORD-605', 'C-605', 215.00, 'completed', '2026-03-25 10:10:00', '2026-03-25 11:00:00'),
    ('ORD-607', 'C-607', 120.00, 'pending',   '2026-03-25 10:15:00', '2026-03-25 11:00:00'),
    ('ORD-607', 'C-607', 120.00, 'pending',   '2026-03-25 10:15:00', '2026-03-25 12:00:00'),
    ('ORD-607', 'C-607', 120.00, 'pending',   '2026-03-25 10:15:00', '2026-03-25 13:00:00'),
    ('ORD-608', 'C-608', 55.00,  'pending',   '2026-03-25 10:20:00', '2026-03-25 11:00:00'),
    ('ORD-609', 'C-609', 180.00, 'shipped',   '2026-03-25 10:25:00', '2026-03-25 11:00:00')
""")

# 6 existing + 3 new inserts = 9 total (3 updates overwrite in place)
MEDAL_EX6_EXPECTED_COUNT = 9

# COMMAND ----------

# ============================================================
# Exercise 7: Gold incremental refresh with MERGE
# ============================================================
# Existing gold: 5 aggregate rows (date+region combos).
# New silver: 12 rows - some hit existing combos (update), some are new combos (insert).
# User must recalculate aggregates from new silver and MERGE into gold.

spark.sql(f"""
CREATE OR REPLACE TABLE {CATALOG}.{SCHEMA}.gold_summary_ex7 (
    order_date DATE,
    region STRING,
    order_count INT,
    total_revenue DOUBLE,
    avg_order_amount DOUBLE
) USING DELTA
""")

spark.sql(f"""
INSERT OVERWRITE {CATALOG}.{SCHEMA}.gold_summary_ex7 VALUES
    ('2026-03-20', 'US-East', 3, 280.00, 93.33),
    ('2026-03-20', 'US-West', 2, 250.00, 125.00),
    ('2026-03-20', 'EU-West', 3, 255.00, 85.00),
    ('2026-03-21', 'US-East', 2, 250.00, 125.00),
    ('2026-03-21', 'US-West', 3, 255.00, 85.00)
""")

spark.sql(f"""
CREATE OR REPLACE TABLE {CATALOG}.{SCHEMA}.silver_new_ex7 (
    order_id STRING,
    customer_id STRING,
    amount DOUBLE,
    status STRING,
    order_date DATE,
    region STRING
) USING DELTA
""")

spark.sql(f"""
INSERT OVERWRITE {CATALOG}.{SCHEMA}.silver_new_ex7 VALUES
    ('ORD-701', 'C-701', 100.00, 'COMPLETED', '2026-03-20', 'US-East'),
    ('ORD-702', 'C-702', 60.00,  'COMPLETED', '2026-03-20', 'US-East'),
    ('ORD-703', 'C-703', 200.00, 'COMPLETED', '2026-03-20', 'US-West'),
    ('ORD-704', 'C-704', 45.00,  'COMPLETED', '2026-03-21', 'US-East'),
    ('ORD-705', 'C-705', 130.00, 'COMPLETED', '2026-03-21', 'US-East'),
    ('ORD-706', 'C-706', 90.00,  'COMPLETED', '2026-03-21', 'US-East'),
    ('ORD-707', 'C-707', 75.00,  'COMPLETED', '2026-03-21', 'EU-West'),
    ('ORD-708', 'C-708', 150.00, 'COMPLETED', '2026-03-21', 'EU-West'),
    ('ORD-709', 'C-709', 50.00,  'COMPLETED', '2026-03-22', 'US-East'),
    ('ORD-710', 'C-710', 180.00, 'COMPLETED', '2026-03-22', 'US-East'),
    ('ORD-711', 'C-711', 95.00,  'COMPLETED', '2026-03-22', 'US-West'),
    ('ORD-712', 'C-712', 110.00, 'COMPLETED', '2026-03-22', 'US-West')
""")

# New silver aggregated:
#   2026-03-20/US-East: 2 orders, 160.00, avg=80.00  -> UPDATE existing (was 3, 280.00, 93.33)
#   2026-03-20/US-West: 1 order,  200.00, avg=200.00 -> UPDATE existing (was 2, 250.00, 125.00)
#   2026-03-21/US-East: 3 orders, 265.00, avg=88.33  -> UPDATE existing (was 2, 250.00, 125.00)
#   2026-03-21/EU-West: 2 orders, 225.00, avg=112.50 -> INSERT new combo
#   2026-03-22/US-East: 2 orders, 230.00, avg=115.00 -> INSERT new combo
#   2026-03-22/US-West: 2 orders, 205.00, avg=102.50 -> INSERT new combo
# After MERGE: 5 original - 3 updated + 3 new inserts = 8 rows total
# Unchanged: 2026-03-20/EU-West (no new silver for it), 2026-03-21/US-West (no new silver)
# Updated:   2026-03-20/US-East, 2026-03-20/US-West, 2026-03-21/US-East
# Inserted:  2026-03-21/EU-West, 2026-03-22/US-East, 2026-03-22/US-West
MEDAL_EX7_EXPECTED_COUNT = 8

# COMMAND ----------

# ============================================================
# Exercise 8: Data quality report across tiers
# ============================================================
# Bronze: 12 rows with issues (null order_ids, out-of-range dates, negative amounts)
# Silver: 10 rows with issues (orphan customer_ids not in lookup)
# Gold: 8 rows with issues (negative revenue, zero order counts)
# Lookup: 6 valid customers

spark.sql(f"""
CREATE OR REPLACE TABLE {CATALOG}.{SCHEMA}.qc_bronze_ex8 (
    order_id STRING,
    customer_id STRING,
    amount DOUBLE,
    order_date DATE,
    status STRING
) USING DELTA
""")

spark.sql(f"""
INSERT OVERWRITE {CATALOG}.{SCHEMA}.qc_bronze_ex8 VALUES
    ('ORD-801', 'C-801', 50.00,   '2026-03-20', 'completed'),
    ('ORD-802', 'C-802', 120.00,  '2026-03-20', 'completed'),
    (NULL,      'C-803', 80.00,   '2026-03-20', 'pending'),
    ('ORD-804', 'C-804', 200.00,  '2026-03-21', 'completed'),
    (NULL,      'C-805', 65.00,   '2026-03-21', 'shipped'),
    ('ORD-806', 'C-806', -15.00,  '2026-03-21', 'completed'),
    ('ORD-807', 'C-807', 90.00,   '2020-06-15', 'completed'),
    ('ORD-808', 'C-808', 45.00,   '2026-03-22', 'pending'),
    (NULL,      'C-809', 110.00,  '2026-03-22', 'completed'),
    ('ORD-810', 'C-810', 75.00,   '2030-12-01', 'shipped'),
    ('ORD-811', 'C-811', -25.00,  '2026-03-22', 'cancelled'),
    ('ORD-812', 'C-812', 140.00,  '2026-03-22', 'completed')
""")

# Bronze issues: 3 null order_ids, 2 out-of-range dates, 2 negative amounts
MEDAL_EX8_BRONZE_NULL_IDS = 3
MEDAL_EX8_BRONZE_BAD_DATES = 2
MEDAL_EX8_BRONZE_NEG_AMOUNTS = 2

spark.sql(f"""
CREATE OR REPLACE TABLE {CATALOG}.{SCHEMA}.qc_silver_ex8 (
    order_id STRING,
    customer_id STRING,
    amount DOUBLE,
    order_date DATE,
    region STRING
) USING DELTA
""")

spark.sql(f"""
INSERT OVERWRITE {CATALOG}.{SCHEMA}.qc_silver_ex8 VALUES
    ('ORD-901', 'C-801', 50.00,  '2026-03-20', 'US-East'),
    ('ORD-902', 'C-802', 120.00, '2026-03-20', 'US-West'),
    ('ORD-903', 'C-803', 80.00,  '2026-03-20', 'EU-West'),
    ('ORD-904', 'C-899', 200.00, '2026-03-21', 'US-East'),
    ('ORD-905', 'C-898', 65.00,  '2026-03-21', 'US-West'),
    ('ORD-906', 'C-804', 90.00,  '2026-03-21', 'EU-West'),
    ('ORD-907', 'C-897', 45.00,  '2026-03-22', 'US-East'),
    ('ORD-908', 'C-805', 110.00, '2026-03-22', 'US-West'),
    ('ORD-909', 'C-896', 75.00,  '2026-03-22', 'EU-West'),
    ('ORD-910', 'C-806', 140.00, '2026-03-22', 'US-East')
""")

# Silver issues: 4 orphan customer_ids (C-899, C-898, C-897, C-896 not in lookup)
MEDAL_EX8_SILVER_ORPHANS = 4

spark.sql(f"""
CREATE OR REPLACE TABLE {CATALOG}.{SCHEMA}.qc_gold_ex8 (
    order_date DATE,
    region STRING,
    order_count INT,
    total_revenue DOUBLE
) USING DELTA
""")

spark.sql(f"""
INSERT OVERWRITE {CATALOG}.{SCHEMA}.qc_gold_ex8 VALUES
    ('2026-03-20', 'US-East', 3,  280.00),
    ('2026-03-20', 'US-West', 2,  250.00),
    ('2026-03-20', 'EU-West', 0,  0.00),
    ('2026-03-21', 'US-East', 4,  310.00),
    ('2026-03-21', 'US-West', 1,  -50.00),
    ('2026-03-21', 'EU-West', 2,  180.00),
    ('2026-03-22', 'US-East', 3,  275.00),
    ('2026-03-22', 'EU-West', 0,  0.00)
""")

# Gold issues: 2 zero order_counts, 1 negative revenue
MEDAL_EX8_GOLD_ZERO_COUNTS = 2
MEDAL_EX8_GOLD_NEG_REVENUE = 1

spark.sql(f"""
CREATE OR REPLACE TABLE {CATALOG}.{SCHEMA}.qc_customers_ex8 (
    customer_id STRING,
    name STRING,
    region STRING
) USING DELTA
""")

spark.sql(f"""
INSERT OVERWRITE {CATALOG}.{SCHEMA}.qc_customers_ex8 VALUES
    ('C-801', 'Alice',   'US-East'),
    ('C-802', 'Bob',     'US-West'),
    ('C-803', 'Carol',   'EU-West'),
    ('C-804', 'Dave',    'US-East'),
    ('C-805', 'Eve',     'US-West'),
    ('C-806', 'Frank',   'EU-West')
""")

# COMMAND ----------

# Clean up any previous exercise output tables (idempotent)
for tbl in [
    "bronze_orders_ex1",
    "bronze_orders_ex2",
    "silver_orders_ex3",
    "silver_orders_ex4",
    "gold_daily_summary_ex5",
    "silver_orders_ex6_merged",
    "gold_summary_ex7_merged",
    "quality_report_ex8",
]:
    spark.sql(f"DROP TABLE IF EXISTS {CATALOG}.{SCHEMA}.{tbl}")

# COMMAND ----------

print(f"Setup complete. All exercise tables in {CATALOG}.{SCHEMA}.")
print(f"Ex1: {MEDAL_EX1_RAW_COUNT} raw rows -> bronze_orders_ex1")
print(f"Ex2: {MEDAL_EX2_RAW_COUNT} raw rows -> bronze_orders_ex2 (add metadata)")
print(f"Ex3: 14 bronze rows -> {MEDAL_EX3_EXPECTED_COUNT} after dedup -> silver_orders_ex3")
print(f"Ex4: 12 dirty rows -> {MEDAL_EX4_EXPECTED_COUNT} after cleaning -> silver_orders_ex4")
print(f"Ex5: 20 silver rows -> {MEDAL_EX5_EXPECTED_COUNT} gold aggregates -> gold_daily_summary_ex5")
print(f"Ex6: 6 silver + 8 bronze batch -> {MEDAL_EX6_EXPECTED_COUNT} after MERGE -> silver_orders_ex6_merged")
print(f"Ex7: 5 gold + 12 new silver -> {MEDAL_EX7_EXPECTED_COUNT} after MERGE -> gold_summary_ex7_merged")
print(f"Ex8: Quality checks across 3 tiers -> quality_report_ex8")
