# Databricks notebook source
# data_generator.py
# Synthetic data generator for Fintech Transaction Monitoring lab

import random
import time
from datetime import datetime, timedelta
from pyspark.sql import functions as F
from pyspark.sql.types import *

# MAGIC %run ./variables

# COMMAND ----------

# 1. Logic for generating static dimension data
def bootstrap():
    print("🚀 Starting bootstrap data generation...")
    # 1.1 Customers (10,000 rows)
    countries = ["US", "DE", "FR", "UK", "NL", "IT", "ES", "CA"]
    risk_tiers = ["Low", "Medium", "High"]

    countries_arr = ",".join([f"'{c}'" for c in countries])
    risk_tiers_arr = ",".join([f"'{r}'" for r in risk_tiers])

    customers_df = spark.range(10000).select(
        F.col("id").cast("string").alias("customer_id"),
        F.expr("concat('Customer_', id)").alias("name"),
        F.expr("concat('customer_', id, '@example.com')").alias("email"),
        F.expr(f"element_at(array({countries_arr}), (abs(hash(id)) % {len(countries)}) + 1)").alias("country"),
        F.expr(f"element_at(array({risk_tiers_arr}), (abs(hash(id)) % {len(risk_tiers)}) + 1)").alias("risk_tier"),
        F.expr("date_sub(current_date(), cast(abs(hash(id)) % 365 as int))").alias("created_at")
    )

    customers_df.write.mode("overwrite").option("header", "true").csv(CUSTOMERS_RAW_PATH)
    print(f"✅ Generated {customers_df.count()} customers at {CUSTOMERS_RAW_PATH}")

    # 1.2 Merchants (5,000 rows)
    mcc_codes = ["5411", "5812", "5912", "4814", "7011", "4121"]
    mcc_codes_arr = ",".join([f"'{m}'" for m in mcc_codes])

    merchants_df = spark.range(5000).select(
        F.col("id").cast("string").alias("merchant_id"),
        F.expr("concat('Merchant_', id)").alias("name"),
        F.expr(f"element_at(array({mcc_codes_arr}), (abs(hash(id)) % {len(mcc_codes)}) + 1)").alias("category_code"),
        F.expr(f"element_at(array({countries_arr}), (abs(hash(id)) % {len(countries)}) + 1)").alias("country"),
        F.expr("cast(abs(hash(id * 7)) % 100 as int)").alias("risk_score"),
        F.expr("date_sub(current_date(), cast(abs(hash(id)) % 730 as int))").alias("onboarded_at")
    )

    merchants_df.write.mode("overwrite").json(MERCHANTS_RAW_PATH)
    print(f"✅ Generated {merchants_df.count()} merchants at {MERCHANTS_RAW_PATH}")
    
    # Generate initial transactions
    current_time = datetime.now().replace(microsecond=0)
    for i in range(NUM_BATCHES):
        batch_time = (current_time - timedelta(minutes=(NUM_BATCHES - i) * 5)).strftime("%Y-%m-%d %H:%M:%S")
        generate_batch(n_files=1, records_per_file=BATCH_SIZE, start_timestamp=batch_time)

# 2. Logic for generating streaming transaction batches
def generate_batch(n_files=1, records_per_file=100, start_timestamp=None):
    if start_timestamp is None:
        start_timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        
    channels = ["online", "POS", "mobile"]
    currencies = ["USD", "usd", "EUR", "eur", "GBP", "gbp"]
    countries = ["US", "DE", "FR", "UK", "NL", "IT", "ES", "CA"]

    channels_arr = ",".join([f"'{c}'" for c in channels])
    currencies_arr = ",".join([f"'{c}'" for c in currencies])
    countries_arr = ",".join([f"'{c}'" for c in countries])
    
    for i in range(n_files):
        batch_id = int(time.time() * 1000) + i
        txns = spark.range(records_per_file).select(
            F.expr(f"concat('TXN_', hex(hash(id + {batch_id})))").alias("txn_id"),
            F.expr(f"cast(abs(hash(id + {batch_id})) % 10000 as string)").alias("customer_id"),
            F.expr(f"case when rand() < {NULL_MERCHANT_RATE} then null else cast(abs(hash(id + {batch_id} * 5555)) % 5000 as string) end").alias("merchant_id"),
            F.expr("round(rand() * 1000, 2)").alias("amount"),
            F.expr(f"element_at(array({currencies_arr}), (abs(hash(id)) % {len(currencies)}) + 1)").alias("currency"),
            F.expr(f"element_at(array({channels_arr}), (abs(hash(id)) % {len(channels)}) + 1)").alias("channel"),
            F.expr(f"element_at(array({countries_arr}), (abs(hash(id * 13)) % {len(countries)}) + 1)").alias("ip_country"),
            F.expr(f"from_unixtime(unix_timestamp('{start_timestamp}') + id + {i} * {records_per_file})").cast("timestamp").alias("timestamp")
        )
        
        # Add Duplicate Gateway Retries (2% of volume)
        dupes = txns.limit(int(records_per_file * DUPLICATE_RATE)).withColumn("timestamp", F.col("timestamp") + F.expr("interval 1 minute"))
        txns_with_dupes = txns.unionAll(dupes)
        
        # Add Amount Anomalies (voids/refunds)
        txns_final = txns_with_dupes.withColumn("amount", 
            F.expr("case when rand() < 0.005 then 0.00 when rand() < 0.005 then amount * -1 else amount end")
        )
        
        # Write batch to JSON
        txns_final.coalesce(1).write.mode("append").json(TRANSACTIONS_RAW_PATH)
        
        # Inject malformed records (for _rescued_data testing)
        malformed_records = [
            '{"txn_id": "MALF_' + str(batch_id) + '_1", "amount": "one hundred", "timestamp": "invalid_date"}',
            '{"txn_id": "MALF_' + str(batch_id) + '_2", "customer_id": 12345, "broken_json: true',
            '{"txn_id": "MALF_' + str(batch_id) + '_3", "currency": "USD", "amount": 50.0, "unexpected_nested": {"foo": "bar"}}'
        ]
        malformed_df = spark.createDataFrame([(r,) for r in malformed_records], ["value"])
        malformed_df.coalesce(1).write.mode("append").text(TRANSACTIONS_RAW_PATH)

        print(f"✅ Generated transaction batch with {txns_final.count()} valid rows and {len(malformed_records)} malformed records")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Conditional Execution
# MAGIC If this notebook is run with `action=bootstrap`, it will populate the initial tables.

# COMMAND ----------

dbutils.widgets.text("action", "none")
if dbutils.widgets.get("action") == "bootstrap":
    bootstrap()
