# Databricks notebook source
# data_generator.py
# Synthetic data generator for Fintech Transaction Monitoring lab
# This notebook only DEFINES functions. 00_Setup_Environment calls bootstrap();
# the lab notebooks call generate_batch() to simulate new payment files.

# COMMAND ----------

# MAGIC %run ./variables

# COMMAND ----------

import uuid
from datetime import datetime, timedelta
from pyspark.sql import functions as F
from pyspark.sql.types import *

# Customer ids (every one of them exists in the customer dimension of 110,000 customers, ids 0-109999):
#   10000-109999  background traffic (about 0.5 payments per customer over the 3 hours of setup data)
#   9990-9994     the planted fraud ring (velocity burst, caught by every rule)
#   9995          the planted straddle customer (caught only by the 1-hour sliding window)
#   9998, 9999    the malformed records (type mismatch, unexpected field); they stay in Bronze
NUM_CUSTOMERS = 110000
NUM_MERCHANTS = 5000
BACKGROUND_CUSTOMER_OFFSET = 10000
BACKGROUND_CUSTOMER_POOL = 100000
PLANTED_CUSTOMER_IDS = ["9990", "9991", "9992", "9993", "9994"]
PLANTED_TXNS_PER_CUSTOMER = 7
STRADDLE_CUSTOMER_ID = "9995"
STRADDLE_MINUTES = [-4, -3, -2, 1, 2, 3]  # around a full hour: 3 payments in each aligned 10-minute window
STRADDLE_AMOUNT = 900.0
MALFORMED_TYPE_CUSTOMER_ID = "9998"
MALFORMED_FIELD_CUSTOMER_ID = "9999"
PLANTED_IP_COUNTRY = "BR"  # not in the customer country list, so every planted row is a geo mismatch

# Merchant updates file (landed next to the dimension export): 3 re-scored merchants + 10 new ones.
# The new merchants 5000-5009 also receive NEW_MERCHANT_RATE of background payments before they
# are in the dimension snapshot (late-arriving dimension rows).
NEW_MERCHANT_IDS = list(range(NUM_MERCHANTS, NUM_MERCHANTS + 10))
UPDATED_MERCHANT_COUNT = 3
UPDATED_RISK_SCORE = 95

GEO_MISMATCH_RATE = 0.05  # background payments whose IP country differs from the customer's home country
COUNTRIES = ["US", "DE", "FR", "UK", "NL", "IT", "ES", "CA"]
RISK_TIERS = ["Low", "Medium", "High"]
MCC_CODES = ["5411", "5812", "5912", "4814", "7011", "4121"]


def _pick(values, hash_expr):
    # SQL expression: one element of `values`, chosen by a hash expression salted per column
    values_arr = ",".join([f"'{v}'" for v in values])
    return f"element_at(array({values_arr}), cast(pmod({hash_expr}, {len(values)}) + 1 as int))"


def _merchant_columns():
    # Each column uses its own salted hash, so columns are independent
    return [
        F.col("id").cast("string").alias("merchant_id"),
        F.expr("concat('Merchant_', id)").alias("name"),
        F.expr(_pick(MCC_CODES, "hash(id, 'category_code')")).alias("category_code"),
        F.expr(_pick(COUNTRIES, "hash(id, 'merchant_country')")).alias("country"),
        F.expr("cast(pmod(hash(id, 'risk_score'), 100) as int)").alias("risk_score"),
        F.expr("date_sub(current_date(), cast(pmod(hash(id, 'onboarded_at'), 730) as int))").alias("onboarded_at"),
    ]

# COMMAND ----------

# 1. Logic for generating static dimension data
def bootstrap():
    print("🚀 Starting bootstrap data generation...")
    # 1.1 Customers (110,000 rows). The home country expression is reused by generate_batch() for ip_country.
    customers_df = spark.range(NUM_CUSTOMERS).select(
        F.col("id").cast("string").alias("customer_id"),
        F.expr("concat('Customer_', id)").alias("name"),
        F.expr("concat('customer_', id, '@example.com')").alias("email"),
        F.expr(_pick(COUNTRIES, "hash(id, 'customer_country')")).alias("country"),
        F.expr(_pick(RISK_TIERS, "hash(id, 'risk_tier')")).alias("risk_tier"),
        F.expr("date_sub(current_date(), cast(pmod(hash(id, 'created_at'), 365) as int))").alias("created_at")
    )

    customers_df.write.mode("overwrite").option("header", "true").csv(CUSTOMERS_RAW_PATH)
    print(f"✅ Generated {customers_df.count()} customers at {CUSTOMERS_RAW_PATH}")

    # 1.2 Merchants (5,000 rows: the dimension as of the last full export)
    merchants_df = spark.range(NUM_MERCHANTS).select(*_merchant_columns())

    merchants_df.write.mode("overwrite").json(MERCHANTS_RAW_PATH)
    print(f"✅ Generated {merchants_df.count()} merchants at {MERCHANTS_RAW_PATH}")

    # 1.3 Planted merchants, chosen in Spark from the dimension with a deterministic ordering
    risky_merchants, straddle_merchant, updated_merchants = choose_planted_merchants(merchants_df)

    # 1.4 Merchant updates file: 3 re-scored merchants + 10 newly onboarded ones (notebook 02 MERGEs it)
    write_merchant_updates(merchants_df, updated_merchants)

    # 1.5 Background window: batch i starts BATCH_SIZE seconds plus (NUM_BATCHES - i) * 5 minutes before setup,
    # and spans BATCH_SIZE seconds, so background payments run from setup - 3h12m to setup - 5 min (none in the future)
    current_time = datetime.now().replace(microsecond=0)
    batch_starts = [current_time - timedelta(seconds=BATCH_SIZE) - timedelta(minutes=(NUM_BATCHES - i) * 5)
                    for i in range(NUM_BATCHES)]

    # 1.6 Planted ring and straddle customer, placed BEFORE the earliest background event (never late for any watermark)
    plant_fraud_cohort(customers_df, risky_merchants, straddle_merchant,
                       batch_starts[0].strftime("%Y-%m-%d %H:%M:%S"))

    # 1.7 Background transactions (5 batches, fixed tags and seeds so bootstrap is repeatable)
    for i, batch_start in enumerate(batch_starts):
        generate_batch(n_files=1, records_per_file=BATCH_SIZE, start_timestamp=batch_start.strftime("%Y-%m-%d %H:%M:%S"),
                       seed=1000 + i * 10, batch_tag=f"BOOT{i}")


# 2. Merchants for the suspicious customers, picked from the dimension itself
def choose_planted_merchants(merchants_df):
    by_id = F.col("merchant_id").cast("int")
    # The 5 lowest merchant_ids with risk_score > 80: one per ring customer
    risky_merchants = [r.merchant_id for r in (merchants_df
        .filter("risk_score > 80").orderBy(by_id).limit(len(PLANTED_CUSTOMER_IDS)).collect())]
    # The lowest merchant_ids with risk_score <= 80: the first takes the straddle payments, the next 3 are re-scored
    low_risk_merchants = [r.merchant_id for r in (merchants_df
        .filter("risk_score <= 80").orderBy(by_id).limit(1 + UPDATED_MERCHANT_COUNT).collect())]
    if len(risky_merchants) < len(PLANTED_CUSTOMER_IDS) or len(low_risk_merchants) < 1 + UPDATED_MERCHANT_COUNT:
        raise RuntimeError("Not enough merchants on either side of risk_score 80 to plant the cohort")
    return risky_merchants, low_risk_merchants[0], low_risk_merchants[1:]


# 3. Merchant updates: what the merchant team sends after the last full export
def write_merchant_updates(merchants_df, updated_merchants):
    rescored_df = (merchants_df
        .filter(F.col("merchant_id").isin(updated_merchants))
        .withColumn("risk_score", F.lit(UPDATED_RISK_SCORE)))
    onboarded_df = (spark.range(NEW_MERCHANT_IDS[0], NEW_MERCHANT_IDS[-1] + 1)
        .select(*_merchant_columns())
        .withColumn("onboarded_at", F.current_date()))
    updates_df = rescored_df.unionByName(onboarded_df)

    updates_df.coalesce(1).write.mode("overwrite").json(MERCHANT_UPDATES_RAW_PATH)
    print(f"✅ Merchant updates at {MERCHANT_UPDATES_RAW_PATH}: merchants {', '.join(updated_merchants)} "
          f"re-scored to {UPDATED_RISK_SCORE}, {len(NEW_MERCHANT_IDS)} new merchants {NEW_MERCHANT_IDS[0]}-{NEW_MERCHANT_IDS[-1]}")


# 4. Suspicious-customer payments: fixed, not random, so the Notebook 03 and 04 results are the same on every run
def plant_fraud_cohort(customers_df, risky_merchants, straddle_merchant, earliest_background):
    planted_ids = PLANTED_CUSTOMER_IDS + [STRADDLE_CUSTOMER_ID]
    planted_customers = customers_df.filter(F.col("customer_id").isin(planted_ids)).count()
    if planted_customers != len(planted_ids):
        raise RuntimeError(f"Expected {len(planted_ids)} planted customers in the dimension, found {planted_customers}")
    if PLANTED_IP_COUNTRY in COUNTRIES:
        raise RuntimeError(f"{PLANTED_IP_COUNTRY} is a customer country: the planted rows would not be geo mismatches")

    # Ring, customer k, payment n: amount 850 + 20n + 2k (850..978, each > 800), online (card-not-present),
    # IP in BR, at a merchant with risk_score > 80. Score under notebook 03: 30 + 20 + 25 + 15 = 90 > 70.
    # Offsets k*100 + n*10 seconds (0..460 s) from RING_START, a 10-minute boundary 60 minutes before the
    # 10-minute boundary at or below the earliest background event: all 35 payments fall in ONE 10-minute
    # tumbling window (7 per customer > 5), and each customer's 7 payments ($6,370 + $14 x k, so $6,370-$6,426) sit in exactly
    # 12 of the 1-hour sliding windows (slide 5 minutes).
    rows = []
    for k, customer_id in enumerate(PLANTED_CUSTOMER_IDS):
        for n in range(PLANTED_TXNS_PER_CUSTOMER):
            rows.append((f"TXN_PLANTED_{customer_id}_{n}", customer_id, risky_merchants[k],
                         float(850 + 20 * n + 2 * k), "USD", "online", PLANTED_IP_COUNTRY, "ring", k * 100 + n * 10))

    # Straddle customer: six $900 online payments from BR at a merchant with risk_score <= 80, at
    # STRADDLE_HOUR -4, -3, -2, +1, +2, +3 minutes. STRADDLE_HOUR is the full hour at least 60 minutes before
    # RING_START. Each aligned 10-minute window holds 3 (no velocity spike), each payment scores
    # 30 + 20 + 0 + 15 = 65 (no alert), each 1-hour tumbling window holds $2,700, and exactly 11 sliding
    # windows (starts STRADDLE_HOUR - 55 min .. - 5 min) hold all six: $5,400 > $5,000.
    for m, minute in enumerate(STRADDLE_MINUTES):
        rows.append((f"TXN_PLANTED_{STRADDLE_CUSTOMER_ID}_{m}", STRADDLE_CUSTOMER_ID, straddle_merchant,
                     STRADDLE_AMOUNT, "USD", "online", PLANTED_IP_COUNTRY, "straddle", minute * 60))

    ring_start = f"(floor(unix_timestamp('{earliest_background}') / 600) * 600 - 3600)"
    straddle_hour = f"(floor({ring_start} / 3600) * 3600 - 3600)"
    planted_df = (spark.createDataFrame(rows,
            "txn_id STRING, customer_id STRING, merchant_id STRING, amount DOUBLE, currency STRING, channel STRING, ip_country STRING, kind STRING, offset_s INT")
        .withColumn("timestamp", F.expr(
            f"timestamp_seconds(case when kind = 'ring' then {ring_start} else {straddle_hour} end + offset_s)"))
        .drop("kind", "offset_s"))

    planted_df.coalesce(1).write.mode("append").json(TRANSACTIONS_RAW_PATH)
    print(f"✅ Planted {len(rows)} transactions: fraud ring {', '.join(PLANTED_CUSTOMER_IDS)} at merchants "
          f"{', '.join(risky_merchants)}; straddle customer {STRADDLE_CUSTOMER_ID} at merchant {straddle_merchant}")

# 5. Logic for generating streaming transaction batches
def generate_batch(n_files=1, records_per_file=100, start_timestamp=None, seed=42, batch_tag=None):
    if start_timestamp is None:
        start_timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    start_dt = datetime.strptime(start_timestamp, "%Y-%m-%d %H:%M:%S")

    channels = ["online", "POS", "mobile"]
    currencies = ["USD", "usd"]  # the gateway settles in USD; some integrations send the code in lowercase
    countries_arr = ",".join([f"'{c}'" for c in COUNTRIES])
    # The customer's home country: the same expression as the customer dimension (dimension ids are bigint)
    home_idx = f"pmod(hash(cast(customer_id as bigint), 'customer_country'), {len(COUNTRIES)})"

    for i in range(n_files):
        # A unique tag per file keeps txn_ids unique across batches: the only duplicates are the deliberate gateway retries below
        file_tag = f"{batch_tag}_{i}" if batch_tag else uuid.uuid4().hex[:12]
        s = seed + i * 5
        # Every hash draw is salted with the file tag and the column name, so columns are independent
        txns = spark.range(records_per_file).select(
            F.col("id"),
            F.expr(f"concat('TXN_', '{file_tag}', '_', id)").alias("txn_id"),
            F.expr(f"cast({BACKGROUND_CUSTOMER_OFFSET} + pmod(hash(id, '{file_tag}', 'customer'), {BACKGROUND_CUSTOMER_POOL}) as string)").alias("customer_id"),
            F.expr(f"case when rand({s}) < {NEW_MERCHANT_RATE} "
                   f"then cast({NEW_MERCHANT_IDS[0]} + pmod(hash(id, '{file_tag}', 'new_merchant'), {len(NEW_MERCHANT_IDS)}) as string) "
                   f"else cast(pmod(hash(id, '{file_tag}', 'merchant'), {NUM_MERCHANTS}) as string) end").alias("merchant_id"),
            F.expr(f"round(rand({s + 1}) * 1000, 2)").alias("amount"),
            F.expr(_pick(currencies, f"hash(id, '{file_tag}', 'currency')")).alias("currency"),
            F.expr(_pick(channels, f"hash(id, '{file_tag}', 'channel')")).alias("channel"),
            F.expr(f"from_unixtime(unix_timestamp('{start_timestamp}') + id + {i} * {records_per_file})").cast("timestamp").alias("timestamp")
        )

        # IP country: the customer's home country, except GEO_MISMATCH_RATE of payments (travel, VPNs,
        # cross-border shopping) that come from a different country (never the home one)
        txns = txns.withColumn("ip_country", F.expr(
            f"element_at(array({countries_arr}), cast(case when rand({s + 4}) < {GEO_MISMATCH_RATE} "
            f"then pmod({home_idx} + 1 + pmod(hash(id, '{file_tag}', 'ip_country'), {len(COUNTRIES) - 1}), {len(COUNTRIES)}) "
            f"else {home_idx} end + 1 as int))"))

        # Amount anomalies (about 1%: $0 voids and negative refunds), applied BEFORE the retries are added,
        # because a gateway retry is the same message resent and carries the original's amount
        txns = txns.withColumn("amount",
            F.expr(f"case when rand({s + 2}) < 0.005 then 0.00 when rand({s + 3}) < 0.005 then amount * -1 else amount end"))

        # Duplicate gateway retries (2% of volume): same txn_id and amount, timestamp one minute later
        dupes = (txns.filter(F.col("id") < int(records_per_file * DUPLICATE_RATE))
            .withColumn("timestamp", F.col("timestamp") + F.expr("interval 1 minute")))
        txns_final = txns.unionByName(dupes).drop("id")

        # Write batch to JSON
        txns_final.coalesce(1).write.mode("append").json(TRANSACTIONS_RAW_PATH)

        # Malformed records for the _rescued_data column, one text file per generated file.
        # Both carry a valid timestamp (the file's own first event time) and a customer_id that only they use,
        # so they survive the watermark dedup and reach Bronze with _rescued_data populated:
        # _1 sends amount as text (type mismatch), _2 sends a field the contract does not have.
        event_time = (start_dt + timedelta(seconds=i * records_per_file)).strftime("%Y-%m-%dT%H:%M:%S")
        malformed_records = [
            '{"txn_id": "MALF_' + file_tag + '_1", "customer_id": "' + MALFORMED_TYPE_CUSTOMER_ID + '", "merchant_id": "7", "amount": "one hundred", "currency": "USD", "channel": "online", "ip_country": "US", "timestamp": "' + event_time + '"}',
            '{"txn_id": "MALF_' + file_tag + '_2", "customer_id": "' + MALFORMED_FIELD_CUSTOMER_ID + '", "merchant_id": "7", "amount": 50.0, "currency": "USD", "channel": "online", "ip_country": "US", "timestamp": "' + event_time + '", "unexpected_nested": {"foo": "bar"}}'
        ]
        malformed_df = spark.createDataFrame([(r,) for r in malformed_records], ["value"])
        malformed_df.coalesce(1).write.mode("append").text(TRANSACTIONS_RAW_PATH)

        print(f"✅ Generated transaction batch with {txns_final.count()} valid rows and {len(malformed_records)} malformed records")
