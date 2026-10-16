"""
Apparel Retail 360: Data Generator

This script generates continuous, simulated data streams for LoomLane Outfitters, a
fictional apparel chain, designed to feed a Lakeflow Spark Declarative Pipelines
(formerly Delta Live Tables / DLT) pipeline. It creates four concurrent streams: sales
transactions, customers, products (items), and stores. Each stream writes its rows to
its own landing folder as a Delta table.

The data is realistic but imperfect on purpose: your pipeline has to handle its data
quality issues, transformations and referential integrity.

The backfill is paced like a real month (40 sales per store and day). The live stream
runs at demo speed (500 sales every 30 seconds) so new data shows up in every pipeline
update, which makes today's rows far larger than any backfilled day.

Feel free to reach out to me if you have any questions. My contact details are available on dataengineer.wiki.

------------------------------------------------------------------------------------
Data Streams Generated
------------------------------------------------------------------------------------

Types below are as written to the landing Delta tables: the generator writes whole
numbers as long (bigint) and decimals as double. The bronze tasks cast IDs and counts
to int.

1.  **Sales Stream (`generate_sales_stream`)**
    *   **Description:** This is the fact stream, representing individual sales transactions.
        It depends on the dimension streams (customers, products, stores) for foreign keys.
        The generator will wait until the dimension tables have initial data before
        producing sales records.
    *   **Schema:** `transaction_id` (long), `store_id` (long), `event_time` (timestamp),
        `customer_id` (long), `product_id` (long), `quantity` (long), `unit_price` (double),
        `total_amount` (double), `payment_method` (string), `discount_applied` (double),
        `tax_amount` (double).
    *   **Behavior:**
        *   On the first run (empty sales table) it backfills `BACKFILL_DAYS` (30) days of
            history: `BACKFILL_SALES_PER_STORE_DAY` (40) sales per store and day, between
            09:00 and 21:00 UTC, each sold to a customer who had already joined. Only the
            bootstrap customers and products appear in the backfill. With the 5 bootstrap
            stores that is about 6,000 rows. Then it streams live.
        *   Each live batch holds `TRANSACTIONS_PER_BATCH` (500) sales.
        *   Retail arithmetic: `unit_price` is the product's current catalog price; about
            30% of lines get a 10%, 20% or 30% discount (`discount_applied` is the amount);
            `total_amount` is the net line value (quantity x unit_price minus the discount);
            `tax_amount` is 8% of `total_amount`.
        *   A customer buys at their home store 80% of the time; low product IDs sell the
            most (a long-tail best-seller curve); stores 'Under Renovation' do not trade.
        *   Resumes `transaction_id` from the last known value in the target Delta table
            to prevent key collisions across restarts.

2.  **Customer Stream (`generate_customer_stream`)**
    *   **Description:** A dimension stream for customer data. It simulates both the creation
        of new customers and change records for existing ones (same `customer_id`, newer
        `last_update_time`).
    *   **Schema:** `customer_id` (long), `name` (string), `email` (string), `address` (string),
        `join_date` (timestamp), `loyalty_points` (long), `phone_number` (string), `age` (long),
        `gender` (string), `last_update_time` (timestamp).
    *   **Behavior:**
        *   On the first run, it bootstraps an initial set of customers. Bootstrap records
            are dated one day before the sales backfill starts, so every historic sale has
            a dimension version valid at its time.
        *   Each batch sends 1 to `CUSTOMER_BATCH_SIZE` records; 30% are changes to an existing
            customer. A change copies the current record and changes at most one contact
            detail: the address (50% of changes), the email (25%) or the phone number (15%),
            and every change adds 5-120 loyalty points. Name, gender, age and join date never
            change.

3.  **Product/Item Stream (`generate_item_stream`)**
    *   **Description:** A dimension stream for product information. It simulates new product
        introductions and updates to existing product details.
    *   **Schema:** `product_id` (long), `name` (string), `category` (string), `brand` (string),
        `price` (double), `stock_quantity` (long), `size` (string), `color` (string),
        `description` (string), `last_update_time` (timestamp).
    *   **Behavior:**
        *   On the first run, it bootstraps an initial catalog of products. Bootstrap records
            are dated one day before the sales backfill starts, so every historic sale has
            a dimension version valid at its time.
        *   Each batch sends 1 to `PRODUCT_BATCH_SIZE` records; 30% are changes to an existing
            product: a price change between -30% and +10% (40% of changes, mostly markdowns),
            a stock level change only (50%) or a new description (10%). Name, category, brand,
            size and color never change.

4.  **Store Stream (`generate_store_stream`)**
    *   **Description:** A dimension stream for store locations and their details.
        It simulates new store openings and updates to existing store information.
    *   **Schema:** `store_id` (long), `name` (string), `address` (string), `manager` (string),
        `open_date` (timestamp), `status` (string), `phone_number` (string),
        `last_update_time` (timestamp).
    *   **Behavior:**
        *   On the first run, it bootstraps an initial set of stores. Bootstrap records are
            dated one day before the sales backfill starts, so every historic sale has a
            dimension version valid at its time (a store's `open_date` is earlier still).
        *   Each batch sends 0 to `STORE_BATCH_SIZE` (2) records; 95% are changes to an
            existing store, so at demo speed a new store opens about every 10 minutes. A
            change sets a new manager (50% of changes; 1 in 5 of those leaves the position
            vacant), changes the status (30%: an open store goes 'Under Renovation' 1 time
            in 4, a store under renovation reopens) or the phone number (20%). Name, address
            and open date never change. Setting `STORE_BATCH_SIZE` to 0 stops store records
            entirely.

------------------------------------------------------------------------------------
Expected Data Quality Issues & Constraints (For the Lakeflow Pipeline to Handle)
------------------------------------------------------------------------------------

The generator plants dirty data for your pipeline to catch. Rates are in `PLANTED`
below. Defects are planted on the row the source writes, never in the generator's own
memory of the record, so a later change record for the same ID arrives clean. New
records carry every defect of their stream; change records carry only the defects of
the attribute they change.

1.  **Negative Values:**
    *   `sales.quantity`: Represents a product return (5% of sales).
    *   `sales.discount_applied`: Written with the wrong sign on 2% of sales. Invalid data
        that needs to be flagged or cleaned.
    *   `customers.loyalty_points`: A points reversal applied twice (1%).
    *   `products.stock_quantity`: An oversold item (2%).

2.  **NULL/Missing Values:**
    *   `customers.email`: A customer may not provide an email address (2% of new customers).
    *   `products.brand`: Missing on 2% of new products.
    *   `stores.manager`: A store might have a vacant manager position (2% of new stores, and
        1 in 5 manager changes).

3.  **Out-of-range and invalid values:**
    *   `customers.age`: 0, 7 or 142 (1%: digits of a birth year typed into the age field).
    *   `customers.email`: The '@' is missing (1%).
    *   `customers.gender`: 'M', 'F' or '' (2%: a legacy till code or a blank).
    *   `products.category`: 'Kidswear', a category the reference list does not know yet (1%).
    *   `products.price`: Cents typed as dollars, the price x 100 (1%).
    *   `stores.status`: 'open' in lowercase, from a second store system (3%).
    *   `sales.payment_method`: 'CARD' or 'Apple Pay' from an unmapped terminal (1%).
    *   Business keys are never NULL. The bronze fail rules guard them anyway: a NULL
        key fails the update for that table and everything downstream of it, and every
        rerun stops on the same record until you change the rule, or remove the record
        at the source and run a full refresh.

4.  **Duplicates and Updates (Change Data Capture - CDC):**
    *   The customer, product, and store streams generate records with existing IDs but
        updated information and a new `last_update_time`. The Lakeflow pipeline must
        handle this CDC-like data with AUTO CDC (`dp.create_auto_cdc_flow` in Python,
        `AUTO CDC INTO` in SQL) and keep every version as SCD Type 2 (Tasks 9-11).

5.  **Referential Integrity:**
    *   The script maintains referential integrity at the source: it loads the existing
        records from the Delta tables on startup (`load_current_records`), publishes a new
        ID to the sales stream only after its record is written, and only generates sales
        for IDs that exist in the dimension streams. The pipeline still has to cope with
        a dimension record that is dropped or arrives late: that is why gold uses left
        joins (Task 17).

6.  **Event Time Skew:**
    *   The `event_time` in the sales stream and `last_update_time` in dimension streams
        are intentionally delayed by up to `LATENCY_MAX_S` seconds to simulate network
        latency and out-of-order data arrival. AUTO CDC orders dimension changes by
        `last_update_time`; a watermark would bound lateness for any stateful streaming
        aggregation.

Repeatability: Faker and each stream's random generator are seeded (`SEED`), but
timestamps follow the clock and every batch is timed, so row counts depend on how long
the generator runs.

"""


import random, threading
from datetime import datetime, timezone, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed
import pyspark.sql.functions as F
from pyspark.sql import Window
from pyspark.sql.types import StructType, StructField, LongType, DoubleType, StringType, TimestampType

from variables import *
# Databricks: Ensure Faker is installed
try:
    from faker import Faker

except ImportError:
    # Databricks magic command to install Faker
    %pip install faker
    from faker import Faker


# --- Common Settings & Execution ---
CONFIG = {
    # General stream settings
    "BATCH_INTERVAL_S": 30,
    "LATENCY_MAX_S": 60,
    "SEED": 42,                     # Seeds Faker and each stream's random generator

    # Initial data generation counts (for first run on empty tables)
    "INITIAL_STORE_COUNT": 5,
    "INITIAL_CUSTOMER_COUNT": 50,
    "INITIAL_PRODUCT_COUNT": 20,

    # One-time sales history on the first run (empty sales table)
    "BACKFILL_DAYS": 30,
    "BACKFILL_SALES_PER_STORE_DAY": 40,

    # Per-batch generation counts
    "TRANSACTIONS_PER_BATCH": 500,  # Total transactions for all stores
    "CUSTOMER_BATCH_SIZE": 30,      # Max new/updated customers per batch
    "PRODUCT_BATCH_SIZE": 20,       # Max new/updated products per batch
    "STORE_BATCH_SIZE": 2,          # Max new/updated stores per batch (set to 0 to stop store records)

    # Business behavior
    "CUSTOMER_UPDATE_SHARE": 0.30,  # Share of customer records that change an existing customer
    "PRODUCT_UPDATE_SHARE": 0.30,   # Share of product records that change an existing product
    "STORE_UPDATE_SHARE": 0.95,     # Share of store records that change an existing store
    "VACANCY_SHARE": 0.20,          # Share of manager changes that leave the position vacant
    "HOME_STORE_SHARE": 0.80,       # Share of a customer's sales made at their home store
    "DISCOUNT_SHARE": 0.30,         # Share of sales lines with a discount
    "TAX_RATE": 0.08,
}

# --- Planted data quality issues: share of written rows that get each defect ---
PLANTED = {
    "customer_email_null": 0.02,        # new customer gives no email (a business fact, not an error)
    "customer_email_no_at": 0.01,       # '@' missing from the email
    "customer_bad_age": 0.01,           # 0, 7 or 142: digits of a birth year typed into the age field
    "customer_negative_points": 0.01,   # a points reversal applied twice
    "customer_bad_gender": 0.02,        # 'M', 'F' or '': a legacy till code or a blank
    "product_negative_stock": 0.02,     # an oversold item
    "product_unknown_category": 0.01,   # 'Kidswear': not in the reference list yet
    "product_null_brand": 0.02,         # brand missing from the catalog feed
    "product_price_typo": 0.01,         # cents typed as dollars (price x 100)
    "store_vacant_manager": 0.02,       # new store opens with no manager
    "store_status_lowercase": 0.03,     # 'open' from a second store system
    "sale_return": 0.05,                # negative quantity = a return (business meaning, not an error)
    "sale_negative_discount": 0.02,     # discount written with the wrong sign
    "sale_bad_payment_method": 0.01,    # 'CARD' / 'Apple Pay' from an unmapped terminal
}

# --- Pre-generated lists for realism ---
product_types = ['T-Shirt', 'Jeans', 'Jacket', 'Dress', 'Shoes', 'Hat', 'Scarf', 'Sweater', 'Pants', 'Shirt']
sizes = ['XS', 'S', 'M', 'L', 'XL']

categories = ['Casual Wear', 'Formal Wear', 'Sportswear', 'Accessories', 'Footwear']
brands = [
    'Urban Threads', 'Peak Performance', 'Vivid Apparel', 'Heritage Denim', 'Metro Style',
    'Canvas Collective', 'Elemental Gear', 'Luxe Layers', 'Stride & Co.', 'Echo Streetwear',
    'Summit Outfitters', 'Blue Horizon', 'Modern Muse', 'Threadsmiths', 'Crestline',
    'Northbridge', 'Vogue Venture', 'Atlas Attire', 'Pulse Activewear', 'Signature Stitch'
]
payment_methods = ['Cash', 'Credit Card', 'Debit Card', 'Mobile Pay', 'Gift Card']
unmapped_payment_methods = ['CARD', 'Apple Pay']
discount_rates = [0.10, 0.20, 0.30]

# --- Landing table schemas (explicit, so a batch whose column is all NULL still writes) ---
SALES_SCHEMA = StructType([
    StructField("transaction_id", LongType()), StructField("store_id", LongType()),
    StructField("event_time", TimestampType()), StructField("customer_id", LongType()),
    StructField("product_id", LongType()), StructField("quantity", LongType()),
    StructField("unit_price", DoubleType()), StructField("total_amount", DoubleType()),
    StructField("payment_method", StringType()), StructField("discount_applied", DoubleType()),
    StructField("tax_amount", DoubleType()),
])
CUSTOMER_SCHEMA = StructType([
    StructField("customer_id", LongType()), StructField("name", StringType()),
    StructField("email", StringType()), StructField("address", StringType()),
    StructField("join_date", TimestampType()), StructField("loyalty_points", LongType()),
    StructField("phone_number", StringType()), StructField("age", LongType()),
    StructField("gender", StringType()), StructField("last_update_time", TimestampType()),
])
PRODUCT_SCHEMA = StructType([
    StructField("product_id", LongType()), StructField("name", StringType()),
    StructField("category", StringType()), StructField("brand", StringType()),
    StructField("price", DoubleType()), StructField("stock_quantity", LongType()),
    StructField("size", StringType()), StructField("color", StringType()),
    StructField("description", StringType()), StructField("last_update_time", TimestampType()),
])
STORE_SCHEMA = StructType([
    StructField("store_id", LongType()), StructField("name", StringType()),
    StructField("address", StringType()), StructField("manager", StringType()),
    StructField("open_date", TimestampType()), StructField("status", StringType()),
    StructField("phone_number", StringType()), StructField("last_update_time", TimestampType()),
])

# --- Shared state: the current clean record per ID, as each source system holds it ---
# A dimension stream publishes a record here only after it is written to its landing
# table, so the sales stream never references an ID that is not in the landing data yet.
state_lock = threading.Lock()
customers = {}   # customer_id -> record
products = {}    # product_id -> record
stores = {}      # store_id -> record
stop = threading.Event()  # set when one stream fails or the run is stopped


def make_faker(seed):
    fake = Faker()
    fake.seed_instance(seed)
    return fake


def write_rows(rows, schema, path):
    """Append dict rows to a landing Delta table, columns in schema order."""
    df = spark.createDataFrame([tuple(row[f.name] for f in schema.fields) for row in rows], schema)
    df.write.format("delta").mode("append").save(path)


def as_utc(value):
    """Timestamps read back from Delta arrive without a time zone; the generator works in UTC."""
    if isinstance(value, datetime) and value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value


def load_current_records(path, key):
    """Load the latest record per ID from a landing Delta table on startup.

    On first run, the target path might not exist or may exist without a Delta
    transaction log (serverless raises DELTA_MISSING_TRANSACTION_LOG or
    DELTA_TABLE_NOT_FOUND for the same empty folder). In those cases, treat as empty and
    let the stream bootstrap.
    """
    try:
        df = spark.read.format("delta").load(path)
        latest = Window.partitionBy(key).orderBy(F.col("last_update_time").desc())
        rows = df.withColumn("_rn", F.row_number().over(latest)).filter("_rn = 1").drop("_rn").collect()
        records = {row[key]: {k: as_utc(v) for k, v in row.asDict().items()} for row in rows}
        print(f"[{datetime.now()}] Success: Loaded {len(records)} existing records from {path}")
        return records
    except Exception as e:
        msg = str(e)
        benign_indicators = (  # serverless reports an empty folder either way
            "DELTA_MISSING_TRANSACTION_LOG",
            "DELTA_TABLE_NOT_FOUND",
            "doesn't exist",
            "Incompatible format detected",
            "is not a Delta table",
            "Path does not exist",
            "does not exist",
            "No such file or directory",
            "not found",
        )
        if any(ind in msg for ind in benign_indicators):
            print(f"[{datetime.now()}] Info: No existing Delta table at {path} ({msg.splitlines()[0]}). Starting fresh.")
            return {}
        # Re-raise unexpected errors
        raise

def make_sale(transaction_id, ts, store_id, customer_id, product_id, unit_price, rng, config):
    """One sales line with retail arithmetic and the planted sales defects."""
    quantity = rng.randint(1, 5)
    gross = round(quantity * unit_price, 2)
    sign_error = rng.random() < PLANTED["sale_negative_discount"]
    discounted = sign_error or rng.random() < config["DISCOUNT_SHARE"]
    discount = round(gross * rng.choice(discount_rates), 2) if discounted else 0.0
    total_amount = round(gross - discount, 2)  # net line value
    if rng.random() < PLANTED["sale_return"]:
        quantity, total_amount = -quantity, -total_amount
    if rng.random() < PLANTED["sale_bad_payment_method"]:
        payment_method = rng.choice(unmapped_payment_methods)
    else:
        payment_method = rng.choice(payment_methods)
    return {
        "transaction_id": transaction_id, "store_id": store_id, "event_time": ts,
        "customer_id": customer_id, "product_id": product_id, "quantity": quantity,
        "unit_price": unit_price, "total_amount": total_amount, "payment_method": payment_method,
        "discount_applied": -discount if sign_error else discount,
        "tax_amount": round(total_amount * config["TAX_RATE"], 2),
    }


def best_seller_weights(product_ids):
    """Long-tail popularity: the n-th product sells 1/n as often as the first."""
    return [1 / (i + 1) for i in range(len(product_ids))]


def backfill_sales(path, next_id, cust, prod, st, home_store, rng, config):
    """Write BACKFILL_DAYS of history once, so daily and lifetime tables have more than one day."""
    today = datetime.now(timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0)
    # Only bootstrap IDs (1..INITIAL_*_COUNT, dated before the window) have a version valid
    # at every backfilled sale; IDs created live are dated now.
    cust = {c: r for c, r in cust.items() if c <= config["INITIAL_CUSTOMER_COUNT"]}
    prod = {p: r for p, r in prod.items() if p <= config["INITIAL_PRODUCT_COUNT"]}
    product_ids = sorted(prod)
    weights = best_seller_weights(product_ids)
    rows = []
    for days_ago in range(config["BACKFILL_DAYS"], 0, -1):
        day = today - timedelta(days=days_ago)
        joined = [c for c in sorted(cust) if cust[c]["join_date"] < day]
        if not joined:
            continue
        for store_id in sorted(st):
            if st[store_id]["open_date"] >= day:  # a store opened today has no history
                continue
            regulars = [c for c in joined if home_store[c] == store_id]
            for _ in range(config["BACKFILL_SALES_PER_STORE_DAY"]):
                ts = day + timedelta(seconds=rng.uniform(9 * 3600, 21 * 3600))  # store hours
                use_regular = regulars and rng.random() < config["HOME_STORE_SHARE"]
                customer_id = rng.choice(regulars) if use_regular else rng.choice(joined)
                product_id = rng.choices(product_ids, weights=weights)[0]
                rows.append(make_sale(next_id, ts, store_id, customer_id, product_id,
                                      prod[product_id]["price"], rng, config))
                next_id += 1
    if rows:
        write_rows(rows, SALES_SCHEMA, path)
    print(f"[{datetime.now()}] Sales stream: Backfilled {len(rows)} historical transactions ({config['BACKFILL_DAYS']} days).")
    return next_id


def generate_sales_stream(path: str, config: dict):
    rng = random.Random(config["SEED"])
    home_store = {}  # customer_id -> the store where that customer shops most

    # Initialize transaction_id counter from the existing Delta table
    try:
        max_id = spark.read.format("delta").load(path).selectExpr("max(transaction_id)").collect()[0][0]
    except Exception:
        max_id = None
    transaction_id_counter = (max_id or 0) + 1
    needs_backfill = max_id is None
    print(f"[{datetime.now()}] Sales stream: Starting transaction_id from {transaction_id_counter}")

    while not stop.is_set():
        with state_lock:
            cust, prod, st = dict(customers), dict(products), dict(stores)

        if not (cust and prod and st):
            print(f"[{datetime.now()}] Sales stream is waiting: Dimension data is not yet available.")
            stop.wait(10)
            continue

        all_stores = sorted(st)
        for customer_id in sorted(cust):
            if customer_id not in home_store:
                home_store[customer_id] = rng.choice(all_stores)

        if needs_backfill:
            transaction_id_counter = backfill_sales(path, transaction_id_counter, cust, prod, st,
                                                    home_store, rng, config)
            needs_backfill = False

        now = datetime.now(timezone.utc)
        trading = [s for s in all_stores if st[s]["status"] != "Under Renovation"] or all_stores
        customer_ids = sorted(cust)
        product_ids = sorted(prod)
        weights = best_seller_weights(product_ids)
        data = []
        for product_id in rng.choices(product_ids, weights=weights, k=config["TRANSACTIONS_PER_BATCH"]):
            ts = now - timedelta(seconds=rng.uniform(0, config["LATENCY_MAX_S"]))
            customer_id = rng.choice(customer_ids)
            home = home_store[customer_id]
            if home in trading and rng.random() < config["HOME_STORE_SHARE"]:
                store_id = home
            else:
                store_id = rng.choice(trading)
            data.append(make_sale(transaction_id_counter, ts, store_id, customer_id, product_id,
                                  prod[product_id]["price"], rng, config))
            transaction_id_counter += 1

        write_rows(data, SALES_SCHEMA, path)
        print(f"[{datetime.now()}] Sales stream: Ingested {len(data)} new transactions.")
        stop.wait(config['BATCH_INTERVAL_S'])

def new_customer(customer_id, ts, rng, fake):
    """A clean new customer record and the row the source writes for it (with planted defects)."""
    name = fake.name()
    record = {
        "customer_id": customer_id,
        "name": name,
        "email": None if rng.random() < PLANTED["customer_email_null"] else fake.email(),
        "address": fake.address().replace('\n', ', '),
        "join_date": ts - timedelta(days=rng.randint(1, 365)),
        "loyalty_points": rng.randint(0, 1000),
        "phone_number": fake.phone_number(),
        "age": rng.randint(18, 70),
        "gender": rng.choice(['Male', 'Female', 'Other']),
        "last_update_time": ts,
    }
    row = dict(record)
    if row["email"] and rng.random() < PLANTED["customer_email_no_at"]:
        row["email"] = name.lower().replace(' ', '.') + '.example.com'
    if rng.random() < PLANTED["customer_bad_age"]:
        row["age"] = rng.choice([0, 7, 142])
    if rng.random() < PLANTED["customer_negative_points"]:
        row["loyalty_points"] = -rng.randint(1, 50)
    if rng.random() < PLANTED["customer_bad_gender"]:
        row["gender"] = rng.choice(['M', 'F', ''])
    return record, row


def change_customer(current, ts, rng, fake):
    """A change to an existing customer: contact details and points only."""
    record = dict(current, last_update_time=ts)
    pick = rng.random()
    if pick < 0.50:
        record["address"] = fake.address().replace('\n', ', ')
    elif pick < 0.75:
        record["email"] = fake.email()
    elif pick < 0.90:
        record["phone_number"] = fake.phone_number()
    # Otherwise only the points change: an in-place (untracked) column downstream
    record["loyalty_points"] += rng.randint(5, 120)
    row = dict(record)
    if 0.50 <= pick < 0.75 and rng.random() < PLANTED["customer_email_no_at"]:
        row["email"] = record["name"].lower().replace(' ', '.') + '.example.com'
    if rng.random() < PLANTED["customer_negative_points"]:
        row["loyalty_points"] = -rng.randint(1, 50)
    return record, row


def generate_customer_stream(path: str, config: dict):
    rng = random.Random(config["SEED"] + 1)
    fake = make_faker(config["SEED"] + 1)
    current = load_current_records(path, "customer_id")
    for record in current.values():  # a restart must not carry a planted defect forward
        if record["loyalty_points"] is None or record["loyalty_points"] < 0:
            record["loyalty_points"] = 0
        if record["age"] is None or not 18 <= record["age"] <= 100:
            record["age"] = rng.randint(18, 70)
        if record["gender"] not in ('Male', 'Female', 'Other'):
            record["gender"] = rng.choice(['Male', 'Female', 'Other'])
        if record["email"] is not None and '@' not in record["email"]:  # NULL email is a business fact, kept
            record["email"] = fake.email()

    if not current:
        print(f"[{datetime.now()}] Customer stream: Bootstrapping initial {config['INITIAL_CUSTOMER_COUNT']} customers...")
        # Dated one day before the sales backfill starts, so every historic sale has a version
        start = datetime.now(timezone.utc) - timedelta(days=config["BACKFILL_DAYS"] + 1)
        rows = []
        for cid in range(1, config['INITIAL_CUSTOMER_COUNT'] + 1):
            record, row = new_customer(cid, start, rng, fake)
            current[cid] = record
            rows.append(row)
        write_rows(rows, CUSTOMER_SCHEMA, path)
        print(f"[{datetime.now()}] Customer stream: Initial bootstrap complete.")

    with state_lock:
        customers.update(current)
    customer_id_counter = max(current, default=0) + 1

    while not stop.is_set():
        now = datetime.now(timezone.utc)
        rows, changed = [], {}
        num_customers = rng.randint(1, config['CUSTOMER_BATCH_SIZE'])

        for _ in range(num_customers):
            ts = now - timedelta(seconds=rng.uniform(0, config['LATENCY_MAX_S']))
            if current and rng.random() < config["CUSTOMER_UPDATE_SHARE"]:
                customer_id = rng.choice(sorted(current))
                record, row = change_customer(changed.get(customer_id, current[customer_id]), ts, rng, fake)
            else:
                customer_id = customer_id_counter
                customer_id_counter += 1
                record, row = new_customer(customer_id, ts, rng, fake)
            changed[customer_id] = record
            rows.append(row)

        write_rows(rows, CUSTOMER_SCHEMA, path)
        current.update(changed)
        with state_lock:
            customers.update(changed)
        print(f"[{datetime.now()}] Customer stream: Ingested {len(rows)} new/updated records.")
        stop.wait(config['BATCH_INTERVAL_S'])

def new_product(product_id, ts, rng, fake):
    """A clean new product record and the row the source writes for it (with planted defects)."""
    color = fake.color_name()
    record = {
        "product_id": product_id,
        "name": f"{fake.word().capitalize()} {color} {rng.choice(product_types)}",
        "category": rng.choice(categories),
        "brand": rng.choice(brands),
        "price": round(rng.uniform(10.0, 150.0), 2),
        "stock_quantity": rng.randint(0, 200),
        "size": rng.choice(sizes),
        "color": color,
        "description": fake.sentence(nb_words=8),
        "last_update_time": ts,
    }
    row = dict(record)
    if rng.random() < PLANTED["product_negative_stock"]:
        row["stock_quantity"] = -rng.randint(1, 5)
    if rng.random() < PLANTED["product_unknown_category"]:
        row["category"] = 'Kidswear'
    if rng.random() < PLANTED["product_null_brand"]:
        row["brand"] = None
    if rng.random() < PLANTED["product_price_typo"]:
        row["price"] = round(record["price"] * 100, 2)
    return record, row


def change_product(current, ts, rng, fake):
    """A change to an existing product: price, stock or description only."""
    record = dict(current, last_update_time=ts)
    row_defects = {}
    pick = rng.random()
    if pick < 0.40:  # mostly markdowns
        record["price"] = max(5.0, round(record["price"] * (1 + rng.uniform(-0.3, 0.1)), 2))
        if rng.random() < PLANTED["product_price_typo"]:
            # From at least $10, so a typo is always >= $1,000 and always dropped downstream
            row_defects["price"] = round(max(record["price"], 10.0) * 100, 2)
    elif pick < 0.90:  # stock only: an in-place (untracked) column downstream
        record["stock_quantity"] = rng.randint(0, 200)
        if rng.random() < PLANTED["product_negative_stock"]:
            row_defects["stock_quantity"] = -rng.randint(1, 5)
    else:
        record["description"] = fake.sentence(nb_words=8)
    return record, dict(record, **row_defects)


def generate_item_stream(path: str, config: dict):
    rng = random.Random(config["SEED"] + 2)
    fake = make_faker(config["SEED"] + 2)
    current = load_current_records(path, "product_id")
    for record in current.values():  # a restart must not carry a planted defect forward
        if record["price"] is not None and record["price"] >= 1000:
            record["price"] = round(record["price"] / 100, 2)
        if record["stock_quantity"] is None or record["stock_quantity"] < 0:
            record["stock_quantity"] = 0
        if record["category"] not in categories:
            record["category"] = rng.choice(categories)
        if record["brand"] is None:
            record["brand"] = rng.choice(brands)

    if not current:
        print(f"[{datetime.now()}] Product stream: Bootstrapping initial {config['INITIAL_PRODUCT_COUNT']} products...")
        # Dated one day before the sales backfill starts, so every historic sale has a version
        start = datetime.now(timezone.utc) - timedelta(days=config["BACKFILL_DAYS"] + 1)
        rows = []
        for pid in range(1, config['INITIAL_PRODUCT_COUNT'] + 1):
            record, row = new_product(pid, start, rng, fake)
            current[pid] = record
            rows.append(row)
        write_rows(rows, PRODUCT_SCHEMA, path)
        print(f"[{datetime.now()}] Product stream: Initial bootstrap complete.")

    with state_lock:
        products.update(current)
    product_id_counter = max(current, default=0) + 1

    while not stop.is_set():
        now = datetime.now(timezone.utc)
        rows, changed = [], {}
        num_items = rng.randint(1, config['PRODUCT_BATCH_SIZE'])
        for _ in range(num_items):
            ts = now - timedelta(seconds=rng.uniform(0, config['LATENCY_MAX_S']))
            if current and rng.random() < config["PRODUCT_UPDATE_SHARE"]:
                product_id = rng.choice(sorted(current))
                record, row = change_product(changed.get(product_id, current[product_id]), ts, rng, fake)
            else:
                product_id = product_id_counter
                product_id_counter += 1
                record, row = new_product(product_id, ts, rng, fake)
            changed[product_id] = record
            rows.append(row)

        write_rows(rows, PRODUCT_SCHEMA, path)
        current.update(changed)
        with state_lock:
            products.update(changed)
        print(f"[{datetime.now()}] Product stream: Ingested {len(rows)} new/updated records.")
        stop.wait(config['BATCH_INTERVAL_S'])

def new_store(store_id, ts, open_date, rng, fake):
    """A clean new store record and the row the source writes for it (with planted defects)."""
    record = {
        "store_id": store_id,
        "name": f"Store {store_id} - {fake.city()}",
        "address": fake.address().replace('\n', ', '),
        "manager": None if rng.random() < PLANTED["store_vacant_manager"] else fake.name(),
        "open_date": open_date,
        "status": 'Open',
        "phone_number": fake.phone_number(),
        "last_update_time": ts,
    }
    row = dict(record)
    if rng.random() < PLANTED["store_status_lowercase"]:
        row["status"] = row["status"].lower()
    return record, row


def change_store(current, ts, rng, fake, config):
    """A change to an existing store: manager, status or phone number only."""
    record = dict(current, last_update_time=ts)
    pick = rng.random()
    if pick < 0.50:
        record["manager"] = None if rng.random() < config["VACANCY_SHARE"] else fake.name()
    elif pick < 0.80:
        if record["status"] == 'Under Renovation':
            record["status"] = 'Open'
        elif rng.random() < 0.25:
            record["status"] = 'Under Renovation'
    else:
        record["phone_number"] = fake.phone_number()
    row = dict(record)
    # Only a record that changes the status carries the status defect
    if record["status"] != current["status"] and rng.random() < PLANTED["store_status_lowercase"]:
        row["status"] = row["status"].lower()
    return record, row


def generate_store_stream(path: str, config: dict):
    rng = random.Random(config["SEED"] + 3)
    fake = make_faker(config["SEED"] + 3)
    current = load_current_records(path, "store_id")
    for record in current.values():  # a restart must not carry a planted defect forward
        if record["status"] not in ('Open', 'Under Renovation'):
            record["status"] = 'Open'

    if not current:
        print(f"[{datetime.now()}] Store stream: Bootstrapping initial {config['INITIAL_STORE_COUNT']} stores...")
        now = datetime.now(timezone.utc)
        # Dated one day before the sales backfill starts, so every historic sale has a version
        start = now - timedelta(days=config["BACKFILL_DAYS"] + 1)
        rows = []
        for store_id in range(1, config['INITIAL_STORE_COUNT'] + 1):
            open_date = now - timedelta(days=rng.randint(365, 1825))
            record, row = new_store(store_id, start, open_date, rng, fake)
            current[store_id] = record
            rows.append(row)
        write_rows(rows, STORE_SCHEMA, path)
        print(f"[{datetime.now()}] Store stream: Initial bootstrap complete.")

    with state_lock:
        stores.update(current)
    store_id_counter = max(current, default=0) + 1

    while not stop.is_set():
        now = datetime.now(timezone.utc)
        rows, changed = [], {}
        num_stores = rng.randint(0, config['STORE_BATCH_SIZE'])

        for _ in range(num_stores):
            ts = now - timedelta(seconds=rng.uniform(0, config['LATENCY_MAX_S']))
            if current and rng.random() < config["STORE_UPDATE_SHARE"]:
                store_id = rng.choice(sorted(current))
                record, row = change_store(changed.get(store_id, current[store_id]), ts, rng, fake, config)
            else:
                store_id = store_id_counter
                store_id_counter += 1
                record, row = new_store(store_id, ts, ts, rng, fake)  # a new store opens today
            changed[store_id] = record
            rows.append(row)

        if rows:
            write_rows(rows, STORE_SCHEMA, path)
            current.update(changed)
            with state_lock:
                stores.update(changed)
            print(f"[{datetime.now()}] Store stream: Ingested {len(rows)} new/updated records.")

        stop.wait(config['BATCH_INTERVAL_S'])



streams = [
    # Raw data directories
    (RAW_SALES_PATH, generate_sales_stream),
    (RAW_CUSTOMERS_PATH, generate_customer_stream),
    (RAW_PRODUCTS_PATH, generate_item_stream),
    (RAW_STORES_PATH, generate_store_stream)
]

# Runs until you stop the cell. A failure in any stream stops all four and is raised here,
# instead of leaving the other three running with missing or stale data.
stop.clear()
executor = ThreadPoolExecutor(max_workers=len(streams))
futures = [executor.submit(fn, path, CONFIG) for path, fn in streams]
try:
    for f in as_completed(futures):
        f.result()  # re-raises the first stream error instead of hiding it
finally:
    stop.set()  # the other streams finish their current batch and exit
    executor.shutdown(wait=True)

