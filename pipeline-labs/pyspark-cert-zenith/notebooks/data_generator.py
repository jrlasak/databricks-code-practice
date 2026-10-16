# Databricks notebook source
"""
Zenith Online Analytics Pipeline - Synthetic Data Generator

This script generates synthetic data for a fictional e-commerce platform, "Zenith Online."
It feeds the Zenith Online lab for the Databricks Certified Associate Developer for
Apache Spark exam.

The script produces three distinct datasets:
1.  A batch file of customer profiles in CSV format.
2.  A batch file of product details in Parquet format.
3.  A continuous stream of user events as JSON Lines files (one event per line), simulating a real-time feed.

------------------------------------------------------------------------------------
Data Generation Logic & Intentionally Introduced Challenges
------------------------------------------------------------------------------------

1.  **Batch Data (`generate_batch_data`)**:
    *   **Customers (CSV):** A list of customers with static information. One file is generated
      at the start of the run to represent a dimension table that is loaded infrequently.
    *   **Products (Parquet):** A catalog of products. This is also generated as a single file
      at the start, in Parquet, so the Bronze layer reads a columnar format that carries its own schema.
      Each category has its own product nouns and price band (`PRODUCT_CATALOG`), so a name,
      its category and its price agree, and every name is unique (it ends in a model code
      built from the `product_id`, e.g. `Z101`).

2.  **Streaming Data (`generate_streaming_events`)**:
    *   **User Events (JSON Lines):** Each call writes one batch of user actions (`view_product`,
      `add_to_cart`, `purchase`), or loops, one batch per interval, with `infinite=True`. Each batch of events is
      written as a new JSON Lines file (one event object per line) to a specific directory,
      which Databricks Auto Loader or `readStream` can pick up.
    *   **Data Skew:** The generator intentionally introduces data skew.
      A small subset of `product_id`s (defined in `POPULAR_PRODUCT_IDS`) has a significantly
      higher probability of appearing in events of every type (views, add-to-carts and
      purchases alike), so each product keeps a view > cart > purchase funnel and the hot
      products are also the best sellers. Notebook 03 measures this skew, applies the two-stage
      salting pattern to it and shows when that pattern pays off.
    *   **Planted Data Quality Issues:** Applied after the main event loop, to a random sample of
      the events:
        - Client retries: about 1% of events are re-sent as exact copies (same `event_id`,
          same `event_timestamp`), as a tracking SDK does when a beacon call times out.
          The Silver layer's watermarked de-duplication removes them.
        - Late sign-ups: about 0.5% of events carry a `user_id` from 1001-1050, accounts
          created after the nightly CRM export, so `customer_profiles.csv` does not have them
          yet. The Silver left join keeps them with a null region.
        - Delisted products: about 0.2% of events carry a `product_id` from 501-505, products
          removed from the catalog export after the event fired. The Silver inner join drops
          them, because they cannot be priced.

------------------------------------------------------------------------------------
How to Use in Databricks
------------------------------------------------------------------------------------

You do not run this notebook yourself. `00_Setup_Environment` creates the catalog,
schemas, volumes and directories, loads this notebook with `%run ./data_generator`, and
then calls:

1.  `generate_batch_data()`: writes the customer CSV and the product Parquet file.
2.  `generate_streaming_events(infinite=False, stream_events_per_batch=50000)`: writes ONE
    JSON Lines file of 50,000 events plus 500 retried copies (50,500 lines) and stops
    (no endless loop to cancel).

All paths come from `variables.py`. `random` is seeded once (42), so every run draws the
same 500 products, the same 1,000 customer IDs and locations (their signup dates move
with the run date) and the same event mix; event timestamps stay relative to the moment
of the run, which the 3-minute watermark in the Silver layer depends on.
"""

import os
import json
import csv
import random
import time
import uuid
from datetime import datetime, timezone, timedelta

# COMMAND ----------

# MAGIC %run ./variables

# COMMAND ----------

# Seed once so two runs draw the same products, customers and event mix.
random.seed(42)

# pandas and pyarrow write the Parquet file (pre-installed on Databricks).
try:
    import pandas as pd
    import pyarrow as pa
    import pyarrow.parquet as pq
    PANDAS_AVAILABLE = True
except ImportError:
    PANDAS_AVAILABLE = False

# --- Configuration ---
CONFIG = {
    # File Paths
    "RAW_STREAMING_PATH": RAW_STREAMING_PATH,
    "RAW_BATCH_CUSTOMERS_PATH": RAW_BATCH_CUSTOMERS_PATH,
    "RAW_BATCH_PRODUCTS_PATH": RAW_BATCH_PRODUCTS_PATH,

    # Data Generation Counts
    "CUSTOMER_COUNT": 1000,
    "PRODUCT_COUNT": 500,

    # Streaming Settings
    "STREAM_EVENTS_PER_BATCH": 5000,
    "STREAM_BATCH_INTERVAL_S": 30,

    # Data Skew Configuration
    # The 5 products in this week's flash sale: viewed, carted and bought much more often than the rest.
    "POPULAR_PRODUCT_IDS": [101, 210, 315, 420, 55],
    "SKEW_FACTOR_PROBABILITY": 0.75, # 75% of all events (any type) will be for a popular product.

    # Planted data quality issues (share of the events in one batch)
    "RETRY_RATE": 0.01,                       # exact re-sent copies (client retries)
    "LATE_SIGNUP_RATE": 0.005,                # user_id not in the customer CSV yet
    "LATE_SIGNUP_USER_IDS": (1001, 1050),
    "DELISTED_RATE": 0.002,                   # product_id no longer in the product catalog
    "DELISTED_PRODUCT_IDS": (501, 505),

    # Streaming Mode: If True, stream is infinite; if False, only one batch is generated.
    "STREAM_INFINITE": True
}

# --- Pre-generated lists for creating realistic data ---
first_names = ['Liam', 'Olivia', 'Noah', 'Emma', 'Oliver', 'Ava', 'Elijah', 'Charlotte', 'William', 'Sophia']
last_names = ['Smith', 'Johnson', 'Williams', 'Brown', 'Jones', 'Garcia', 'Miller', 'Davis', 'Rodriguez', 'Martinez']
locations = ['New York', 'Los Angeles', 'Chicago', 'Houston', 'Phoenix', 'Philadelphia', 'San Antonio', 'San Diego']

product_adjectives = ['Classic', 'Modern', 'Rustic', 'Ergonomic', 'Sleek', 'Durable', 'Lightweight', 'Heavy Duty']
# Category -> (product nouns, lowest price, highest price in USD): names, categories and prices agree.
PRODUCT_CATALOG = {
    'Furniture': (['Chair', 'Table', 'Desk', 'Bookcase', 'Sofa'], 89.00, 899.99),
    'Home Decor': (['Lamp', 'Clock', 'Vase', 'Mirror'], 15.50, 249.99),
    'Electronics': (['Keyboard', 'Monitor', 'Headphones', 'Webcam'], 29.99, 599.99),
    'Office Supplies': (['Stapler', 'Desk Organizer', 'Notebook Set', 'Pen Set'], 15.50, 79.99),
}

# --- Global lists to ensure referential integrity ---
existing_customer_ids = []
existing_product_ids = []

def generate_batch_data():
    """
    Generates the initial batch files for customers (CSV) and products (Parquet).
    Called once by 00_Setup_Environment.
    """
    print(f"[{datetime.now()}] Starting generation of initial batch data...")

    # --- 1. Generate Customer Data (CSV) ---
    customers_path = CONFIG["RAW_BATCH_CUSTOMERS_PATH"]
    customer_filepath = os.path.join(customers_path, "customer_profiles.csv")
    
    with open(customer_filepath, 'w', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(['customer_id', 'signup_date', 'location'])
        
        for i in range(1, CONFIG["CUSTOMER_COUNT"] + 1):
            customer_id = i
            existing_customer_ids.append(customer_id)
            signup_date = (datetime.now() - timedelta(days=random.randint(30, 1000))).strftime('%Y-%m-%d')
            location = random.choice(locations)
            writer.writerow([customer_id, signup_date, location])
    
    print(f"[{datetime.now()}] Successfully wrote {len(existing_customer_ids)} customers to: {customer_filepath}")

    # --- 2. Generate Product Data (Parquet) ---
    if not PANDAS_AVAILABLE:
        print("Pandas/Pyarrow not found. Skipping Parquet generation.")
        return

    products_path = CONFIG["RAW_BATCH_PRODUCTS_PATH"]
    os.makedirs(products_path, exist_ok=True)
    product_filepath = os.path.join(products_path, "product_details.parquet")
    
    product_data = []
    for i in range(1, CONFIG["PRODUCT_COUNT"] + 1):
        product_id = i
        existing_product_ids.append(product_id)
        category = random.choice(list(PRODUCT_CATALOG))
        nouns, lowest_price, highest_price = PRODUCT_CATALOG[category]
        noun = random.choice(nouns)
        # The model code (Z001-Z500) makes every product name unique.
        product_name = f"{random.choice(product_adjectives)} {noun} Z{product_id:03d}"
        price = round(random.uniform(lowest_price, highest_price), 2)
        product_data.append({
            'product_id': product_id,
            'product_name': product_name,
            'category': category,
            'price': price
        })
    
    product_df = pd.DataFrame(product_data)
    product_df.to_parquet(product_filepath)

    print(f"[{datetime.now()}] Successfully wrote {len(existing_product_ids)} products to: {product_filepath}")
    print("-" * 50)


def generate_streaming_events(infinite=None, stream_events_per_batch=None, stream_batch_interval_s=None):
    """
    Writes user event data as JSON Lines files, one file per batch.
    If infinite is True, runs in a loop until the cell is cancelled.
    If infinite is False, generates only one batch.
    If infinite is None, uses CONFIG["STREAM_INFINITE"].
    """
    if infinite is None:
        infinite = CONFIG.get("STREAM_INFINITE", True)
    
    if stream_events_per_batch is None:
        stream_events_per_batch = CONFIG.get("STREAM_EVENTS_PER_BATCH", 5000)
    if stream_batch_interval_s is None:
        stream_batch_interval_s = CONFIG.get("STREAM_BATCH_INTERVAL_S", 30)

    streaming_path = CONFIG["RAW_STREAMING_PATH"]
    os.makedirs(streaming_path, exist_ok=True)

    print(f"[{datetime.now()}] Starting event stream generation...")
    print(f"Data will be written to: {streaming_path}")
    print(f"Data Skew active: {CONFIG['SKEW_FACTOR_PROBABILITY']:.0%} of events (every type) target {CONFIG['POPULAR_PRODUCT_IDS']}")
    print(f"Streaming mode: {'infinite' if infinite else 'single batch'}")
    print("-" * 50)

    def write_event_batch():
        events = []
        now = datetime.now(timezone.utc)
        for _ in range(stream_events_per_batch):
            event_type = random.choices(['view_product', 'add_to_cart', 'purchase'], weights=[0.7, 0.2, 0.1], k=1)[0]
            # The hot-product draw applies to every event type, so each product keeps a
            # view > cart > purchase funnel and the hot products are also the best sellers.
            if random.random() < CONFIG['SKEW_FACTOR_PROBABILITY']:
                product_id = random.choice(CONFIG['POPULAR_PRODUCT_IDS'])
            else:
                product_id = random.choice(existing_product_ids)
            event = {
                "event_id": str(uuid.uuid4()),
                "event_timestamp": (now - timedelta(seconds=random.randint(0, 30))).isoformat(),
                "user_id": random.choice(existing_customer_ids),
                "event_type": event_type,
                "product_id": product_id,
                "session_id": str(uuid.uuid4())
            }
            events.append(event)

        # --- Planted data quality issues ---
        # 1. Late sign-ups: accounts created after the nightly CRM export, so
        #    customer_profiles.csv does not have them yet.
        first_id, last_id = CONFIG['LATE_SIGNUP_USER_IDS']
        for event in random.sample(events, k=round(len(events) * CONFIG['LATE_SIGNUP_RATE'])):
            event['user_id'] = random.randint(first_id, last_id)
        # 2. Delisted products: removed from the catalog export after the event fired.
        first_id, last_id = CONFIG['DELISTED_PRODUCT_IDS']
        for event in random.sample(events, k=round(len(events) * CONFIG['DELISTED_RATE'])):
            event['product_id'] = random.randint(first_id, last_id)
        # 3. Client retries: the tracking SDK re-sends a beacon whose call timed out, so the
        #    copy is exact (same event_id, same event_timestamp). Drawn last, so each copy
        #    matches its original exactly.
        retries = [dict(event) for event in random.sample(events, k=round(len(events) * CONFIG['RETRY_RATE']))]
        events.extend(retries)

        file_timestamp = datetime.now().strftime('%Y%m%d_%H%M%S_%f')
        output_filepath = os.path.join(streaming_path, f"events_{file_timestamp}.json")
        # JSON Lines: one event object per line, the layout Auto Loader's json format reads by default.
        with open(output_filepath, 'w') as f:
            for event in events:
                f.write(json.dumps(event) + "\n")
        print(f"[{datetime.now()}] Wrote batch of {len(events)} events ({len(retries)} of them retried copies) to {output_filepath}")

    if infinite:
        while True:
            write_event_batch()
            time.sleep(stream_batch_interval_s)
    else:
        write_event_batch()
