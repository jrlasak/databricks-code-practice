"""
Databricks Apparel DLT Pipeline Data Generator

This script generates continuous, simulated data streams for an apparel retail company,
designed to feed a Databricks Delta Live Tables (DLT) pipeline. It creates four
concurrent streams: sales transactions, customers, products (items), and stores.

The primary purpose is to produce realistic but imperfect data that mimics a real-world
scenario, allowing the DLT pipeline to be tested on its ability to handle data quality
issues, transformations, and referential integrity.

Feel free to reach out to me if you have any questions. My contact details are available on dataengineer.wiki.

------------------------------------------------------------------------------------
Data Streams Generated
------------------------------------------------------------------------------------

1.  **Sales Stream (`generate_sales_stream`)**
    *   **Description:** This is the fact stream, representing individual sales transactions.
        It depends on the dimension streams (customers, products, stores) for foreign keys.
        The generator will wait until the dimension tables have initial data before
        producing sales records.
    *   **Schema:** `transaction_id` (int), `store_id` (int), `event_time` (timestamp),
        `customer_id` (int), `product_id` (int), `quantity` (int), `unit_price` (float),
        `total_amount` (float), `payment_method` (string), `discount_applied` (float),
        `tax_amount` (float).
    *   **Behavior:**
        *   Generates a fixed number of transactions per batch, controlled by `TRANSACTIONS_PER_BATCH`.
        *   Resumes `transaction_id` from the last known value in the target Delta table
          to prevent key collisions across restarts.

2.  **Customer Stream (`generate_customer_stream`)**
    *   **Description:** A dimension stream for customer data. It simulates both the creation
        of new customers and the updating of existing ones (Type 1 SCD).
    *   **Schema:** `customer_id` (int), `name` (string), `email` (string), `address` (string),
        `join_date` (timestamp), `loyalty_points` (int), `phone_number` (string), `age` (int),
        `gender` (string), `last_update_time` (timestamp).
    *   **Behavior:**
        *   On the first run, it bootstraps an initial set of customers.
        *   In subsequent batches, it adds a mix of new customers and updates to existing ones.

3.  **Product/Item Stream (`generate_item_stream`)**
    *   **Description:** A dimension stream for product information. It simulates new product
        introductions and updates to existing product details (e.g., price, stock).
    *   **Schema:** `product_id` (int), `name` (string), `category` (string), `brand` (string),
        `price` (float), `stock_quantity` (int), `size` (string), `color` (string),
        `description` (string), `last_update_time` (timestamp).
    *   **Behavior:**
        *   On the first run, it bootstraps an initial catalog of products.
        *   Continuously adds new products and updates to existing ones in subsequent batches.

4.  **Store Stream (`generate_store_stream`)**
    *   **Description:** A dimension stream for store locations and their details.
        It simulates new store openings and updates to existing store information.
    *   **Schema:** `store_id` (int), `name` (string), `address` (string), `manager` (string),
        `open_date` (timestamp), `status` (string), `phone_number` (string),
        `last_update_time` (timestamp).
    *   **Behavior:**
        *   On the first run, it bootstraps an initial set of stores.
        *   Continuously adds new stores and updates to existing ones. Setting
          `STORE_BATCH_SIZE` to 0 will halt the creation of new stores.

------------------------------------------------------------------------------------
Expected Data Quality Issues & Constraints (For DLT Pipeline to Handle)
------------------------------------------------------------------------------------

This generator intentionally introduces "dirty" data to test the robustness of the
downstream DLT pipeline. The pipeline should be built with expectations to handle:

1.  **Negative Values:**
    *   `sales.quantity`: Represents a product return.
    *   `sales.discount_applied`: Invalid data that needs to be flagged or cleaned.

2.  **NULL/Missing Values:**
    *   `customers.email`: A customer may not provide an email address.
    *   `stores.manager`: A store might have a vacant manager position.

3.  **Duplicates and Updates (Change Data Capture - CDC):**
    *   The customer, product, and store streams generate records with existing IDs but
        updated information and a new `last_update_time`. The DLT pipeline must correctly
        handle this CDC-like data, for instance, by using `APPLY CHANGES INTO` to
        merge the latest version of a record into the target dimension table.

4.  **Referential Integrity:**
    *   The script maintains referential integrity at the source by loading existing IDs
        from the Delta tables (`load_existing_ids`) and only generating sales for IDs
        that exist in the dimension streams. However, the DLT pipeline should still
        enforce this with constraints to catch any potential violations (e.g., a dimension
        record is dropped or arrives late).

5.  **Event Time Skew:**
    *   The `event_time` in the sales stream and `last_update_time` in dimension streams
        are intentionally delayed by up to `LATENCY_MAX_S` seconds to simulate network
        latency and out-of-order data arrival. The DLT pipeline should be designed to handle
        this, typically by using watermarking on the event time columns.

"""


import time, random
from datetime import datetime, timezone, timedelta
from concurrent.futures import ThreadPoolExecutor
import pyspark.sql.functions as F
from pyspark.sql.utils import AnalysisException
import sys
import os

from variables import *
# Databricks: Ensure Faker is installed
try:
    from faker import Faker

except ImportError:
    # Databricks magic command to install Faker
    %pip install faker
    from faker import Faker


# --- Add Faker for realistic data generation ---
fake = Faker()

# --- Common Settings & Execution ---
CONFIG = {
    # General stream settings
    "BATCH_INTERVAL_S": 30,
    "LATENCY_MAX_S": 60,

    # Initial data generation counts (for first run on empty tables)
    "INITIAL_STORE_COUNT": 5,
    "INITIAL_CUSTOMER_COUNT": 50,
    "INITIAL_PRODUCT_COUNT": 20,

    # Per-batch generation counts
    "TRANSACTIONS_PER_BATCH": 500, # Total transactions for all stores
    "CUSTOMER_BATCH_SIZE": 30,      # Max new/updated customers per batch
    "PRODUCT_BATCH_SIZE": 20,       # Max new/updated products per batch
    "STORE_BATCH_SIZE": 10,         # Max new/updated stores per batch (set to 0 to prevent new stores)
}

# --- Pre-generated lists for realism ---
product_types = ['T-Shirt', 'Jeans', 'Jacket', 'Dress', 'Shoes', 'Hat', 'Scarf', 'Sweater', 'Pants', 'Shirt']
colors = ['Red', 'Blue', 'Green', 'Black', 'White', 'Yellow', 'Gray', 'Purple', 'Orange', 'Pink']
sizes = ['XS', 'S', 'M', 'L', 'XL']
product_names = [f"{color} {ptype} {size}" for ptype in product_types for color in colors for size in sizes][:200]

store_locations = ['Downtown', 'Mall', 'Suburb', 'Airport', 'Outlet']

categories = ['Casual Wear', 'Formal Wear', 'Sportswear', 'Accessories', 'Footwear']
brands = [
    'Urban Threads', 'Peak Performance', 'Vivid Apparel', 'Heritage Denim', 'Metro Style',
    'Canvas Collective', 'Elemental Gear', 'Luxe Layers', 'Stride & Co.', 'Echo Streetwear',
    'Summit Outfitters', 'Blue Horizon', 'Modern Muse', 'Threadsmiths', 'Crestline',
    'Northbridge', 'Vogue Venture', 'Atlas Attire', 'Pulse Activewear', 'Signature Stitch'
]
payment_methods = ['Cash', 'Credit Card', 'Debit Card', 'Mobile Pay', 'Gift Card']

# --- Global lists to track existing IDs for referential integrity ---
existing_customers = []
existing_products = []
existing_stores = []

def load_existing_ids(path, id_column):
    """Helper function to load existing IDs from a Delta table on startup.

    On first run, the target path might not exist or may exist without a Delta
    transaction log. In those cases, treat as empty and let the stream bootstrap.
    """
    try:
        df = spark.read.format("delta").load(path)
        ids = [row[id_column] for row in df.select(id_column).distinct().collect()]
        print(f"[{datetime.now()}] Success: Loaded {len(ids)} existing IDs from {path}")
        return ids
    except Exception as e:
        msg = str(e)
        benign_indicators = (
            "DELTA_MISSING_TRANSACTION_LOG",
            "Incompatible format detected",
            "is not a Delta table",
            "Path does not exist",
            "does not exist",
            "No such file or directory",
            "not found",
        )
        if any(ind in msg for ind in benign_indicators):
            print(f"[{datetime.now()}] Info: No existing Delta table at {path} ({msg.splitlines()[0]}). Starting fresh.")
            return []
        # Re-raise unexpected errors
        raise

def generate_sales_stream(path: str, config: dict):
    # --- KEY CHANGE: Initialize transaction_id counter from existing Delta table ---
    try:
        max_id = spark.read.format("delta").load(path).selectExpr("max(transaction_id)").collect()[0][0]
        transaction_id_counter = (max_id or 0) + 1
        print(f"[{datetime.now()}] Sales stream: Resuming transaction_id from {transaction_id_counter}")
    except Exception:
        transaction_id_counter = 1
        print(f"[{datetime.now()}] Sales stream: Starting transaction_id from 1")
    
    while True:
        now = datetime.now(timezone.utc)
        data = []

        if not all([existing_customers, existing_products, existing_stores]):
            print(f"[{datetime.now()}] Sales stream is waiting: Dimension data is not yet available.")
            time.sleep(10)
            continue

        # --- KEY CHANGE: Generate a fixed number of transactions per batch ---
        for _ in range(config['TRANSACTIONS_PER_BATCH']):
            ts = now - timedelta(seconds=random.uniform(0, config['LATENCY_MAX_S']))
            customer_id = random.choice(existing_customers)
            product_id = random.choice(existing_products)
            store_id = random.choice(existing_stores) # Assign transaction to a random store

            quantity = random.randint(1, 5)
            if random.random() < 0.05: quantity = -quantity
            unit_price = round(random.uniform(10.0, 100.0), 2)
            total_amount = round(quantity * unit_price, 2)
            discount = round(random.uniform(0, 20), 2)
            if random.random() < 0.02: discount = -discount

            data.append({
                "transaction_id": transaction_id_counter, "store_id": store_id, "event_time": ts,
                "customer_id": customer_id, "product_id": product_id, "quantity": quantity,
                "unit_price": unit_price, "total_amount": total_amount, "payment_method": random.choice(payment_methods),
                "discount_applied": discount, "tax_amount": round(total_amount * 0.08, 2)
            })
            transaction_id_counter += 1

        if data:
            df = spark.createDataFrame(data)
            df.write.format("delta").mode("append").save(path)
            print(f"[{datetime.now()}] Sales stream: Ingested {len(data)} new transactions.")


        time.sleep(config['BATCH_INTERVAL_S'])

def generate_customer_stream(path: str, config: dict):
    global existing_customers
    existing_customers = load_existing_ids(path, "customer_id")
    
    if not existing_customers:
        print(f"[{datetime.now()}] Customer stream: Bootstrapping initial {config['INITIAL_CUSTOMER_COUNT']} customers...")
        now = datetime.now(timezone.utc)
        initial_data = []
        for cid in range(1, config['INITIAL_CUSTOMER_COUNT'] + 1):
            name = fake.name()
            email = fake.email() if random.random() >= 0.02 else None
            initial_data.append({
                "customer_id": cid,
                "name": name,
                "email": email,
                "address": fake.address().replace('\n', ', '),
                "join_date": now - timedelta(days=random.randint(1, 365)),
                "loyalty_points": random.randint(0, 1000),
                "phone_number": f"+1 {random.randint(100000000, 999999999)}", 
                "age": random.randint(18, 70),
                "gender": random.choice(['Male', 'Female', 'Other']),
                "last_update_time": now
            })
        if initial_data:
            df = spark.createDataFrame(initial_data)
            df.write.format("delta").mode("append").save(path)
            existing_customers = [row['customer_id'] for row in initial_data]
            print(f"[{datetime.now()}] Customer stream: Initial bootstrap complete.")

    customer_id_counter = max(existing_customers, default=0) + 1

    while True:
        now = datetime.now(timezone.utc)
        data = []
        num_customers = random.randint(1, config['CUSTOMER_BATCH_SIZE'])

        for _ in range(num_customers):
            ts = now - timedelta(seconds=random.uniform(0, config['LATENCY_MAX_S']))
            if existing_customers and random.random() < 0.3:
                customer_id = random.choice(existing_customers)
            else:
                customer_id = customer_id_counter
                if customer_id not in existing_customers:
                    existing_customers.append(customer_id)
                customer_id_counter += 1

            name = fake.name()
            email = fake.email() if random.random() >= 0.02 else None
            data.append({
                "customer_id": customer_id,
                "name": name,
                "email": email,
                "address": fake.address().replace('\n', ', '),
                "join_date": now - timedelta(days=random.randint(1, 365)),
                "loyalty_points": random.randint(0, 1000),
                "phone_number": fake.phone_number(),
                "age": random.randint(18, 70),
                "gender": random.choice(['Male', 'Female', 'Other']),
                "last_update_time": ts
            })
        
        if data:
            df = spark.createDataFrame(data)
            df.write.format("delta").mode("append").save(path)
            print(f"[{datetime.now()}] Customer stream: Ingested {len(data)} new/updated records.")
        time.sleep(config['BATCH_INTERVAL_S'])

def generate_item_stream(path: str, config: dict):
    global existing_products
    existing_products = load_existing_ids(path, "product_id")

    if not existing_products:
        print(f"[{datetime.now()}] Item stream: Bootstrapping initial {config['INITIAL_PRODUCT_COUNT']} products...")
        now = datetime.now(timezone.utc)
        initial_data = []
        for pid in range(1, config['INITIAL_PRODUCT_COUNT'] + 1):
            name = fake.word().capitalize() + " " + fake.color_name() + " " + random.choice(sizes)
            initial_data.append({
                "product_id": pid,
                "name": name,
                "category": random.choice(categories),
                "brand": random.choice(brands),
                "price": round(random.uniform(10.0, 150.0), 2),
                "stock_quantity": random.randint(0, 200),
                "size": name.split()[-1],
                "color": name.split()[1] if len(name.split()) > 1 else fake.color_name(),
                "description": fake.sentence(nb_words=8),
                "last_update_time": now
            })
        if initial_data:
            df = spark.createDataFrame(initial_data)
            df.write.format("delta").mode("append").save(path)
            existing_products = [row['product_id'] for row in initial_data]
            print(f"[{datetime.now()}] Item stream: Initial bootstrap complete.")

    product_id_counter = max(existing_products, default=0) + 1

    while True:
        now = datetime.now(timezone.utc)
        data = []
        num_items = random.randint(1, config['PRODUCT_BATCH_SIZE'])
        for _ in range(num_items):
            ts = now - timedelta(seconds=random.uniform(0, config['LATENCY_MAX_S']))
            name = fake.word().capitalize() + " " + fake.color_name() + " " + random.choice(sizes)
            if existing_products and random.random() < 0.3:
                product_id = random.choice(existing_products)
            else:
                product_id = product_id_counter
                if product_id not in existing_products:
                    existing_products.append(product_id)
                product_id_counter += 1

            data.append({
                "product_id": product_id,
                "name": name,
                "category": random.choice(categories),
                "brand": random.choice(brands),
                "price": round(random.uniform(10.0, 150.0), 2),
                "stock_quantity": random.randint(0, 200),
                "size": name.split()[-1],
                "color": name.split()[1] if len(name.split()) > 1 else fake.color_name(),
                "description": fake.sentence(nb_words=8),
                "last_update_time": ts
            })

        if data:
            df = spark.createDataFrame(data)
            df.write.format("delta").mode("append").save(path)
            print(f"[{datetime.now()}] Item stream: Ingested {len(data)} new/updated records.")
        time.sleep(config['BATCH_INTERVAL_S'])

def generate_store_stream(path: str, config: dict):
    global existing_stores
    existing_stores = load_existing_ids(path, "store_id")

    if not existing_stores:
        print(f"[{datetime.now()}] Store stream: Bootstrapping initial {config['INITIAL_STORE_COUNT']} stores...")
        now = datetime.now(timezone.utc)
        initial_data = []
        for store_id in range(1, config['INITIAL_STORE_COUNT'] + 1):
            if store_id not in existing_stores:
                existing_stores.append(store_id)
            initial_data.append({
                "store_id": store_id,
                "name": f"Store {store_id} - {fake.city()}",
                "address": fake.address().replace('\n', ', '),
                "manager": fake.name() if random.random() >= 0.02 else None,
                "open_date": now - timedelta(days=random.randint(365, 1825)),
                "status": 'Open',
                "phone_number": fake.phone_number(),
                "last_update_time": now
            })
        if initial_data:
            df = spark.createDataFrame(initial_data)
            df.write.format("delta").mode("append").save(path)
            print(f"[{datetime.now()}] Store stream: Initial bootstrap complete.")

    store_id_counter = max(existing_stores, default=0) + 1
    
    while True:
        now = datetime.now(timezone.utc)
        data = []
        num_stores = random.randint(0, config['STORE_BATCH_SIZE'])

        for _ in range(num_stores):
            ts = now - timedelta(seconds=random.uniform(0, config['LATENCY_MAX_S']))
            if existing_stores and random.random() < 0.5:
                store_id = random.choice(existing_stores)
            else:
                store_id = store_id_counter
                if store_id not in existing_stores:
                    existing_stores.append(store_id)
                store_id_counter += 1

            data.append({
                "store_id": store_id,
                "name": f"Store {store_id} - {fake.city()}",
                "address": fake.address().replace('\n', ', '),
                "manager": fake.name() if random.random() >= 0.02 else None,
                "open_date": now - timedelta(days=random.randint(365, 1825)),
                "status": random.choice(['Open', 'Under Renovation']),
                "phone_number": fake.phone_number(),
                "last_update_time": ts
            })

        if data:
            df = spark.createDataFrame(data)
            df.write.format("delta").mode("append").save(path)
            print(f"[{datetime.now()}] Store stream: Ingested {len(data)} new/updated records.")

        time.sleep(config['BATCH_INTERVAL_S'])



streams = [
    # Raw data directories
    (RAW_SALES_PATH, generate_sales_stream),
    (RAW_CUSTOMERS_PATH, generate_customer_stream),
    (RAW_PRODUCTS_PATH, generate_item_stream),
    (RAW_STORES_PATH, generate_store_stream)
]

with ThreadPoolExecutor(max_workers=len(streams)) as executor:
    for path, generator_func in streams:
        executor.submit(
            generator_func, path, CONFIG
        )
    # The following line is for interactive development/notebooks. 
    # In a real job, you might remove it or have other logic.
    executor.shutdown(wait=True)

