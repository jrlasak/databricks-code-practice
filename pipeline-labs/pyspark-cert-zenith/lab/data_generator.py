"""
Zenith Online - Synthetic Data Generator for Spark Certification Project

This script generates synthetic data for a fictional e-commerce platform, "Zenith Online."
It is designed to provide the necessary data for a learning project preparing for the
Databricks Certified Associate Developer for Apache Spark exam.

The script produces three distinct datasets:
1.  A batch file of customer profiles in CSV format.
2.  A batch file of product details in Parquet format.
3.  A continuous stream of user events as individual JSON files, simulating a real-time feed.

------------------------------------------------------------------------------------
Data Generation Logic & Intentionally Introduced Challenges
------------------------------------------------------------------------------------

1.  **Batch Data (`generate_batch_data`)**:
    *   **Customers (CSV):** A list of customers with static information. One file is generated
      at the start of the run to represent a dimension table that is loaded infrequently.
    *   **Products (Parquet):** A catalog of products. This is also generated as a single file
      at the start. The Parquet format is chosen to cover this common columnar format in Spark.

2.  **Streaming Data (`generate_streaming_events`)**:
    *   **User Events (JSON):** This function runs in a continuous loop to simulate a stream
      of user actions (`view_product`, `add_to_cart`, `purchase`). Each batch of events is
      written as a new JSON file to a specific directory, which Databricks Auto Loader
      or `readStream` can pick up.
    *   **Data Skew (Critical for the Project):** The generator intentionally introduces data skew.
      A small subset of `product_id`s (defined in `POPULAR_PRODUCT_IDS`) has a significantly
      higher probability of appearing in `view_product` events. This is designed to test
      the student's ability to identify and handle data skew during aggregation tasks
      in the Spark project (e.g., using salting).
    *   **Referential Integrity:** The event stream only uses `user_id`s and `product_id`s that
      were previously generated in the batch customer and product files, ensuring that joins
      in the downstream pipeline will be meaningful.

------------------------------------------------------------------------------------
How to Use in Databricks
------------------------------------------------------------------------------------

1.  **Create a Project Volume:** In your Databricks workspace, create a Volume named
    `zenith_online` (or choose another name and update the `BASE_PATH` variable).
2.  **Run the Script:** Copy and paste this code into a new Databricks notebook and run it.
    The script will create the necessary directory structure within the Volume.
3.  **Observe Data Generation:** The script will print status messages as it writes batch
    files and then each new stream file.
4.  **Stop the Stream:** To stop generating data, interrupt or cancel the notebook execution.

This setup provides a realistic starting point (the "raw" layer) for a Bronze-Silver-Gold
ETL pipeline project.
"""

import os
import json
import csv
import random
import time
import uuid
from datetime import datetime, timezone, timedelta

from variables import *

# Try to import pandas and pyarrow, which are needed for Parquet export.
# These are pre-installed in Databricks runtimes.
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
    # These 5 products will be viewed much more frequently than others.
    "POPULAR_PRODUCT_IDS": [101, 210, 315, 420, 55],
    "SKEW_FACTOR_PROBABILITY": 0.75, # 75% of 'view_product' events will be for a popular product.

    # Streaming Mode: If True, stream is infinite; if False, only one batch is generated.
    "STREAM_INFINITE": True
}

# --- Pre-generated lists for creating realistic data ---
first_names = ['Liam', 'Olivia', 'Noah', 'Emma', 'Oliver', 'Ava', 'Elijah', 'Charlotte', 'William', 'Sophia']
last_names = ['Smith', 'Johnson', 'Williams', 'Brown', 'Jones', 'Garcia', 'Miller', 'Davis', 'Rodriguez', 'Martinez']
locations = ['New York', 'Los Angeles', 'Chicago', 'Houston', 'Phoenix', 'Philadelphia', 'San Antonio', 'San Diego']

product_adjectives = ['Classic', 'Modern', 'Rustic', 'Ergonomic', 'Sleek', 'Durable', 'Lightweight', 'Heavy Duty']
product_nouns = ['Chair', 'Table', 'Lamp', 'Desk', 'Bookcase', 'Sofa', 'Clock', 'Keyboard', 'Monitor']
categories = ['Furniture', 'Home Decor', 'Electronics', 'Office Supplies']

# --- Global lists to ensure referential integrity ---
existing_customer_ids = []
existing_product_ids = []

def generate_batch_data():
    """
    Generates the initial batch files for customers (CSV) and products (Parquet).
    This function runs once at the beginning of the script.
    """
    print(f"[{datetime.now()}] Starting generation of initial batch data...")

    # --- 1. Generate Customer Data (CSV) ---
    customers_path = CONFIG["RAW_BATCH_CUSTOMERS_PATH"]
    ##os.makedirs(customers_path, exist_ok=True)
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
        product_name = f"{random.choice(product_adjectives)} {random.choice(product_nouns)}"
        category = random.choice(categories)
        price = round(random.uniform(15.50, 899.99), 2)
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
    Generates a continuous stream of user event data in JSON format.
    If infinite is True, runs in a loop until the script is manually stopped.
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
    print(f"Data Skew active: {CONFIG['SKEW_FACTOR_PROBABILITY']*100}% of 'view_product' events target {CONFIG['POPULAR_PRODUCT_IDS']}")
    print(f"Streaming mode: {'infinite' if infinite else 'single batch'}")
    print("-" * 50)

    def write_event_batch():
        events = []
        now = datetime.now(timezone.utc)
        for _ in range(stream_events_per_batch):
            event_type = random.choices(['view_product', 'add_to_cart', 'purchase'], weights=[0.7, 0.2, 0.1], k=1)[0]
            if event_type == 'view_product' and random.random() < CONFIG['SKEW_FACTOR_PROBABILITY']:
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
        file_timestamp = datetime.now().strftime('%Y%m%d_%H%M%S_%f')
        output_filepath = os.path.join(streaming_path, f"events_{file_timestamp}.json")
        with open(output_filepath, 'w') as f:
            json.dump(events, f, indent=4)
        print(f"[{datetime.now()}] Wrote batch of {len(events)} events to {output_filepath}")

    if infinite:
        while True:
            write_event_batch()
            time.sleep(stream_batch_interval_s)
    else:
        write_event_batch()


# # --- Main Execution Block ---
# if __name__ == "__main__":
#     # First, generate the one-time batch files
#     generate_batch_data()
    
#     # Then, start the continuous stream generation
#     # This will run indefinitely until the notebook cell is interrupted or cancelled,
#     # or just once if CONFIG["STREAM_INFINITE"] is set to False.
#     generate_streaming_events()