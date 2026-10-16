# Databricks notebook source
# MAGIC %md
# MAGIC # Synthetic Data Generator for the Data Engineer Associate Certification Lab
# MAGIC
# MAGIC This notebook generates realistic e-commerce data with intentional quality issues for learning purposes.
# MAGIC
# MAGIC **Features:**
# MAGIC - Generates customers, products, sales, and web events
# MAGIC - Injects data quality issues (duplicates, nulls, format inconsistencies)
# MAGIC - Creates data skew with a truncated Lomax (Pareto) distribution: a few products and customers carry most of the sales
# MAGIC - Spreads orders over the 180 days ending 2026-06-30, with a time-of-day pattern (peak at 14:00)
# MAGIC - Seeded: every run writes the same records (no clock reads, no random UUIDs)
# MAGIC - Sized for Databricks Free Edition: 50,000 orders and about 100,000 events before the planted duplicates
# MAGIC
# MAGIC **Estimated Runtime**: 5-10 minutes
# MAGIC
# MAGIC **Prerequisites**: Run `00_Setup_Environment` (it runs this notebook for you)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Import Configuration and Libraries

# COMMAND ----------

# MAGIC %run ./variables

# COMMAND ----------

import math
import random
import uuid
import json
from datetime import datetime, timedelta
import numpy as np
from pyspark.sql import functions as F
from pyspark.sql.types import *

# Seed once from the config: every id, value and date below comes from these two generators
random.seed(DATA_GEN_CONFIG["seed"])
np.random.seed(DATA_GEN_CONFIG["seed"])

# Every date is counted back from a fixed day, never from the clock
BASE_DATE = datetime.strptime(DATA_GEN_CONFIG["base_date"], "%Y-%m-%d")

print("✅ Libraries imported successfully")
print(f"   Seed: {DATA_GEN_CONFIG['seed']}, base date: {BASE_DATE.date()}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Helper Functions

# COMMAND ----------

def seeded_uuid():
    """A UUID drawn from the seeded `random` generator, so every run produces the same ids (uuid.uuid4() would not)."""
    return str(uuid.UUID(int=random.getrandbits(128), version=4))

def inject_nulls(value, null_rate):
    """
    Randomly return None based on null_rate probability.

    Args:
        value: The value to potentially nullify
        null_rate: Probability of returning None (0.0 to 1.0)

    Returns:
        Original value or None
    """
    return None if random.random() < null_rate else value

def inject_format_inconsistency(value, field_type="text"):
    """
    Randomly introduce format inconsistencies.

    Args:
        value: The value to modify
        field_type: Type of field (text, email, etc.)

    Returns:
        Modified value with format inconsistencies
    """
    if value is None:
        return None

    rand = random.random()

    if field_type == "text":
        if rand < 0.2:  # 20% extra whitespace
            return f"  {value}  "
        elif rand < 0.3:  # 10% mixed case
            return value.swapcase()
        elif rand < 0.35:  # 5% ALL CAPS
            return value.upper()

    elif field_type == "email":
        if rand < 0.1:  # 10% mixed case
            return ''.join(c.upper() if i % 2 else c.lower()
                          for i, c in enumerate(value))

    return value

def weighted_random_choice(choices_dict):
    """
    Make a random choice based on weighted probabilities.

    Args:
        choices_dict: Dict with choices as keys and weights as values

    Returns:
        Selected choice
    """
    choices = list(choices_dict.keys())
    weights = list(choices_dict.values())
    return random.choices(choices, weights=weights, k=1)[0]

def generate_skewed_index(num_items, skew_factor=0.8):
    """
    Pick an index with a truncated Lomax (Pareto) distribution. Index 0 is the most popular item.
    Shape 1.16 puts ~77% of picks in the top 20% (products), 0.9 puts ~70% there (customers);
    the top product gets ~1% of picks.

    Args:
        num_items: Total number of items
        skew_factor: 0.8 for the product skew, 0.7 for the customer skew

    Returns:
        Index (int) in [0, num_items)
    """
    shape = 1.16 if skew_factor >= 0.8 else 0.9
    while True:
        idx = int(np.random.pareto(shape) * num_items * 0.1)
        if idx < num_items:
            return idx

def add_timestamp_jitter(timestamp, max_minutes=30):
    """
    Shift 5% of timestamps up to max_minutes earlier (a source clock that runs behind).

    Args:
        timestamp: Original timestamp
        max_minutes: Maximum minutes of jitter

    Returns:
        Modified timestamp
    """
    if random.random() < 0.05:  # 5% of timestamps shifted earlier
        jitter_minutes = random.randint(-max_minutes, 0)
        return timestamp + timedelta(minutes=jitter_minutes)
    return timestamp

# COMMAND ----------

# MAGIC %md
# MAGIC ## Generate Customer Data

# COMMAND ----------

def generate_customers(num_customers):
    """
    Generate synthetic customer data with quality issues.

    Args:
        num_customers: Number of customers to generate

    Returns:
        List of customer dicts
    """
    print(f"Generating {num_customers:,} customers...")

    customers = []
    first_names = ["John", "Jane", "Michael", "Emily", "David", "Sarah", "Robert", "Lisa",
                   "James", "Mary", "William", "Patricia", "Richard", "Jennifer", "Thomas"]
    last_names = ["Smith", "Johnson", "Williams", "Brown", "Jones", "Garcia", "Miller",
                  "Davis", "Rodriguez", "Martinez", "Hernandez", "Lopez", "Gonzalez"]

    # Generate base customers
    for i in range(num_customers):
        customer_id = seeded_uuid()
        first_name = random.choice(first_names)
        last_name = random.choice(last_names)
        email_base = f"{first_name.lower()}.{last_name.lower()}@email.com"

        # Add user number to avoid exact duplicates
        email = f"{first_name.lower()}.{last_name.lower()}.{i}@email.com"

        customer = {
            "customer_id": customer_id,
            "email": email,
            "first_name": inject_nulls(inject_format_inconsistency(first_name, "text"),
                                      DATA_GEN_CONFIG["null_rate_low"]),
            "last_name": inject_nulls(inject_format_inconsistency(last_name, "text"),
                                     DATA_GEN_CONFIG["null_rate_low"]),
            # Customers register before the 180-day order window starts
            "registration_date": (BASE_DATE - timedelta(days=random.randint(180, 910))).strftime("%Y-%m-%d"),
            "location": inject_nulls(random.choice(US_LOCATIONS),
                                    DATA_GEN_CONFIG["null_rate_medium"]),
            "phone": inject_nulls(f"+1-{random.randint(200,999)}-{random.randint(100,999)}-{random.randint(1000,9999)}",
                                 DATA_GEN_CONFIG["null_rate_high"]),
            "loyalty_tier": inject_nulls(weighted_random_choice(LOYALTY_TIER_WEIGHTS),
                                        0.30)
        }

        # Inject email format issues (1%)
        if random.random() < 0.01:
            customer["email"] = customer["email"].replace("@", "")  # Invalid email

        customers.append(customer)

    # Inject duplicates (2%): the same customer record re-sent with a later registration_date; Silver keeps the first registration
    num_duplicates = int(num_customers * DATA_GEN_CONFIG["duplicate_rate"])
    for _ in range(num_duplicates):
        duplicate = random.choice(customers).copy()
        duplicate["registration_date"] = (BASE_DATE - timedelta(days=random.randint(1, 30))).strftime("%Y-%m-%d")
        customers.append(duplicate)

    print(f"✅ Generated {len(customers):,} customer records (including {num_duplicates:,} duplicates)")
    return customers

# COMMAND ----------

# Generate and save customers
customers = generate_customers(DATA_GEN_CONFIG["num_customers"])

# Convert to Spark DataFrame
customers_df = spark.createDataFrame(customers)

# Write to landing volume as JSON
customers_df.coalesce(10).write.mode("overwrite").format("json").save(CUSTOMERS_LANDING_PATH)

print(f"✅ Customer data written to: {CUSTOMERS_LANDING_PATH}")
print(f"   File count: {len(dbutils.fs.ls(CUSTOMERS_LANDING_PATH))}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Generate Product Data

# COMMAND ----------

def generate_products(num_products):
    """
    Generate synthetic product data with quality issues.

    Args:
        num_products: Number of products to generate

    Returns:
        List of product dicts
    """
    print(f"Generating {num_products:,} products...")

    products = []

    product_name_templates = {
        "Electronics": [
            "Wireless {adj} {item}",
            "Smart {item}",
            "{adj} {item} Pro",
            "Premium {item}"
        ],
        "Clothing": [
            "{adj} {item}",
            "Classic {item}",
            "Designer {item}",
            "Comfort {item}"
        ],
        "Home & Garden": [
            "{adj} {item}",
            "Modern {item}",
            "Rustic {item}",
            "Elegant {item}"
        ],
        "Sports & Outdoors": [
            "{adj} {item}",
            "Pro {item}",
            "Adventure {item}",
            "Performance {item}"
        ],
        "Books": [
            "The {adj} {item}",
            "{item}: A Story",
            "The Complete {item}",
            "Guide to {item}"
        ]
    }

    adjectives = ["Premium", "Deluxe", "Essential", "Ultimate", "Professional", "Advanced"]

    items_by_category = {
        "Electronics": ["Headphones", "Speaker", "Monitor", "Keyboard", "Mouse", "Tablet", "Charger", "Camera"],
        "Clothing": ["T-Shirt", "Jeans", "Jacket", "Dress", "Sweater", "Shoes", "Hat", "Scarf"],
        "Home & Garden": ["Chair", "Table", "Lamp", "Rug", "Curtains", "Vase", "Clock", "Mirror"],
        "Sports & Outdoors": ["Bike", "Tent", "Backpack", "Yoga Mat", "Dumbbells", "Water Bottle"],
        "Books": ["Mystery", "Adventure", "Science", "History", "Fiction", "Philosophy"]
    }

    for i in range(num_products):
        product_id = f"P-{1001 + i}"
        category = weighted_random_choice(CATEGORY_WEIGHTS)

        # Generate product name; the item decides the subcategory
        template = random.choice(product_name_templates[category])
        item = random.choice(items_by_category[category])
        adj = random.choice(adjectives)
        product_name = template.format(adj=adj, item=item)
        subcategory = ITEM_SUBCATEGORY.get(item) or random.choice(["Men", "Women", "Kids"])

        # Generate prices inside the category's range, log-uniform: most items sit at the cheap end
        # of the range, as in a real catalog; cost is 40-70% of price
        price_min, price_max = CATEGORY_PRICE_RANGES[category]
        base_price = round(math.exp(random.uniform(math.log(price_min), math.log(price_max))), 2)
        cost = round(base_price * random.uniform(0.4, 0.7), 2)

        # Inject margin issues (2%): cost above price, a data entry error in the catalog
        if random.random() < 0.02:
            cost = round(base_price * random.uniform(1.05, 1.3), 2)

        product = {
            "product_id": product_id,
            "product_name": product_name,
            "category": inject_format_inconsistency(category, "text"),
            "subcategory": inject_nulls(subcategory, DATA_GEN_CONFIG["null_rate_medium"]),
            "price": str(base_price),  # Written as a string, as the catalog export sends it; notebook 03 casts it to decimal(10,2)
            "cost": inject_nulls(str(cost), DATA_GEN_CONFIG["null_rate_medium"]),
            "inventory_count": inject_nulls(random.randint(0, 500), DATA_GEN_CONFIG["null_rate_low"]),
            "last_updated": BASE_DATE.strftime("%Y-%m-%dT%H:%M:%S")
        }

        # Inject negative prices (1%)
        if random.random() < 0.01:
            product["price"] = str(-abs(float(product["price"])))

        products.append(product)

    # Inject duplicates (1%): an older catalog snapshot of the same product_id, different inventory
    num_duplicates = int(num_products * 0.01)
    for _ in range(num_duplicates):
        duplicate = random.choice(products).copy()
        duplicate["inventory_count"] = random.randint(0, 500)
        duplicate["last_updated"] = (BASE_DATE - timedelta(hours=random.randint(1, 24))).strftime("%Y-%m-%dT%H:%M:%S")
        products.append(duplicate)

    print(f"✅ Generated {len(products):,} product records (including {num_duplicates:,} duplicates)")
    return products

# COMMAND ----------

# Generate and save products
products = generate_products(DATA_GEN_CONFIG["num_products"])

# Convert to Spark DataFrame
products_df = spark.createDataFrame(products)

# Write to landing volume as CSV (with header)
products_df.coalesce(5).write.mode("overwrite").format("csv").option("header", "true").save(PRODUCTS_LANDING_PATH)

print(f"✅ Product data written to: {PRODUCTS_LANDING_PATH}")
print(f"   File count: {len(dbutils.fs.ls(PRODUCTS_LANDING_PATH))}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Generate Sales Data (Streaming)

# COMMAND ----------

def generate_sales_batch(batch_num, batch_size, customer_ids, product_ids, product_prices):
    """
    Generate a batch of sales transactions with nested line items.

    Args:
        batch_num: Batch number (for order ID generation)
        batch_size: Number of orders to generate
        customer_ids: List of distinct customer IDs (index 0 = most active)
        product_ids: List of distinct product IDs (index 0 = best seller)
        product_prices: Dict of product_id -> catalog price

    Returns:
        List of sales transaction dicts
    """
    sales = []

    for i in range(batch_size):
        # Spread orders over the order_history_days ending BASE_DATE
        order_day = BASE_DATE - timedelta(days=random.randint(0, DATA_GEN_CONFIG["order_history_days"] - 1))
        order_id = f"ORD-{order_day.strftime('%Y%m')}-{batch_num * batch_size + i:05d}"

        # Use skewed distribution for customer and product selection
        customer_id = customer_ids[generate_skewed_index(len(customer_ids),
                                                         DATA_GEN_CONFIG["customer_skew_factor"])]

        # Generate line items (1-5 items per order)
        num_items = random.choices([1, 2, 3, 4, 5], weights=[0.4, 0.3, 0.2, 0.07, 0.03], k=1)[0]
        line_items = []

        for _ in range(num_items):
            product_id = product_ids[generate_skewed_index(len(product_ids),
                                                           DATA_GEN_CONFIG["product_skew_factor"])]
            quantity = random.choices([1, 2, 3], weights=[0.75, 0.2, 0.05], k=1)[0]
            # The line sells at the catalog price; 20% of lines carry a 5-20% discount
            catalog_price = product_prices[product_id]
            discount = random.uniform(0.8, 0.95) if random.random() < 0.2 else 1.0
            unit_price = round(catalog_price * discount, 2)

            # Inject invalid quantities (0.5%)
            if random.random() < 0.005:
                quantity = random.choice([0, -1])

            line_items.append({
                "product_id": product_id,
                "quantity": quantity,
                "unit_price": unit_price
            })

        # Generate order timestamp with time-of-day pattern
        hour_of_day = int(np.random.normal(14, 4))  # Peak at 2 PM
        hour_of_day = max(0, min(23, hour_of_day))
        order_timestamp = order_day.replace(
            hour=hour_of_day,
            minute=random.randint(0, 59),
            second=random.randint(0, 59)
        )

        # Shift 5% of order times up to 30 minutes earlier
        order_timestamp = add_timestamp_jitter(order_timestamp)

        sale = {
            "order_id": order_id,
            "customer_id": customer_id,
            "order_timestamp": order_timestamp.strftime("%Y-%m-%dT%H:%M:%S"),
            "order_status": random.choices(
                ["completed", "pending", "cancelled"],
                weights=[0.85, 0.10, 0.05],
                k=1
            )[0],
            "payment_method": inject_nulls(weighted_random_choice(PAYMENT_METHOD_WEIGHTS),
                                          DATA_GEN_CONFIG["null_rate_low"]),
            "shipping_address": inject_nulls(random.choice(US_LOCATIONS),
                                            DATA_GEN_CONFIG["null_rate_low"]),
            "line_items": line_items
        }

        # Inject invalid customer IDs (1%)
        if random.random() < 0.01:
            sale["customer_id"] = seeded_uuid()  # Non-existent customer

        sales.append(sale)

    # Inject duplicates (1%): the same order sent twice (a client retry, same order_id)
    num_duplicates = int(batch_size * 0.01)
    for _ in range(num_duplicates):
        sales.append(random.choice(sales).copy())

    return sales

# COMMAND ----------

# Generate sales data in batches (simulate streaming)
print(f"Generating {DATA_GEN_CONFIG['num_sales']:,} sales transactions...")

# Distinct customer and product IDs for foreign keys: one slot per id in the skewed pick, so the
# planted duplicate rows add no extra weight
customer_ids = list(dict.fromkeys(c["customer_id"] for c in customers))
product_ids = list(dict.fromkeys(p["product_id"] for p in products))

# Catalog price per product, for the line items. abs(): the ~1% negative prices are a defect of
# the catalog export, the order system charged the real price
product_prices = {}
for p in products:
    product_prices.setdefault(p["product_id"], abs(float(p["price"])))

batch_size = DATA_GEN_CONFIG["streaming_batch_size"]
num_batches = DATA_GEN_CONFIG["num_sales"] // batch_size

for batch_num in range(num_batches):
    sales_batch = generate_sales_batch(batch_num, batch_size, customer_ids, product_ids, product_prices)

    # Convert to Spark DataFrame
    sales_df = spark.createDataFrame(sales_batch)

    # Write batch to landing volume as JSON
    sales_df.write.mode("append").format("json").save(SALES_LANDING_PATH)

    # Progress indicator
    if (batch_num + 1) % 10 == 0:
        print(f"  Progress: {batch_num + 1}/{num_batches} batches ({((batch_num + 1) / num_batches * 100):.1f}%)")

print(f"✅ Sales data written to: {SALES_LANDING_PATH}")
print(f"   File count: {len(dbutils.fs.ls(SALES_LANDING_PATH))}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Generate Web Events Data (Streaming)

# COMMAND ----------

def generate_events_batch(batch_num, batch_size, customer_ids, product_ids):
    """
    Generate a batch of web events.

    Args:
        batch_num: Batch number
        batch_size: Number of events to generate
        customer_ids: List of valid customer IDs
        product_ids: List of valid product IDs

    Returns:
        List of event dicts
    """
    events = []

    # Generate sessions (each session has multiple events)
    num_sessions = batch_size // 10  # ~10 events per session

    for session_idx in range(num_sessions):
        session_id = f"sess_{seeded_uuid().replace('-', '')[:12]}"

        # 30% anonymous users (no customer_id)
        customer_id = None if random.random() < 0.30 else random.choice(customer_ids)

        device_type = weighted_random_choice(DEVICE_TYPE_WEIGHTS)
        browser = weighted_random_choice(BROWSER_WEIGHTS)

        # One day and one start time per session, drawn from the same window as the orders
        session_day = BASE_DATE - timedelta(days=random.randint(0, DATA_GEN_CONFIG["order_history_days"] - 1))
        hour_of_day = max(0, min(23, int(np.random.normal(14, 4))))  # peak at 2 PM
        session_start = session_day.replace(
            hour=hour_of_day,
            minute=random.randint(0, 59),
            second=random.randint(0, 59),
            microsecond=random.randint(0, 999999)
        )

        # Generate 8-12 events per session
        num_events = random.randint(8, 12)

        for event_idx in range(num_events):
            event_id = seeded_uuid()
            event_type = weighted_random_choice(EVENT_TYPES)

            # Product ID is null for some event types
            product_id = None
            if event_type in ["view_product", "add_to_cart", "remove_from_cart", "checkout_complete"]:
                product_id = product_ids[generate_skewed_index(len(product_ids), 0.8)]
                # 2% invalid product IDs
                if random.random() < 0.02:
                    product_id = f"P-{random.randint(10000, 99999)}"

            # Events 30 seconds apart within the session
            event_timestamp = session_start + timedelta(seconds=event_idx * 30)

            # Shift 5% of event times up to 10 minutes earlier
            event_timestamp = add_timestamp_jitter(event_timestamp, max_minutes=10)

            event = {
                "event_id": event_id,
                "session_id": session_id,
                "customer_id": customer_id,
                "event_timestamp": event_timestamp.strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3],
                "event_type": event_type,
                "product_id": product_id,
                "page_url": f"/products/{product_id}" if product_id else "/",
                "referrer": inject_nulls("https://google.com/search", 0.40),
                "device_type": inject_nulls(device_type, DATA_GEN_CONFIG["null_rate_low"]),
                "browser": inject_nulls(browser, DATA_GEN_CONFIG["null_rate_low"]),
                "ip_address": f"192.168.{random.randint(0,255)}.xxx"
            }

            # Inject invalid event types (0.5%)
            if random.random() < 0.005:
                event["event_type"] = ""

            events.append(event)

    # Inject duplicates (3% - page refreshes)
    num_duplicates = int(len(events) * 0.03)
    for _ in range(num_duplicates):
        events.append(random.choice(events).copy())

    return events

# COMMAND ----------

# Generate events data in batches (simulate high-velocity streaming)
print(f"Generating {DATA_GEN_CONFIG['num_events']:,} web events...")

batch_size = DATA_GEN_CONFIG["streaming_batch_size"] * 10  # Larger batches for events
num_batches = DATA_GEN_CONFIG["num_events"] // batch_size

for batch_num in range(num_batches):
    events_batch = generate_events_batch(batch_num, batch_size, customer_ids, product_ids)

    # Convert to Spark DataFrame
    events_df = spark.createDataFrame(events_batch)

    # Write batch to landing volume as JSON
    events_df.write.mode("append").format("json").save(EVENTS_LANDING_PATH)

    # Progress indicator
    if (batch_num + 1) % 5 == 0:
        print(f"  Progress: {batch_num + 1}/{num_batches} batches ({((batch_num + 1) / num_batches * 100):.1f}%)")

print(f"✅ Web events data written to: {EVENTS_LANDING_PATH}")
print(f"   File count: {len(dbutils.fs.ls(EVENTS_LANDING_PATH))}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Verification and Summary

# COMMAND ----------

print("=" * 70)
print("DATA GENERATION COMPLETE")
print("=" * 70)

# Verify files were created
print("\n📁 File Counts:")
print(f"  Customers: {len(dbutils.fs.ls(CUSTOMERS_LANDING_PATH))} files")
print(f"  Products:  {len(dbutils.fs.ls(PRODUCTS_LANDING_PATH))} files")
print(f"  Sales:     {len(dbutils.fs.ls(SALES_LANDING_PATH))} files")
print(f"  Events:    {len(dbutils.fs.ls(EVENTS_LANDING_PATH))} files")

# Calculate approximate file sizes
def get_dir_size(path):
    files = dbutils.fs.ls(path)
    total_size = sum(f.size for f in files)
    return total_size / (1024 * 1024)  # Convert to MB

print("\n💾 Approximate Data Sizes:")
print(f"  Customers: {get_dir_size(CUSTOMERS_LANDING_PATH):.1f} MB")
print(f"  Products:  {get_dir_size(PRODUCTS_LANDING_PATH):.1f} MB")
print(f"  Sales:     {get_dir_size(SALES_LANDING_PATH):.1f} MB")
print(f"  Events:    {get_dir_size(EVENTS_LANDING_PATH):.1f} MB")
print(f"  TOTAL:     {get_dir_size(CUSTOMERS_LANDING_PATH) + get_dir_size(PRODUCTS_LANDING_PATH) + get_dir_size(SALES_LANDING_PATH) + get_dir_size(EVENTS_LANDING_PATH):.1f} MB")

# Sample data from each source
print("\n📊 Sample Records:")

print("\n  Customers (first 3):")
customers_sample = spark.read.format("json").load(CUSTOMERS_LANDING_PATH).limit(3)
customers_sample.show(truncate=False)

print("\n  Products (first 3):")
products_sample = spark.read.format("csv").option("header", "true").load(PRODUCTS_LANDING_PATH).limit(3)
products_sample.show(truncate=False)

print("\n  Sales (first 2):")
sales_sample = spark.read.format("json").load(SALES_LANDING_PATH).limit(2)
sales_sample.show(truncate=False)

print("\n  Events (first 3):")
events_sample = spark.read.format("json").load(EVENTS_LANDING_PATH).limit(3)
events_sample.show(truncate=False)

print("\n" + "=" * 70)
print("✅ Data generation successful!")
print("=" * 70)
print("\nNext Steps:")
print("1. Open notebooks/01_Environment_Setup_Unity_Catalog")
print("2. Work through the notebooks in numerical order")
print("=" * 70)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Data Quality Issues Summary

# COMMAND ----------

print("📋 INTENTIONAL DATA QUALITY ISSUES INJECTED")
print("=" * 70)
print("\nThese issues are designed for learning purposes:\n")

print("CUSTOMERS:")
print("  - Duplicates: 2% (same customer_id re-sent with a later registration_date)")
print("  - Nulls: 5% first_name / last_name, 10% location, 20% phone, 30% loyalty_tier")
print("  - Format: extra spaces / swapped case / ALL CAPS in names")
print("  - Invalid emails: ~1% missing '@'")

print("\nPRODUCTS:")
print("  - Duplicates: 1% (older catalog snapshot, different inventory_count)")
print("  - Nulls: 10% subcategory and cost, 5% inventory_count")
print("  - Format: category with extra spaces (20%), swapped case (10%), ALL CAPS (5%)")
print("  - Invalid prices: ~1% negative")
print("  - Margin issues: ~2% cost > price")

print("\nSALES:")
print("  - Duplicates: ~1% same order_id (client retries)")
print("  - Order time shifted up to 30 minutes earlier: ~5%")
print("  - Unknown customer IDs: ~1%")
print("  - Invalid quantities: ~0.5% zero or negative")
print("  - Skew: top 20% of products ~77% of line items")

print("\nWEB EVENTS:")
print("  - Duplicates: ~3% (page refreshes)")
print("  - Timestamps shifted up to 10 minutes earlier: ~5%")
print("  - Anonymous sessions: 30%")
print("  - Unknown product IDs: ~2%")
print("  - Empty event_type: ~0.5%")

print("\n" + "=" * 70)
print("Notebook 03 handles the customer, product and sales issues in Silver; events stay in Bronze.")
print("=" * 70)
