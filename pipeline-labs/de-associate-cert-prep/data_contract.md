# Data Contract: Databricks Data Engineer Associate Certification Lab

This document lists every data asset the lab creates: the landing files the generator writes, and every table each notebook writes, with its grain, key columns, and which notebook reads it. Use it as a reference while you code. All names come from `notebooks/variables.py` (catalog `cert_prep_catalog`).

## Unity Catalog objects (created by `00_Setup_Environment.py`)

| Object | Name | Purpose |
| ------ | ---- | ------- |
| Catalog | `cert_prep_catalog` | Holds everything in the lab |
| Schema | `_system` | Checkpoints and metadata |
| Schema | `00_landing` | Raw files (volumes) |
| Schema | `01_bronze` | Raw tables |
| Schema | `02_silver` | Cleaned and conformed tables |
| Schema | `03_gold` | Business aggregations |
| Schema | `dlt_pipeline` | Spare schema for a Lakeflow Spark Declarative Pipelines extension (not used in notebooks 01-05) |
| Volume | `00_landing.customers`, `products`, `sales`, `events` | Landing files (below) |
| Volume | `_system.checkpoints` | Streaming checkpoints and Auto Loader schema locations |

## 📁 Landing files (written by `data_generator.py`, run from `00_Setup_Environment.py`)

| Volume path | Format | Write mode | Records | Fields |
| ----------- | ------ | ---------- | ------- | ------ |
| `/Volumes/cert_prep_catalog/00_landing/customers` | JSON (coalesced to 10 partitions) | overwrite | 10,200 (10,000 customers + 200 duplicates) | customer_id, email, first_name, last_name, registration_date, location, phone, loyalty_tier |
| `/Volumes/cert_prep_catalog/00_landing/products` | CSV with header (coalesced to 5 partitions) | overwrite | 1,010 (1,000 products + 10 duplicates) | product_id, product_name, category, subcategory, price, cost, inventory_count, last_updated |
| `/Volumes/cert_prep_catalog/00_landing/sales` | JSON, 10 appended batches | append | 50,500 rows (50,000 orders in 10 batches of 5,000, plus 50 duplicate orders per batch), plus 500 per run of notebook 02 (below) | order_id, customer_id, order_timestamp, order_status, payment_method, shipping_address, line_items (array of product_id, quantity, unit_price; 1 to 5 items per order) |
| `/Volumes/cert_prep_catalog/00_landing/events` | JSON, 2 appended batches | append | About 103,000 (5,000 sessions of 8 to 12 events per batch, plus 3% duplicates) | event_id, session_id, customer_id, event_timestamp, event_type, product_id, page_url, referrer, device_type, browser, ip_address |

The generator is deterministic: `random` and `numpy` are seeded once from `DATA_GEN_CONFIG["seed"]` (42), every id (customer_id, orphan customer_id, session_id, event_id) comes from the seeded generator, and every date is counted back from `DATA_GEN_CONFIG["base_date"]` (2026-06-30). Two runs write the same records.

Notebook 02 also writes to the sales volume: its "New data arrives" cell copies the first 500 distinct orders (by `order_id`) into one new JSON file dated 2026-07-01 and 2026-07-02 (same time of day) under fixed ids `ORD-202607-X00001` to `X00500`, every time the notebook runs; Bronze grows by 500 rows per run, Silver keeps each id once.

Planted data quality issues:

- **customers**: 200 duplicate rows (copies of random customers, same `customer_id`) re-sent with a later `registration_date` (1 to 30 days before 2026-06-30; the original registration is 180 to 910 days before it); 5% null `first_name` / `last_name`, 10% null `location`, 20% null `phone`, 30% null `loyalty_tier`; extra spaces, swapped case or ALL CAPS in names; about 1% of emails missing the `@`. `phone` is formatted `+1-XXX-XXX-XXXX`.
- **products**: 10 duplicate rows (older catalog snapshots of random products, same `product_id`) with a different `inventory_count` and a `last_updated` 1 to 24 hours before the original's 2026-06-30T00:00:00; `price` drawn log-uniformly from the category's range in `CATEGORY_PRICE_RANGES` (Books $8-40 up to Electronics $20-1,500), so most items sit at the cheap end, written as a string, about 1% negative; `cost` 40-70% of price, about 2% above price; `subcategory` set by the product's item through `ITEM_SUBCATEGORY` (Clothing items not listed there get Men, Women or Kids); 10% null `subcategory` and `cost`, 5% null `inventory_count`; `category` with extra spaces (20%), swapped case (10%) or ALL CAPS (5%). Product ids are `P-1001` to `P-2000`.
- **sales**: `order_timestamp` spread over the 180 days ending 2026-06-30, hour of day centered on 14:00; about 5% of `order_timestamp`s moved up to 30 minutes earlier. Customers and products are picked with a truncated Lomax skew: the top 20% of products carry about 77% of line items, the top 20% of customers about 70% of orders. `quantity` is 1 (75%), 2 (20%) or 3 (5%); `unit_price` is the product's catalog price (absolute value), 20% of lines discounted 5-20%. 1% duplicate orders per batch (same `order_id`); about 1% of orders carry a `customer_id` that is not in the customer file; about 0.5% of line items have a quantity of 0 or -1; 5% null `payment_method` and `shipping_address`. `order_status` is completed (85%), pending (10%) or cancelled (5%).
- **events**: one day and start time per session (same 180-day window, hour centered on 14:00), events 30 seconds apart; about 5% of event timestamps moved up to 10 minutes earlier; 3% duplicate events; 30% of sessions have no `customer_id`; `product_id` only on view_product, add_to_cart, remove_from_cart and checkout_complete, and about 2% of those point to a product that does not exist; about 0.5% empty `event_type`; 40% null `referrer`.

## 🥉 Bronze Layer (`cert_prep_catalog.01_bronze`)

Column types in the Auto Loader tables are inferred (`cloudFiles.inferColumnTypes = true`), and Auto Loader adds a `_rescued_data` column.

### `customers_managed`

| | |
| - | - |
| Written by | `01_Environment_Setup_Unity_Catalog.py` (Exercise 1: Create a Managed Table), `saveAsTable`, overwrite |
| Source | Customer landing JSON, read with `spark.read.json` |
| Grain | One row per landing record (10,200, duplicates included) |
| Read by | `01` only (`DESCRIBE DETAIL`) |

### `customers_raw`

| | |
| - | - |
| Written by | `02_Auto_Loader_Incremental_Ingestion.py`, Auto Loader (JSON) with `availableNow`, append, checkpoint `_system/checkpoints/customers_stream` |
| Grain | One row per landing record (duplicates kept) |
| Columns | The 8 landing fields + `_rescued_data` |
| Read by | `03_Bronze_to_Silver.py` (customers Silver), `05_Production_Workflows_Jobs.py` (existence check) |

### `products_raw`

| | |
| - | - |
| Written by | `02`, Auto Loader (CSV, header, `schemaEvolutionMode = addNewColumns`) with `availableNow`, append, checkpoint `_system/checkpoints/products_stream` |
| Grain | One row per landing record (duplicates kept) |
| Columns | The 8 landing fields + `_rescued_data` |
| Read by | `03` (products Silver), `05` (existence check) |

### `sales_raw`

| | |
| - | - |
| Written by | `02`, Auto Loader (JSON) with `availableNow`, append, checkpoint `_system/checkpoints/sales_stream`; run twice in `02`: Exercise 4 (50,500 rows), then again after the 500-order practice file lands (+500) |
| Grain | One row per order (line items still nested in `line_items`); duplicate orders kept |
| Columns | The 7 landing fields + `_rescued_data` |
| Read by | `03` (as a stream, for sales Silver), `05` (existence check) |

### `events_raw`

| | |
| - | - |
| Written by | `02`, `CREATE TABLE IF NOT EXISTS` with 9 declared columns, then `COPY INTO` (JSON, `COPY_OPTIONS ('mergeSchema' = 'true')`) |
| Declared columns | event_id, customer_id, event_type, product_id, event_timestamp, device_type, browser, session_id, page_url (all STRING); `mergeSchema` adds the remaining landing fields (referrer, ip_address): 11 columns |
| Grain | One row per landing event (duplicates kept) |
| Read by | `02` only (verification and record counts). Events stay in Bronze: no notebook builds Silver or Gold from them |

## 🥈 Silver Layer (`cert_prep_catalog.02_silver`)

### `customers_clean`

| | |
| - | - |
| Written by | `03_Bronze_to_Silver.py`, batch `saveAsTable`, overwrite |
| Grain / key | One row per `customer_id` (deduplicated with `row_number()` over `customer_id`, ordered by `registration_date` ascending: the first registration is the account creation date) |
| Filter | `customer_id` not null AND (`is_valid_email` OR `is_valid_phone`) |
| Read by | `03` (sales join, SCD2 initial load and simulated updates, quality metrics) |

| Field | Type | Description |
| ----- | ---- | ----------- |
| customer_id | string | Customer identifier |
| first_name, last_name | string | Trimmed, `initcap` |
| email | string | Trimmed, lowercase |
| phone | string | Digits only, country code removed |
| location | string | As in Bronze |
| loyalty_tier | string | Bronze, Silver, Gold, Platinum, or null |
| account_created_date | as inferred in Bronze | Renamed from `registration_date` |
| processed_at | timestamp | Processing time |
| is_valid_email | boolean | Email matches the email regex |
| is_valid_phone | boolean | 10 digits after removing the +1 country code |
| data_quality_score | int | 0 to 4: email present + phone present + valid email + valid phone |

### `products_clean`

| | |
| - | - |
| Written by | `03`, batch `saveAsTable`, overwrite |
| Grain / key | One row per `product_id` (deduplicated with `row_number()` over `product_id`, ordered by `last_updated` descending: the latest catalog snapshot wins) |
| Filter | `product_id` and `product_name` not null AND `is_valid_price` |
| Read by | `03` (sales join, quality metrics) |

| Field | Type | Description |
| ----- | ---- | ----------- |
| product_id | string | `P-1001` to `P-2000` |
| product_name | string | Trimmed |
| category | string | Trimmed, initcap: exactly 5 values (Books, Clothing, Electronics, Home & Garden, Sports & Outdoors) |
| subcategory | string | Trimmed, may be null |
| price | decimal(10,2) | Cast from Bronze |
| cost | decimal(10,2) | Cast from Bronze, may be null |
| processed_at | timestamp | Processing time |
| is_valid_price | boolean | 0 < price < 100,000 |
| is_valid_cost | boolean | 0 < cost <= price |
| profit_margin | decimal | (price - cost) / price * 100, null when cost is null |

### `sales_clean`

| | |
| - | - |
| Written by | `03`, Structured Streaming from `sales_raw` (`dropDuplicates(["order_id"])`, then `line_items` exploded, then stream-static join to `customers_clean` and `products_clean`), `availableNow`, append, checkpoint `_system/checkpoints/sales_silver_stream`; only rows with `is_valid_sale = true` |
| Grain | One row per order line item; duplicate orders removed on `order_id` |
| Read by | `04_Silver_to_Gold_Advanced.py` (all three Gold tables, completed orders only), `03` (quality dashboard) |

| Field | Type | Description |
| ----- | ---- | ----------- |
| order_id | string | Order identifier |
| customer_id | string | May not exist in `customers_clean` (left join) |
| customer_name | string | first_name + " " + last_name (`concat_ws`, so a null name part is skipped); `Unknown customer` when the customer is not in `customers_clean` (detected with the `in_dim` marker from the customer side of the join; a known customer can have both names null) |
| product_id | string | From the line item |
| product_name, category, subcategory | string | From `products_clean`; never null for `product_name` and `category` (lines whose product is not in `products_clean` fail `is_valid_sale`) |
| quantity | int | From the line item |
| unit_price | decimal(10,2) | From the line item |
| total_amount | decimal | quantity * unit_price: the LINE amount, not the order total |
| order_status | string | completed, pending, cancelled |
| payment_method | string | May be null |
| order_timestamp | as inferred in Bronze | Order time |
| loyalty_tier | string | From `customers_clean` |
| processed_at | timestamp | Processing time |
| is_valid_sale | boolean | customer_id and product_id not null, quantity > 0, total_amount > 0, product_name not null |

### `dim_customers_scd2`

| | |
| - | - |
| Written by | `03`: created from `customers_clean` if it does not exist (all rows `version = 1`, `is_current = true`); then a `MERGE` closes the current rows of 5 simulated updates (the first 5 customers by `customer_id` with tier Bronze, Silver or Gold, each moved up one tier), and new versions (`version = 2`) are appended only for customers left with no current row, so a re-run inserts nothing |
| Grain / key | One row per `customer_id` + `version`; one row per customer with `is_current = true` |
| Read by | `03` only (verification) |

Columns: customer_id, first_name, last_name, email, phone, location, loyalty_tier, account_created_date (date), effective_start_date (date), effective_end_date (date, null while current), is_current (boolean), version (int).

## 🥇 Gold Layer (`cert_prep_catalog.03_gold`)

All three are written by `04_Silver_to_Gold_Advanced.py` from `sales_clean` filtered to `order_status = 'completed'` (batch, `saveAsTable`, overwrite) and read only inside `04` (`ALTER TABLE ... CLUSTER BY`, `OPTIMIZE` / `OPTIMIZE FULL`, `DESCRIBE DETAIL`, `VACUUM`, `ANALYZE TABLE`). None is partitioned.

### `daily_sales_summary`

| | |
| - | - |
| Grain / key | One row per `sale_date` + `category` |
| Clustered by | `sale_date`, `category` (liquid clustering, set by the writer's `clusterBy`) |
| Columns | sale_date (date of `order_timestamp`), category, total_revenue, order_count (distinct `order_id`), unique_customers, total_units_sold, avg_order_value (total_revenue / order_count), revenue_per_customer, units_per_order, revenue_7day_avg, revenue_30day_avg, orders_7day_total, orders_30day_total (rolling windows per category over the last 7 and 30 calendar days: `rangeBetween` over the day number of `sale_date`) |

### `customer_lifetime_value`

| | |
| - | - |
| Grain / key | One row per known customer (`customer_id` + `customer_name` + `loyalty_tier`); `Unknown customer` lines are excluded |
| Clustered by | `customer_segment`, `ltv_category` (`ALTER TABLE ... CLUSTER BY` then `OPTIMIZE ... FULL` in `04`) |
| Columns | customer_id, customer_name, loyalty_tier, total_spend, total_orders (distinct `order_id`), first_purchase_date, last_purchase_date (both DATE: `to_date` of the earliest / latest `order_timestamp`), total_items_purchased, categories_purchased, avg_order_value (total_spend / total_orders), days_since_first_purchase, days_since_last_purchase (both measured against `as_of_date`, the latest order date in `sales_clean`, not the run date), customer_tenure_days, recency_score, frequency_score, monetary_score (each 1 to 5, `ntile(5)`, 5 = best, ties broken on `customer_id` ascending), rfm_score (3 to 15), customer_segment (Champions, Loyal Customers, Potential Loyalists, At Risk, Hibernating, Lost), ltv_category (High Value >= 90th percentile of total_spend, Medium Value >= 70th percentile, else Low Value; percentiles computed from the data on each run) |

### `product_performance`

| | |
| - | - |
| Grain / key | One row per `product_id` + `product_name` + `category` + `subcategory` |
| Clustered by | `category`, `performance_tier` (`ALTER TABLE ... CLUSTER BY` then `OPTIMIZE ... FULL` in `04`) |
| Columns | product_id, product_name, category, subcategory, total_revenue, total_units_sold, total_orders (distinct `order_id`), unique_customers, avg_selling_price, revenue_per_unit, orders_per_customer, revenue_rank_in_category (`rank()`), revenue_row_number (`row_number()`), category_total_revenue, market_share_pct, cumulative_market_share, performance_tier (Top Performer, Strong Performer, Average Performer, Underperformer) |

## Notebooks that write no tables

- `05_Production_Workflows_Jobs.py` creates widgets (`environment`, `pipeline_mode`, `date`, `enable_quality_checks`, and `job_id` / `run_id` for the job context), checks the landing folders and Bronze tables exist (and raises if one is missing), and removes the widgets at the end.
