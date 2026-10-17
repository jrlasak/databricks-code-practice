# Project Plan

## Information

- **File Organization:**  
  All the files you need to work with are in the `dlt/` folder:
  - Setup scripts: `environment_setup.ipynb`, `environment_maintenance.ipynb`
  - Configuration: `variables.py`
  - Data generation: `data_generator.py`
  - Your Lakeflow pipeline tasks: `01_bronze.py`, `02A_silver.py`, `02B_silver.py`, `02C_silver.py`, `02D_silver.py`, `03_gold.py`
  
- **Getting Help When Stuck:**  
  - **Solution code is included:** Each task in the `dlt/0*.py` files has a "Solution Is Below" section with commented-out code you can review or uncomment.
  - **Complete reference:** For a full working pipeline, see `final_code/final_dlt.py`.
  - Try to solve each task yourself first, then check the solution if needed!
  
- **Configuration:**  
  Many files in this repository rely on `dlt/variables.py` to configure your environment and determine where synthetic data is generated. To change the catalog name or other object names, update them in `dlt/variables.py`; a new catalog name must also go into the pipeline's **Default catalog** (step 4) and replace `apparel_store` in the [Check your work](#check-your-work) queries.
  
- **Resource limits (Free Edition):** `dlt/data_generator.py` runs until you stop it. About 20 minutes of generator time (about 30 live batches) gives clear counts in [Check your work](#check-your-work); stop it after that. A transient "Resources exhausted" error clears after a short wait. If you exceed the daily compute quota, Free Edition shuts compute down for the rest of the day; your data and settings are kept.

---

## Databricks Configuration Requirements

To run this project end-to-end, complete the following setup steps in your Databricks workspace:

1. **Create a Free Edition account and import this repository as a Git folder**: see [README.md > How to Start](README.md#5-how-to-start).

2. **Run the `dlt/environment_setup.ipynb` notebook to set up a catalog, schemas and volumes for the synthetic data generator**. It will use the paths defined in `dlt/variables.py` for creating appropriate objects in Unity Catalog.

3. **Run the synthetic data generator (`dlt/data_generator.py`) before you create the pipeline**

   - Open `dlt/data_generator.py` in the workspace editor and click **Run**. It installs `faker` first, then writes a batch to each of the four landing folders every 30 seconds and runs until you stop it. On its first run the sales stream also backfills 30 days of history (about 6,000 sales) before its first live batch of 500.
   - Nothing lands until `faker` installs and the first batches are written: about 5-6 minutes. Wait until the output shows `Sales stream: Ingested 500 new transactions.` at least twice.
   - Before you start the pipeline, run only the HEALTH CHECK cell of `dlt/environment_maintenance.ipynb` (the first code cell, right under the title; never use Run all, because the cells further down drop the tables and delete the generator's files): all four counts must be above 0 and `missing_customers`, `missing_products`, `missing_stores` at 0.
   - Keep the generator running while you create the pipeline and work on the bronze tasks, and stop it after about 20 minutes in total: the rare rules in the Silver checks and the NULL product names in the Gold checks need that long to show above 0.
   - If a pipeline update fails with `DELTA_MISSING_TRANSACTION_LOG` (or `DELTA_TABLE_NOT_FOUND` on a `/Volumes/.../00_landing/streaming/...` path), one landing folder had no data yet: run the generator longer and re-run the update.
   - **Tip:** [SyntheticDataGenerator.md](SyntheticDataGenerator.md) documents the table schemas and the planted data quality issues.

4. **Create and configure a Lakeflow pipeline**

   - In the sidebar click **New** > **ETL pipeline**, name it, and choose **Add existing assets** (not sample code: that adds its own sample tables to the pipeline). This creates a Lakeflow Spark Declarative Pipelines pipeline (formerly Delta Live Tables / DLT); the rest of this guide calls it the Lakeflow pipeline.
   - Under the pipeline name set **Default catalog** to `apparel_store` and **Default schema** to `02_silver`.
   - In the same dialog, set the **Pipeline root folder** to this repo's `dlt/` folder (to change it after the pipeline exists: **Settings** > **Code assets** > **Configure paths**). The root folder is added to the Python path, which is how every task file finds `variables.py`.
   - As **source code paths** add exactly these six files, one by one, NOT the whole `dlt/` folder (the folder also holds `data_generator.py` and the setup notebooks, which must not run inside the pipeline): `dlt/01_bronze.py`, `dlt/02A_silver.py`, `dlt/02B_silver.py`, `dlt/02C_silver.py`, `dlt/02D_silver.py`, `dlt/03_gold.py`. If the pipeline already lists a `transformations` folder or a sample file, remove it from the source code.
   - Never add `final_code/final_dlt.py` next to the task files: it defines the same tables, and the pipeline fails on the duplicates.
   - Free Edition runs pipelines on serverless compute; there is no cluster to configure.
   - **Tip:** the pipeline produces two dataset types: streaming tables (each new input row is processed once: appended, or for an AUTO CDC target applied as an insert or update) and materialized views (results recomputed from their inputs on each update).

---

## How to Work on the Lakeflow Pipeline

> **Your tasks live in the six `dlt/0*.py` files: one bronze, four silver, one gold.** Work through them in order.

### Working Approach

1. **Read the file headers**: Each `dlt/0*.py` file starts with detailed comments explaining:

   - The goal of that pipeline layer
   - Tips and best practices
   - What you need to implement

2. **Follow the task order**:

   - **`dlt/01_bronze.py`** - Start here: Ingest raw data from the landing folders (Tasks 1-4)
   - **`dlt/02A_silver.py`** - Silver: cleaned views with data quality expectations for all four streams (Tasks 5-8)
   - **`dlt/02B_silver.py`** - Silver: SCD Type 2 dimensions for customers, products and stores (Tasks 9-11)
   - **`dlt/02C_silver.py`** - Silver: sales and returns fact tables (Tasks 12-13)
   - **`dlt/02D_silver.py`** - Silver: current-version views of the three dimensions (Tasks 14-16)
   - **`dlt/03_gold.py`** - Gold: business-ready aggregations (Tasks 17-20)

3. **Understand the data quirks and the target**: Review [SyntheticDataGenerator.md](SyntheticDataGenerator.md) and [data_contract.md](data_contract.md) to understand:

   - Table schemas for each data stream
   - Intentional data quality issues (negatives, nulls, invalid values, CDC updates)
   - Why certain transformations and expectations are needed
   - Every table your pipeline must produce, with its columns, types and keys (data_contract.md)

4. **Write your code**: Each file has sections with tasks where you'll implement:

   - Pipeline table and view definitions (streaming tables, materialized views, temporary views)
   - Data quality expectations
   - Transformations and aggregations
   - SCD Type 2 logic for dimensions
   
   **Each task includes:**
   - Requirements as checkboxes `[ ]`
   - Business logic explanations
   - A "Solution Is Below" section with commented code you can review or uncomment

5. **Test as you go**:

   - Run a pipeline update after each file (or each task) you finish
   - Do that file's checks in [Check your work](#check-your-work) below
   - Check the pipeline UI for data quality metrics and lineage
   - To watch increments, keep `dlt/data_generator.py` running and start another update: each update processes only the data that landed since the previous one

6. **Stuck?** See [Getting Help When Stuck](#information) above.

7. **See it run incrementally** (after the Gold checks pass):
   - Note the result of `` SELECT COUNT(*) FROM apparel_store.`01_bronze`.bronze_sales `` and of the SCD2 versions query in [Check your work](#check-your-work).
   - Start the generator and stop it once the output shows `Sales stream: Ingested 500 new transactions.` twice (a minute or two after it starts, longer if `faker` has to install again), then run one pipeline update. The restart does not backfill again, because the sales table is no longer empty.
   - `bronze_sales` grows by about 1,000 rows (500 per batch), and the update's metrics for `bronze_sales` show about 1,000 rows written, not the whole table: a streaming table reads only the rows that arrived since the previous update.
   - `silver_customers` gains rows: each batch changes about 4-5 existing customers (30% of 1-30 records), and most changes open a new version, so the re-sent `customer_id`s now have one more version and the previous one has `__END_AT` set.
   - The gold materialized views are recomputed and now include the new sales.

### Key Concepts You'll Practice

- **Bronze Layer**: Schema enforcement, metadata tracking, streaming ingestion
- **Silver Layer**: Data quality expectations, cleansing, SCD Type 2 for dimension tracking
- **Gold Layer**: Business aggregations as materialized views over the streaming fact table, time-based analytics

---

## Check your work

Run these in a notebook or the SQL editor after the pipeline update for that file. Expected values come from the generator's rates (see [SyntheticDataGenerator.md](SyntheticDataGenerator.md)), so they are approximate: counts depend on how long the generator ran, and a 1% rule can still show 0 on a short run (run the generator longer for clearer counts). Stop the generator before you compare counts across tables.

### Bronze (`01_bronze.py`)

```sql
SELECT COUNT(*) AS row_count,
       ROUND(COUNT_IF(quantity < 0) / COUNT(*), 3) AS return_share,
       ROUND(COUNT_IF(discount_applied < 0) / COUNT(*), 3) AS bad_discount_share,
       ROUND(COUNT_IF(payment_method NOT IN ('Cash', 'Credit Card', 'Debit Card', 'Mobile Pay', 'Gift Card')) / COUNT(*), 3) AS bad_payment_share
FROM apparel_store.`01_bronze`.bronze_sales
```

- `row_count`: about 6,000 backfilled rows plus 500 per live batch.
- `return_share` about 0.05, `bad_discount_share` about 0.02, `bad_payment_share` about 0.01.
- `` DESCRIBE TABLE apparel_store.`01_bronze`.bronze_sales `` shows `transaction_id` int, `event_time` timestamp, `unit_price` double, `ingest_timestamp` timestamp and `source_file_path` string. `bronze_customers`, `bronze_products` and `bronze_stores` are non-empty, and `DESCRIBE TABLE` on each shows its IDs and counts as int (`customer_id`, `loyalty_points`, `age`; `product_id`, `stock_quantity`; `store_id`), not the landing tables' bigint, and its dates as timestamp.

### Silver data quality (02A views, checked after 02B and 02C)

Right after `02A_silver.py`, only confirm the update succeeds: the four views are pipeline-only, and nothing reads them until `02B_silver.py` (customers, products, stores) and `02C_silver.py` (sales). After 02B and 02C:

1. Run the table queries below. They are the check that counts.
2. Optionally, click a view (or the table that reads it) in the pipeline graph and open the **Data quality** tab. Metrics for a view may be missing, or may appear once per table that reads it (`sales_cleaned_stream` feeds two tables).

Warn rules keep the row, drop rules remove it. The rates below are over all of a view's records, new and changed together:

- `customers_cleaned_stream`: `valid_gender` fails on about 1.5% of records, `realistic_age` and `valid_email` on under 1%, `realistic_loyalty_points` on about 1% (only new records carry the gender and age defects).
- `products_cleaned_stream`: `valid_product_price` drops just under 1% (cents typed as dollars); `non_negative_stock` fails on just under 2%, `valid_brand` on about 1.5%, `valid_category` on under 1%.
- `stores_cleaned_stream`: `manager_assigned` fails on 1 in 10 store records or more, because 1 in 5 manager changes leaves a vacancy and a vacant store reports 'Unknown' on every later record until it gets a new manager. `valid_status` fails on under 1%: only a new store or a status change can carry the lowercase status, so it usually shows 0 in this lab. With about one store record per batch, `manager_assigned` can also show 0 after a short run.
- `sales_cleaned_stream`: `realistic_discount` fails on about 2% of sales, `valid_payment_method` on about 1%.

```sql
SELECT COUNT_IF(gender NOT IN ('Male', 'Female', 'Other')) AS bad_gender,
       COUNT_IF(email IS NOT NULL AND email NOT LIKE '%@%.%') AS bad_email
FROM apparel_store.`02_silver`.silver_customers;

SELECT COUNT_IF(price >= 1000) AS price_typos_kept,
       COUNT_IF(category = 'Kidswear') AS unknown_category,
       COUNT_IF(brand IS NULL) AS missing_brand
FROM apparel_store.`02_silver`.silver_products;

SELECT COUNT_IF(manager = 'Unknown') AS vacant_manager_rows,
       COUNT_IF(status NOT IN ('Open', 'Under Renovation')) AS bad_status
FROM apparel_store.`02_silver`.silver_stores;

SELECT COUNT_IF(payment_method NOT IN ('Cash', 'Credit Card', 'Debit Card', 'Mobile Pay', 'Gift Card')) AS bad_payment
FROM apparel_store.`02_silver`.silver_sales_transactions;
```

- `bad_gender`, `bad_email`, `unknown_category` and `missing_brand` are warned and kept, but rare: they may show 0 after a short run (a 6-minute run writes only a few live batches) and are reliably above 0 after about 20 minutes of generator time (about 30 live batches). `price_typos_kept` is always 0 (dropped).
- `vacant_manager_rows`, `bad_status` and `bad_payment` are warn rules too, so these rows are kept: `vacant_manager_rows` is above 0 after a longer run, `bad_status` usually stays 0 (see `valid_status` above), and `bad_payment` is about 1% of the sales rows.

### Silver SCD2 dimensions (`02B_silver.py`)

```sql
SELECT customer_id, COUNT(*) AS versions, COUNT_IF(__END_AT IS NULL) AS current_rows
FROM apparel_store.`02_silver`.silver_customers
GROUP BY customer_id
HAVING COUNT(*) > 1
```

Rows come back once the generator has run a few batches (customers with history), and every `current_rows` is exactly 1. Pick one `customer_id` from the result:

```sql
SELECT name, address, email, phone_number, __START_AT, __END_AT
FROM apparel_store.`02_silver`.silver_customers
WHERE customer_id = <one of them>
ORDER BY __START_AT
```

The name is the same in every version, while the address, email or phone number changes from one version to the next; every version but the last has `__END_AT` set. In `silver_products` a price change opens a version and a stock-only change updates the current version in place.

### Silver facts (`02C_silver.py`)

```sql
SELECT
  (SELECT COUNT(*) FROM apparel_store.`01_bronze`.bronze_sales) AS bronze_rows,
  (SELECT COUNT(*) FROM apparel_store.`02_silver`.silver_sales_transactions) AS sales_rows,
  (SELECT COUNT(*) FROM apparel_store.`02_silver`.silver_returns_transactions) AS return_rows,
  (SELECT MIN(returned_quantity) FROM apparel_store.`02_silver`.silver_returns_transactions) AS min_returned_qty;

SELECT COUNT_IF(discount_applied < 0) AS negative_discounts,
       ROUND(COUNT_IF(discount_applied > 0) / COUNT(*), 2) AS discounted_share,
       COUNT_IF(ABS(total_amount - (quantity * unit_price - discount_applied)) > 0.01) AS arithmetic_mismatches
FROM apparel_store.`02_silver`.silver_sales_transactions;
```

- `sales_rows` is about 93% of `bronze_rows` (95% are sales, minus the 2% with a negative discount), `return_rows` about 5%, `min_returned_qty` at least 1.
- `negative_discounts` 0, `discounted_share` about 0.30, `arithmetic_mismatches` 0 (`total_amount` is the net line value).

### Silver current views (`02D_silver.py`)

The views are pipeline-only; check the rule they apply on the SCD2 table:

```sql
SELECT COUNT_IF(__END_AT IS NULL) AS current_rows, COUNT(DISTINCT customer_id) AS customers
FROM apparel_store.`02_silver`.silver_customers
```

The two numbers are equal: one current row per customer. Gold (next file) is where the views are used.

### Gold (`03_gold.py`)

```sql
SELECT
  (SELECT COUNT(*) FROM apparel_store.`02_silver`.silver_sales_transactions) AS silver_sales,
  (SELECT COUNT(*) FROM apparel_store.`03_gold`.denormalized_sales_facts) AS denormalized,
  (SELECT ROUND(SUM(total_amount), 2) FROM apparel_store.`03_gold`.denormalized_sales_facts) AS fact_revenue,
  (SELECT ROUND(SUM(total_revenue), 2) FROM apparel_store.`03_gold`.gold_daily_sales_by_store) AS daily_revenue;

SELECT 'customers' AS gold_table, COUNT(*) - COUNT(DISTINCT customer_id) AS duplicate_ids FROM apparel_store.`03_gold`.gold_customer_lifetime_value
UNION ALL SELECT 'products', COUNT(*) - COUNT(DISTINCT product_id) FROM apparel_store.`03_gold`.gold_product_performance
UNION ALL SELECT 'store-days', COUNT(*) - COUNT(DISTINCT sale_date, store_id) FROM apparel_store.`03_gold`.gold_daily_sales_by_store;

SELECT sale_date, COUNT(*) AS stores, ROUND(AVG(total_transactions)) AS avg_transactions
FROM apparel_store.`03_gold`.gold_daily_sales_by_store
GROUP BY sale_date
ORDER BY sale_date;
```

- `denormalized` equals `silver_sales`; `daily_revenue` matches `fact_revenue` within a few cents (each daily row is rounded to the cent).
- Every `duplicate_ids` is 0: one row per customer, per product and per store-day.
- About 31 dates: 30 backfilled days plus today (a run across midnight UTC adds one). Each backfilled day has 5 stores with about 37 transactions each (40 sales per store-day, minus returns and invalid discounts). Today can show a 6th store or more: the live generator opens a new store about every 10 minutes. Today also holds the live batches at demo speed, so its store rows are many times larger than a backfilled day: compare backfilled days with each other.
- `` SELECT COUNT(*) FROM apparel_store.`03_gold`.gold_product_performance WHERE product_name IS NULL `` returns about 1-2 after 20 minutes of generator time (sometimes 0) and grows by about one for every 10 more minutes: products whose only record was dropped for a bad price (Task 6).
- In `gold_customer_lifetime_value`, almost every one of the 50 starting customers (`customer_id` 1-50) has the first backfilled day as `first_purchase_date`. Each of them shops about 4 times a backfilled day at this demo scale, so one or two may start a day later. Every customer created live has today's date; in `gold_product_performance` the lowest `product_id`s have the highest `total_quantity_sold` (the generator's best-seller curve).
