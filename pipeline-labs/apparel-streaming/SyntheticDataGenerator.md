## The Synthetic Data Generator: A Deeper Look

To make this project realistic, we use a Python script (`dlt/data_generator.py`) that acts as a synthetic data generator. Its primary function is to produce continuous, imperfect data streams that mimic the operational systems of LoomLane Outfitters, a fictional apparel chain. This allows us to test our Lakeflow pipeline's ability to handle the complexities of real-world data.

### How It Works

The generator runs four concurrent Python threads, each simulating a different source system, and writes each stream to its own landing folder as a Delta table. This architecture ensures that data from different domains (sales, customers, etc.) arrives independently and sometimes out of sync, just as it would in a real enterprise. The backfill is paced like a real month (40 sales per store and day). The live stream runs at demo speed (500 sales every 30 seconds) so new data shows up in every pipeline update, which makes today's rows far larger than any backfilled day. The first customer, product and store records are dated one day before the sales backfill starts, so every historic sale has a dimension version valid at its time. Only the bootstrap customers and products appear in the backfill.

Faker and each stream's random generator are seeded, but timestamps follow the clock and every batch is timed, so row counts depend on how long the generator runs.

| Stream Name   | Description                                                            | Key Behavior |
| :------------ | :--------------------------------------------------------------------- | :----------- |
| **Sales**     | The central **fact stream** containing individual transaction records. | On first run it backfills 30 days of history (40 sales per store and day, between 09:00 and 21:00 UTC, about 6,000 rows for the 5 starting stores, sold only to the bootstrap customers and only of the bootstrap products), then streams live: 500 sales every 30 seconds. It waits for dimension data to be available to ensure basic referential integrity at the source. A customer buys at their home store 80% of the time, low product IDs sell the most, and stores under renovation do not trade. |
| **Customers** | A **dimension stream** for customer information.                       | Simulates new customers and changes to existing ones (30% of records). A change keeps the customer and changes only the address (50% of changes), the email (25%) or the phone number (15%); every change adds loyalty points. Name, gender, age and join date never change. |
| **Products**  | A **dimension stream** for the product catalog.                        | Simulates new products and changes to existing ones (30% of records): a price change, mostly markdowns (40% of changes), a stock level change only (50%) or a new description (10%). Name, category, brand, size and color never change. |
| **Stores**    | A **dimension stream** for physical store locations.                   | Up to 2 store records per batch, 95% of them changes to an existing store, so a new store opens every few minutes. A change assigns a new manager (50% of changes; 1 in 5 leaves the position vacant), changes the status (30%) or the phone number (20%). Name, address and open date never change. |

### Intentionally "Dirty" Data for Realistic Challenges

The generator is explicitly designed to produce messy data. Your Lakeflow pipeline must be built to handle these specific, intentional data quality issues. Defects are planted on the row the source writes, at the rates below (rates are in `PLANTED` in `dlt/data_generator.py`); a later change record for the same ID arrives clean. New records carry every defect of their stream; change records carry only the defects of the attribute they change. No business key is ever NULL: the bronze fail rules guard against it, but one such row would stop every pipeline update, so nothing plants it.

| Issue | Description | Example from Generator | Pipeline Challenge |
| :---- | :---------- | :--------------------- | :----------------- |
| **Negative Values** | Certain numeric fields contain negative values that have a business meaning or represent errors. | `sales.quantity` is negative on 5% of sales to signify a **product return**. `sales.discount_applied` is negative on 2% of sales (written with the wrong sign), which is **invalid data**. `customers.loyalty_points` is negative on 1% of rows (a points reversal applied twice); `products.stock_quantity` on 2% (an oversold item). | The pipeline must correctly interpret returns and filter out invalid records. |
| **NULL/Missing Values** | Some fields are intentionally left empty to simulate incomplete data capture. | `customers.email` is `NULL` for 2% of new customers who opt not to provide it. `products.brand` is `NULL` on 2% of new products. `stores.manager` is `NULL` when a position is vacant (2% of new stores, 1 in 5 manager changes). | The pipeline must handle these missing values gracefully, for instance, by filling them with a default value like "Unknown". |
| **Out-of-range and Invalid Values** | Values a source system should never send, but does. | `customers.age` is 0, 7 or 142 (1%: digits of a birth year typed into the age field); an email without '@' (1%); `customers.gender` is 'M', 'F' or '' (2%: a legacy till code or a blank); `products.category` is 'Kidswear' (1%: not in the reference list yet); `products.price` is x100 (1%: cents typed as dollars); `stores.status` is 'open' in lowercase (3%: a second store system); `sales.payment_method` is 'CARD' or 'Apple Pay' (1%: an unmapped terminal). | The pipeline must flag these with warn expectations, or drop them where the value would corrupt analytics (the price). |
| **Change Data Capture (CDC) / Updates** | The dimension streams (`customers`, `products`, `stores`) will generate records with existing IDs but updated information. | A record for `customer_id` 123 might first appear with one address, and later another record for `customer_id` 123 appears with a new address and a more recent `last_update_time`. | The pipeline must correctly process this CDC-style data, typically with AUTO CDC (`dp.create_auto_cdc_flow` in Python, `AUTO CDC INTO` in SQL), and keep history as SCD Type 2. |
| **Event Time Skew** | Timestamps are intentionally delayed to simulate network latency and out-of-order data arrival. | The `event_time` in the sales stream is generated with a random delay of up to 60 seconds (`LATENCY_MAX_S`). | Late events matter only to stateful streaming operators: a watermark bounds them there. In this pipeline the aggregates are materialized views recomputed from the full table, so late sales are counted on the next update. |

---

# Synthetic Data Generator Table Schemas

The following tables describe the schema of each data stream generated by the synthetic data generator (`dlt/data_generator.py`).

Types are as written to the landing Delta tables: the generator writes whole numbers as long (bigint) and decimals as double. The bronze tasks cast IDs and counts to int (target types in [data_contract.md](data_contract.md)).

## Sales Table

| Field            | Type          | Description                                                     |
| ---------------- | ------------- | --------------------------------------------------------------- |
| transaction_id   | Long (bigint) | Unique identifier for each sale                                 |
| store_id         | Long (bigint) | Foreign key linking to the `stores` table                       |
| event_time       | Timestamp     | The timestamp when the transaction occurred                     |
| customer_id      | Long (bigint) | Foreign key linking to the `customers` table                    |
| product_id       | Long (bigint) | Foreign key linking to the `products` table                     |
| quantity         | Long (bigint) | Number of units sold. Can be negative for returns               |
| unit_price       | Double        | Catalog price of the product at the time of sale                |
| total_amount     | Double        | Net line amount: quantity x unit_price minus discount_applied (negative for a return) |
| payment_method   | String        | Method used for payment (e.g., Credit Card, Cash)               |
| discount_applied | Double        | Discount amount on the line: 10%, 20% or 30% of quantity x unit_price on about 30% of lines, otherwise 0. Negative on 2% of lines (invalid) |
| tax_amount       | Double        | 8% of total_amount                                              |

## Customers Table

| Field            | Type      | Description                                          |
| ---------------- | --------- | ---------------------------------------------------- |
| customer_id      | Long (bigint) | Unique identifier for each customer                  |
| name             | String    | Full name of the customer                            |
| email            | String    | Customer's email address. Can be NULL                |
| address          | String    | Customer's physical address                          |
| join_date        | Timestamp | The date the customer registered                     |
| loyalty_points   | Long (bigint) | Number of loyalty points the customer has            |
| phone_number     | String    | Customer's contact phone number                      |
| age              | Long (bigint) | Age of the customer                                  |
| gender           | String    | Gender of the customer                               |
| last_update_time | Timestamp | The timestamp of the last modification to the record |

## Products (Items) Table

| Field            | Type      | Description                                          |
| ---------------- | --------- | ---------------------------------------------------- |
| product_id       | Long (bigint) | Unique identifier for each product                   |
| name             | String    | The name of the product                              |
| category         | String    | The category the product belongs to                  |
| brand            | String    | The brand of the product                             |
| price            | Double    | The retail price of the product                      |
| stock_quantity   | Long (bigint) | The number of units currently in stock               |
| size             | String    | The size of the apparel (e.g., S, M, L)              |
| color            | String    | The color of the product                             |
| description      | String    | A brief description of the product                   |
| last_update_time | Timestamp | The timestamp of the last modification to the record |

## Stores Table

| Field            | Type      | Description                                          |
| ---------------- | --------- | ---------------------------------------------------- |
| store_id         | Long (bigint) | Unique identifier for each store                     |
| name             | String    | The name of the store location                       |
| address          | String    | The physical address of the store                    |
| manager          | String    | The name of the store manager. Can be NULL           |
| open_date        | Timestamp | The date the store was opened                        |
| status           | String    | The current operational status of the store          |
| phone_number     | String    | The contact phone number for the store               |
| last_update_time | Timestamp | The timestamp of the last modification to the record |
