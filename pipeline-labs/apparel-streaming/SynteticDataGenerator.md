## The Synthetic Data Generator: A Deeper Look

To make this project realistic, we use a Python script (`dlt/data_generator.py`) that acts as a synthetic data generator. Its primary function is to produce continuous, imperfect data streams that mimic the operational systems of an apparel retail company. This allows us to test our DLT pipeline's ability to handle the complexities of real-world data.

### How It Works

The generator runs four concurrent Python threads, each simulating a different source system. This architecture ensures that data from different domains (sales, customers, etc.) arrives independently and sometimes out of sync, just as it would in a real enterprise.

| Stream Name   | Description                                                            | Key Behavior                                                                                                                                  |
| :------------ | :--------------------------------------------------------------------- | :-------------------------------------------------------------------------------------------------------------------------------------------- |
| **Sales**     | The central **fact stream** containing individual transaction records. | Generates a continuous flow of sales events. It waits for dimension data to be available to ensure basic referential integrity at the source. |
| **Customers** | A **dimension stream** for customer information.                       | Simulates both the creation of new customers and updates to existing ones (e.g., a customer changes their address).                           |
| **Products**  | A **dimension stream** for the product catalog.                        | Simulates new product introductions and updates to existing product details (e.g., price changes, stock level updates).                       |
| **Stores**    | A **dimension stream** for physical store locations.                   | Simulates new store openings and updates to existing store information (e.g., a new manager is assigned).                                     |

### Intentionally "Dirty" Data for Realistic Challenges

The generator is explicitly designed to produce messy data. Your DLT pipeline must be built to handle these specific, intentional data quality issues:

| Issue                                   | Description                                                                                                                | Example from Generator                                                                                                                                                              | Pipeline Challenge                                                                                                                                           |
| :-------------------------------------- | :------------------------------------------------------------------------------------------------------------------------- | :---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | :----------------------------------------------------------------------------------------------------------------------------------------------------------- |
| **Negative Values**                     | Certain numeric fields contain negative values that have a business meaning or represent errors.                           | `sales.quantity` can be negative to signify a **product return**. `sales.discount_applied` can be negative, which is **invalid data**.                                              | The pipeline must correctly interpret returns and filter out invalid records.                                                                                |
| **NULL/Missing Values**                 | Some fields are intentionally left empty to simulate incomplete data capture.                                              | `customers.email` may be `NULL` if a customer opts not to provide it. `stores.manager` can be `NULL` if a position is vacant.                                                       | The pipeline must handle these missing values gracefully, for instance, by filling them with a default value like "Unknown".                                 |
| **Change Data Capture (CDC) / Updates** | The dimension streams (`customers`, `products`, `stores`) will generate records with existing IDs but updated information. | A record for `customer_id` 123 might first appear with one address, and later another record for `customer_id` 123 appears with a new address and a more recent `last_update_time`. | The pipeline must correctly process this CDC-style data, typically by using `APPLY CHANGES INTO` to merge the latest version and track history (SCD Type 2). |
| **Event Time Skew**                     | Timestamps are intentionally delayed to simulate network latency and out-of-order data arrival.                            | The `event_time` in the sales stream is generated with a random delay (`LATENCY_MAX_S`).                                                                                            | The pipeline must use **watermarking** to correctly handle late-arriving data in streaming aggregations.                                                     |

---

Of course. Here is a comprehensive, step-by-step checklist that mirrors every detail of your DLT pipeline code. Following this to-do list will allow for an exact recreation of the pipeline.

---

# Synthetic Data Generator Table Schemas

The following tables describe the schema of each data stream generated by the synthetic data generator (`dlt/data_generator.py`).

## Sales Table

| Field            | Type      | Description                                                     |
| ---------------- | --------- | --------------------------------------------------------------- |
| transaction_id   | Integer   | Unique identifier for each sale                                 |
| store_id         | Integer   | Foreign key linking to the `stores` table                       |
| event_time       | Timestamp | The timestamp when the transaction occurred                     |
| customer_id      | Integer   | Foreign key linking to the `customers` table                    |
| product_id       | Integer   | Foreign key linking to the `products` table                     |
| quantity         | Integer   | Number of units sold. Can be negative for returns               |
| unit_price       | Float     | Price per unit of the product                                   |
| total_amount     | Float     | Total cost of the transaction for this item (quantity \* price) |
| payment_method   | String    | Method used for payment (e.g., Credit Card, Cash)               |
| discount_applied | Float     | Discount amount applied to the transaction                      |
| tax_amount       | Float     | Tax amount calculated on the total                              |

## Customers Table

| Field            | Type      | Description                                          |
| ---------------- | --------- | ---------------------------------------------------- |
| customer_id      | Integer   | Unique identifier for each customer                  |
| name             | String    | Full name of the customer                            |
| email            | String    | Customer's email address. Can be NULL                |
| address          | String    | Customer's physical address                          |
| join_date        | Timestamp | The date the customer registered                     |
| loyalty_points   | Integer   | Number of loyalty points the customer has            |
| phone_number     | String    | Customer's contact phone number                      |
| age              | Integer   | Age of the customer                                  |
| gender           | String    | Gender of the customer                               |
| last_update_time | Timestamp | The timestamp of the last modification to the record |

## Products (Items) Table

| Field            | Type      | Description                                          |
| ---------------- | --------- | ---------------------------------------------------- |
| product_id       | Integer   | Unique identifier for each product                   |
| name             | String    | The name of the product                              |
| category         | String    | The category the product belongs to                  |
| brand            | String    | The brand of the product                             |
| price            | Float     | The retail price of the product                      |
| stock_quantity   | Integer   | The number of units currently in stock               |
| size             | String    | The size of the apparel (e.g., S, M, L)              |
| color            | String    | The color of the product                             |
| description      | String    | A brief description of the product                   |
| last_update_time | Timestamp | The timestamp of the last modification to the record |

## Stores Table

| Field            | Type      | Description                                          |
| ---------------- | --------- | ---------------------------------------------------- |
| store_id         | Integer   | Unique identifier for each store                     |
| name             | String    | The name of the store location                       |
| address          | String    | The physical address of the store                    |
| manager          | String    | The name of the store manager. Can be NULL           |
| open_date        | Timestamp | The date the store was opened                        |
| status           | String    | The current operational status of the store          |
| phone_number     | String    | The contact phone number for the store               |
| last_update_time | Timestamp | The timestamp of the last modification to the record |
