# Data Contract: Real-Time Fintech Transaction Monitoring

This document defines the schemas for all data assets in this lab. Use this as a reference for your code.

## 🥉 Bronze Layer (Ingestion)

### `transactions` (Source: JSON events in Volumes)

These are raw payment events arriving from the ModernPaymentsABC gateway.

| Field       | Type      | Description                             |
| ----------- | --------- | --------------------------------------- |
| txn_id      | string    | Unique transaction identifier           |
| customer_id | string    | Reference to the customer               |
| merchant_id | string    | Reference to the merchant (may not be in the merchant dimension yet) |
| amount      | double    | Transaction amount in USD (the gateway settles in USD) |
| currency    | string    | ISO currency code as sent by the gateway (`USD`, some integrations send `usd`) |
| channel     | string    | online, POS, or mobile                  |
| ip_country  | string    | Originating IP country code             |
| timestamp   | timestamp | Event generation time                   |

## 🥈 Silver Layer (Cleaned & Enriched)

### `transactions`

The source of truth for all transactions: cleaned of duplicates, without the rows whose values were rescued (they stay in Bronze for review), and enriched with dimensions.

Column order is not part of the contract: the joins in Notebook 02 put `merchant_id` and `customer_id` first, and any order with these names and types is correct.

| Field                 | Type      | Description                                             |
| --------------------- | --------- | ------------------------------------------------------- |
| txn_id                | string    | Unique transaction identifier                           |
| customer_id           | string    | Reference to the customer                               |
| merchant_id           | string    | Reference to the merchant (may not be in the merchant dimension yet) |
| amount                | double    | Transaction amount in USD                               |
| currency              | string    | Standardized ISO currency code (uppercase: `USD`)       |
| channel               | string    | online, POS, or mobile                                  |
| ip_country            | string    | Originating IP country code                             |
| timestamp             | timestamp | Event generation time                                   |
| customer_name         | string    | From customer dimension ("Unknown Customer" if missing) |
| customer_country      | string    | From customer dimension ("Unknown" if missing)          |
| risk_tier             | string    | Customer risk tier (Low, Medium, High)                  |
| merchant_name         | string    | From merchant dimension ("Unknown Merchant" if missing) |
| merchant_country      | string    | From merchant dimension ("Unknown" if missing)          |
| merchant_risk_score   | int       | Merchant risk score (0-99)                              |
| category_code         | string    | Merchant Category Code (MCC)                            |
| is_geo_mismatch       | boolean   | True if ip_country != customer_country                  |
| \_ingestion_timestamp | timestamp | When the record was ingested into Bronze                |

### `merchants`

The merchant dimension (Notebook 02, Exercise 3): the last full export (`merchants`, JSON, 5,000 merchants), then MERGEd with today's updates file (`merchant_updates`, JSON, same raw schema: 3 re-scored merchants and 10 new merchants `5000`-`5009`).

| Field               | Type   | Description                            |
| ------------------- | ------ | -------------------------------------- |
| merchant_id         | string | Unique merchant identifier             |
| merchant_name       | string | Merchant name (raw field `name`)       |
| category_code       | string | Merchant Category Code (MCC)           |
| merchant_country    | string | Merchant country (raw field `country`) |
| merchant_risk_score | int    | Risk score 0-99 (raw field `risk_score`) |

### `velocity_spikes`

Customers with more than 5 transactions in a 10-minute tumbling window (Notebook 03, Exercise 1).

| Field             | Type                                   | Description                        |
| ----------------- | -------------------------------------- | ---------------------------------- |
| customer_id       | string                                 | Reference to the customer          |
| window            | `struct<start: timestamp, end: timestamp>` | The 10-minute tumbling window      |
| txn_count         | long                                   | Transactions in the window (> 5)   |
| is_velocity_spike | boolean                                | Always true (pre-filtered)         |

### `amount_spikes`

Customers whose transactions total more than $5,000 in a 1-hour sliding window that advances every 5 minutes (Notebook 03, Exercise 2). One burst appears once per overlapping window.

| Field                        | Type                                   | Description                          |
| ---------------------------- | -------------------------------------- | ------------------------------------ |
| customer_id                  | string                                 | Reference to the customer            |
| window                       | `struct<start: timestamp, end: timestamp>` | The 1-hour sliding window            |
| total_amount                 | double                                 | Sum of amounts in the window (> 5000) |
| is_amount_threshold_exceeded | boolean                                | Always true (pre-filtered)           |

### `fraud_alerts`

High-risk transactions flagged by the rules engine (risk_score > 70).

Same schema as `transactions` plus:

| Field      | Type    | Description                 |
| ---------- | ------- | --------------------------- |
| risk_score | int     | Composite risk score (0-90) |
| is_alert   | boolean | Always true (pre-filtered)  |

## 🥇 Gold Layer (Business Reporting)

### `daily_merchant_summary`

Daily aggregation of transactions by merchant category code for regulatory reporting.

| Field         | Type   | Description                  |
| ------------- | ------ | ---------------------------- |
| report_date   | date   | Transaction date             |
| category_code | string | Merchant Category Code (MCC); null for payments at merchants not yet in the dimension |
| total_txns    | long   | Total number of transactions |
| total_amount  | double | Sum of transaction amounts, net of voids ($0) and refunds (negative) |
| avg_amount    | double | Average transaction amount   |

### `sar_prefill_dataset`

The daily queue of Suspicious Activity Report (SAR) candidates: one row per customer with a fraud alert or a velocity / amount spike, pre-filled with who, how much and when. Missing counts, amounts and scores are 0, not null.

| Field                   | Type      | Description                                                        |
| ----------------------- | --------- | ------------------------------------------------------------------ |
| customer_id             | string    | Reference to the customer                                          |
| total_incidents         | long      | Count of fraud alerts for this customer (0 if none)                |
| total_suspicious_amount | double    | Sum of alerted transaction amounts (0 if none)                     |
| max_risk_score          | int       | Highest risk score across the customer's alerts (0 if none)        |
| first_activity_ts       | timestamp | Earliest alerted transaction (null if the customer has no alerts)  |
| last_activity_ts        | timestamp | Latest alerted transaction (null if the customer has no alerts)    |
| velocity_spike_windows  | long      | Rows for this customer in `velocity_spikes` (0 if none)            |
| amount_spike_windows    | long      | Rows for this customer in `amount_spikes` (0 if none)              |
