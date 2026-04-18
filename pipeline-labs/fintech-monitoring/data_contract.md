# Data Contract: Real-Time Fintech Transaction Monitoring

This document defines the schemas for all data assets in this lab. Use this as a reference for your code.

## 🥉 Bronze Layer (Ingestion)

### `transactions` (Source: JSON events in Volumes)

These are raw payment events arriving from the ModernPaymentsABC gateway.

| Field       | Type      | Description                             |
| ----------- | --------- | --------------------------------------- |
| txn_id      | string    | Unique transaction identifier           |
| customer_id | string    | Reference to the customer               |
| merchant_id | string    | Reference to the merchant (can be null) |
| amount      | double    | Transaction amount in source currency   |
| currency    | string    | ISO currency code (USD, EUR, etc.)      |
| channel     | string    | online, POS, or mobile                  |
| ip_country  | string    | Originating IP country code             |
| timestamp   | timestamp | Event generation time                   |

## 🥈 Silver Layer (Cleaned & Enriched)

### `transactions`

The source of truth for all transactions, cleaned of duplicates and enriched with dimensions.

| Field                 | Type      | Description                                             |
| --------------------- | --------- | ------------------------------------------------------- |
| txn_id                | string    | Unique transaction identifier                           |
| customer_id           | string    | Reference to the customer                               |
| merchant_id           | string    | Reference to the merchant (can be null)                 |
| amount                | double    | Transaction amount in source currency                   |
| currency              | string    | Standardized ISO currency code (uppercase)              |
| channel               | string    | online, POS, or mobile                                  |
| ip_country            | string    | Originating IP country code                             |
| timestamp             | timestamp | Event generation time                                   |
| customer_name         | string    | From customer dimension ("Unknown Customer" if missing) |
| customer_country      | string    | From customer dimension ("Unknown" if missing)          |
| risk_tier             | string    | Customer risk tier (Low, Medium, High)                  |
| merchant_name         | string    | From merchant dimension ("Unknown Merchant" if missing) |
| merchant_country      | string    | From merchant dimension ("Unknown" if missing)          |
| merchant_risk_score   | int       | Merchant risk score (0-100)                             |
| category_code         | string    | Merchant Category Code (MCC)                            |
| is_geo_mismatch       | boolean   | True if ip_country != customer_country                  |
| \_ingestion_timestamp | timestamp | When the record was ingested into Bronze                |

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
| category_code | string | Merchant Category Code (MCC) |
| total_txns    | long   | Total number of transactions |
| total_amount  | double | Sum of transaction amounts   |
| avg_amount    | double | Average transaction amount   |

### `sar_prefill_dataset`

Aggregated fraud alerts by customer for Suspicious Activity Report (SAR) pre-fill.

| Field                   | Type   | Description                             |
| ----------------------- | ------ | --------------------------------------- |
| customer_id             | string | Reference to the customer               |
| total_incidents         | long   | Count of fraud alerts for this customer |
| total_suspicious_amount | double | Sum of flagged transaction amounts      |
| max_risk_score          | int    | Highest risk score across all alerts    |
