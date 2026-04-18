# Databricks notebook source
# variables.py
# Centralized configuration for names, paths, and metadata

# Unity Catalog Namespace
CATALOG_NAME = "ModernPaymentsABC_monitoring"
LANDING_SCHEMA = "00_landing"
BRONZE_SCHEMA = "01_bronze"
SILVER_SCHEMA = "02_silver"
GOLD_SCHEMA = "03_gold"
SYSTEM_SCHEMA = "_system"

# Volume Paths (Unity Catalog)
LANDING_VOLUME_PATH = f"/Volumes/{CATALOG_NAME}/{LANDING_SCHEMA}/raw_data"
CHECKPOINT_PATH = f"/Volumes/{CATALOG_NAME}/{SYSTEM_SCHEMA}/checkpoints"

# Raw Data Paths (Subdirectories in Volume)
TRANSACTIONS_RAW_PATH = f"{LANDING_VOLUME_PATH}/transactions"
CUSTOMERS_RAW_PATH = f"{LANDING_VOLUME_PATH}/customers"
MERCHANTS_RAW_PATH = f"{LANDING_VOLUME_PATH}/merchants"

# Table Names
TABLE_BRONZE_TRANSACTIONS = f"{CATALOG_NAME}.{BRONZE_SCHEMA}.transactions"
TABLE_SILVER_TRANSACTIONS = f"{CATALOG_NAME}.{SILVER_SCHEMA}.transactions"
TABLE_SILVER_ALERTS = f"{CATALOG_NAME}.{SILVER_SCHEMA}.fraud_alerts"
TABLE_GOLD_DAILY_SUMMARY = f"{CATALOG_NAME}.{GOLD_SCHEMA}.daily_merchant_summary"
TABLE_GOLD_SAR_PREFILL = f"{CATALOG_NAME}.{GOLD_SCHEMA}.sar_prefill_dataset"

# Dimension Tables (Silver)
TABLE_SILVER_CUSTOMERS = f"{CATALOG_NAME}.{SILVER_SCHEMA}.customers"
TABLE_SILVER_MERCHANTS = f"{CATALOG_NAME}.{SILVER_SCHEMA}.merchants"

# Data Generation Settings
TOTAL_TXNS = 50000
BATCH_SIZE = 10000
NUM_BATCHES = 5
DUPLICATE_RATE = 0.02
NULL_MERCHANT_RATE = 0.01

print(f"✅ Variables loaded for catalog: {CATALOG_NAME}")
