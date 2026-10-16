# Project Plan: Zenith Online Analytics Pipeline

## Paths and names come from `variables.py`

Every notebook loads `notebooks/variables.py` with `%run ./variables`. It holds all catalog, schema, volume and table names and the key data paths: use its variables instead of hardcoding catalog, schema or path names.

## Getting Started

All the files required to complete this lab are located in the `notebooks/` directory. Run the numbered notebooks in order.

1.  **Environment Setup and Data Generation**: Run `notebooks/00_Setup_Environment` in your Databricks workspace. It creates the necessary catalog, schemas and volumes, then runs `notebooks/data_generator` to generate the synthetic data for the project.
2.  **Bronze Layer**: `notebooks/01_Bronze_Ingestion` (Tasks 1-3) ingests the raw events, customers and products into Bronze tables.
3.  **Silver Layer**: `notebooks/02_Silver_Enrichment` (Tasks 4-5) de-duplicates and enriches the events into the Silver table.
4.  **Gold Layer**: `notebooks/03_Gold_Aggregations` (Tasks 6-7) builds the Gold tables and answers the business questions with SQL.