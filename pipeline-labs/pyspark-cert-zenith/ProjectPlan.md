# Project Plan: Zenith Online Analytics Pipeline

## ⚠️ Consistency Note: Use `variables.py` for Paths and Names

To ensure consistency across all notebooks, **use the variables defined in `notebooks/variables.py`**, which every notebook loads with `%run ./variables`. This file contains all catalog, schema, volume and table names, as well as key data paths.
**Do not hardcode catalog, schema, or path names** - always reference the variables.

## I. Getting Started

All the files required to complete this lab are located in the `notebooks/` directory. Run the numbered notebooks in order.

1.  **Environment Setup and Data Generation**: Run `notebooks/00_Setup_Environment` in your Databricks workspace. It creates the necessary catalog, schemas and volumes, then runs `notebooks/data_generator` to generate the synthetic data for the project.
2.  **Bronze Layer**: `notebooks/01_Bronze_Ingestion` (Tasks 1-3) ingests the raw events, customers and products into Bronze tables.
3.  **Silver Layer**: `notebooks/02_Silver_Enrichment` (Tasks 4-5) de-duplicates and enriches the events into the Silver table.
4.  **Gold Layer**: `notebooks/03_Gold_Aggregations` (Tasks 6-7) builds the Gold tables and answers the business questions with SQL.