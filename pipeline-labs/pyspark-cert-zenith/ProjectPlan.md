# Project Plan: Zenith Online Analytics Pipeline

## ⚠️ Consistency Note: Use `variables.py` for Paths and Names

To ensure consistency across all scripts and notebooks, **import and use the variables defined in `lab/variables.py`**. This file contains all catalog, schema, and volume names, as well as key data paths.
**Do not hardcode catalog, schema, or path names**—always reference the variables.

## I. Getting Started

All the files required to complete this lab are located in the `lab/` directory.

1.  **Environment Setup**: Run the `lab/environment_setup.ipynb` notebook in your Databricks workspace to create the necessary catalogs, schemas, and volumes.
2.  **Data Generation**: Run the `lab/data_generator.py` script to generate the synthetic data for the project.
3.  **ELT Pipeline**: The main logic of the project is in `lab/ELT.py`. This file contains the instructions and code for the end-to-end pipeline.