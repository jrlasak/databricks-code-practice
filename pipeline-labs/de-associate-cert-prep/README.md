# Databricks Data Engineer Associate Certification Lab

> Independent educational resource; not endorsed by Databricks, Inc. "Databricks" and "Delta Lake" are trademarks of their respective owners.

## Author

<img src="https://dataengineer.wiki/download/profilepicture.jpg?utm_source=github&utm_medium=readme&utm_campaign=databricks-code-practice" alt="Jakub Lasak" width="80" style="border-radius: 50%;" />

**Jakub Lasak** - Databricks Data Engineer (ex-Uber). I write about the Databricks work that gets you hired and promoted.

- 🔗 [LinkedIn](https://www.linkedin.com/in/jrlasak/) - Databricks projects and tips
- 📬 [Substack Newsletter](https://dataengineer.wiki/substack?utm_source=github&utm_medium=readme&utm_campaign=databricks-code-practice) - Exclusive content for Data Engineers
- 🎧 [Podcast](https://dataengineer.wiki/podcast?utm_source=github&utm_medium=readme&utm_campaign=databricks-code-practice) - Databricks and data engineering, on Spotify, Apple, and YouTube
- 🌐 [DataEngineer.wiki](https://dataengineer.wiki/?utm_source=github&utm_medium=readme&utm_campaign=databricks-code-practice) - Interview prep and free Databricks practice
- 🚀 [More Practice Labs](https://github.com/jrlasak/databricks-code-practice) - more pipeline labs, deep-dives, and exercises in this repo

> **Looking for more info on passing the Databricks Data Engineer Associate Certification exam?**  
> Check out helpful resources including YouTube videos and official Databricks courses at [dataengineer.wiki/certifications/data-engineer-associate](https://dataengineer.wiki/certifications/data-engineer-associate?utm_source=github&utm_medium=readme&utm_campaign=databricks-code-practice).

## High-Level Overview

This lab provides hands-on practice to prepare for the Databricks Certified Data Engineer Associate exam. You build an end-to-end medallion pipeline on a synthetic retail dataset, entirely in Databricks Free Edition.

**The scenario.** You are the data engineer at CartHarborHQ, a fictional US online retailer with 10,000 customers and 1,000 products across Electronics, Clothing, Home & Garden, Sports & Outdoors and Books. Customer, catalog, order and clickstream files land in a Unity Catalog volume every day. Merchandising wants daily revenue by category, the loyalty team wants RFM segments to pick who to target, and category managers want their products ranked by revenue. You build that pipeline in notebooks 02 to 04 and schedule it in notebook 05.

The lab covers:

- Ingesting JSON and CSV files from Unity Catalog volumes with Auto Loader and COPY INTO.
- Implementing the Medallion Architecture (Bronze, Silver, Gold layers).
- Performing data transformations, quality checks, and implementing SCD Type 2.
- Orchestrating workflows with Databricks Jobs and multi-task dependencies.
- Managing data governance using Unity Catalog with role-based access control.

## How It Prepares You for the Exam

This lab is structured to cover the key topics outlined in [the official Databricks Data Engineer Associate exam info](https://www.databricks.com/learn/certification/data-engineer-associate). By completing the notebooks, you will gain practical experience in the following areas:

### Section 1: Databricks Intelligence Platform

- **What you'll practice:** Choosing between serverless and classic compute (notebook 01) and laying out Gold tables with liquid clustering instead of partitions (notebook 04).

### Section 2: Development and Ingestion

- **What you'll practice:** Sharing configuration across notebooks with `%run`, ingesting JSON and CSV files with Auto Loader, loading batch increments with COPY INTO, and handling schema evolution and `_rescued_data`.

### Section 3: Data Processing & Transformations

- **What you'll practice:** Implementing the three layers of the Medallion Architecture (Bronze, Silver, Gold), performing data quality transformations, implementing Slowly Changing Dimensions (SCD Type 2), and computing complex aggregations with PySpark window functions.

### Section 4: Productionizing Data Pipelines

- **What you'll practice:** Creating and configuring Databricks Jobs, implementing multi-task workflows with dependencies, using job parameters with widgets, implementing error handling and retry logic, and using serverless compute.

### Section 5: Data Governance & Quality

- **What you'll practice:** Understanding Unity Catalog's three-level namespace (catalog.schema.table), creating and managing catalogs, schemas, and volumes, creating managed tables, implementing access control with GRANT and REVOKE statements, and data quality validation patterns.

## Prerequisites

- Comfortable with the PySpark DataFrame API and basic SQL; Delta Lake basics.
- A Databricks Free Edition workspace where you are the admin: notebook 01 has you create 3 groups (`data_engineers`, `data_analysts`, `data_scientists`) in Settings before its GRANT exercises.
- About 4.5 hours in total: 15 minutes of setup, then 30 minutes to 1 hour per notebook, best done one notebook per sitting; each notebook states its own time.

> ⚠️ **Before you start**: Disable AI code suggestions in your Databricks workspace.
> Open your profile menu, go to **Settings → Developer**, and under **Code Editor** turn **OFF**:
> - **Automatic Genie Code Autocomplete**
> - **Autocomplete as you type**
>
> This lab is designed to build muscle memory: auto-completions defeat the purpose.

## Additional Exam Topics

This lab focuses on core data engineering workflows. If you want to practice additional exam topics such as **Lakeflow Spark Declarative Pipelines (formerly DLT)** or **Delta table optimization techniques**, check out the other hands-on labs in this repo - the [Apparel declarative pipeline](../apparel-streaming/) and the [Delta Optimization deep-dive](../../deep-dives/optimization-techniques/).

## Get 50% Off Your Certification

Register for a Databricks Virtual Learning Festival and complete the required courses within its timeline to receive a 50%-off certification voucher.

Upcoming festivals: [Databricks Community events](https://community.databricks.com/t5/events/eb-p/databricks-community-events)

## How to Start

1. **Create a Databricks Account**
   - Sign up for a [Databricks Free Edition account](https://www.databricks.com/learn/free-edition) if you don't already have one.
   - Familiarize yourself with the workspace, serverless compute and the notebook interface.

2. **Import this repository to Databricks**
   - In your Workspace, click **Create** > **Git folder**, paste the repository URL and click **Create Git folder**.
   - Authenticate with GitHub if prompted, and select the main branch.
   - The repo will appear as a folder in your workspace, allowing you to edit, run notebooks, and manage files directly from Databricks.
   - For more details, see the official Databricks documentation: [Git folders](https://docs.databricks.com/aws/en/repos/).

3. Open the `notebooks/` folder and run `00_Setup_Environment.py` to create the Unity Catalog infrastructure and generate data.

4. Follow the numbered notebooks (`01` to `05`) to build the pipeline.

   Each exercise has a `TODO` cell for your code, a commented-out solution right after it, and a 🔍 cell that tells you what you should see. Try the exercise before you scroll to the solution.

   Keep `data_contract.md` open while you code: it lists every table with its grain and key.
