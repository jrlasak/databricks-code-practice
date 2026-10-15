# E-commerce User Behavior Analytics Pipeline

> Independent educational resource; not endorsed by Databricks, Inc. "Databricks" and "Delta Lake" are trademarks of their respective owners.

### Author

<img src="https://dataengineer.wiki/download/profilepicture.jpg?utm_source=github&utm_medium=readme&utm_campaign=databricks-code-practice" alt="Jakub Lasak" width="80" style="border-radius: 50%;" />

**Jakub Lasak** - Helping you interview like seniors, execute like seniors, and think like seniors.

- 🔗 [LinkedIn](https://www.linkedin.com/in/jrlasak/) - Databricks projects and tips
- 📬 [Substack Newsletter](https://dataengineer.wiki/substack?utm_source=github&utm_medium=readme&utm_campaign=databricks-code-practice) - Exclusive content for Data Engineers
- 🎧 [Podcast](https://dataengineer.wiki/podcast?utm_source=github&utm_medium=readme&utm_campaign=databricks-code-practice) - Databricks and data engineering, on Spotify, Apple, and YouTube
- 🌐 [DataEngineer.wiki](https://dataengineer.wiki/?utm_source=github&utm_medium=readme&utm_campaign=databricks-code-practice) - Training materials and resources
- 🚀 [More Practice Labs](https://github.com/jrlasak/databricks-code-practice) - more pipeline labs, deep-dives, and exercises in this repo

### Project Overview

This project is a comprehensive, hands-on lab designed to prepare you for the **[Databricks Certified Associate Developer for Apache Spark](https://www.databricks.com/learn/certification/apache-spark-developer-associate)** certification exam. Instead of studying abstract concepts, you will build a realistic, end-to-end data pipeline that solves a real-world business problem, applying dozens of the specific skills tested on the exam.

---

### **Why Complete This Project?**

Passing a certification exam requires more than just memorizing documentation. It requires understanding _how_ and _why_ specific tools and techniques are used in practice. This project bridges that gap by providing:

- **A Realistic Scenario:** You aren't just running random code snippets. You are building a complete solution from raw data to valuable business insights, following the industry-standard Medallion Architecture.
- **Purposeful Complexity:** The project is intentionally designed to include common data engineering challenges. The synthetic data generator creates **data skew** - a frequent source of performance bottlenecks - which you will measure in the Gold notebook and handle with the two-stage salting pattern, learning when that pattern pays off. It also plants three data quality issues real event feeds carry: client retries re-sent as exact duplicates, events from customers who signed up after the last CRM export, and events for products delisted from the catalog.
- **Comprehensive Certification Coverage:** The pipeline is carefully crafted to touch upon topics from all major sections of the certification exam, from Spark architecture and the DataFrame API to Structured Streaming and performance tuning.
- **Serverless-Ready Code:** The entire project is designed to run on **Databricks Free Edition**, so you can complete it at no cost while learning to work within the constraints of a serverless Spark environment.

---

#### **1. Project Context and Scenario**

You are a Data Engineer at **"Zenith Online,"** a rapidly growing e-commerce platform. The marketing and product teams are struggling to make data-driven decisions because their raw user data is disconnected and hard to analyze. They have come to you with a critical business need:

_"We need to understand our user journey. What products are people looking at, and which of them do they actually buy? How does revenue split across categories? Who are our most valuable customers? We need reliable, aggregated data to answer these questions."_

Your mission is to take on this request and build an automated, scalable data pipeline. You will process raw, messy user event data, combine it with customer and product information, and produce clean, aggregated "Gold" tables ready for the analytics team. This pipeline will be the single source of truth for user behavior analytics at Zenith Online.

---

#### **2. Data Sources (To be generated synthetically)**

- **Raw User Events (JSON Lines, Streaming):** A continuous stream of JSON files, one event per line, representing user actions on the website. Each event has a timestamp, user ID, event type (`view_product`, `add_to_cart`, `purchase`), and associated data like `product_id`. This dataset is **intentionally skewed** across all event types: 5 specific products draw 75% of all events (views, add-to-carts and purchases alike), so they are both the most viewed and the best-selling products. One setup run writes a compressed sample: 50,000 events stamped within 30 seconds stand in for a day of storefront traffic from 1,000 customers. The window is tight on purpose, so the 3-minute watermark in the Silver layer never drops a re-run's file as late data.
- **Customer Profiles (CSV, Batch):** A CSV file containing customer details like `customer_id`, `signup_date`, and `location`. This is a dimension table that is updated periodically.
- **Product Details (Parquet, Batch):** A Parquet file containing product information like `product_id`, `product_name`, `category`, and `price`. This is another dimension table.

---

### **How This Project Prepares You for the Certification Exam**

This project provides practical, hands-on experience covering the majority of the topics in the **Databricks Certified Associate Developer for Apache Spark** exam outline.

#### **Section 1: Apache Spark Architecture and Components**

- Every task builds transformations that run only when an action (`count`, a write, `display`) fires. On serverless, the **See performance** link under a cell lists the queries that cell ran, and it appears only on cells that trigger an action.
- You will use key features from multiple Spark Modules, including **Spark SQL**, **DataFrames**, **Structured Streaming** and **Pandas UDFs**. The pandas API on Spark (`pyspark.pandas`) is not used here.

#### **Section 2: Using Spark SQL**

- **Reading Multiple Formats:** The Bronze layer ingests data from three different file formats: **JSON** (with Auto Loader), **CSV**, and **Parquet**.
- **Writing to Delta Tables:** You will write data to Delta tables using different save modes (`append` for streaming, `overwrite` for batch).
- **Partitioning on write:** The Gold daily table is written with `partitionBy()`, the DataFrameWriter API the exam covers. On Databricks you would not partition a table this small: Databricks recommends no partitioning under 1 TB and liquid clustering for managed tables.
- **Executing SQL Queries:** The final step of the project uses `spark.sql()` to query the Gold layer Delta tables directly, demonstrating how to use SQL for analytics.

#### **Section 3: Developing Apache Spark™ DataFrame/DataSet API Applications**

- **Manipulating Columns:** The Silver layer extensively uses functions like `withColumn()`, `withColumnRenamed()`, and `select()` to clean and structure the data.
- **Data Deduplication:** You will de-duplicate a single event stream on `event_id` and its event time, under a watermark that bounds the deduplication state.
- **Aggregate Operations:** The Gold layer uses a wide range of aggregations, including `groupBy().agg()`, `count()`, `sum()`, and `approx_count_distinct()`.
- **Date and Timestamp Functions:** The pipeline uses `to_timestamp()`, `to_date()`, and `current_timestamp()` to handle time-based data correctly.
- **Combining DataFrames (Joins):** You will implement both `inner` and `left` joins in the Silver layer to enrich event data.
- **Broadcast Joins:** You will explicitly use `F.broadcast()` on a dimension table, a critical optimization technique for joining a large DataFrame with a smaller one.
- **Managing Schemas:** You will define and apply a `StructType` schema during data ingestion, so every column gets the type you chose instead of an inferred one. An explicit schema fixes the types; it does not reject nulls (a file read marks every column nullable).
- **User-Defined Functions (UDFs):** You will create and apply a **Pandas UDF** that maps cities to regions. The same mapping is a one-line built-in (`F.when` with `isin`) that avoids the Python worker entirely; the UDF is here to practice the API the exam tests, and in production built-in functions come first.
- **Window Functions:** The Gold layer ranks products by revenue within each day and category with `rank()` over a window spec, on the batch Gold DataFrame.

#### **Section 4: Troubleshooting and Tuning Apache Spark DataFrame API Applications**

- **Handling Data Skew:** The data is intentionally skewed, and you will implement the two-stage **salting** pattern in the Gold layer and learn when it pays off: Spark pre-aggregates counts and sums on every task before the shuffle, so salting a `groupBy` matters only where Spark has no map-side partial step, such as a grouped-aggregate Pandas UDF.
- **Partitioning:** You will use `partitionBy` on the Gold write (one directory per `event_date`), check it with `DESCRIBE DETAIL` (`partitionColumns`), and learn why Databricks recommends against it below 1 TB, where liquid clustering (`CLUSTER BY`) is the default choice.
- **Adaptive Query Execution (AQE):** AQE is enabled by default on Databricks. Its skew handling splits skewed partitions in sort-merge and shuffle hash joins. It does not apply to a `groupBy` aggregation like the Gold one, so the skew decision there is yours.

#### **Section 5: Structured Streaming**

- **End-to-End Streaming Pipeline:** The Bronze-to-Silver pipeline is a complete Structured Streaming application.
- **Streaming Sources and Sinks:** You will use Auto Loader (`cloudFiles`) as a streaming source and Delta Lake as a streaming sink.
- **Streaming Deduplication with Watermarks:** You will combine `withWatermark()` with `dropDuplicates()` on the event ID and the event-time column. The watermark evicts dedup state only when the event-time column is in the key (`dropDuplicatesWithinWatermark` covers copies whose timestamps differ).

#### **Section 7: Using Pandas API on Spark**

- **Creating and Invoking Pandas UDFs:** You will define a Pandas UDF with the `@pandas_udf` decorator and apply it to a Spark DataFrame column. That is the part of this exam section the lab practises; the pandas API on Spark itself (`pyspark.pandas`) is not used here.

## Prerequisites

- Python and the PySpark DataFrame API at a working level (select, filter, groupBy, joins)
- Delta tables and Unity Catalog three-level names (`catalog.schema.table`)
- A Databricks Free Edition workspace with serverless compute
- About 1.5 hours, best split by notebook: 01 ~30 min, 02 ~30 min, 03 ~30 min

> ⚠️ **Before you start**: Disable AI code suggestions in your Databricks workspace.
> Go to **User Settings → Developer → AI-powered code completion** and turn **OFF**:
> - **Autocomplete as you type**
> - **Automatic Assistant Autocomplete**
> This lab is designed to build muscle memory: auto-completions defeat the purpose.

## How to Start

1. **Create a Databricks Account**
   - Sign up for a [Databricks Free Edition account](https://www.databricks.com/learn/free-edition) if you don't already have one.
   - Familiarize yourself with the workspace and the notebook interface (Free Edition runs notebooks on serverless compute, so there is no cluster to create).

2. **Import this repository to Databricks**
   - In the sidebar, click **Workspace**, open the folder you want the lab in, click **Create > Git folder**, paste the GitHub URL of this repository and click **Create Git folder**.
   - Authenticate with GitHub if prompted, and select the main branch.
   - The repo will appear as a folder in your workspace, allowing you to edit, run notebooks, and manage files directly from Databricks.
   - For more details, see [Git folders](https://docs.databricks.com/aws/en/repos/git-operations-with-repos).

3. Open the `notebooks/` folder and run `00_Setup_Environment` top to bottom. It creates the `zenith_online` catalog, its schemas and volumes, and runs `data_generator` for you to populate the volumes with the raw data (you do not run `data_generator` yourself).
4. Work through `01_Bronze_Ingestion` (Tasks 1-3) and then `02_Silver_Enrichment` (Tasks 4-5), following the step-by-step instructions in each notebook.
5. Finish with `03_Gold_Aggregations` (Tasks 6-7) to complete the end-to-end ETL pipeline. The three business questions at the end are worked Spark SQL examples over your Gold tables, each with what it should show: run them and compare.
