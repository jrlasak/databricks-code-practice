### **Project Title: E-commerce User Behavior Analytics Pipeline**

> Independent educational resource; not endorsed by Databricks, Inc. "Databricks" and "Delta Lake" are trademarks of their respective owners.

### Author

<img src="https://dataengineer.wiki/download/profilepicture.jpg" alt="Jakub Lasak" width="80" style="border-radius: 50%;" />

**Jakub Lasak** — Helping you interview like seniors, execute like seniors, and think like seniors.

- 🔗 [LinkedIn](https://www.linkedin.com/in/jrlasak/) - Databricks projects and tips
- 📬 [Substack Newsletter](https://dataengineer.wiki/substack) - Exclusive content for Data Engineers
- 🌐 [DataEngineer.wiki](http://dataengineer.wiki/) - Training materials and resources
- 🚀 [More Practice Labs](https://dataengineer.wiki/projects) - Delta Live Tables, table optimization, and more

### Project Overview

This project is a comprehensive, hands-on lab designed to prepare you for the **[Databricks Certified Associate Developer for Apache Spark](https://www.databricks.com/learn/certification/apache-spark-developer-associate)** certification exam. Instead of studying abstract concepts, you will build a realistic, end-to-end data pipeline that solves a real-world business problem, applying dozens of the specific skills tested on the exam.

---

### **Why Complete This Project?**

Passing a certification exam requires more than just memorizing documentation. It requires understanding _how_ and _why_ specific tools and techniques are used in practice. This project bridges that gap by providing:

- **A Realistic Scenario:** You aren't just running random code snippets. You are building a complete solution from raw data to valuable business insights, following the industry-standard Medallion Architecture.
- **Purposeful Complexity:** The project is intentionally designed to include common data engineering challenges. The synthetic data generator creates **data skew**—a frequent source of performance bottlenecks—which you will diagnose and solve.
- **Comprehensive Certification Coverage:** The pipeline is carefully crafted to touch upon topics from all major sections of the certification exam, from Spark architecture and the DataFrame API to Structured Streaming and performance tuning.
- **Serverless-Ready Code:** The entire project is designed to run on the **Databricks Free Community Edition**, which means you can complete it without any cost, while learning to work within the constraints of a modern, serverless Spark environment.

---

#### **1. Project Context and Scenario**

You are a Data Engineer at **"Zenith Online,"** a rapidly growing e-commerce platform. The marketing and product teams are struggling to make data-driven decisions because their raw user data is disconnected and hard to analyze. They have come to you with a critical business need:

_"We need to understand our user journey. What products are people looking at? What actions lead to a purchase? Who are our most valuable customers? We need reliable, aggregated data to answer these questions."_

Your mission is to take on this request and build an automated, scalable data pipeline. You will process raw, messy user event data, combine it with customer and product information, and produce clean, aggregated "Gold" tables ready for the analytics team. This pipeline will be the single source of truth for user behavior analytics at Zenith Online.

---

#### **2. Data Sources (To be generated synthetically)**

- **Raw User Events (JSON, Streaming):** A continuous stream of JSON files representing user actions on the website. Each event has a timestamp, user ID, event type (`view_product`, `add_to_cart`, `purchase`), and associated data like `product_id`. This dataset will be **intentionally skewed**, with a few specific products generating a disproportionately high number of `view_product` events.
- **Customer Profiles (CSV, Batch):** A CSV file containing customer details like `customer_id`, `signup_date`, and `location`. This is a dimension table that is updated periodically.
- **Product Details (Parquet, Batch):** A Parquet file containing product information like `product_id`, `product_name`, `category`, and `price`. This is another dimension table.

---

### **How This Project Prepares You for the Certification Exam**

This project provides practical, hands-on experience covering the majority of the topics in the **Databricks Certified Associate Developer for Apache Spark** exam outline.

#### **Section 1: Apache Spark Architecture and Components**

- You will see the entire **execution hierarchy** in action (transformations are defined, and actions trigger jobs).
- The pipeline demonstrates **lazy evaluation**—the entire plan is built before any data is processed.
- You will use key features from multiple Spark Modules, including **Spark SQL**, **DataFrames**, **Structured Streaming**, and the **Pandas API on Spark**.

#### **Section 2: Using Spark SQL**

- **Reading Multiple Formats:** The Bronze layer ingests data from three different file formats: **JSON** (with Auto Loader), **CSV**, and **Parquet**.
- **Writing to Delta Tables:** You will write data to Delta tables using different save modes (`append` for streaming, `overwrite` for batch).
- **Partitioning for Optimization:** The final Gold table is written with `partitionBy()`, a key strategy for optimizing data retrieval.
- **Executing SQL Queries:** The final step of the project uses `spark.sql()` to query the Gold layer Delta tables directly, demonstrating how to use SQL for analytics.

#### **Section 3: Developing Apache Spark™ DataFrame/DataSet API Applications**

- **Manipulating Columns:** The Silver layer extensively uses functions like `withColumn()`, `withColumnRenamed()`, and `select()` to clean and structure the data.
- **Data Deduplication:** You will perform stateful stream-to-stream deduplication using a watermark.
- **Aggregate Operations:** The Gold layer uses a wide range of aggregations, including `groupBy().agg()`, `count()`, `sum()`, and `approx_count_distinct()`.
- **Date and Timestamp Functions:** The pipeline uses `to_timestamp()`, `to_date()`, and `current_timestamp()` to handle time-based data correctly.
- **Combining DataFrames (Joins):** You will implement both `inner` and `left` joins in the Silver layer to enrich event data.
- **Broadcast Joins:** You will explicitly use `F.broadcast()` on a dimension table, a critical optimization technique for joining a large DataFrame with a smaller one.
- **Managing Schemas:** You will define and apply a `StructType` schema during data ingestion to ensure data quality and prevent errors.
- **User-Defined Functions (UDFs):** You will create and apply a **Pandas UDF** to perform complex data transformation that is difficult to express with built-in functions.

#### **Section 4: Troubleshooting and Tuning Apache Spark DataFrame API Applications**

- **Handling Data Skew:** This is a core focus of the project. The data is intentionally skewed, and you will implement a **salting** technique in the Gold layer to mitigate the skew and reduce shuffling, a common performance bottleneck.
- **Partitioning:** You will use `partitionBy` when writing the final Gold table to dramatically improve query performance for downstream users.
- **Adaptive Query Execution (AQE):** While the code runs on Databricks where AQE is enabled by default, this project provides a perfect scenario to understand the problems AQE helps solve, such as dynamically handling skew and optimizing shuffles.

#### **Section 5: Structured Streaming**

- **End-to-End Streaming Pipeline:** The Bronze-to-Silver pipeline is a complete Structured Streaming application.
- **Streaming Sources and Sinks:** You will use Auto Loader (`cloudFiles`) as a streaming source and Delta Lake as a streaming sink.
- **Streaming Deduplication with Watermarks:** You will use `withWatermark()` combined with `dropDuplicates()` to manage duplicate data from the stream with exactly-once semantics.
- **Window and Aggregation:** The final Gold layer includes a window function (`rank()`) applied over aggregated data.

#### **Section 7: Using Pandas API on Spark**

- **Creating and Invoking Pandas UDFs:** You will define a Pandas UDF with the `@pandas_udf` decorator and apply it to a Spark DataFrame column, demonstrating a powerful way to leverage Python libraries within your Spark transformations.

## How to Start

1. **Create a Databricks Account**
   - Sign up for a [Databricks Free Edition account](https://www.databricks.com/learn/free-edition) if you don't already have one.
   - Familiarize yourself with the workspace, clusters, and notebook interface.

2. **Import this repository to Databricks**
   - In Databricks, go to the Workspace sidebar and click the "Repos" section, click "Add Repo".
     - Alternatively, go to your personal folder, click "create" and select "git folder".
   - Paste the GitHub URL for this repository.
   - Authenticate with GitHub if prompted, and select the main branch.
   - The repo will appear as a folder in your workspace, allowing you to edit, run notebooks, and manage files directly from Databricks.
   - For more details, see the official Databricks documentation: [Repos in Databricks](https://docs.databricks.com/repos/index.html).

3. Open the `lab/` folder and run `environment_setup.ipynb` to create the necessary catalogs, schemas, and volumes.
4. Run `data_generator.py` to populate your volumes with the raw data.
5. Open `ELT.py` and follow the step-by-step instructions to build the end-to-end ETL pipeline.
