# Databricks Data Engineer Associate Certification Lab

> Independent educational resource; not endorsed by Databricks, Inc. "Databricks" and "Delta Lake" are trademarks of their respective owners.

## Author

<img src="https://dataengineer.wiki/download/profilepicture.jpg" alt="Jakub Lasak" width="80" style="border-radius: 50%;" />

**Jakub Lasak** — Helping you interview like seniors, execute like seniors, and think like seniors.

- 🔗 [LinkedIn](https://www.linkedin.com/in/jrlasak/) - Databricks projects and tips
- 📬 [Substack Newsletter](https://dataengineer.wiki/substack) - Exclusive content for Data Engineers
- 🌐 [DataEngineer.wiki](http://dataengineer.wiki/) - Training materials and resources
- 🚀 [More Practice Labs](https://dataengineer.wiki/projects) - Delta Live Tables, table optimization, and more

> **Looking for more info on passing the Databricks Data Engineer Associate Certification exam?**  
> Check out helpful resources including YouTube videos and official Databricks courses at [dataengineer.wiki/certifications/data-engineer-associate](https://dataengineer.wiki/certifications/data-engineer-associate).

## High-Level Overview

This lab provides hands-on practice to prepare for the Databricks Certified Data Engineer Associate exam. You will build a production-grade, end-to-end data pipeline using real-world scenarios and datasets. The exercises are designed to be completed within the Databricks Free Community Edition, allowing you to develop practical skills without any cost.

The lab covers the entire data engineering lifecycle, including:

- Ingesting raw data from various sources using Auto Loader and COPY INTO.
- Implementing the Medallion Architecture (Bronze, Silver, Gold layers).
- Performing data transformations, quality checks, and implementing SCD Type 2.
- Orchestrating workflows with Databricks Jobs and multi-task dependencies.
- Managing data governance using Unity Catalog with role-based access control.

## How It Prepares You for the Exam

This lab is structured to cover the key topics outlined in [the official Databricks Data Engineer Associate exam info](https://www.databricks.com/learn/certification/data-engineer-associate). By completing the notebooks, you will gain practical experience in the following areas:

### Section 1: Databricks Intelligence Platform

- **What you'll practice:** Enabling features that simplify data layout decisions, understanding the value of the Data Intelligence Platform, and identifying the applicable compute for specific use cases.

### Section 2: Development and Ingestion

- **What you'll practice:** Leveraging Notebooks functionality, working with Auto Loader from various sources (JSON, CSV), using COPY INTO for batch incremental loads, and handling schema evolution.

### Section 3: Data Processing & Transformations

- **What you'll practice:** Implementing the three layers of the Medallion Architecture (Bronze, Silver, Gold), performing data quality transformations, implementing Slowly Changing Dimensions (SCD Type 2), and computing complex aggregations with PySpark window functions.

### Section 4: Productionizing Data Pipelines

- **What you'll practice:** Creating and configuring Databricks Jobs, implementing multi-task workflows with dependencies, using job parameters with widgets, implementing error handling and retry logic, and using serverless compute.

### Section 5: Data Governance & Quality

- **What you'll practice:** Understanding Unity Catalog's three-level namespace (catalog.schema.table), creating and managing catalogs, schemas, and volumes, creating managed tables, implementing access control with GRANT and REVOKE statements, and data quality validation patterns.

## Additional Exam Topics

This lab focuses on core data engineering workflows. If you want to practice additional exam topics such as **Delta Live Tables (DLT) pipelines** or **Delta table optimization techniques**, check out my other hands-on labs at [dataengineer.wiki/projects](https://dataengineer.wiki/projects/).

## Get 50% Off Your Certification

Register for a "Virtual Learning Festival", complete the required courses in the timeline provided to automatically receive 50% off the certification.

Look for upcoming Databricks Festival here - https://community.databricks.com/t5/events/eb-p/databricks-community-events

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

3. Open the `notebooks/` folder and run `00_Setup_Environment.py` to create the Unity Catalog infrastructure and generate data.

4. Follow the numbered notebooks (`01` to `06`) to build the pipeline.

   Each exercise includes a `TODO` area for your code and a commented-out solution for verification.
