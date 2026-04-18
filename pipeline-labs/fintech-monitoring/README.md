# Lab: Real-Time Fintech Transaction Monitoring Pipeline

> Independent educational resource; not endorsed by Databricks, Inc. "Databricks" and "Delta Lake" are trademarks of their respective owners.

## Author

<img src="https://dataengineer.wiki/download/profilepicture.jpg" alt="Jakub Lasak" width="80" style="border-radius: 50%;" />

**Jakub Lasak** — Helping you interview like seniors, execute like seniors, and think like seniors.

- 🔗 [LinkedIn](https://www.linkedin.com/in/jrlasak/) - Databricks projects and tips
- 📬 [Substack Newsletter](https://dataengineer.wiki/substack) - Exclusive content for Data Engineers
- 🌐 [DataEngineer.wiki](http://dataengineer.wiki/) - Training materials and resources
- 🚀 [More Practice Labs](https://dataengineer.wiki/projects) - Delta Live Tables, table optimization, and more

## Business Scenario

**Company**: ModernPaymentsABC - a payment processor handling 500K+ transactions/day.

**Problem**: Fraud is rising, and overnight batch detection is too slow. The ops team needs real-time alerts for suspicious activity (velocity spikes, geo-anomalies), and compliance needs daily Suspicious Activity Reports (SARs).

**Your Role**: Senior Data Engineer building the end-to-end monitoring pipeline on Databricks.

---

## Learning Objectives

By completing this lab, you will be able to:

- **Ingest Streaming JSON** with Auto Loader and capture malformed data using the Rescued Data Column.
- **Implement Watermarked Deduplication** to handle technical payment gateway retries.
- **Perform Stream-Static Joins** to enrich real-time events with customer and merchant reference data.
- **Design a Rules Engine** using Tumbling and Sliding windows for velocity detection.
- **Build a Medallion Architecture** that serves dual SLAs: real-time streaming alerts and batch Gold reporting.
- **Optimize for Performance** using Liquid Clustering.

---

## Architecture Overview

1. **Bronze**: Raw ingestion via Auto Loader + Watermarked Dedup.
2. **Silver**: Enriched transactions + Real-time risk scoring & alerts.
3. **Gold**: Aggregated merchant summaries and SAR pre-fill datasets.

---

## Prerequisites

- Basic knowledge of PySpark and Delta Lake.
- Access to a Databricks workspace (Free Edition compatible).

⚠️ **Before you start**: Disable AI code suggestions in your Databricks workspace.
Go to **User Settings → Developer → AI-powered code completion** and turn **OFF**:

- **Autocomplete as you type**
- **Automatic Assistant Autocomplete**
  This lab is designed to build muscle memory - auto-completions defeat the purpose.

---

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

4. Follow the numbered notebooks (`01` to `04`) to build the pipeline.

   Each exercise includes a **STUDENT EXERCISE** area for your code and a commented-out **SOLUTION** for verification.

---

## Certification Alignment

This lab prepares you for:

- **Databricks Data Engineer Associate**: Auto Loader, Medallion, Delta basics.
- **Databricks Data Engineer Professional**: Streaming, Watermarks, Windowing, Table optimization.
