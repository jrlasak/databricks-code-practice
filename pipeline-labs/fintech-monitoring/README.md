# Lab: Real-Time Fintech Transaction Monitoring Pipeline

> Independent educational resource; not endorsed by Databricks, Inc. "Databricks" and "Delta Lake" are trademarks of their respective owners.

## Author

<img src="https://dataengineer.wiki/download/profilepicture.jpg?utm_source=github&utm_medium=readme&utm_campaign=databricks-code-practice" alt="Jakub Lasak" width="80" style="border-radius: 50%;" />

**Jakub Lasak** - Databricks Data Engineer (ex-Uber). I write about the Databricks work that gets you hired and promoted.

- 🔗 [LinkedIn](https://www.linkedin.com/in/jrlasak/) - Databricks projects and tips
- 📬 [Substack Newsletter](https://dataengineer.wiki/substack?utm_source=github&utm_medium=readme&utm_campaign=databricks-code-practice) - Exclusive content for Data Engineers
- 🎧 [Podcast](https://dataengineer.wiki/podcast?utm_source=github&utm_medium=readme&utm_campaign=databricks-code-practice) - Databricks and data engineering, on Spotify, Apple, and YouTube
- 🌐 [DataEngineer.wiki](https://dataengineer.wiki/?utm_source=github&utm_medium=readme&utm_campaign=databricks-code-practice) - Interview prep and free Databricks practice
- 🚀 [More Practice Labs](https://github.com/jrlasak/databricks-code-practice) - more pipeline labs, deep-dives, and exercises in this repo

## Business Scenario

**Company**: ModernPaymentsABC - a payment processor handling 500K+ transactions/day.

**Problem**: Fraud is rising, and overnight batch detection is too slow. The ops team needs real-time alerts for suspicious activity (velocity spikes, geo-anomalies), and compliance needs a daily queue of Suspicious Activity Report (SAR) candidates, pre-filled with who, how much and when.

**Your Role**: Senior Data Engineer building the end-to-end monitoring pipeline on Databricks.

---

## Learning Objectives

By completing this lab, you will be able to:

- **Ingest Streaming JSON** with Auto Loader and keep values that do not match the schema (type mismatches, unexpected fields) in the Rescued Data Column.
- **Implement Watermarked Deduplication** to handle technical payment gateway retries.
- **Perform Stream-Static Joins** to enrich real-time events with customer and merchant reference data.
- **Design a Rules Engine** using Tumbling and Sliding windows for velocity detection.
- **Build a Medallion Architecture** that serves dual SLAs: real-time streaming alerts and batch Gold reporting.
- **Optimize for Performance** using Liquid Clustering.

---

## Architecture Overview

1. **Bronze**: Raw ingestion via Auto Loader + Watermarked Dedup.
2. **Silver**: Enriched transactions, velocity and amount spike tables (tumbling and sliding windows), and real-time risk scoring & alerts.
3. **Gold**: Daily per-category summaries and a SAR candidate queue.

---

## Prerequisites

- Basic knowledge of PySpark and Delta Lake.
- Structured Streaming basics: `readStream`/`writeStream`, checkpoints, triggers. The lab uses them from Notebook 01 on without re-teaching them.
- Time: about 5 hours: 15 minutes of setup, then 1 to 1.5 hours for each of Notebooks 01 to 04. Each notebook is one sitting, so you can spread the lab over several days.
- Access to a Databricks workspace (Free Edition compatible).

⚠️ **Before you start**: Disable AI code suggestions in your Databricks workspace.
Open your profile menu, go to **Settings → Developer**, and under **Code Editor** turn **OFF**:

- **Automatic Genie Code Autocomplete**
- **Autocomplete as you type**

This lab builds muscle memory; auto-completions defeat the purpose.

---

## How to Start

1. **Create a Databricks Account**
   - Sign up for a [Databricks Free Edition account](https://www.databricks.com/learn/free-edition) if you don't already have one.
   - Familiarize yourself with the workspace, serverless compute and the notebook interface.

2. **Import this repository to Databricks**
   - In Databricks, open **Workspace**, go to your home folder and click **Create > Git folder**.
   - Paste the GitHub URL for this repository.
   - Authenticate with GitHub if prompted, and select the main branch.
   - The repo will appear as a folder in your workspace, allowing you to edit, run notebooks, and manage files directly from Databricks.
   - For more details, see the official Databricks documentation: [Git folders in Databricks](https://docs.databricks.com/repos/index.html).

3. Open the `notebooks/` folder and run `00_Setup_Environment.py` to create the Unity Catalog infrastructure and generate data.

4. Follow the numbered notebooks (`01` to `04`) to build the pipeline.

   Each exercise includes a **STUDENT EXERCISE** area for your code and a commented-out **SOLUTION** for verification.

5. **Stuck after a streaming write went wrong?** Re-run `00_Setup_Environment.py` (it drops every table and checkpoint and regenerates the data), then run the notebooks in order again. Re-running a corrected streaming cell on its own does nothing: its checkpoint already marked the files as processed.

---

## Certification Alignment

This lab prepares you for:

- **Databricks Data Engineer Associate**: Auto Loader, Medallion, Delta basics.
- **Databricks Data Engineer Professional**: Streaming, Watermarks, Windowing, Table optimization.
