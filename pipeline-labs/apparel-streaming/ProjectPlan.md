# Project Plan

## Information

- **File Organization:**  
  All the files you need to work with are in the `dlt/` folder:
  - Setup scripts: `environment_setup.ipynb`, `environment_maintenance.ipynb`
  - Configuration: `variables.py`
  - Data generation: `data_generator.py`
  - Your DLT pipeline tasks: `01_bronze.py`, `02A_silver.py`, `02B_silver.py`, `02C_silver.py`, `02D_silver.py`, `03_gold.py`
  
- **Getting Help When Stuck:**  
  - **Solution code is included:** Each task in the `dlt/0*.py` files has a "Solution Is Below" section with commented-out code you can review or uncomment.
  - **Complete reference:** For a full working pipeline, see `final_code/final_dlt.py`.
  - Try to solve each task yourself first, then check the solution if needed!
  
- **Configuration:**  
  Many files in this repository rely on `dlt/variables.py` to configure your environment and determine where synthetic data is generated. To change the catalog name or other object names, simply update them in `dlt/variables.py`.
  
- **Resource Limits:**  
  If you see a "Resources exhausted" error:
  - Stop the `dlt/data_generator.py` script if it's running.
  - Wait a short time before trying again.
  - _Note: This is a limitation of Databricks Free Edition._

---

## Databricks Configuration Requirements

> **Tip:** If you are new to Databricks, take time to explore the workspace UI and documentation. Understanding catalogs, schemas, and volumes will help you later.

To run this project end-to-end, complete the following setup steps in your Databricks workspace:

1. **Create a Databricks Account**

   - Sign up for a [Databricks Free Edition account](https://www.databricks.com/learn/free-edition) if you don’t already have one.
   - Familiarize yourself with the workspace, clusters, and notebook interface.

2. **Import this repository to Databricks**

   - In Databricks, go to the Workspace sidebar and click the "Repos" section, click "Add Repo".
     - Alternatively, go to your personal folder, click "create" and select "git folder".
   - Paste the GitHub URL for this repository.
   - Authenticate with GitHub if prompted, and select the main branch.
   - The repo will appear as a folder in your workspace, allowing you to edit, run notebooks, and manage files directly from Databricks.
   - For more details, see the official Databricks documentation: [Repos in Databricks](https://docs.databricks.com/repos/index.html).

3. **Run the `dlt/environment_setup.ipynb` notebook to set up a catalog, schemas and volumes for the synthetic data generator**. It will use the paths defined in `dlt/variables.py` for creating appropriate objects in Unity Catalog.

4. **Create and Configure a DLT Pipeline**

   - In Databricks, create a new Delta Live Tables (DLT) pipeline.
   - Set the pipeline to use your project's `dlt/` folder as the source.
   - Set the default catalog to `apparel_store`.
   - Set the target schema to, for example, `02_silver` (or as appropriate for your workflow).
   - Configure cluster and permissions as needed.
   - Set the source folder to the dlt/ folder
   - Add the following files as pipeline source code:
     - `dlt/01_bronze.py` - Bronze layer ingestion
     - `dlt/02A_silver.py`, `dlt/02B_silver.py`, `dlt/02C_silver.py`, `dlt/02D_silver.py` - Silver layer transformations
     - `dlt/03_gold.py` - Gold layer aggregations
   - **Tip:** Review the DLT pipeline settings and documentation. Understand the difference between streaming and batch tables.

5. **Run the synthetic data generator (`dlt/data_generator.py`) to initialize some data.**
   - It will stream infinitely until stopped, so stop it after a few minutes.
   - **Tip:** Check the output location and schema of the generated data. Use the provided markdown for table schemas and data quirks.

---

## How to Work on the DLT Pipeline

> **Your tasks are now organized in separate files!**  
> Each file in the `dlt/` folder that starts with `0*` contains specific tasks for that layer of the pipeline. Work through them in order.

### Working Approach

1. **Read the file headers**: Each `dlt/0*.py` file starts with detailed comments explaining:

   - The goal of that pipeline layer
   - Tips and best practices
   - What you need to implement

2. **Follow the task order**:

   - **`dlt/01_bronze.py`** - Start here: Ingest raw data from source volumes
   - **`dlt/02A_silver.py`** - Clean sales data and create intermediate views
   - **`dlt/02B_silver.py`** - Implement Customer dimension with SCD Type 2
   - **`dlt/02C_silver.py`** - Implement Product dimension with SCD Type 2
   - **`dlt/02D_silver.py`** - Implement Store dimension with SCD Type 2
   - **`dlt/03_gold.py`** - Create business-ready aggregations

3. **Understand the data quirks**: Review [SynteticDataGenerator.md](SynteticDataGenerator.md) to understand:

   - Table schemas for each data stream
   - Intentional data quality issues (negatives, nulls, CDC updates)
   - Why certain transformations and expectations are needed

4. **Write your code**: Each file has sections with tasks where you'll implement:

   - DLT table and view definitions
   - Data quality expectations
   - Transformations and aggregations
   - SCD Type 2 logic for dimensions
   
   **Each task includes:**
   - Clear requirements with checkboxes `[ ]`
   - Business logic explanations
   - A "Solution Is Below" section with commented code you can review or uncomment

5. **Test as you go**:

   - Run the `dlt/data_generator.py` script to generate live data
   - Update your DLT pipeline in Databricks to include the files you're working on
   - Observe how your transformations handle the streaming data
   - Check the DLT pipeline UI for data quality metrics and lineage

6. **Need help?**: 
   - **First**: Scroll down below each task to find the "Solution Is Below" section with working code (commented out)
   - **Second**: Review the complete pipeline in `final_code/final_dlt.py` for the full reference implementation
   - **Tip**: Try solving each task yourself first to maximize learning!

### Key Concepts You'll Practice

- **Bronze Layer**: Schema enforcement, metadata tracking, streaming ingestion
- **Silver Layer**: Data quality expectations, cleansing, SCD Type 2 for dimension tracking
- **Gold Layer**: Business aggregations, time-based analytics, watermarking for late data

**Ready to start?** Open `dlt/01_bronze.py` and begin with the Bronze layer tasks!
