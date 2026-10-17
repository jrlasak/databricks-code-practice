# Databricks notebook source
# MAGIC %md
# MAGIC # Notebook 01: Environment Setup & Unity Catalog
# MAGIC
# MAGIC **Exam Coverage**: Sections 1 (Databricks Intelligence Platform) & 5 (Data Governance & Quality)
# MAGIC
# MAGIC **Duration**: about 30 minutes
# MAGIC
# MAGIC ---
# MAGIC ## Learning Objectives
# MAGIC By the end of this notebook, you will be able to:
# MAGIC - Understand Unity Catalog's three-level namespace (catalog.schema.table)
# MAGIC - Navigate the catalog, schemas and volumes the setup notebook created
# MAGIC - Create managed tables in Unity Catalog
# MAGIC - Implement access control using GRANT and REVOKE
# MAGIC - Understand serverless compute and its benefits
# MAGIC ---

# COMMAND ----------

# MAGIC %md
# MAGIC ## Section 1: Introduction and Setup
# MAGIC **The scenario.** You are the data engineer at CartHarborHQ, a fictional US online retailer with 10,000 customers and 1,000 products across Electronics, Clothing, Home & Garden, Sports & Outdoors and Books. Customer, catalog, order and clickstream files land in a Unity Catalog volume every day.
# MAGIC
# MAGIC Unity Catalog provides centralized governance for all data and AI assets in Databricks.
# MAGIC ### Three-Level Namespace
# MAGIC ```
# MAGIC catalog.schema.table
# MAGIC ```
# MAGIC This structure enables:
# MAGIC - Logical separation of environments (dev, staging, prod)
# MAGIC - Organizational boundaries
# MAGIC - Fine-grained access control

# COMMAND ----------

# MAGIC %md
# MAGIC Import shared variables and configuration

# COMMAND ----------

# MAGIC %run ./variables

# COMMAND ----------

# Set the current catalog and schema for this session
spark.sql(f"USE CATALOG {CATALOG_NAME}")
spark.sql(f"USE SCHEMA {BRONZE_SCHEMA}")

# Verify current context
print(f"Current Catalog: {spark.catalog.currentCatalog()}")
print(f"Current Schema: {spark.catalog.currentDatabase()}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Section 2: Explore Unity Catalog Structure
# MAGIC Unity Catalog organizes data in a hierarchy. The cells below list what notebook 00 created.

# COMMAND ----------

# Display all catalogs available to you
spark.sql("SHOW CATALOGS").show()

# COMMAND ----------

# Display all schemas in the current catalog
spark.sql(f"SHOW SCHEMAS IN {CATALOG_NAME}").show()

# COMMAND ----------

# Explore the landing volume structure in the landing zone for sales data
display(dbutils.fs.ls(SALES_LANDING_PATH))

# COMMAND ----------

# List tables in the bronze schema
# Empty on a first run: run it again after Exercise 1 creates customers_managed

spark.sql(f"SHOW TABLES IN {CATALOG_NAME}.{BRONZE_SCHEMA}").show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Understanding Volumes
# MAGIC Volumes provide governed access to files (non-tabular data).
# MAGIC
# MAGIC **Three-level namespace for volumes:**
# MAGIC ```
# MAGIC catalog.schema.volume
# MAGIC ```
# MAGIC **Types:**
# MAGIC - **Managed**: Unity Catalog manages lifecycle and storage
# MAGIC - **External**: Points to external cloud storage

# COMMAND ----------

# MAGIC %md
# MAGIC ## Section 3: Create Your First Managed Table
# MAGIC **Managed tables**: Unity Catalog manages both metadata AND data files.
# MAGIC - Drop the table → deletes metadata + data
# MAGIC - Storage location is automatic
# MAGIC
# MAGIC We'll load customer data from the landing zone and create a managed table.

# COMMAND ----------

# Define the path to sample customer data
customers_landing_path = f"{LANDING_BASE_PATH}/customers/"

# Read JSON data from the landing zone
customers_df = spark.read.json(customers_landing_path)

# Display the schema and sample data
customers_df.printSchema()
display(customers_df.limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ### 🎯 EXERCISE 1: Create a Managed Table
# MAGIC **Your task**: Save the `customers_df` DataFrame as a managed Delta table.
# MAGIC
# MAGIC **Why**: UC owns a managed table's files, so nobody tracks a storage path and DROP TABLE leaves no orphaned data.
# MAGIC
# MAGIC **Requirements:**
# MAGIC - Table name: the `managed_table_name` variable, already set in the cell below to `{CATALOG_NAME}.{BRONZE_SCHEMA}.customers_managed`
# MAGIC - Delta format, overwrite mode (this is the first load)
# MAGIC - A managed table: you give a table name, never a storage path
# MAGIC
# MAGIC **Hint**: the DataFrame writer has a method that registers the output as a Unity Catalog table instead of writing to a path.

# COMMAND ----------

# ✏️ STUDENT EXERCISE
# TODO: Create a managed table

# Table name (provided)
managed_table_name = f"{CATALOG_NAME}.{BRONZE_SCHEMA}.customers_managed"

# Write your code here:
customers_df.write \




print(f"Created managed table: {managed_table_name}")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC **Solution below** ⬇️

# COMMAND ----------

# ✅ SOLUTION: Create Managed Table

# managed_table_name = f"{CATALOG_NAME}.{BRONZE_SCHEMA}.customers_managed"

# customers_df.write \
#     .format("delta") \
#     .mode("overwrite") \
#     .saveAsTable(managed_table_name)

# print(f"✅ Created managed table: {managed_table_name}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 🔍 Verify Your Results
# MAGIC Run the cell below. You should see:
# MAGIC - `format` = delta, and a `location` you never typed: for a managed table Unity Catalog picks the path itself, inside the managed storage location of the table's schema, or of its catalog, or of the metastore, whichever is set closest to the table.
# MAGIC - **10,200** rows: 10,000 customers + 200 planted duplicates (notebook 03 removes them on the way to Silver).

# COMMAND ----------

# 🔍 Verify: check table details to see where data is stored, and the row count
display(spark.sql(f"DESCRIBE DETAIL {managed_table_name}"))
print(f"Rows in {managed_table_name}: {spark.table(managed_table_name).count():,} (10,200 on a first run)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Section 3b: Create Security Groups
# MAGIC Before we can grant permissions, we need to create the groups that will receive those permissions.
# MAGIC ### What are Groups?
# MAGIC **Groups** are collections of users that share the same access permissions. With groups, a new hire joins one group instead of collecting GRANTs one by one, and an audit reads three group grants instead of one per user.
# MAGIC ### Persona-Based Groups
# MAGIC We'll create one group for each data team role this lab grants access to:
# MAGIC | Group | Role | Typical Access |
# MAGIC |-------|------|----------------|
# MAGIC | `data_engineers` | Build and maintain pipelines | Full access to Bronze, Silver, Gold |
# MAGIC | `data_analysts` | Create reports and dashboards | Read-only on Gold |
# MAGIC | `data_scientists` | Build ML models | Read-only on Silver (features) and Gold (labels) |
# MAGIC ### Group Management Notes
# MAGIC **In production:**
# MAGIC - Groups are typically created by workspace admins
# MAGIC - Often synced from corporate identity providers (Azure AD, Okta, etc.)
# MAGIC - Group membership is managed centrally
# MAGIC
# MAGIC **For this lab:**
# MAGIC - Groups must be created manually in workspace settings (this creates account groups, the only groups Unity Catalog can grant to)
# MAGIC - You'll need workspace admin or account admin privileges
# MAGIC - Groups persist across workspace sessions
# MAGIC ### 🔧 How to Create Groups Manually
# MAGIC **Step-by-step instructions:**
# MAGIC 1. **Open the groups page:**
# MAGIC    - Click your username (top right) > **Settings** > **Identity and access**
# MAGIC    - OR append `/settings/workspace/identity-and-access/groups` to your workspace URL
# MAGIC 2. **Create Groups:**
# MAGIC    - Next to **Groups**, click **Manage** > **Add group** > **Add new**, enter the name, click **Add**
# MAGIC    - Repeat for each of the following groups:
# MAGIC      - `data_engineers`
# MAGIC      - `data_analysts`
# MAGIC      - `data_scientists`
# MAGIC 3. **Add Yourself to Groups (Optional):**
# MAGIC    - Click on a group name
# MAGIC    - Click **Add Members**
# MAGIC    - Add your user email
# MAGIC    - This allows you to test `is_account_group_member()` later
# MAGIC
# MAGIC **⚠️ Important:** Complete this step before proceeding to the GRANT statements below, as they depend on these groups existing.

# COMMAND ----------

# Verify groups exist (run this after creating groups in Settings > Identity and access)
print("📋 Checking for required groups...")
print("="*70)

required_groups = [
    "data_engineers",
    "data_analysts",
    "data_scientists"
]

try:
    all_groups = [row.name for row in spark.sql("SHOW GROUPS").collect()]

    missing_groups = []
    for group in required_groups:
        if group in all_groups:
            print(f"✅ {group} - Found")
        else:
            print(f"❌ {group} - NOT FOUND")
            missing_groups.append(group)

    if missing_groups:
        print(f"\n⚠️  Missing groups: {', '.join(missing_groups)}")
        print(f"⚠️  Please create these groups in Settings > Identity and access before proceeding.")
        print(f"⚠️  Navigate to: [Workspace URL]/settings/workspace/identity-and-access/groups")
    else:
        print(f"\n✅ All required groups exist!")

except Exception as e:
    print(f"⚠️  Could not verify groups: {e}")
    print(f"⚠️  You may need admin privileges to list groups")

print("="*70)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Checking Your Group Membership
# MAGIC Run the cell below to see your user and whether you belong to each of the three groups.
# MAGIC
# MAGIC **Note:** Adding users to groups is done in workspace settings, not SQL. `is_account_group_member()` checks membership at the account level, where Unity Catalog grants live. If you skipped the optional "Add yourself" step, every group shows `❌ Not a member`: expected, and the GRANT cells below still work.

# COMMAND ----------

# Check current user and their group memberships
current_user_email = spark.sql("SELECT current_user()").collect()[0][0]
print(f"Current user: {current_user_email}")

# Check if you're a member of the key groups
print("\nYour group memberships:")
for group in required_groups:
    try:
        is_member = spark.sql(f"SELECT is_account_group_member('{group}')").collect()[0][0]
        status = "✅ Member" if is_member else "❌ Not a member"
        print(f"  {group}: {status}")
    except Exception as e:
        print(f"  {group}: ⚠️  Could not check ({str(e)[:50]})")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Section 4: Access Control with GRANT and REVOKE
# MAGIC Unity Catalog provides fine-grained access control.
# MAGIC ### Access Levels
# MAGIC - **Catalog level**: All schemas and tables
# MAGIC - **Schema level**: All tables in schema
# MAGIC - **Table level**: Specific tables
# MAGIC ### Common Privileges
# MAGIC | Privilege | What It Allows |
# MAGIC |-----------|----------------|
# MAGIC | `SELECT` | Read data |
# MAGIC | `MODIFY` | Insert, update, delete |
# MAGIC | `CREATE TABLE` / `CREATE SCHEMA` | Create tables in a schema / schemas in a catalog |
# MAGIC | `USE CATALOG` / `USE SCHEMA` | Reach the objects inside (required to read anything) |
# MAGIC | `ALL PRIVILEGES` | All permissions |
# MAGIC ### Reaching a table takes three privileges
# MAGIC To read a table, a principal needs `USE CATALOG` on the catalog, `USE SCHEMA` on the schema and `SELECT` on the table (or on the schema, which then covers every current and future table in it). `USE CATALOG` and `USE SCHEMA` grant no data access on their own; they only open the path. (`USAGE` is the legacy Hive metastore name and does not exist in Unity Catalog.)

# COMMAND ----------

# Example: Grant full access to data engineers on bronze schema

spark.sql(f"""
    GRANT USE CATALOG ON CATALOG {CATALOG_NAME} TO `data_engineers`
""")

spark.sql(f"""
    GRANT ALL PRIVILEGES ON SCHEMA {CATALOG_NAME}.{BRONZE_SCHEMA} TO `data_engineers`
""")

print("✅ Granted USE CATALOG and full access to data_engineers on bronze schema")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ### 🎯 EXERCISE 2: Grant Permissions for Data Analysts
# MAGIC **Scenario**: Data analysts need read-only access to the Gold schema (business reports).
# MAGIC
# MAGIC **Your task**: Write GRANT statements for the `data_analysts` group.
# MAGIC
# MAGIC **Why**: Grant to groups, never people: when an analyst joins or leaves, one membership changes instead of every GRANT.
# MAGIC
# MAGIC **Requirements:**
# MAGIC 1. `USE CATALOG` on the catalog
# MAGIC 2. `USE SCHEMA` on the gold schema
# MAGIC 3. `SELECT` on the gold schema
# MAGIC
# MAGIC **SQL Syntax:**
# MAGIC ```sql
# MAGIC GRANT privilege ON object_type object_name TO `principal`
# MAGIC ```
# MAGIC **Hint**: Use `{CATALOG_NAME}` and `{GOLD_SCHEMA}` variables

# COMMAND ----------

# ✏️ STUDENT EXERCISE
# TODO: Grant read-only access to data analysts on Gold schema

# 1. USE CATALOG on the catalog


# 2. USE SCHEMA on gold schema


# 3. SELECT on gold schema


print("Granted read-only access to data_analysts on gold schema")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC **Solution below** ⬇️

# COMMAND ----------

# ✅ SOLUTION: Grant Permissions for Data Analysts

# spark.sql(f"""
#     GRANT USE CATALOG ON CATALOG {CATALOG_NAME} TO `data_analysts`
# """)

# spark.sql(f"""
#     GRANT USE SCHEMA ON SCHEMA {CATALOG_NAME}.{GOLD_SCHEMA} TO `data_analysts`
# """)

# spark.sql(f"""
#     GRANT SELECT ON SCHEMA {CATALOG_NAME}.{GOLD_SCHEMA} TO `data_analysts`
# """)

# print("✅ Granted read-only access to data_analysts on gold schema")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ### 🎯 EXERCISE 3: Grant Permissions for Data Scientists
# MAGIC **Scenario**: Data scientists need read-only access to BOTH Silver and Gold schemas.
# MAGIC
# MAGIC **Your task**: Write GRANT statements for the `data_scientists` group.
# MAGIC
# MAGIC **Why**: Scientists read Silver features and Gold labels, never Bronze; the grant enforces that boundary instead of leaving it to habit.
# MAGIC
# MAGIC **Requirements:**
# MAGIC 1. `USE CATALOG` on the catalog
# MAGIC 2. `USE SCHEMA` + `SELECT` on silver
# MAGIC 3. `USE SCHEMA` + `SELECT` on gold
# MAGIC
# MAGIC **Hint**: You'll need 5 GRANT statements total

# COMMAND ----------

# ✏️ STUDENT EXERCISE
# TODO: Grant read-only access to data scientists on Silver and Gold

# 1. USE CATALOG on the catalog


# 2. USE SCHEMA and SELECT on silver schema


# 3. USE SCHEMA and SELECT on gold schema


print("Granted read-only access to data_scientists on silver and gold")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC **Solution below** ⬇️

# COMMAND ----------

# ✅ SOLUTION: Grant Permissions for Data Scientists

# spark.sql(f"""
#     GRANT USE CATALOG ON CATALOG {CATALOG_NAME} TO `data_scientists`
# """)

# spark.sql(f"""
#     GRANT USE SCHEMA ON SCHEMA {CATALOG_NAME}.{SILVER_SCHEMA} TO `data_scientists`
# """)

# spark.sql(f"""
#     GRANT SELECT ON SCHEMA {CATALOG_NAME}.{SILVER_SCHEMA} TO `data_scientists`
# """)

# spark.sql(f"""
#     GRANT USE SCHEMA ON SCHEMA {CATALOG_NAME}.{GOLD_SCHEMA} TO `data_scientists`
# """)

# spark.sql(f"""
#     GRANT SELECT ON SCHEMA {CATALOG_NAME}.{GOLD_SCHEMA} TO `data_scientists`
# """)

# print("✅ Granted read-only access to data_scientists on silver and gold")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 🔍 Verify Your Results
# MAGIC Run the cell below. You should see:
# MAGIC - **Catalog**: `USE CATALOG` for `data_engineers`, `data_analysts` and `data_scientists`.
# MAGIC - **Gold**: `USE SCHEMA` and `SELECT` for `data_analysts` and for `data_scientists`.
# MAGIC - **Silver**: `USE SCHEMA` and `SELECT` for `data_scientists` only.
# MAGIC
# MAGIC A missing row means that GRANT did not run. Without the `USE CATALOG` and `USE SCHEMA` rows, the `SELECT` grant reads nothing.

# COMMAND ----------

# 🔍 Verify grants on the catalog, gold and silver
display(spark.sql(f"SHOW GRANTS ON CATALOG {CATALOG_NAME}"))
display(spark.sql(f"SHOW GRANTS ON SCHEMA {CATALOG_NAME}.{GOLD_SCHEMA}"))
display(spark.sql(f"SHOW GRANTS ON SCHEMA {CATALOG_NAME}.{SILVER_SCHEMA}"))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Revoking Permissions
# MAGIC To revoke permissions, use `REVOKE`:
# MAGIC ```sql
# MAGIC REVOKE privilege ON object_type object_name FROM `principal`
# MAGIC ```
# MAGIC **Example:**

# COMMAND ----------

# Example: Revoke SELECT permission (commented out)
# spark.sql(f"""
#     REVOKE SELECT ON SCHEMA {CATALOG_NAME}.{GOLD_SCHEMA} FROM `data_analysts`
# """)

# Note: Commented to preserve our grants

# COMMAND ----------

# MAGIC %md
# MAGIC ## Section 5: Serverless Compute
# MAGIC ### What is Serverless?
# MAGIC Serverless compute eliminates cluster configuration and management.
# MAGIC
# MAGIC **Key characteristics:**
# MAGIC - Starts in seconds for notebooks; serverless jobs offer a standard mode (cheaper, a few minutes to start) and a performance-optimized mode (faster start)
# MAGIC - Autoscaling and Photon are on automatically, no cluster config
# MAGIC - Billed on the DBUs the workload consumes, no idle cluster
# MAGIC ### Serverless vs Custom Clusters
# MAGIC | Feature | Serverless | Custom Clusters |
# MAGIC |---------|------------|----------------|
# MAGIC | **Startup** | Seconds (notebooks) | Minutes |
# MAGIC | **Configuration** | Automatic | Manual |
# MAGIC | **Scaling** | Auto | Manual/auto rules |
# MAGIC | **Use case** | Recommended for jobs and interactive work | Workloads needing custom Spark config, libraries or instance types serverless does not support |
# MAGIC | **Billing** | DBUs consumed | DBUs while the cluster is up |
# MAGIC ### Best Practices
# MAGIC - ✅ Use for development and exploration
# MAGIC - ✅ Use for ad-hoc queries
# MAGIC - ✅ Databricks recommends serverless for all job tasks; pick classic compute only when you need a config serverless does not allow
# MAGIC
# MAGIC **This lab runs on serverless: it is the only compute in Databricks Free Edition.**

# COMMAND ----------

# MAGIC %md
# MAGIC ## Section 6: Summary and Checkpoint
# MAGIC ### 🎯 Key Concepts
# MAGIC **1. Unity Catalog Three-Level Namespace**
# MAGIC - `catalog.schema.table` structure
# MAGIC - Enables environment separation and access control
# MAGIC
# MAGIC **2. Catalogs, Schemas, and Volumes**
# MAGIC - Hierarchical organization
# MAGIC - Volumes for governed file access
# MAGIC
# MAGIC **3. Managed Tables**
# MAGIC - UC controls storage, DROP deletes data
# MAGIC - Best for data exclusively used in Databricks
# MAGIC
# MAGIC **4. Access Control**
# MAGIC - GRANT/REVOKE for permissions
# MAGIC - Persona-based patterns (engineers, analysts, scientists)
# MAGIC - Privileges: SELECT, MODIFY, CREATE TABLE, USE CATALOG, USE SCHEMA, ALL PRIVILEGES
# MAGIC
# MAGIC **5. Serverless Compute**
# MAGIC - Starts in seconds for notebooks, autoscaling and Photon automatic
# MAGIC - Recommended for jobs and interactive work
# MAGIC - The only compute in Databricks Free Edition
# MAGIC ### ✅ Exam Checklist
# MAGIC Can you:
# MAGIC - [ ] Write three-level namespace references?
# MAGIC - [ ] Create managed tables with saveAsTable()?
# MAGIC - [ ] Write GRANT statements for different personas?
# MAGIC - [ ] Explain why USE CATALOG + USE SCHEMA are required to reach a table?
# MAGIC - [ ] Describe serverless compute benefits?
# MAGIC ### 📚 Next Steps
# MAGIC **Notebook 02** covers:
# MAGIC - Auto Loader for incremental ingestion
# MAGIC - Schema evolution and inference
# MAGIC - Streaming vs batch patterns
# MAGIC ---
# MAGIC **🎉 Notebook Complete!** 
# MAGIC Your managed table and group grants are in place. Next: Notebook 02.
