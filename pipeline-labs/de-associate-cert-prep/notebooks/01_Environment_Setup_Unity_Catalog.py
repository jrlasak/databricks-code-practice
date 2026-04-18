# Databricks notebook source
# MAGIC %md
# MAGIC # Notebook 01: Environment Setup & Unity Catalog
# MAGIC **Exam Coverage**: Sections 1 (Databricks Lakehouse Platform) & 5 (Data Governance)
# MAGIC **Duration**: 30-45 minutes
# MAGIC ---
# MAGIC ## Learning Objectives
# MAGIC By the end of this notebook, you will be able to:
# MAGIC - Understand Unity Catalog's three-level namespace (catalog.schema.table)
# MAGIC - Create and manage catalogs, schemas, and volumes
# MAGIC - Create managed tables in Unity Catalog
# MAGIC - Implement access control using GRANT and REVOKE
# MAGIC - Understand serverless compute and its benefits
# MAGIC ---

# COMMAND ----------

# MAGIC %md
# MAGIC ## Section 1: Introduction and Setup
# MAGIC Unity Catalog provides centralized governance for all data and AI assets in Databricks.
# MAGIC ### Three-Level Namespace
# MAGIC ```
# MAGIC catalog.schema.table
# MAGIC ```
# MAGIC This structure enables:
# MAGIC - Logical separation of environments (dev, staging, prod)
# MAGIC - Organizational boundaries
# MAGIC - Fine-grained access control
# MAGIC Let's set up our environment.

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
# MAGIC Unity Catalog organizes data in a hierarchy. Let's explore what's been set up.

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
# It will probably show no tables - rerun it once we create something!

spark.sql(f"SHOW TABLES IN {CATALOG_NAME}.{BRONZE_SCHEMA}").show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Understanding Volumes
# MAGIC Volumes provide governed access to files (non-tabular data).
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
# MAGIC **Requirements:**
# MAGIC - Table name: `{CATALOG_NAME}.{BRONZE_SCHEMA}.customers_managed`
# MAGIC - Format: Delta
# MAGIC - Mode: Overwrite (since this is the first load)
# MAGIC - Use `.saveAsTable()` to create a managed table
# MAGIC **Key syntax:**
# MAGIC ```python
# MAGIC df.write \
# MAGIC .format("delta") \
# MAGIC .mode("overwrite") \
# MAGIC .saveAsTable("catalog.schema.table")
# MAGIC ```
# MAGIC **Hint**: The table name variable `managed_table_name` is created for you below.

# COMMAND ----------

# TODO: Create a managed table

# Table name (provided)
managed_table_name = f"{CATALOG_NAME}.{BRONZE_SCHEMA}.customers_managed"

# Write your code here:
customers_df.write \




print(f"Created managed table: {managed_table_name}")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC **Check the solution below** ⬇️

# COMMAND ----------

# ✅ SOLUTION: Create Managed Table

# managed_table_name = f"{CATALOG_NAME}.{BRONZE_SCHEMA}.customers_managed"

# customers_df.write \
#     .format("delta") \
#     .mode("overwrite") \
#     .saveAsTable(managed_table_name)

# print(f"✅ Created managed table: {managed_table_name}")

# COMMAND ----------

# Check table details to see where data is stored
display(spark.sql(f"DESCRIBE DETAIL {managed_table_name}"))

# COMMAND ----------

# MAGIC %md
# MAGIC **Observation**: Notice the `location` field. For managed tables, Unity Catalog automatically determines this location within the metastore's managed storage.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Section 3b: Create Security Groups
# MAGIC Before we can grant permissions, we need to create the groups that will receive those permissions.
# MAGIC ### What are Groups?
# MAGIC **Groups** are collections of users that share the same access permissions. Using groups instead of individual users:
# MAGIC - ✅ Simplifies permission management
# MAGIC - ✅ Ensures consistent access across teams
# MAGIC - ✅ Makes auditing easier
# MAGIC - ✅ Follows security best practices
# MAGIC ### Persona-Based Groups
# MAGIC We'll create groups for common data team roles:
# MAGIC | Group | Role | Typical Access |
# MAGIC |-------|------|----------------|
# MAGIC | `data_engineers` | Build and maintain pipelines | Full access to Bronze, Silver, Gold |
# MAGIC | `data_analysts` | Create reports and dashboards | Read-only on Silver and Gold |
# MAGIC | `data_scientists` | Build ML models | Read-only on Silver (feature engineering) |
# MAGIC | `business_users` | View business reports | Read-only on specific Gold views |
# MAGIC | `managers` | Oversee operations | Read-only on Gold (high-level metrics) |
# MAGIC ### Group Management Notes
# MAGIC **In production:**
# MAGIC - Groups are typically created by workspace admins
# MAGIC - Often synced from corporate identity providers (Azure AD, Okta, etc.)
# MAGIC - Group membership is managed centrally
# MAGIC **For this lab:**
# MAGIC - Groups must be created manually via the Admin Console (not SQL)
# MAGIC - You'll need workspace admin or account admin privileges
# MAGIC - Groups persist across workspace sessions
# MAGIC ### 🔧 How to Create Groups Manually
# MAGIC **Step-by-step instructions:**
# MAGIC 1. **Navigate to Admin Console:**
# MAGIC    - Click your profile icon (top right)
# MAGIC    - Select **Settings** → **Admin Console**
# MAGIC    - OR append `/settings/workspace/identity-and-access/groups` to your workspace URL
# MAGIC 2. **Create Groups:**
# MAGIC    - Click **Groups** tab (left sidebar)
# MAGIC    - Click **Create Group**
# MAGIC    - Create each of the following groups:
# MAGIC      - `data_engineers`
# MAGIC      - `data_analysts`
# MAGIC      - `data_scientists`
# MAGIC      - `business_users`
# MAGIC      - `managers`
# MAGIC    - After creating them, click on each group name, go to "Entitlements" and check all 3 boxes.
# MAGIC 3. **Add Yourself to Groups (Optional):**
# MAGIC    - Click on a group name
# MAGIC    - Click **Add Members**
# MAGIC    - Add your user email
# MAGIC    - This allows you to test `is_member()` functions later
# MAGIC **Direct URL Pattern:**
# MAGIC ```
# MAGIC https://dbc-XXX.cloud.databricks.com/settings/workspace/identity-and-access/groups
# MAGIC ```
# MAGIC (Replace `dbc-XXX` with your unique workspace URL)
# MAGIC
# MAGIC **⚠️ Important:** Complete this step before proceeding to the GRANT statements below, as they depend on these groups existing.

# COMMAND ----------

# Verify groups exist (run this after creating groups in Admin Console)
print("📋 Checking for required groups...")
print("="*70)

required_groups = [
    "data_engineers",
    "data_analysts",
    "data_scientists",
    "business_users",
    "managers"
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
        print(f"⚠️  Please create these groups in the Admin Console before proceeding.")
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
# MAGIC To see which groups you belong to, you can check your current user and group memberships.
# MAGIC
# MAGIC **Note:** Adding users to groups is done via the Admin Console UI, not SQL.

# COMMAND ----------

# Check current user and their group memberships
current_user_email = spark.sql("SELECT current_user()").collect()[0][0]
print(f"Current user: {current_user_email}")

# Check if you're a member of the key groups
print("\nYour group memberships:")
for group in required_groups:
    try:
        is_member = spark.sql(f"SELECT is_member('{group}')").collect()[0][0]
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
# MAGIC | `CREATE` | Create new objects |
# MAGIC | `USAGE` | Access child objects (prerequisite) |
# MAGIC | `ALL PRIVILEGES` | All permissions |
# MAGIC ### The USAGE Privilege (Metastore Limitation)
# MAGIC **Important Note**: In Unity Catalog metastore version 1.0, the `USAGE` privilege is not supported on catalogs and schemas.
# MAGIC - In newer metastore versions, `USAGE` would be a prerequisite to access child objects
# MAGIC - For metastore v1.0, you can grant privileges directly (e.g., `SELECT`, `ALL PRIVILEGES`) without `USAGE`
# MAGIC Let's implement persona-based access control.

# COMMAND ----------

# Example: Grant full access to data engineers on bronze schema

spark.sql(f"""
    GRANT ALL PRIVILEGES ON SCHEMA {CATALOG_NAME}.{BRONZE_SCHEMA} TO `data_engineers`
""")

print("✅ Granted full access to data_engineers on bronze schema")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ### 🎯 EXERCISE 2: Grant Permissions for Data Analysts
# MAGIC **Scenario**: Data analysts need read-only access to the Gold schema (business reports).
# MAGIC **Your task**: Write GRANT statements for the `data_analysts` group.
# MAGIC **Requirements:**
# MAGIC 1. Grant `SELECT` on the gold schema (read-only permission)
# MAGIC **SQL Syntax:**
# MAGIC ```sql
# MAGIC GRANT privilege ON object_type object_name TO `principal`
# MAGIC ```
# MAGIC **Hint**: Use `{CATALOG_NAME}` and `{GOLD_SCHEMA}` variables
# MAGIC
# MAGIC **Note**: We're skipping USAGE privilege due to metastore v1.0 limitations.

# COMMAND ----------

# TODO: Grant read-only access to data analysts on Gold schema

# Grant SELECT on gold schema
spark.sql(f"""

""")

print("Granted read-only access to data_analysts on gold schema")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC **Solution below** ⬇️

# COMMAND ----------

# ✅ SOLUTION: Data Analysts - Gold Read-Only

# spark.sql(f"""
#     GRANT SELECT ON SCHEMA {CATALOG_NAME}.{GOLD_SCHEMA} TO `data_analysts`
# """)

# print("✅ Granted read-only access to data_analysts on gold schema")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ### 🎯 EXERCISE 3: Grant Permissions for Data Scientists
# MAGIC **Scenario**: Data scientists need read-only access to BOTH Silver and Gold schemas.
# MAGIC **Your task**: Write GRANT statements for the `data_scientists` group.
# MAGIC **Requirements:**
# MAGIC 1. Grant `SELECT` on silver schema
# MAGIC 2. Grant `SELECT` on gold schema
# MAGIC **Hint**: You'll need 2 GRANT statements total
# MAGIC **Note**: We're skipping USAGE privilege due to metastore v1.0 limitations.

# COMMAND ----------

# TODO: Grant read-only access to data scientists on Silver and Gold

# 1. SELECT on silver schema


# 2. SELECT on gold schema


print("Granted read-only access to data_scientists on silver and gold")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC **Solution below** ⬇️

# COMMAND ----------

# ✅ SOLUTION: Data Scientists - Silver and Gold Read-Only

# spark.sql(f"""
#     GRANT SELECT ON SCHEMA {CATALOG_NAME}.{SILVER_SCHEMA} TO `data_scientists`
# """)

# spark.sql(f"""
#     GRANT SELECT ON SCHEMA {CATALOG_NAME}.{GOLD_SCHEMA} TO `data_scientists`
# """)

# print("✅ Granted read-only access to data_scientists on silver and gold")

# COMMAND ----------

# View current grants on bronze schema
display(spark.sql(f"SHOW GRANTS ON SCHEMA {CATALOG_NAME}.{BRONZE_SCHEMA}"))

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
# MAGIC - ⚡ Instant startup (no provisioning)
# MAGIC - 📈 Auto-scaling based on workload
# MAGIC - 💰 Pay only for resources used
# MAGIC - 🔧 Pre-warmed and optimized
# MAGIC ### Serverless vs Custom Clusters
# MAGIC | Feature | Serverless | Custom Clusters |
# MAGIC |---------|------------|----------------|
# MAGIC | **Startup Time** | Instant | 3-5 minutes |
# MAGIC | **Configuration** | Automatic | Manual |
# MAGIC | **Scaling** | Auto | Manual/auto rules |
# MAGIC | **Use Case** | Ad-hoc, development | Production, specific configs |
# MAGIC | **Cost** | Per query | Per uptime |
# MAGIC ### Best Practices
# MAGIC - ✅ Use for development and exploration
# MAGIC - ✅ Use for ad-hoc queries
# MAGIC - ❌ Not ideal for long-running batch jobs (use custom clusters)
# MAGIC
# MAGIC ### We're going to use serverless for this lab, as this is the only option in Databricks Free Edition

# COMMAND ----------

# MAGIC %md
# MAGIC ## Section 6: Summary and Checkpoint
# MAGIC ### 🎯 Key Concepts
# MAGIC **1. Unity Catalog Three-Level Namespace**
# MAGIC - `catalog.schema.table` structure
# MAGIC - Enables environment separation and access control
# MAGIC **2. Catalogs, Schemas, and Volumes**
# MAGIC - Hierarchical organization
# MAGIC - Volumes for governed file access
# MAGIC **3. Managed Tables**
# MAGIC - UC controls storage, DROP deletes data
# MAGIC - Best for data exclusively used in Databricks
# MAGIC **4. Access Control**
# MAGIC - GRANT/REVOKE for permissions
# MAGIC - Persona-based patterns (engineers, analysts, scientists)
# MAGIC - Privileges: SELECT, MODIFY, CREATE, USAGE, ALL PRIVILEGES
# MAGIC **5. Serverless Compute**
# MAGIC - Instant startup, auto-scaling
# MAGIC - Best for development and ad-hoc queries
# MAGIC - Community Edition limitations
# MAGIC ### ✅ Exam Checklist
# MAGIC Can you:
# MAGIC - [ ] Write three-level namespace references?
# MAGIC - [ ] Create managed tables with saveAsTable()?
# MAGIC - [ ] Write GRANT statements for different personas?
# MAGIC - [ ] Understand the USAGE prerequisite?
# MAGIC - [ ] Describe serverless compute benefits?
# MAGIC ### 📚 Next Steps
# MAGIC **Notebook 02** covers:
# MAGIC - Auto Loader for incremental ingestion
# MAGIC - Schema evolution and inference
# MAGIC - Streaming vs batch patterns
# MAGIC ---
# MAGIC **🎉 Notebook Complete!** 
# MAGIC You've mastered Unity Catalog basics. Save your work and proceed to Notebook 02.
