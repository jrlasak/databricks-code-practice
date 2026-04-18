# Databricks notebook source
# MAGIC %md
# MAGIC # Notebook 06: Production Workflows & Jobs
# MAGIC
# MAGIC **Exam Coverage**: Section 4 (Production Pipelines)
# MAGIC
# MAGIC **Duration**: 45-60 minutes
# MAGIC
# MAGIC ---
# MAGIC ## Learning Objectives
# MAGIC By the end of this notebook, you will be able to:
# MAGIC - Create and configure Databricks Jobs through the UI
# MAGIC - Implement multi-task workflows with dependencies
# MAGIC - Configure job parameters using widgets
# MAGIC - Implement error handling with try/except and dbutils.notebook.exit
# MAGIC - Monitor and troubleshoot job execution
# MAGIC - Understand Free Edition job limitations
# MAGIC ---

# COMMAND ----------

# MAGIC %md
# MAGIC ## Section 1: Introduction to Databricks Jobs
# MAGIC **Databricks Jobs** provide orchestration for notebooks, Python scripts, and JARs.
# MAGIC ### 🎯 Key Concepts
# MAGIC **Job Components**
# MAGIC | Component | Purpose | Example |
# MAGIC |-----------|---------|--------|
# MAGIC | **Task** | Unit of work | Notebook, Python script, JAR |
# MAGIC | **Dependency** | Execution order | Task B runs after Task A |
# MAGIC | **Cluster** | Compute resources | Job cluster, all-purpose cluster |
# MAGIC | **Schedule** | When to run | Manual, cron, triggered |
# MAGIC | **Parameters** | Runtime config | Environment, date, mode |
# MAGIC ### Job Types
# MAGIC 1. **Single-Task Job**: One notebook/script
# MAGIC 2. **Multi-Task Job**: Multiple tasks with dependencies (DAG)
# MAGIC 3. **Triggered Job**: Runs on external events
# MAGIC 4. **Scheduled Job**: Cron-based scheduling
# MAGIC ### ⚠️ Free Edition Limitations
# MAGIC **Important restrictions:**
# MAGIC - ❌ Maximum **5 tasks** per workflow
# MAGIC - ❌ No job clusters (must use all-purpose clusters)
# MAGIC - ❌ No advanced scheduling
# MAGIC - ❌ Limited concurrency (single-node)
# MAGIC
# MAGIC **For production**: Full Databricks workspace recommended.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Section 2: Job Parameters with Widgets
# MAGIC **Widgets** enable parameterized notebooks that accept runtime configuration.
# MAGIC ### Widget Types
# MAGIC | Type | Purpose | Example |
# MAGIC |------|---------|--------|
# MAGIC | `text` | String input | Environment name, date |
# MAGIC | `dropdown` | Select from options | mode: full/incremental |
# MAGIC | `combobox` | Dropdown + free text | Region selection |
# MAGIC | `multiselect` | Multiple selections | Tables to process |
# MAGIC ### Widget Syntax
# MAGIC ```python
# MAGIC # Create widget
# MAGIC dbutils.widgets.text("name", "default_value", "Label")
# MAGIC # Get widget value
# MAGIC value = dbutils.widgets.get("name")
# MAGIC # Remove widget
# MAGIC dbutils.widgets.remove("name")
# MAGIC # Remove all
# MAGIC dbutils.widgets.removeAll()
# MAGIC ```
# MAGIC **Key points:**
# MAGIC - Widgets appear at top of notebook
# MAGIC - Jobs pass parameters via widget names
# MAGIC - Default values used if not provided

# COMMAND ----------

# Import shared variables
%run ./variables

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ### 🎯 EXERCISE 1: Create Job Parameter Widgets
# MAGIC **Your task**: Create widgets for job parameterization.
# MAGIC
# MAGIC **Requirements:**
# MAGIC Create 4 widgets:
# MAGIC 1. **environment** (text)
# MAGIC - Default: "dev"
# MAGIC - Label: "Environment (dev/staging/prod)"
# MAGIC 2. **pipeline_mode** (dropdown)
# MAGIC - Options: ["full", "incremental"]
# MAGIC - Default: "incremental"
# MAGIC - Label: "Pipeline Mode"
# MAGIC 3. **date** (text)
# MAGIC - Default: "" (empty)
# MAGIC - Label: "Processing Date (YYYY-MM-DD, blank=today)"
# MAGIC 4. **enable_quality_checks** (dropdown)
# MAGIC - Options: ["true", "false"]
# MAGIC - Default: "true"
# MAGIC - Label: "Enable Quality Checks"
# MAGIC
# MAGIC **Functions:**
# MAGIC ```python
# MAGIC dbutils.widgets.text("name", "default", "label")
# MAGIC dbutils.widgets.dropdown("name", "default", ["opt1", "opt2"], "label")
# MAGIC ```

# COMMAND ----------

# TODO: Create job parameter widgets

# 1. Environment widget (text)
dbutils.widgets.text(  # TODO

# 2. Pipeline mode widget (dropdown)
dbutils.widgets.dropdown(  # TODO

# 3. Date widget (text)
dbutils.widgets.text(  # TODO

# 4. Quality checks widget (dropdown)
dbutils.widgets.dropdown(  # TODO

print("✓ Widgets created")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC **Solution below** ⬇️

# COMMAND ----------

# ✅ SOLUTION: Create Widgets

# dbutils.widgets.text("environment", "dev", "Environment (dev/staging/prod)")
# dbutils.widgets.dropdown("pipeline_mode", "incremental", ["full", "incremental"], "Pipeline Mode")
# dbutils.widgets.text("date", "", "Processing Date (YYYY-MM-DD, blank=today)")
# dbutils.widgets.dropdown("enable_quality_checks", "true", ["true", "false"], "Enable Quality Checks")

# print("✅ Widgets created")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ### Reading Widget Values
# MAGIC Now let's read and use the widget values.

# COMMAND ----------

# Read widget values
environment = dbutils.widgets.get("environment")
pipeline_mode = dbutils.widgets.get("pipeline_mode")
processing_date = dbutils.widgets.get("date")
enable_quality_checks = dbutils.widgets.get("enable_quality_checks") == "true"

# Use today's date if not specified
if not processing_date:
    from datetime import datetime
    processing_date = datetime.now().strftime("%Y-%m-%d")

print(f"Environment: {environment}")
print(f"Pipeline Mode: {pipeline_mode}")
print(f"Processing Date: {processing_date}")
print(f"Quality Checks: {enable_quality_checks}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Section 3: Task Execution Pattern
# MAGIC Each task in a multi-task job is typically a separate notebook.
# MAGIC ### Common Task Pattern
# MAGIC ```python
# MAGIC try:
# MAGIC # Task logic
# MAGIC result = execute_task()
# MAGIC # Success
# MAGIC dbutils.notebook.exit("SUCCESS")
# MAGIC except Exception as e:
# MAGIC # Failure
# MAGIC error_msg = str(e)
# MAGIC dbutils.notebook.exit(f"FAILED: {error_msg}")
# MAGIC ```
# MAGIC ### Task 1: Data Validation
# MAGIC Validates source data availability before processing.

# COMMAND ----------

# Task 1: Data Validation - Provided as example

print("=== Task 1: Data Validation ===")

try:
    # Check source data exists
    customers_exists = len(dbutils.fs.ls(CUSTOMERS_LANDING_PATH)) > 0
    products_exists = len(dbutils.fs.ls(PRODUCTS_LANDING_PATH)) > 0
    sales_exists = len(dbutils.fs.ls(SALES_LANDING_PATH)) > 0
    
    print(f"✓ Customers data: {'Found' if customers_exists else 'Missing'}")
    print(f"✓ Products data: {'Found' if products_exists else 'Missing'}")
    print(f"✓ Sales data: {'Found' if sales_exists else 'Missing'}")
    
    if not (customers_exists and products_exists and sales_exists):
        raise Exception("Missing required source data")
    
    # Check Bronze tables exist
    tables_to_check = [
        CUSTOMERS_BRONZE_TABLE,
        PRODUCTS_BRONZE_TABLE,
        SALES_BRONZE_TABLE
    ]
    
    for table in tables_to_check:
        table_exists = spark.catalog.tableExists(table)
        print(f"✓ Table {table}: {'Exists' if table_exists else 'Missing'}")
    
    print("\n✅ Data validation passed")
    validation_result = "SUCCESS"
    
except Exception as e:
    print(f"\n✗ Data validation failed: {str(e)}")
    validation_result = f"FAILED: {str(e)}"

print(f"\nValidation Result: {validation_result}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Section 4: Error Handling Patterns
# MAGIC Production jobs need robust error handling.
# MAGIC ### Key Patterns
# MAGIC 1. **Try-Except Blocks**: Catch and handle errors
# MAGIC 2. **dbutils.notebook.exit()**: Pass results to orchestrator
# MAGIC 3. **Retry Logic**: Automatic retry for transient failures
# MAGIC 4. **Alerting**: Notify on failures
# MAGIC ### dbutils.notebook.exit() Usage
# MAGIC ```python
# MAGIC # Success
# MAGIC dbutils.notebook.exit("SUCCESS")
# MAGIC # Failure with details
# MAGIC dbutils.notebook.exit(json.dumps({
# MAGIC "status": "FAILED",
# MAGIC "error": "Connection timeout",
# MAGIC "records_processed": 1000
# MAGIC }))
# MAGIC ```
# MAGIC **Important**: Return value accessible by downstream tasks.

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ### 🎯 EXERCISE 2: Implement Retry Logic
# MAGIC **Your task**: Create a function that retries failed tasks.
# MAGIC **Requirements:**
# MAGIC Function signature:
# MAGIC ```python
# MAGIC def execute_task_with_retry(task_name, task_function, max_retries=3)
# MAGIC ```
# MAGIC **Logic:**
# MAGIC 1. Loop up to `max_retries` attempts
# MAGIC 2. Try to execute `task_function()`
# MAGIC 3. On success:
# MAGIC - Return dict with: `{"status": "SUCCESS", "task": task_name, "attempt": N, "timestamp": ..., "result": ...}`
# MAGIC 4. On exception:
# MAGIC - If retries remaining: print error, retry
# MAGIC - If max retries reached: return `{"status": "FAILED", "task": task_name, "error": ...}`
# MAGIC
# MAGIC **Hint**: Use a while loop and track attempt count.

# COMMAND ----------

# TODO: Implement retry logic function

import json
from datetime import datetime

def execute_task_with_retry(task_name, task_function, max_retries=3):
    """
    Execute a task with automatic retry logic.
    
    Args:
        task_name: Name of the task
        task_function: Function to execute
        max_retries: Maximum retry attempts
    
    Returns:
        dict: Task execution result
    """
    attempt = 0
    
    while attempt < max_retries:
        try:
            # TODO: Print attempt info
            
            # TODO: Execute task function
            
            # TODO: Return success dict
            
            
            
            
            
        except Exception as e:
            # TODO: Increment attempt
            
            # TODO: Print error
            
            # TODO: Check if max retries reached
            
                # TODO: Return failure dict
                
                
                
                
                
            # TODO: Print retry message
            

# Test the function
def sample_task():
    print("  Executing task logic...")
    return {"records_processed": 1000}

result = execute_task_with_retry("sample_task", sample_task, max_retries=3)
print(f"\nTask Result:")
print(json.dumps(result, indent=2))

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC **Solution below** ⬇️

# COMMAND ----------

# ✅ SOLUTION: Retry Logic

# import json
# from datetime import datetime

# def execute_task_with_retry(task_name, task_function, max_retries=3):
#     """
#     Execute a task with automatic retry logic.
#     """
#     attempt = 0
    
#     while attempt < max_retries:
#         try:
#             print(f"[{task_name}] Attempt {attempt + 1}/{max_retries}")
            
#             result = task_function()
            
#             return {
#                 "status": "SUCCESS",
#                 "task": task_name,
#                 "attempt": attempt + 1,
#                 "timestamp": datetime.now().isoformat(),
#                 "result": result
#             }
            
#         except Exception as e:
#             attempt += 1
#             error_msg = str(e)
#             print(f"[{task_name}] Error on attempt {attempt}: {error_msg}")
            
#             if attempt >= max_retries:
#                 return {
#                     "status": "FAILED",
#                     "task": task_name,
#                     "attempt": attempt,
#                     "timestamp": datetime.now().isoformat(),
#                     "error": error_msg
#                 }
            
#             print(f"[{task_name}] Retrying...")

# # Test
# def sample_task():
#     print("  Executing task logic...")
#     return {"records_processed": 1000}

# result = execute_task_with_retry("sample_task", sample_task, max_retries=3)
# print(f"\n✅ Task Result:")
# print(json.dumps(result, indent=2))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Section 5: Creating Multi-Task Jobs in the UI
# MAGIC ### 🚀 Step-by-Step Job Creation
# MAGIC **1. Navigate to Workflows**
# MAGIC - Click **Jobs & Pipelines ** in left sidebar
# MAGIC - Click **Job** in the top right corner
# MAGIC
# MAGIC **2. Configure Job Settings**
# MAGIC | Setting | Value |
# MAGIC |---------|-------|
# MAGIC | **Job Name** | `cert_prep_pipeline` |
# MAGIC | **Description** | Medallion architecture ETL pipeline |
# MAGIC
# MAGIC **3. Add Tasks (Up to 5 for Free Edition)**
# MAGIC
# MAGIC **Task 1: Bronze Ingestion**
# MAGIC - Task Name: `bronze_ingestion`
# MAGIC - Type: Notebook
# MAGIC - **Source**: `02_Auto_Loader_Incremental_Ingestion`
# MAGIC - Cluster: Serverless
# MAGIC - Parameters: `pipeline_mode: incremental`
# MAGIC
# MAGIC **Task 2: Silver Transformation**
# MAGIC - Task Name: `silver_transformation`
# MAGIC - Type: Notebook
# MAGIC - **Source**: `03_Bronze_to_Silver`
# MAGIC - **Depends On**: `bronze_ingestion`
# MAGIC - Cluster: Serverless
# MAGIC - Parameters: `enable_quality_checks: true`
# MAGIC
# MAGIC **Task 3: Gold Aggregation**
# MAGIC - Task Name: `gold_aggregation`
# MAGIC - Type: Notebook
# MAGIC - **Source**: `04_Silver_to_Gold_Advanced`
# MAGIC - **Depends On**: `silver_transformation`
# MAGIC - Cluster: Serverless
# MAGIC
# MAGIC **4. Configure Schedule (Optional)**
# MAGIC - **Trigger Type**: Cron
# MAGIC - **Cron Expression**: `0 2 * * *` (daily at 2 AM)
# MAGIC - **Timezone**: Your timezone
# MAGIC
# MAGIC Don't forget to stop the job later on if you decide to schedule it
# MAGIC
# MAGIC **5. Configure Alerts (Optional)**
# MAGIC - On Failure: Email notification
# MAGIC - On Success: Optional
# MAGIC
# MAGIC **6. Save and Run**
# MAGIC - Click **Create**
# MAGIC - Click **Run now** to test
# MAGIC ### Task Dependencies Visualization
# MAGIC ```
# MAGIC validate_data
# MAGIC ↓
# MAGIC bronze_ingestion
# MAGIC ↓
# MAGIC silver_transformation
# MAGIC ↓
# MAGIC gold_aggregation
# MAGIC ↓
# MAGIC summary_report
# MAGIC ```
# MAGIC ### ⚠️ Free Edition Constraints
# MAGIC - ✅ Maximum **5 tasks** (we use exactly 5)
# MAGIC - ❌ No job clusters (use all-purpose)
# MAGIC - ❌ No email alerts (basic only)
# MAGIC - ❌ Simple cron scheduling only

# COMMAND ----------

# MAGIC %md
# MAGIC ## Section 6: Monitoring Jobs
# MAGIC The Databricks Jobs UI provides comprehensive monitoring.
# MAGIC ### 📊 Job Run Details
# MAGIC For each run:
# MAGIC | View | Information |
# MAGIC |------|-------------|
# MAGIC | **Run Status** | Success, Failed, Running, Cancelled |
# MAGIC | **Task Timeline** | Visual execution timeline |
# MAGIC | **Task DAG** | Dependency graph |
# MAGIC | **Logs** | stdout, stderr per task |
# MAGIC | **Spark UI** | Detailed execution metrics |
# MAGIC | **Parameters** | Runtime parameters used |
# MAGIC ### Accessing Job Context Programmatically

# COMMAND ----------

# Example: Get current job context (only in job execution)

try:
    job_id = dbutils.notebook.entry_point.getDbutils().notebook().getContext().jobId().get()
    run_id = dbutils.notebook.entry_point.getDbutils().notebook().getContext().currentRunId().get()
    
    print(f"Job ID: {job_id}")
    print(f"Run ID: {run_id}")
except:
    print("ℹ️  Not running in job context (expected in interactive mode)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Section 7: Troubleshooting Common Issues
# MAGIC ### Issue 1: Task Failure
# MAGIC **Symptoms**: Task shows as failed
# MAGIC
# MAGIC **Diagnosis:**
# MAGIC 1. Check task logs for error messages
# MAGIC 2. Review Spark UI for details
# MAGIC 3. Verify cluster resources
# MAGIC
# MAGIC
# MAGIC **Solutions:**
# MAGIC - Add try-except error handling
# MAGIC - Implement retry logic
# MAGIC - Increase cluster size
# MAGIC ### Issue 2: Task Timeout
# MAGIC **Symptoms**: Task runs indefinitely
# MAGIC
# MAGIC **Diagnosis:**
# MAGIC 1. Check for never-terminating streams
# MAGIC 2. Look for blocking operations
# MAGIC 3. Review for cartesian products
# MAGIC
# MAGIC **Solutions:**
# MAGIC - Add timeout configurations
# MAGIC - Use `trigger(availableNow=True)`
# MAGIC - Optimize queries with filters
# MAGIC ### Issue 3: Parameter Passing
# MAGIC **Symptoms**: Widget values not received
# MAGIC
# MAGIC **Diagnosis:**
# MAGIC 1. Verify widget names match parameters
# MAGIC 2. Check for typos
# MAGIC
# MAGIC **Solutions:**
# MAGIC - Use consistent naming
# MAGIC - Add default values
# MAGIC - Log parameters at task start
# MAGIC ### Issue 4: Dependency Failures
# MAGIC **Symptoms**: Downstream tasks fail
# MAGIC
# MAGIC **Diagnosis:**
# MAGIC 1. Check task dependencies
# MAGIC 2. Review upstream exit codes
# MAGIC
# MAGIC **Solutions:**
# MAGIC - Configure conditional dependencies
# MAGIC - Add data validation
# MAGIC - Use "If" conditions on dependencies

# COMMAND ----------

# MAGIC %md
# MAGIC ## Section 8: Job Best Practices
# MAGIC ### 1️⃣ Idempotency
# MAGIC Jobs should produce same result if run multiple times.
# MAGIC - ✅ Use `INSERT OVERWRITE` for full refreshes
# MAGIC - ✅ Use `MERGE` for incremental updates
# MAGIC - ✅ Check for existing data
# MAGIC ### 2️⃣ Parameterization
# MAGIC Make jobs configurable.
# MAGIC - ✅ Use widgets for runtime config
# MAGIC - ✅ Avoid hardcoded values
# MAGIC - ✅ Support multiple environments
# MAGIC ### 3️⃣ Error Handling
# MAGIC Implement comprehensive error handling.
# MAGIC - ✅ Try-except blocks
# MAGIC - ✅ Retry logic for transient failures
# MAGIC - ✅ Log errors with context
# MAGIC - ✅ Meaningful status codes
# MAGIC ### 4️⃣ Monitoring
# MAGIC Set up monitoring for production.
# MAGIC - ✅ Configure failure alerts
# MAGIC - ✅ Track SLA metrics
# MAGIC - ✅ Monitor data quality
# MAGIC - ✅ Job health dashboards
# MAGIC ### 5️⃣ Resource Management
# MAGIC Optimize cluster usage.
# MAGIC - ✅ Use job clusters (when available)
# MAGIC - ✅ Right-size for workload
# MAGIC - ✅ Enable autoscaling
# MAGIC - ✅ Use spot instances (non-critical)
# MAGIC ### 6️⃣ Testing
# MAGIC Test before production.
# MAGIC - ✅ Test with sample data
# MAGIC - ✅ Verify task dependencies
# MAGIC - ✅ Test failure scenarios
# MAGIC - ✅ Validate parameters

# COMMAND ----------

# MAGIC %md
# MAGIC ## Section 9: Summary and Checkpoint
# MAGIC ### 🎯 Key Concepts Covered
# MAGIC
# MAGIC **1. Databricks Jobs**
# MAGIC - Multi-task workflows
# MAGIC - Task dependencies (DAG)
# MAGIC - Job vs all-purpose clusters
# MAGIC
# MAGIC **2. Job Parameters**
# MAGIC - Widget creation and usage
# MAGIC - Parameter passing between tasks
# MAGIC - Environment-specific config
# MAGIC
# MAGIC **3. Error Handling**
# MAGIC - Try-except patterns
# MAGIC - Retry logic
# MAGIC - `dbutils.notebook.exit()` for task status
# MAGIC
# MAGIC **4. Job Configuration**
# MAGIC - UI-based creation
# MAGIC - Task dependencies
# MAGIC - Scheduling and triggers
# MAGIC
# MAGIC **5. Monitoring**
# MAGIC - Run details and logs
# MAGIC - Common failure patterns
# MAGIC - Debugging techniques
# MAGIC
# MAGIC **6. Free Edition Limits**
# MAGIC - 5 task maximum
# MAGIC - No job clusters
# MAGIC - Limited scheduling
# MAGIC
# MAGIC ### ✅ Exam Checklist
# MAGIC Can you:
# MAGIC - [ ] Create widgets and read parameter values?
# MAGIC - [ ] Configure multi-task jobs with dependencies?
# MAGIC - [ ] Implement error handling with try-except?
# MAGIC - [ ] Use `dbutils.notebook.exit()` for task status?
# MAGIC - [ ] Explain job cluster vs all-purpose cluster?
# MAGIC - [ ] Configure job scheduling with cron?
# MAGIC - [ ] Monitor and troubleshoot job runs?
# MAGIC ### 📚 Next Steps
# MAGIC **To Create Your Job:**
# MAGIC 1. ✅ Organize notebooks by task
# MAGIC 2. ✅ Navigate to Workflows → Jobs
# MAGIC 3. ✅ Create job with 5 tasks
# MAGIC 4. ✅ Configure dependencies
# MAGIC 5. ✅ Add parameters and schedule
# MAGIC 6. ✅ Run and monitor
# MAGIC
# MAGIC **🎉 Notebook Complete!**
# MAGIC You've learned production job orchestration. Create your multi-task workflow in the UI to see it in action!

# COMMAND ----------

# Clean up widgets
dbutils.widgets.removeAll()
print("✅ Widgets removed")
