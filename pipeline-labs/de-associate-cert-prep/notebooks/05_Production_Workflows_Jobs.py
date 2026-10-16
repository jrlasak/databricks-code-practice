# Databricks notebook source
# MAGIC %md
# MAGIC # Notebook 05: Production Workflows & Jobs
# MAGIC
# MAGIC **Exam Coverage**: Section 4 (Productionizing Data Pipelines)
# MAGIC
# MAGIC **Duration**: about 45 minutes
# MAGIC
# MAGIC ---
# MAGIC ## Learning Objectives
# MAGIC By the end of this notebook, you will be able to:
# MAGIC - Create and configure Databricks Jobs through the UI
# MAGIC - Implement multi-task workflows with dependencies
# MAGIC - Configure job parameters using widgets
# MAGIC - Fail a task on error and pass values between tasks with task values
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
# MAGIC | **Compute** | Where tasks run | Serverless jobs compute (classic: job cluster) |
# MAGIC | **Schedule** | When to run | Manual, cron, triggered |
# MAGIC | **Parameters** | Runtime config | Environment, date, mode |
# MAGIC ### Job Types
# MAGIC 1. **Single-Task Job**: One notebook/script
# MAGIC 2. **Multi-Task Job**: Multiple tasks with dependencies (DAG)
# MAGIC 3. **Triggered Job**: Runs on external events
# MAGIC 4. **Scheduled Job**: Cron-based scheduling
# MAGIC ### ⚠️ Free Edition Limitations
# MAGIC **Important restrictions:**
# MAGIC - Serverless compute only: no job clusters and no all-purpose clusters (jobs run on serverless jobs compute)
# MAGIC - At most 5 job tasks running at the same time across the account
# MAGIC - No custom compute configuration; a daily compute quota
# MAGIC
# MAGIC **For production**: a paid Databricks workspace lifts these limits.

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

# MAGIC %run ./variables

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ### 🎯 EXERCISE 1: Create Job Parameter Widgets
# MAGIC **Your task**: Create widgets for job parameterization.
# MAGIC
# MAGIC **Why**: one notebook serves dev and prod; the job passes `environment=prod` instead of someone editing a hardcoded name. Change the dropdown to `full` and re-run the cells below to see the branch switch.
# MAGIC
# MAGIC **Requirements** (create 4 widgets):
# MAGIC 1. **environment**: text, default "dev", label "Environment (dev/staging/prod)"
# MAGIC 2. **pipeline_mode**: dropdown, options ["full", "incremental"], default "incremental", label "Pipeline Mode"
# MAGIC 3. **date**: text, default "" (empty), label "Processing Date (YYYY-MM-DD, blank=today)"
# MAGIC 4. **enable_quality_checks**: dropdown, options ["true", "false"], default "true", label "Enable Quality Checks"
# MAGIC
# MAGIC **Functions:**
# MAGIC ```python
# MAGIC dbutils.widgets.text("name", "default", "label")
# MAGIC dbutils.widgets.dropdown("name", "default", ["opt1", "opt2"], "label")
# MAGIC ```

# COMMAND ----------

# ✏️ STUDENT EXERCISE
# TODO: Create job parameter widgets

# 1. Environment widget (text)
dbutils.widgets.text(  # TODO

# 2. Pipeline mode widget (dropdown)
dbutils.widgets.dropdown(  # TODO

# 3. Date widget (text)
dbutils.widgets.text(  # TODO

# 4. Quality checks widget (dropdown)
dbutils.widgets.dropdown(  # TODO

print("✅ Widgets created")

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
# MAGIC The cell below reads the four widgets.

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

# 🔍 The parameter changes what the notebook does: switch pipeline_mode and re-run this cell
if pipeline_mode == "full":
    print("FULL mode: would drop the Bronze checkpoints and reprocess every landed file")
else:
    print(f"INCREMENTAL mode: only files newer than the checkpoint, for {processing_date}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Section 3: Task Execution Pattern
# MAGIC Each task in a multi-task job is typically a separate notebook.
# MAGIC ### Common Task Pattern
# MAGIC ```python
# MAGIC try:
# MAGIC     result = execute_task()
# MAGIC     dbutils.jobs.taskValues.set(key="records_processed", value=result)
# MAGIC except Exception as e:
# MAGIC     print(f"Task failed: {e}")
# MAGIC     raise  # re-raise so the job marks the task Failed and stops downstream tasks
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
    
    print(f"{'✅' if customers_exists else '❌'} Customers data: {'Found' if customers_exists else 'Missing'}")
    print(f"{'✅' if products_exists else '❌'} Products data: {'Found' if products_exists else 'Missing'}")
    print(f"{'✅' if sales_exists else '❌'} Sales data: {'Found' if sales_exists else 'Missing'}")
    
    if not (customers_exists and products_exists and sales_exists):
        raise Exception("Missing required source data")
    
    # Check Bronze tables exist
    tables_to_check = [
        CUSTOMERS_BRONZE_TABLE,
        PRODUCTS_BRONZE_TABLE,
        SALES_BRONZE_TABLE
    ]

    missing_tables = [t for t in tables_to_check if not spark.catalog.tableExists(t)]
    for table in tables_to_check:
        print(f"{'❌' if table in missing_tables else '✅'} Table {table}: {'Missing' if table in missing_tables else 'Exists'}")

    if missing_tables:
        raise Exception(f"Missing Bronze tables: {missing_tables}")

    print("\n✅ Data validation passed")
    validation_result = "SUCCESS"

except Exception as e:
    print(f"\n❌ Data validation failed: {str(e)}")
    raise  # re-raise: in a job this marks the task Failed, so Silver never runs on missing data

print(f"\nValidation Result: {validation_result}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Section 4: Error Handling Patterns
# MAGIC ### Key Patterns
# MAGIC 1. **Try-Except Blocks**: Catch, log, then re-raise
# MAGIC 2. **Task values**: Pass results to downstream tasks (`dbutils.jobs.taskValues`)
# MAGIC 3. **Retry Logic**: Automatic retry for transient failures
# MAGIC 4. **Alerting**: Notify on failures
# MAGIC ### dbutils.notebook.exit() Usage
# MAGIC `dbutils.notebook.exit(value)` ends a notebook successfully and returns a string to a caller that used `dbutils.notebook.run`. It never fails the task.
# MAGIC ```python
# MAGIC dbutils.notebook.exit(json.dumps({"status": "SUCCESS", "records_processed": 1000}))
# MAGIC ```
# MAGIC **Important**: downstream job tasks read values set with `dbutils.jobs.taskValues.set()`, via `{{tasks.<task_name>.values.<key>}}` in their parameters. To fail a task, raise an exception.

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ### 🎯 EXERCISE 2: Implement Retry Logic
# MAGIC **Your task**: Write a function that retries a failing call, from an empty cell.
# MAGIC
# MAGIC In a job, set retries on the task itself (task settings > Retries). That is the default for transient failures. A Python retry loop like this one is for retrying one call (an API, a flaky read) inside a task, when the task should not restart from the top.
# MAGIC
# MAGIC **Requirements:**
# MAGIC 1. `execute_task_with_retry(task_name, task_function, max_retries=3)` calls `task_function` up to `max_retries` times, printing each attempt, each error and each retry
# MAGIC 2. On success, return `{"status": "SUCCESS", "task": task_name, "attempt": N, "timestamp": ..., "result": ...}`
# MAGIC 3. When the last attempt fails, return `{"status": "FAILED", "task": task_name, "attempt": N, "timestamp": ..., "error": ...}`
# MAGIC
# MAGIC **Hint**: Use a while loop and track the attempt count.
# MAGIC
# MAGIC **You should see** (in the test cell after the solution) attempts 1 and 2 fail and attempt 3 return `SUCCESS` with `"attempt": 3`, then a second run with `max_retries=2` that fails both attempts and returns `"status": "FAILED"` with `"attempt": 2`.

# COMMAND ----------

# ✏️ STUDENT EXERCISE
# TODO: Write execute_task_with_retry(task_name, task_function, max_retries=3) from scratch
# (the requirements are in the cell above; the test is in the cell after the solution)

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

# COMMAND ----------

# 🔍 Verify: a task that fails twice with a transient error, then succeeds
import json

calls = [0]

def sample_task():
    calls[0] += 1
    if calls[0] < 3:
        raise ConnectionError(f"transient failure {calls[0]}")
    return {"records_processed": 1000}

result = execute_task_with_retry("sample_task", sample_task, max_retries=3)
print(f"\nTask Result:")
print(json.dumps(result, indent=2))

# Second run: only 2 attempts allowed and the task fails both, so the function returns the FAILED result
calls[0] = 0
result = execute_task_with_retry("always_fails", sample_task, max_retries=2)
print(json.dumps(result, indent=2))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Section 5: Creating Multi-Task Jobs in the UI
# MAGIC ### 🚀 Step-by-Step Job Creation
# MAGIC **Prerequisite**: finish every exercise in notebooks 02-04 (or uncomment their solutions). A TODO stub left in place is a syntax error and fails the task.
# MAGIC
# MAGIC **1. Open Jobs & Pipelines**
# MAGIC - Click **Jobs & Pipelines** in the left sidebar
# MAGIC - Click **Create** > **Job**
# MAGIC
# MAGIC **2. Configure Job Settings**
# MAGIC | Setting | Value |
# MAGIC |---------|-------|
# MAGIC | **Job Name** | `cert_prep_pipeline` |
# MAGIC | **Description** | Medallion architecture ETL pipeline |
# MAGIC
# MAGIC **3. Add Tasks** (click the **Notebook** tile, enter the task name, select the notebook in the **Path** field)
# MAGIC
# MAGIC **Task 1: Bronze Ingestion**
# MAGIC - Task Name: `bronze_ingestion`
# MAGIC - Type: Notebook
# MAGIC - **Path**: `02_Auto_Loader_Incremental_Ingestion` (Source: Workspace)
# MAGIC - Compute: Serverless
# MAGIC
# MAGIC **Task 2: Silver Transformation**
# MAGIC - Task Name: `silver_transformation`
# MAGIC - Type: Notebook
# MAGIC - **Path**: `03_Bronze_to_Silver` (Source: Workspace)
# MAGIC - **Depends On**: `bronze_ingestion`
# MAGIC - Compute: Serverless
# MAGIC
# MAGIC **Task 3: Gold Aggregation**
# MAGIC - Task Name: `gold_aggregation`
# MAGIC - Type: Notebook
# MAGIC - **Path**: `04_Silver_to_Gold_Advanced` (Source: Workspace)
# MAGIC - **Depends On**: `silver_transformation`
# MAGIC - Compute: Serverless
# MAGIC
# MAGIC Job parameters reach a notebook's widgets by name (this notebook reads `pipeline_mode`). Notebooks 02-04 define no widgets, so this job passes no parameters.
# MAGIC
# MAGIC **4. Configure Schedule (Optional)**
# MAGIC - Click **Add trigger** > **Scheduled**, choose **Advanced**, every day at 02:00, your timezone
# MAGIC - Or tick **Show Cron Syntax**: `0 0 2 * * ?` (Quartz cron: seconds come first, and `?` fills the day-of-week field)
# MAGIC
# MAGIC In this lab the only file that lands between runs is the 500-order practice file notebook 02 writes each time it runs: Bronze grows by 500 rows, and Silver drops them as orders it already has. In production the source systems drop new files every day.
# MAGIC
# MAGIC If you add the schedule, pause it when you finish the lab, or it runs every day.
# MAGIC
# MAGIC **5. Save and Run**
# MAGIC - Click **Save task** after each task, then **Run now**
# MAGIC
# MAGIC **What you should see**: 3 green tasks in sequence in the run view. Then run `DESCRIBE HISTORY cert_prep_catalog.03_gold.daily_sales_summary` in a SQL cell yourself: the job's overwrite (and any OPTIMIZE or VACUUM entry notebook 04 committed after it) has the job's details in the `job` column, where the versions from your interactive runs show `null`.
# MAGIC ### Task Dependencies Visualization
# MAGIC ```
# MAGIC bronze_ingestion
# MAGIC ↓
# MAGIC silver_transformation
# MAGIC ↓
# MAGIC gold_aggregation
# MAGIC ```
# MAGIC ### ⚠️ Free Edition Constraints
# MAGIC - ✅ Serverless compute for every task
# MAGIC - ✅ At most 5 tasks running concurrently: this 3-task chain runs one task at a time

# COMMAND ----------

# MAGIC %md
# MAGIC ## Section 6: Monitoring Jobs
# MAGIC ### 📊 Job Run Details
# MAGIC For each run:
# MAGIC | View | Information |
# MAGIC |------|-------------|
# MAGIC | **Run Status** | Success, Failed, Running, Cancelled |
# MAGIC | **Task Timeline** | Visual execution timeline |
# MAGIC | **Task DAG** | Dependency graph |
# MAGIC | **Logs** | stdout, stderr per task |
# MAGIC | **Query profile** | Per-statement metrics and plans: expand a task in the run's timeline view and click a statement (serverless has no Spark UI; classic compute shows the Spark UI instead) |
# MAGIC | **Parameters** | Runtime parameters used |
# MAGIC ### Accessing job context with dynamic value references

# COMMAND ----------

# Example: job context through task parameters (empty when you run the notebook interactively)
dbutils.widgets.text("job_id", "")
dbutils.widgets.text("run_id", "")
# In the job task, set parameters job_id = {{job.id}} and run_id = {{job.run_id}}
print(f"Job ID: {dbutils.widgets.get('job_id') or 'n/a (interactive)'}")
print(f"Run ID: {dbutils.widgets.get('run_id') or 'n/a (interactive)'}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Section 7: Troubleshooting Common Issues
# MAGIC ### Issue 1: Task Failure
# MAGIC **Symptoms**: Task shows as failed
# MAGIC
# MAGIC **Diagnosis:**
# MAGIC 1. Check task logs for error messages
# MAGIC 2. Expand the failed task in the run's timeline view and open the query profile of the slow or failed statement (serverless has no Spark UI; on classic compute, use the Spark UI)
# MAGIC
# MAGIC **Solutions:**
# MAGIC - Read the error in the task output and fix the code or data it names
# MAGIC - Set task retries (task settings > Retries) when the error is transient
# MAGIC ### Issue 2: Task Timeout
# MAGIC **Symptoms**: Task runs indefinitely
# MAGIC
# MAGIC **Diagnosis:**
# MAGIC 1. Check for never-terminating streams
# MAGIC 2. Look for blocking operations
# MAGIC 3. Review for cartesian products
# MAGIC
# MAGIC **Solutions:**
# MAGIC - Set a timeout on the task (task settings > Timeout); if the run is slow rather than stuck, check that the job's **Performance optimized** setting is on (standard mode starts slower and uses less compute), or process less data per run
# MAGIC - Use `trigger(availableNow=True)`
# MAGIC - Optimize queries with filters
# MAGIC ### Issue 3: Parameter Passing
# MAGIC **Symptoms**: Widget values not received
# MAGIC
# MAGIC **Diagnosis:**
# MAGIC 1. Compare the job parameter names with the widget names the notebook reads: they must match exactly
# MAGIC
# MAGIC **Solutions:**
# MAGIC - Give every widget a default value
# MAGIC - Print the parameters at the start of the task
# MAGIC ### Issue 4: Dependency Failures
# MAGIC **Symptoms**: Downstream tasks show **Upstream failed** and did not run
# MAGIC
# MAGIC **Diagnosis:**
# MAGIC 1. Find the first failed task in the run's DAG: the tasks after it did not run and show **Upstream failed**
# MAGIC 2. Read that task's error
# MAGIC
# MAGIC **Solutions:**
# MAGIC - Fix the upstream task first
# MAGIC - Add data validation at the start of the downstream task
# MAGIC - When a task must run whatever happens upstream (a cleanup or a notification), set that task's **Run if dependencies** to **All done**

# COMMAND ----------

# MAGIC %md
# MAGIC ## Section 8: Job Best Practices
# MAGIC ### 1️⃣ Idempotency
# MAGIC A re-run must give the same result as the first run.
# MAGIC - ✅ Use `INSERT OVERWRITE` for full refreshes
# MAGIC - ✅ Use `MERGE` for incremental updates
# MAGIC - ✅ `CREATE TABLE IF NOT EXISTS` and checkpoints, so a re-run loads nothing twice
# MAGIC ### 2️⃣ Parameterization
# MAGIC Make jobs configurable.
# MAGIC - ✅ Use widgets for runtime config
# MAGIC - ✅ Avoid hardcoded values
# MAGIC - ✅ Support multiple environments
# MAGIC ### 3️⃣ Error Handling
# MAGIC Log, then re-raise: the task fails and downstream tasks stop.
# MAGIC - ✅ Try-except blocks that re-raise
# MAGIC - ✅ Task retries for transient failures
# MAGIC - ✅ Log errors with context
# MAGIC ### 4️⃣ Monitoring
# MAGIC - ✅ Configure failure alerts
# MAGIC - ✅ Track SLA metrics
# MAGIC - ✅ Monitor data quality
# MAGIC - ✅ Job health dashboards
# MAGIC ### 5️⃣ Compute
# MAGIC - ✅ Serverless for job tasks (Databricks' recommendation; autoscaling is automatic)
# MAGIC - ✅ Standard performance mode for cost, performance-optimized when start time matters
# MAGIC - ✅ Classic job clusters only for configs serverless does not support
# MAGIC ### 6️⃣ Testing
# MAGIC - ✅ Run the job once on dev data before you schedule it
# MAGIC - ✅ Make one task fail on purpose and check that the downstream tasks stop

# COMMAND ----------

# MAGIC %md
# MAGIC ## Section 9: Summary and Checkpoint
# MAGIC ### 🎯 Key Concepts Covered
# MAGIC
# MAGIC **1. Databricks Jobs**
# MAGIC - Multi-task workflows
# MAGIC - Task dependencies (DAG)
# MAGIC - Serverless jobs compute vs classic job and all-purpose clusters
# MAGIC
# MAGIC **2. Job Parameters**
# MAGIC - Widget creation and usage
# MAGIC - Parameter passing between tasks
# MAGIC - Environment-specific config
# MAGIC
# MAGIC **3. Error Handling**
# MAGIC - Try-except patterns that re-raise
# MAGIC - Retry logic (task retries in the job, a loop for one flaky call)
# MAGIC - Raise to fail a task; task values to pass results downstream
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
# MAGIC - Serverless only
# MAGIC - 5 concurrent job tasks per account
# MAGIC
# MAGIC ### ✅ Exam Checklist
# MAGIC Can you:
# MAGIC - [ ] Create widgets and read parameter values?
# MAGIC - [ ] Configure multi-task jobs with dependencies?
# MAGIC - [ ] Implement error handling with try-except?
# MAGIC - [ ] Fail a task by raising, and pass values with task values?
# MAGIC - [ ] Explain when a job needs classic compute instead of serverless?
# MAGIC - [ ] Configure job scheduling with cron?
# MAGIC - [ ] Monitor and troubleshoot job runs?
# MAGIC ### 📚 Next Steps
# MAGIC **To Create Your Job:**
# MAGIC - [ ] Organize notebooks by task
# MAGIC - [ ] Open Jobs & Pipelines → Create → Job
# MAGIC - [ ] Create the 3-task job
# MAGIC - [ ] Configure dependencies
# MAGIC - [ ] Add a schedule (optional; no parameters: notebooks 02-04 read none)
# MAGIC - [ ] Run and monitor
# MAGIC
# MAGIC **🎉 Notebook Complete!**
# MAGIC Build the 3-task job in the UI and watch it run 02 → 03 → 04.

# COMMAND ----------

# Clean up widgets
dbutils.widgets.removeAll()
print("✅ Widgets removed")
