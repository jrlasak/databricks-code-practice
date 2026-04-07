# Databricks notebook source
# COMMAND ----------
# MAGIC %md
# MAGIC # MERGE Operations - Solutions
# MAGIC **Topic**: Delta Lake | **Exercises**: 9
# MAGIC
# MAGIC Reference solutions, hints, and common mistakes for each exercise.
# MAGIC Try solving the exercises first before looking here.

# COMMAND ----------
# MAGIC %md
# MAGIC ## Exercise 1: Basic Upsert
# MAGIC
# MAGIC **Hints**:
# MAGIC 1. MERGE INTO uses a USING clause to specify the source table
# MAGIC 2. The ON clause defines how rows are matched (like a JOIN condition)
# MAGIC 3. UPDATE SET * and INSERT * copy all columns from source
# MAGIC
# MAGIC **Common mistakes**:
# MAGIC - Forgetting the ON clause (syntax error)
# MAGIC - Using = instead of SET * (must explicitly list columns or use SET *)
# MAGIC - Swapping target and source (MERGE INTO is always the target)

# COMMAND ----------

# EXERCISE_KEY: merge_ex1
CATALOG = "db_code"
SCHEMA = "merge_operations"
BASE_SCHEMA = "delta_lake"

spark.sql(f"""
    MERGE INTO {CATALOG}.{SCHEMA}.merge_ex1_target t
    USING {CATALOG}.{SCHEMA}.merge_ex1_source s
    ON t.order_id = s.order_id
    WHEN MATCHED THEN UPDATE SET *
    WHEN NOT MATCHED THEN INSERT *
""")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Exercise 2: Insert-Only Merge
# MAGIC
# MAGIC **Hints**:
# MAGIC 1. You only need ONE when-clause in this MERGE
# MAGIC 2. Matched rows should be completely ignored
# MAGIC 3. Think about which clause handles "not in target yet"
# MAGIC
# MAGIC **Common mistakes**:
# MAGIC - Adding a WHEN MATCHED clause that does nothing (unnecessary)
# MAGIC - Using INSERT INTO instead of MERGE (doesn't check for duplicates)
# MAGIC - Confusing NOT MATCHED with NOT MATCHED BY SOURCE

# COMMAND ----------

# EXERCISE_KEY: merge_ex2
spark.sql(f"""
    MERGE INTO {CATALOG}.{SCHEMA}.merge_ex2_target t
    USING {CATALOG}.{SCHEMA}.merge_ex2_source s
    ON t.order_id = s.order_id
    WHEN NOT MATCHED THEN INSERT *
""")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Exercise 3: Update-Only Merge
# MAGIC
# MAGIC **Hints**:
# MAGIC 1. Mirror of Exercise 2 - only ONE when-clause needed
# MAGIC 2. New records in source should be silently dropped
# MAGIC 3. Only rows that exist in BOTH target and source get modified
# MAGIC
# MAGIC **Common mistakes**:
# MAGIC - Adding WHEN NOT MATCHED (this would insert new rows)
# MAGIC - Using UPDATE table SET instead of MERGE (would update without matching)

# COMMAND ----------

# EXERCISE_KEY: merge_ex3
spark.sql(f"""
    MERGE INTO {CATALOG}.{SCHEMA}.merge_ex3_target t
    USING {CATALOG}.{SCHEMA}.merge_ex3_source s
    ON t.order_id = s.order_id
    WHEN MATCHED THEN UPDATE SET *
""")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Exercise 4: Deduplicate Before Merge
# MAGIC
# MAGIC **Hints**:
# MAGIC 1. MERGE fails if the source has multiple rows matching the same target row
# MAGIC 2. Use ROW_NUMBER() or QUALIFY to keep only the latest row per order_id
# MAGIC 3. Create a temp view or CTE with the deduped data, then MERGE from that
# MAGIC
# MAGIC **Common mistakes**:
# MAGIC - Trying to MERGE directly from a source with duplicates (runtime error)
# MAGIC - Using DISTINCT instead of ROW_NUMBER (DISTINCT doesn't pick "latest")
# MAGIC - Ordering ROW_NUMBER ASC instead of DESC (keeps oldest, not newest)

# COMMAND ----------

# EXERCISE_KEY: merge_ex4
# Approach 1: Temp view with QUALIFY
spark.sql(f"""
    CREATE OR REPLACE TEMP VIEW merge_ex4_deduped AS
    SELECT * FROM {CATALOG}.{SCHEMA}.merge_ex4_source
    QUALIFY ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY updated_at DESC) = 1
""")

spark.sql(f"""
    MERGE INTO {CATALOG}.{SCHEMA}.merge_ex4_target t
    USING merge_ex4_deduped s
    ON t.order_id = s.order_id
    WHEN MATCHED THEN UPDATE SET *
    WHEN NOT MATCHED THEN INSERT *
""")

# Approach 2: Subquery (also valid)
# spark.sql(f"""
#     MERGE INTO {CATALOG}.{SCHEMA}.merge_ex4_target t
#     USING (
#         SELECT * FROM {CATALOG}.{SCHEMA}.merge_ex4_source
#         QUALIFY ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY updated_at DESC) = 1
#     ) s
#     ON t.order_id = s.order_id
#     WHEN MATCHED THEN UPDATE SET *
#     WHEN NOT MATCHED THEN INSERT *
# """)

# COMMAND ----------
# MAGIC %md
# MAGIC ## Exercise 5: Conditional Merge - Only Update If Newer
# MAGIC
# MAGIC **Hints**:
# MAGIC 1. WHEN MATCHED accepts an additional AND condition
# MAGIC 2. Compare the source and target `updated_at` timestamps
# MAGIC 3. Rows that match but fail the condition are silently skipped
# MAGIC
# MAGIC **Common mistakes**:
# MAGIC - Putting the timestamp condition in the ON clause (changes matching, not filtering)
# MAGIC - Using >= instead of > (equal timestamps shouldn't trigger an update)
# MAGIC - Forgetting WHEN NOT MATCHED for new inserts

# COMMAND ----------

# EXERCISE_KEY: merge_ex5
spark.sql(f"""
    MERGE INTO {CATALOG}.{SCHEMA}.merge_ex5_target t
    USING {CATALOG}.{SCHEMA}.merge_ex5_source s
    ON t.order_id = s.order_id
    WHEN MATCHED AND s.updated_at > t.updated_at THEN UPDATE SET *
    WHEN NOT MATCHED THEN INSERT *
""")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Exercise 6: MERGE with DELETE Clause
# MAGIC
# MAGIC **Hints**:
# MAGIC 1. WHEN MATCHED THEN DELETE is valid Delta Lake syntax
# MAGIC 2. You need TWO WHEN MATCHED clauses: one for delete, one for update
# MAGIC 3. Databricks evaluates WHEN clauses in order - put DELETE first
# MAGIC
# MAGIC **Common mistakes**:
# MAGIC - Putting UPDATE before DELETE (UPDATE catches all matched rows first)
# MAGIC - Using a separate DELETE statement instead of MERGE DELETE clause
# MAGIC - Checking status on the target instead of the source

# COMMAND ----------

# EXERCISE_KEY: merge_ex6
spark.sql(f"""
    MERGE INTO {CATALOG}.{SCHEMA}.merge_ex6_target t
    USING {CATALOG}.{SCHEMA}.merge_ex6_source s
    ON t.order_id = s.order_id
    WHEN MATCHED AND s.status = 'cancelled' THEN DELETE
    WHEN MATCHED THEN UPDATE SET *
    WHEN NOT MATCHED THEN INSERT *
""")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Exercise 7: Multi-Condition MERGE
# MAGIC
# MAGIC **Hints**:
# MAGIC 1. You need three WHEN clauses: DELETE, conditional UPDATE, and INSERT
# MAGIC 2. Clause order: most specific conditions first (cancelled check, then timestamp check)
# MAGIC 3. Rows that match but fail all WHEN MATCHED conditions are silently skipped
# MAGIC
# MAGIC **Common mistakes**:
# MAGIC - Wrong clause ordering (general UPDATE catches rows before DELETE condition is checked)
# MAGIC - Forgetting that "no matching WHEN clause" means "do nothing" (not an error)
# MAGIC - Adding a catch-all WHEN MATCHED THEN UPDATE as the second clause (would update stale records)

# COMMAND ----------

# EXERCISE_KEY: merge_ex7
spark.sql(f"""
    MERGE INTO {CATALOG}.{SCHEMA}.merge_ex7_target t
    USING {CATALOG}.{SCHEMA}.merge_ex7_source s
    ON t.order_id = s.order_id
    WHEN MATCHED AND s.status = 'cancelled' THEN DELETE
    WHEN MATCHED AND s.updated_at > t.updated_at THEN UPDATE SET *
    WHEN NOT MATCHED THEN INSERT *
""")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Exercise 8: SCD Type 2 with MERGE
# MAGIC
# MAGIC **Hints**:
# MAGIC 1. SCD Type 2 requires two operations: expire old + insert new
# MAGIC 2. Step 1: MERGE to set is_current=false and effective_end_date on matched records
# MAGIC 3. Step 2: INSERT all source records as new current versions
# MAGIC
# MAGIC **Common mistakes**:
# MAGIC - Trying to do it in a single MERGE (possible but much more complex)
# MAGIC - Forgetting to filter on is_current=true in the MERGE ON clause
# MAGIC - Not inserting new versions for ALL source records (only inserting new customers)
# MAGIC - Using UPDATE SET * (would overwrite the customer_id and other fields)

# COMMAND ----------

# EXERCISE_KEY: merge_ex8
# Step 1: Expire existing records that have updates
spark.sql(f"""
    MERGE INTO {CATALOG}.{SCHEMA}.merge_ex8_target t
    USING {CATALOG}.{SCHEMA}.merge_ex8_source s
    ON t.customer_id = s.customer_id AND t.is_current = true
    WHEN MATCHED THEN UPDATE SET
        is_current = false,
        effective_end_date = current_date()
""")

# Step 2: Insert new current versions for ALL source customers
spark.sql(f"""
    INSERT INTO {CATALOG}.{SCHEMA}.merge_ex8_target
    SELECT customer_id, name, email, region, tier,
           true AS is_current,
           current_date() AS effective_start_date,
           DATE '9999-12-31' AS effective_end_date
    FROM {CATALOG}.{SCHEMA}.merge_ex8_source
""")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Exercise 9: MERGE with Schema Evolution
# MAGIC
# MAGIC **Hints**:
# MAGIC 1. Delta Lake blocks schema changes by default during MERGE
# MAGIC 2. Use `MERGE WITH SCHEMA EVOLUTION` to allow the source to add new columns
# MAGIC 3. The syntax goes between MERGE and INTO: `MERGE WITH SCHEMA EVOLUTION INTO ...`
# MAGIC
# MAGIC **Common mistakes**:
# MAGIC - Forgetting WITH SCHEMA EVOLUTION (MERGE fails with schema mismatch error)
# MAGIC - Placing WITH SCHEMA EVOLUTION after INTO (wrong position)
# MAGIC - Manually adding columns first (works but the exercise is about auto-evolution)

# COMMAND ----------

# EXERCISE_KEY: merge_ex9
spark.sql(f"""
    MERGE WITH SCHEMA EVOLUTION INTO {CATALOG}.{SCHEMA}.merge_ex9_target t
    USING {CATALOG}.{SCHEMA}.merge_ex9_source s
    ON t.order_id = s.order_id
    WHEN MATCHED THEN UPDATE SET *
    WHEN NOT MATCHED THEN INSERT *
""")
