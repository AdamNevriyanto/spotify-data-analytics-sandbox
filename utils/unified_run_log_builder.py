# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# DBTITLE 1,Unified Run Log Builder
# MAGIC %md
# MAGIC # Unified Run Log Builder
# MAGIC
# MAGIC This notebook builds a single Delta table that consolidates **SDP Pipeline runs** and **Regular Job runs** into one unified log. It captures:
# MAGIC
# MAGIC | Column | Description |
# MAGIC | --- | --- |
# MAGIC | `run_type` | PIPELINE or JOB |
# MAGIC | `id` | pipeline_id or job_id |
# MAGIC | `name` | pipeline or job name |
# MAGIC | `run_id` | update_id (pipelines) or run_id (jobs) |
# MAGIC | `status` | COMPLETED / FAILED / CANCELED |
# MAGIC | `start_time` | UTC timestamp |
# MAGIC | `end_time` | UTC timestamp |
# MAGIC | `duration_seconds` | Computed duration |
# MAGIC | `total_rows_processed` | From event_log (pipelines only, NULL for jobs) |
# MAGIC
# MAGIC > **Requirements:**
# MAGIC > - Must run on a **SQL Warehouse** or **Shared cluster** (event_log() is not accessible from assigned clusters)
# MAGIC > - User needs SELECT access to `system.lakeflow.*` system tables
# MAGIC >
# MAGIC > **Tip:** Schedule this notebook as a daily job to keep the log table fresh.

# COMMAND ----------

# DBTITLE 1,Configuration
# Configuration - Set your target catalog, schema, and table name
catalog = "mii_workspace"
schema = "default"
table = "unified_run_log"

# How many days of history to include
lookback_days = 30

print(f"Target table: {catalog}.{schema}.{table}")
print(f"Lookback period: {lookback_days} days")

# COMMAND ----------

# DBTITLE 1,Get all pipeline IDs
# Get all distinct pipeline IDs from the last N days
pipeline_ids_df = spark.sql(f"""
  SELECT DISTINCT pipeline_id
  FROM system.lakeflow.pipeline_update_timeline
  WHERE period_start_time > CURRENT_TIMESTAMP() - INTERVAL {lookback_days} DAYS
""")

pipeline_ids = [row.pipeline_id for row in pipeline_ids_df.collect()]
print(f"Found {len(pipeline_ids)} pipelines with activity in the last {lookback_days} days")
print(pipeline_ids)

# COMMAND ----------

# DBTITLE 1,Get pipeline row counts from event_log
from pyspark.sql import functions as F
from functools import reduce

# Query event_log for each pipeline to get row counts per update
# event_log() requires a SQL Warehouse or Shared cluster
row_count_dfs = []
skipped_pipelines = []

for pid in pipeline_ids:
    try:
        df = spark.sql(f"""
            SELECT
              origin.pipeline_id,
              origin.update_id,
              SUM(TRY_CAST(details:flow_progress.metrics.num_output_rows AS BIGINT)) AS total_rows_processed
            FROM event_log(pipeline_id => '{pid}')
            WHERE event_type = 'flow_progress'
              AND details:flow_progress.status = 'COMPLETED'
            GROUP BY origin.pipeline_id, origin.update_id
        """)
        row_count_dfs.append(df)
    except Exception as e:
        skipped_pipelines.append((pid, str(e)[:100]))
        print(f"⚠️ Skipped pipeline {pid}: {str(e)[:100]}")

# Union all row count DataFrames
if row_count_dfs:
    row_counts_df = reduce(lambda a, b: a.union(b), row_count_dfs)
    row_counts_df.createOrReplaceTempView("pipeline_row_counts")
    print(f"\n✅ Successfully collected row counts from {len(row_count_dfs)} pipelines")
else:
    row_counts_df = None
    print("\n⚠️ No row counts collected (event_log may not be accessible from this cluster)")

if skipped_pipelines:
    print(f"\n⚠️ Skipped {len(skipped_pipelines)} pipelines due to errors")

# COMMAND ----------

# DBTITLE 1,Build unified run log DataFrame
# Build the unified log combining pipeline runs and job runs
unified_df = spark.sql(f"""
  WITH pipeline_runs AS (
    SELECT
      'PIPELINE' AS run_type,
      t.pipeline_id AS id,
      p.name AS name,
      t.update_id AS run_id,
      t.result_state AS status,
      t.period_start_time AS start_time,
      t.period_end_time AS end_time,
      TIMESTAMPDIFF(SECOND, t.period_start_time, t.period_end_time) AS duration_seconds
    FROM system.lakeflow.pipeline_update_timeline t
    LEFT JOIN (
      SELECT *, ROW_NUMBER() OVER(PARTITION BY workspace_id, pipeline_id ORDER BY change_time DESC) AS rn
      FROM system.lakeflow.pipelines
    ) p ON t.pipeline_id = p.pipeline_id AND t.workspace_id = p.workspace_id AND p.rn = 1
    WHERE t.result_state IS NOT NULL
      AND t.period_start_time > CURRENT_TIMESTAMP() - INTERVAL {lookback_days} DAYS
  ),
  job_runs AS (
    SELECT
      'JOB' AS run_type,
      t.job_id AS id,
      j.name AS name,
      t.run_id AS run_id,
      t.result_state AS status,
      t.period_start_time AS start_time,
      t.period_end_time AS end_time,
      TIMESTAMPDIFF(SECOND, t.period_start_time, t.period_end_time) AS duration_seconds
    FROM system.lakeflow.job_run_timeline t
    LEFT JOIN (
      SELECT *, ROW_NUMBER() OVER(PARTITION BY workspace_id, job_id ORDER BY change_time DESC) AS rn
      FROM system.lakeflow.jobs
    ) j ON t.job_id = j.job_id AND t.workspace_id = j.workspace_id AND j.rn = 1
    WHERE t.result_state IS NOT NULL
      AND t.period_start_time > CURRENT_TIMESTAMP() - INTERVAL {lookback_days} DAYS
  )
  SELECT * FROM pipeline_runs
  UNION ALL
  SELECT * FROM job_runs
""")

# Join with row counts (if available)
if row_counts_df is not None:
    final_df = unified_df.join(
        row_counts_df,
        (unified_df.id == row_counts_df.pipeline_id) & (unified_df.run_id == row_counts_df.update_id),
        "left"
    ).select(
        unified_df["run_type"],
        unified_df["id"],
        unified_df["name"],
        unified_df["run_id"],
        unified_df["status"],
        unified_df["start_time"],
        unified_df["end_time"],
        unified_df["duration_seconds"],
        row_counts_df["total_rows_processed"]
    )
else:
    final_df = unified_df.withColumn("total_rows_processed", F.lit(None).cast("bigint"))

final_df = final_df.orderBy(F.col("start_time").desc())
print(f"✅ Unified log built: {final_df.count()} total rows")
final_df.printSchema()

# COMMAND ----------

# DBTITLE 1,Write to Delta table
# Write the unified run log to a Delta table using MERGE (keeps historical data)
target_table = f"{catalog}.{schema}.{table}"

# Create table if it doesn't exist
spark.sql(f"""
  CREATE TABLE IF NOT EXISTS {target_table} (
    run_type STRING,
    id STRING,
    name STRING,
    run_id STRING,
    status STRING,
    start_time TIMESTAMP,
    end_time TIMESTAMP,
    duration_seconds LONG,
    total_rows_processed LONG
  )
""")

# Use MERGE to upsert: insert new runs, update existing ones (e.g. status changes)
final_df.createOrReplaceTempView("new_runs")

spark.sql(f"""
  MERGE INTO {target_table} AS target
  USING new_runs AS source
  ON target.id = source.id AND target.run_id = source.run_id
  WHEN MATCHED AND source.status != target.status THEN
    UPDATE SET
      status = source.status,
      end_time = source.end_time,
      duration_seconds = source.duration_seconds,
      total_rows_processed = source.total_rows_processed
  WHEN NOT MATCHED THEN
    INSERT *
""")

row_count = spark.sql(f"SELECT COUNT(*) as cnt FROM {target_table}").collect()[0].cnt
print(f"✅ MERGE completed on: {target_table}")
print(f"   Total rows in table: {row_count}")

# COMMAND ----------

# DBTITLE 1,Verify written table
# Display a sample from the written table to confirm success
result = spark.sql(f"""
  SELECT *
  FROM {catalog}.{schema}.{table}
  ORDER BY start_time DESC
  LIMIT 20
""")

print(f"Table: {catalog}.{schema}.{table}")
print(f"Total rows: {result.count()}")
display(result)