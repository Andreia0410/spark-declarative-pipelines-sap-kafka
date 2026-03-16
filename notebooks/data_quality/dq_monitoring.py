"""
Data Quality Monitoring — SDP Event Log Analysis
=================================================
Queries the DLT event log to surface data quality metrics
across all pipeline expectations.

Run this notebook against any SDP pipeline to get a consolidated
DQ report. No records are dropped — this is purely observational.

Usage:
  Set PIPELINE_ID to the target DLT pipeline ID before running.
"""

# COMMAND ----------

# MAGIC %md
# MAGIC ## Data Quality Dashboard — Spark Declarative Pipelines

# COMMAND ----------

PIPELINE_ID = dbutils.widgets.get("pipeline_id")  # set via widget or hardcode for dev

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1. Overall Pass Rate by Expectation

# COMMAND ----------

dq_summary = spark.sql(f"""
    SELECT
        expectations.name                                   AS expectation_name,
        SUM(expectations.passed_records)                    AS passed_records,
        SUM(expectations.failed_records)                    AS failed_records,
        SUM(expectations.passed_records)
            + SUM(expectations.failed_records)              AS total_records,
        ROUND(
            SUM(expectations.passed_records) * 100.0
            / NULLIF(
                SUM(expectations.passed_records)
                + SUM(expectations.failed_records), 0
            ), 2
        )                                                   AS pass_rate_pct
    FROM
        event_log('{PIPELINE_ID}')
    LATERAL VIEW EXPLODE(
        details:flow_progress.data_quality.expectations
    ) t AS expectations
    WHERE event_type = 'flow_progress'
      AND details:flow_progress.data_quality IS NOT NULL
    GROUP BY 1
    ORDER BY pass_rate_pct ASC
""")

display(dq_summary)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2. Failure Trend — Last 24 Hours

# COMMAND ----------

failure_trend = spark.sql(f"""
    SELECT
        DATE_TRUNC('hour', timestamp)                       AS hour_bucket,
        expectations.name                                   AS expectation_name,
        SUM(expectations.failed_records)                    AS failed_records
    FROM
        event_log('{PIPELINE_ID}')
    LATERAL VIEW EXPLODE(
        details:flow_progress.data_quality.expectations
    ) t AS expectations
    WHERE event_type = 'flow_progress'
      AND timestamp >= NOW() - INTERVAL 24 HOURS
      AND expectations.failed_records > 0
    GROUP BY 1, 2
    ORDER BY 1 DESC, 3 DESC
""")

display(failure_trend)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3. Flow Throughput — Records Processed per Update

# COMMAND ----------

throughput = spark.sql(f"""
    SELECT
        timestamp,
        details:flow_progress.name                          AS flow_name,
        details:flow_progress.metrics.num_output_rows       AS output_rows,
        details:flow_progress.metrics.num_input_rows        AS input_rows,
        details:flow_progress.`status`                      AS status
    FROM
        event_log('{PIPELINE_ID}')
    WHERE event_type = 'flow_progress'
      AND details:flow_progress.metrics.num_output_rows IS NOT NULL
    ORDER BY timestamp DESC
    LIMIT 100
""")

display(throughput)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4. Alert: Expectations Below 95% Pass Rate

# COMMAND ----------

alerts = dq_summary.filter("pass_rate_pct < 95.0 OR pass_rate_pct IS NULL")

if alerts.count() > 0:
    print("⚠️  WARNING: The following expectations are below the 95% threshold:")
    display(alerts)
else:
    print("✅ All expectations are passing above 95%. Pipeline is healthy.")
