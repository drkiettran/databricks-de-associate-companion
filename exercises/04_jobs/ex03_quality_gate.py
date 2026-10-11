# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # Ch 4 · Exercise 03 — `quality_gate`  [FE]
# MAGIC Counts "bad" records and publishes the count as a **task value**, which the
# MAGIC If/else task branches on. Depends on `extract`.
# MAGIC
# MAGIC This is the pattern the docs describe: a task maintains a count of invalid
# MAGIC records in a value, and the workflow branches when bad records appear.

# COMMAND ----------

from pyspark.sql import functions as F

CATALOG, SCHEMA = "workspace", "de_cert"
TABLE = f"{CATALOG}.{SCHEMA}.day1_job_input"
spark.sql(f"USE CATALOG {CATALOG}"); spark.sql(f"USE SCHEMA {SCHEMA}")

# Flip this to force the other branch: 25.0 -> some rows exceed it -> bad_records > 0
THRESHOLD = 25.0
THRESHOLD = 999.0

# COMMAND ----------

bad = spark.table(TABLE).filter(F.col("value") > THRESHOLD).count()
total = spark.table(TABLE).count()
print(f"checked {total} rows; {bad} exceed threshold {THRESHOLD}")

# Publish for downstream tasks. Only numeric / string / boolean values can be
# referenced in an If/else operand.
dbutils.jobs.taskValues.set(key="bad_records", value=bad)
dbutils.jobs.taskValues.set(key="checked_rows", value=total)
print(f"set taskValue bad_records = {bad}")

# COMMAND ----------

# MAGIC %md
# MAGIC The If/else task reads this as `{{tasks.quality_gate.values.bad_records}}`.
# MAGIC With the condition `> 0`, a non-zero count takes the **true** branch
# MAGIC (`quarantine`); zero takes the **false** branch (`report`).