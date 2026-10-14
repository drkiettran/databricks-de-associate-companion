# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # Ch 4 · Exercise 04 — `quarantine`  (TRUE branch)  [FE]
# MAGIC Runs only when `quality_gate` found bad records. Reads the upstream task value
# MAGIC to prove data crossed the task boundary.

# COMMAND ----------

from pyspark.sql import functions as F

CATALOG, SCHEMA = "workspace", "de_cert"
TABLE = f"{CATALOG}.{SCHEMA}.day1_job_input"
QUAR  = f"{CATALOG}.{SCHEMA}.day2_quarantine"
spark.sql(f"USE CATALOG {CATALOG}"); spark.sql(f"USE SCHEMA {SCHEMA}")

# debugValue lets this notebook also run interactively, outside a job.
bad = dbutils.jobs.taskValues.get(taskKey="quality_gate", key="bad_records",
                                  default=0, debugValue=-1)
print(f"quarantine branch fired. upstream reported bad_records = {bad}")

# COMMAND ----------

(spark.table(TABLE).filter(F.col("value") > 25.0)
   .write.mode("overwrite").saveAsTable(QUAR))
n = spark.table(QUAR).count()
print(f"wrote {n} quarantined rows to {QUAR}")
assert n == bad or bad == -1, f"quarantined {n} but upstream said {bad}"
display(spark.table(QUAR).limit(5))

# COMMAND ----------