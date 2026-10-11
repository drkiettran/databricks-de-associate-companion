# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # Ch 4 · Exercise 06 — `process_region`  (NESTED task)  [FE]
# MAGIC The task the **For each** loop runs once per input value. It reads the current
# MAGIC iteration value from a job parameter wired to `{{input}}`.
# MAGIC
# MAGIC A For each construct is always TWO parts: the For each task (holding the list)
# MAGIC and this nested task (doing the work).

# COMMAND ----------

from pyspark.sql import functions as F

dbutils.widgets.text("region", "east")       # receives {{input}} from the loop
region = dbutils.widgets.get("region")

CATALOG, SCHEMA = "workspace", "de_cert"
TABLE = f"{CATALOG}.{SCHEMA}.day1_job_input"
spark.sql(f"USE CATALOG {CATALOG}"); spark.sql(f"USE SCHEMA {SCHEMA}")

# COMMAND ----------

df = spark.table(TABLE).filter(F.col("region") == region)
n = df.count()
avg = df.agg(F.round(F.avg("value"), 2)).first()[0]
print(f"iteration for region='{region}': {n} rows, avg value {avg}")
assert n > 0, f"no rows for region {region} — check the loop input wiring"
