# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # Ch 4 · Exercise 05 — `list_regions`  [FE]
# MAGIC Produces the list the **For each** task loops over. This is the realistic
# MAGIC pattern: inputs generated dynamically by an upstream task rather than hardcoded.

# COMMAND ----------

CATALOG, SCHEMA = "workspace", "de_cert"
TABLE = f"{CATALOG}.{SCHEMA}.day1_job_input"
spark.sql(f"USE CATALOG {CATALOG}"); spark.sql(f"USE SCHEMA {SCHEMA}")

regions = sorted(r["region"] for r in
                 spark.table(TABLE).select("region").distinct().collect())
print("regions found:", regions)

# The For each task reads this as {{tasks.list_regions.values.regions}}.
# A JSON-style list of strings is what the loop expects.
dbutils.jobs.taskValues.set(key="regions", value=regions)
print("set taskValue regions =", regions)