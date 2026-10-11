# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # Ch 4 · Exercise 01 — `extract`  (task 1 of 2)  [FE]
# MAGIC Section 4 (Working with Lakeflow Jobs).
# MAGIC
# MAGIC This notebook becomes the **upstream** task in a two-task job. It writes a small
# MAGIC scratch table that the `report` task reads. Deliberately tiny — the lesson is
# MAGIC orchestration, not logic.
# MAGIC
# MAGIC Run it once by hand first to confirm it works standalone, then wire it into a job.

# COMMAND ----------

from pyspark.sql import functions as F

CATALOG, SCHEMA = "workspace", "de_cert"
TABLE = f"{CATALOG}.{SCHEMA}.day1_job_input"

spark.sql(f"USE CATALOG {CATALOG}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {SCHEMA}")
spark.sql(f"USE SCHEMA {SCHEMA}")

# COMMAND ----------

df = (spark.range(0, 100)
        .withColumn("region", F.when(F.col("id") % 2 == 0, "east").otherwise("west"))
        .withColumn("value",  (F.col("id") * 7 % 50).cast("double"))
        .withColumn("_written_at", F.current_timestamp()))

df.write.mode("overwrite").saveAsTable(TABLE)

n = spark.table(TABLE).count()
print(f"extract wrote {n} rows to {TABLE}")
assert n == 100, f"expected 100 rows, got {n}"
display(spark.table(TABLE).limit(5))

# COMMAND ----------

# MAGIC %md
# MAGIC ### What this task contributes to the DAG
# MAGIC The `report` task depends on this table existing. That dependency is what the
# MAGIC **Depends on** setting encodes — run order, not data transfer. The downstream task
# MAGIC does not receive this DataFrame; it re-reads the table from Unity Catalog.