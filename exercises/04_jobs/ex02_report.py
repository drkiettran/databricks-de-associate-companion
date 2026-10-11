# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # Ch 4 · Exercise 02 — `report`  (task 2 of 2)  [FE]
# MAGIC Section 4 (Working with Lakeflow Jobs).
# MAGIC
# MAGIC This notebook becomes the **downstream** task. It reads what `extract` wrote.
# MAGIC
# MAGIC If it runs *before* `extract` — i.e. the DAG dependency is missing — the read below
# MAGIC fails with `TABLE_OR_VIEW_NOT_FOUND` on a fresh schema. **That failure is the whole
# MAGIC point of the exercise:** it's what the "Depends on" edge exists to prevent.

# COMMAND ----------

CATALOG, SCHEMA = "workspace", "de_cert"
TABLE = f"{CATALOG}.{SCHEMA}.day1_job_input"

spark.sql(f"USE CATALOG {CATALOG}")
spark.sql(f"USE SCHEMA {SCHEMA}")

# COMMAND ----------

# Explicit, readable failure if the upstream task hasn't run.
if not spark.catalog.tableExists(TABLE):
    raise AssertionError(
        f"{TABLE} does not exist — the 'extract' task has not run. "
        "In the job, check that this task's 'Depends on' points at 'extract'.")

# COMMAND ----------

report = spark.sql(f"""
  SELECT region,
         COUNT(*)              AS n,
         ROUND(AVG(value), 2)  AS avg_value,
         MAX(_written_at)      AS upstream_written_at
  FROM   {TABLE}
  GROUP  BY region
  ORDER  BY region
""")
display(report)

rows = report.collect()
assert len(rows) == 2, f"expected 2 region groups, got {len(rows)}"
print("report task read the extract task's output successfully.")
print(f"upstream wrote at: {rows[0]['upstream_written_at']}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### What to observe in the run view
# MAGIC - This task stays **Pending** until `extract` reaches **Succeeded**, then runs.
# MAGIC - `upstream_written_at` proves it read data this run produced, in order.
# MAGIC - Remove the dependency and re-run: both tasks start in parallel and this one
# MAGIC   can fail. Re-add it to fix. That contrast is the day's lesson.