# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # Ch 4 · Exercise 09 — `params_demo`  [FE]
# MAGIC
# MAGIC Shows how values actually reach notebook code. **Every one of these arrives as a
# MAGIC widget**, because dynamic value references are resolved by the jobs service and
# MAGIC handed in as task parameters — they cannot be written inside notebook code.
# MAGIC
# MAGIC Also carries a deliberate failure switch for the repair-run exercise.

# COMMAND ----------

# Widgets are how parameters land. Defaults make the notebook runnable interactively.
dbutils.widgets.text("region",       "east",    "Region (job parameter)")
dbutils.widgets.text("run_id",       "local",   "{{job.run_id}}")
dbutils.widgets.text("trigger_type", "unknown", "{{job.trigger.type}}")
dbutils.widgets.text("repair_count", "0",       "{{job.repair_count}}")
dbutils.widgets.text("fail_me",      "false",   "Set true to force a failure")

region       = dbutils.widgets.get("region")
run_id       = dbutils.widgets.get("run_id")
trigger_type = dbutils.widgets.get("trigger_type")
repair_count = dbutils.widgets.get("repair_count")
fail_me      = dbutils.widgets.get("fail_me").strip().lower()

# COMMAND ----------

# MAGIC %md ## 1. What the jobs service resolved and passed in

# COMMAND ----------

print("=" * 62)
print(f"  region        : {region}")
print(f"  job.run_id    : {run_id}")
print(f"  trigger.type  : {trigger_type}   <- ONETIME / CRON / FILE_ARRIVAL")
print(f"  repair_count  : {repair_count}   <- increments on each repair")
print("=" * 62)
print()
print("Note: these are plain strings. The {{ }} references were replaced by the")
print("jobs service BEFORE this notebook started. Writing {{job.run_id}} in a")
print("Python cell would print those literal characters, not a value.")

# COMMAND ----------

# MAGIC %md ## 2. Use the parameter for real — filter by the region passed in

# COMMAND ----------

from pyspark.sql import functions as F

CATALOG, SCHEMA = "workspace", "de_cert"
TABLE = f"{CATALOG}.{SCHEMA}.day1_job_input"
spark.sql(f"USE CATALOG {CATALOG}"); spark.sql(f"USE SCHEMA {SCHEMA}")

if spark.catalog.tableExists(TABLE):
    df = spark.table(TABLE).filter(F.col("region") == region)
    n = df.count()
    avg = df.agg(F.round(F.avg("value"), 2)).first()[0]
    print(f"region='{region}': {n} rows, avg value {avg}")
    print("Re-run with a different region parameter and this changes —")
    print("one job definition, different inputs. That is parameterization.")
else:
    print(f"{TABLE} missing — run the extract task first.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. The repair switch
# MAGIC Set the task parameter `fail_me` to `true` to force a failure, then use
# MAGIC **Repair run** to re-run only this task. Clear it back to `false` in the repair
# MAGIC dialog — parameters entered there override the existing values.

# COMMAND ----------

if fail_me == "true":
    raise RuntimeError(
        f"Deliberate failure for the repair exercise (repair_count={repair_count}). "
        "Set fail_me=false in the Repair run dialog and repair this run.")

print("fail_me is false — task succeeded.")
print(f"If this run was repaired, repair_count should now be > 0: {repair_count}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### What to observe
# MAGIC - Every value above arrived through a **widget**, never from `{{ }}` in code.
# MAGIC - `trigger.type` reads `one_time` for a manual run. The system tables (ex08) record the same run as `ONETIME` — the format differs by surface, which matters because If/else operands compare as strings.
# MAGIC - After a repair, `repair_count` is greater than zero — proof the run was repaired
# MAGIC   rather than restarted.