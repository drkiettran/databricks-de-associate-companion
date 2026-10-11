# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # Ch 4 · Exercise 08 — System tables probe  [VERIFY, read-only]
# MAGIC
# MAGIC Settles the open question: is `system.lakeflow` reachable on Free Edition?
# MAGIC Every query is wrapped — a failure is a **finding**, not a problem. Read-only
# MAGIC throughout; nothing is created, written, or dropped.
# MAGIC
# MAGIC Run it top to bottom and report what Section 1 prints.

# COMMAND ----------

from pyspark.sql import functions as F

def probe(label, sql, show=5):
    """Run a read-only query; report success or the exact error class."""
    print(f"\n--- {label} ---")
    try:
        df = spark.sql(sql)
        n = df.count()
        print(f"  OK — {n} row(s)")
        if n:
            display(df.limit(show))
        return True
    except Exception as ex:
        first = str(ex).splitlines()[0][:140]
        print(f"  BLOCKED — {first}")
        return False

# COMMAND ----------

# MAGIC %md ## 1. Is the schema reachable at all?  (the key question)

# COMMAND ----------

available = probe("catalogs visible", "SHOW CATALOGS")
sys_ok = probe("schemas in `system`", "SHOW SCHEMAS IN system")
lakeflow_ok = probe("tables in system.lakeflow", "SHOW TABLES IN system.lakeflow")

print("\n" + "=" * 70)
print("KEY FINDING:", "system.lakeflow IS reachable" if lakeflow_ok
      else "system.lakeflow is NOT reachable on this workspace")
print("=" * 70)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. The four tables
# MAGIC Only meaningful if Section 1 succeeded. Each is attempted independently so a
# MAGIC partial entitlement still shows up.

# COMMAND ----------

probe("jobs (definitions, SCD2)",
      "SELECT * FROM system.lakeflow.jobs")
probe("job_tasks (task definitions, SCD2)",
      "SELECT * FROM system.lakeflow.job_tasks")
probe("job_run_timeline (per-run)",
      "SELECT * FROM system.lakeflow.job_run_timeline ORDER BY period_start_time DESC")
probe("job_task_run_timeline (per-task)",
      "SELECT * FROM system.lakeflow.job_task_run_timeline ORDER BY period_start_time DESC")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. The SCD2 deduplication pattern
# MAGIC `jobs` and `job_tasks` keep history of definition changes, so a naive select
# MAGIC returns several rows per job. QUALIFY to the current version.

# COMMAND ----------

probe("current job definitions only (SCD2 dedup)", """
  SELECT job_id, name, creator_id, change_time
  FROM   system.lakeflow.jobs
  QUALIFY row_number() OVER (PARTITION BY job_id ORDER BY change_time DESC) = 1
""")

# COMMAND ----------

# MAGIC %md ## 4. Useful analytical shapes (the patterns the exam describes)

# COMMAND ----------

probe("runs by result status", """
  SELECT result_state, COUNT(*) AS runs
  FROM   system.lakeflow.job_run_timeline
  GROUP  BY result_state
  ORDER  BY runs DESC
""")

probe("trigger type per run — CRON / FILE_ARRIVAL / TABLE / CONTINUOUS / MULTIPLE", """
  SELECT trigger_type, COUNT(*) AS runs
  FROM   system.lakeflow.job_run_timeline
  GROUP  BY trigger_type
""")

probe("slowest task runs", """
  SELECT task_key,
         ROUND(AVG(unix_timestamp(period_end_time)
                 - unix_timestamp(period_start_time)), 1) AS avg_seconds,
         COUNT(*) AS runs
  FROM   system.lakeflow.job_task_run_timeline
  GROUP  BY task_key
  ORDER  BY avg_seconds DESC
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## What to check
# MAGIC - **The KEY FINDING line from Section 1** — reachable or not, and the exact error.
# MAGIC - If reachable: whether your own jobs appear, and which trigger types show up
# MAGIC   (ex07_file_watch produces CRON and FILE_ARRIVAL runs once you attach triggers).
# MAGIC - If not: nothing more to do here — the four tables and their contents remain
# MAGIC   examinable as concepts, and that is what the exam actually tests.