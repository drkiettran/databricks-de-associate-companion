# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # Ch 7 · Exercise 01 — Run history, baselines, and pipeline health  [FE]
# MAGIC
# MAGIC Reads the job system tables to answer the questions the Jobs UI answers by eye: how long
# MAGIC does this job normally take, is the latest run unusual, how often does it fail, and which
# MAGIC task dominates? Read-only — it changes nothing.
# MAGIC
# MAGIC Set `JOB_NAME` below to one job, or leave it empty to analyze every job you can see.

# COMMAND ----------

JOB_NAME = ""          # e.g. "my_daily_job"; empty means all jobs
SLOW_FACTOR = 1.5      # flag a run slower than this multiple of its job's median

def probe(label, sql, show=True):
    """Run one statement to completion; report OK or the full error. Returns True on success."""
    print(f"\n--- {label}")
    try:
        df = spark.sql(sql)
        rows = df.collect() if show else []   # force execution BEFORE declaring success
    except Exception as ex:
        lines = str(ex).strip().splitlines()
        print("  BLOCKED —", lines[0][:300])
        for extra in lines[1:8]:
            print("           ", extra[:300])
        return False
    print("  OK")
    for r in rows:
        print("   ", tuple(r))
    return True

# Check which columns this workspace's system tables actually have, so the queries below
# adapt instead of failing when a column is missing.
RUN_COLS  = set(spark.table("system.lakeflow.job_run_timeline").columns)
JOB_COLS  = set(spark.table("system.lakeflow.jobs").columns)
def col_or_null(c):
    return f"t.{c}" if c in RUN_COLS else "CAST(NULL AS BIGINT)"
print("duration columns present:",
      sorted(c for c in ("run_duration_seconds", "queue_duration_seconds",
                         "execution_duration_seconds") if c in RUN_COLS) or "none")

# The latest definition of each job. system.lakeflow.jobs keeps a row per change,
# so take the most recent one per job_id before joining.
JOB_FILTER = f"AND j.name = '{JOB_NAME}'" if JOB_NAME else ""
latest = ("QUALIFY row_number() OVER (PARTITION BY workspace_id, job_id "
          "ORDER BY change_time DESC) = 1") if "change_time" in JOB_COLS else ""
spark.sql(f"""
CREATE OR REPLACE TEMP VIEW job_names AS
SELECT job_id, name FROM system.lakeflow.jobs {latest}
""")
print("job filter:", JOB_NAME or "(all jobs)",
      "| latest-definition dedup:", "on" if latest else "OFF (no change_time column)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. One row per run — computed two ways
# MAGIC `job_run_timeline` slices a long run into hourly rows, so a run can appear more than once.
# MAGIC Duration is computed from the slices' span, and also read from `run_duration_seconds`,
# MAGIC to see whether the two agree.

# COMMAND ----------

ok = probe("build one row per run", f"""
CREATE OR REPLACE TEMP VIEW runs AS
SELECT t.job_id,
       coalesce(j.name, '(job no longer defined)')           AS job_name,
       t.run_id,
       min(t.period_start_time)                               AS started,
       max(t.period_end_time)                                 AS ended,
       count(*)                                               AS slices,
       timestampdiff(SECOND, min(t.period_start_time),
                             max(t.period_end_time))          AS span_seconds,
       max({col_or_null("run_duration_seconds")})              AS run_duration_seconds,
       max({col_or_null("queue_duration_seconds")})            AS queue_seconds,
       max_by(t.result_state, t.period_end_time)              AS result_state,
       max_by(t.trigger_type, t.period_end_time)              AS trigger_type
FROM system.lakeflow.job_run_timeline t
LEFT JOIN job_names j ON t.job_id = j.job_id
WHERE 1 = 1 {JOB_FILTER}
GROUP BY t.job_id, j.name, t.run_id
""", show=False)

probe("runs per job, and whether the two duration measures agree", """
SELECT job_name,
       count(*)                                                        AS runs,
       sum(CASE WHEN slices > 1 THEN 1 ELSE 0 END)                     AS multi_slice_runs,
       sum(CASE WHEN run_duration_seconds IS NULL THEN 1 ELSE 0 END)   AS no_duration_column,
       sum(CASE WHEN abs(span_seconds - run_duration_seconds) > 5
                THEN 1 ELSE 0 END)                                     AS measures_disagree
FROM runs GROUP BY job_name ORDER BY runs DESC
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. The outcomes that actually occur
# MAGIC `result_state` is `NULL` for a run with no terminal state yet. Any rate must decide what
# MAGIC to do with those rows.

# COMMAND ----------

probe("result_state values and counts", """
SELECT coalesce(result_state, 'NULL') AS result_state, count(*) AS runs
FROM runs GROUP BY result_state ORDER BY runs DESC
""")
probe("trigger types", """
SELECT coalesce(trigger_type, 'NULL') AS trigger_type, count(*) AS runs
FROM runs GROUP BY trigger_type ORDER BY runs DESC
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Failure rate — terminal runs only

# COMMAND ----------

probe("failure rate per job", """
SELECT job_name,
       count(*)                                                  AS terminal_runs,
       sum(CASE WHEN result_state <> 'SUCCEEDED' THEN 1 ELSE 0 END) AS not_succeeded,
       round(100.0 * sum(CASE WHEN result_state <> 'SUCCEEDED' THEN 1 ELSE 0 END)
             / count(*), 1)                                      AS failure_pct
FROM runs
WHERE result_state IS NOT NULL
GROUP BY job_name ORDER BY failure_pct DESC, terminal_runs DESC
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Baseline — and runs that stand out against it
# MAGIC A job's **median** successful duration is its baseline; the median resists the occasional
# MAGIC slow run better than the mean. Any run beyond `SLOW_FACTOR` × its baseline is flagged.

# COMMAND ----------

probe("baseline per job — successful runs", """
SELECT job_name,
       count(*)                                     AS successful_runs,
       round(percentile(span_seconds, 0.5))         AS median_seconds,
       round(percentile(span_seconds, 0.9))         AS p90_seconds,
       max(span_seconds)                            AS max_seconds
FROM runs WHERE result_state = 'SUCCEEDED'
GROUP BY job_name ORDER BY successful_runs DESC
""")

probe(f"runs slower than {SLOW_FACTOR} x their job's median", f"""
WITH base AS (
  SELECT job_id, percentile(span_seconds, 0.5) AS median_seconds
  FROM runs WHERE result_state = 'SUCCEEDED' GROUP BY job_id
)
SELECT r.job_name, r.run_id, r.started, r.span_seconds,
       round(b.median_seconds)                       AS baseline,
       round(r.span_seconds / b.median_seconds, 2)   AS times_baseline,
       r.result_state
FROM runs r JOIN base b ON r.job_id = b.job_id
WHERE b.median_seconds > 0 AND r.span_seconds > {SLOW_FACTOR} * b.median_seconds
ORDER BY times_baseline DESC
LIMIT 15
""")

probe("each job's latest run against its baseline", """
WITH base AS (
  SELECT job_id, percentile(span_seconds, 0.5) AS median_seconds
  FROM runs WHERE result_state = 'SUCCEEDED' GROUP BY job_id
),
latest AS (
  SELECT *, row_number() OVER (PARTITION BY job_id ORDER BY started DESC) AS rn FROM runs
)
SELECT l.job_name, l.started, l.span_seconds, round(b.median_seconds) AS baseline,
       round(l.span_seconds / b.median_seconds, 2) AS times_baseline, l.result_state
FROM latest l LEFT JOIN base b ON l.job_id = b.job_id
WHERE l.rn = 1 ORDER BY l.started DESC
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Which task dominates?
# MAGIC The same question at task level, from `job_task_run_timeline`.

# COMMAND ----------

probe("average task duration, by job and task", f"""
WITH task_runs AS (
  SELECT tt.job_id, tt.task_key, tt.run_id,
         timestampdiff(SECOND, min(tt.period_start_time), max(tt.period_end_time)) AS seconds,
         max_by(tt.result_state, tt.period_end_time) AS result_state
  FROM system.lakeflow.job_task_run_timeline tt
  GROUP BY tt.job_id, tt.task_key, tt.run_id
)
SELECT coalesce(j.name, '(job no longer defined)') AS job_name, task_key,
       count(*) AS task_runs, round(avg(seconds)) AS avg_seconds, max(seconds) AS max_seconds
FROM task_runs tr LEFT JOIN job_names j ON tr.job_id = j.job_id
WHERE 1 = 1 {JOB_FILTER}
GROUP BY j.name, task_key ORDER BY job_name, avg_seconds DESC
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Jobs that no longer exist
# MAGIC Deleting a job removes it from the Jobs UI, but its history stays in the system tables.

# COMMAND ----------

probe("runs whose job has no current definition", """
SELECT job_id, count(*) AS runs, min(started) AS first_run, max(started) AS last_run
FROM runs WHERE job_name = '(job no longer defined)'
GROUP BY job_id ORDER BY last_run DESC
""")
if "delete_time" in JOB_COLS:
    probe("jobs whose latest record says they were deleted", f"""
    SELECT name, delete_time FROM system.lakeflow.jobs
    WHERE delete_time IS NOT NULL {latest}
    ORDER BY delete_time DESC
    """)
else:
    print("\n--- no delete_time column in system.lakeflow.jobs — skipped")

# COMMAND ----------

# MAGIC %md
# MAGIC ## What to check
# MAGIC - **Section 1:** whether `run_duration_seconds` is populated, and whether it agrees with the
# MAGIC   span of each run's timeline slices.
# MAGIC - **Section 2:** which `result_state` values appear — including `NULL` — and which trigger types.
# MAGIC - **Section 3:** each job's failure rate, counting terminal runs only.
# MAGIC - **Section 4:** each job's median and 90th-percentile duration, which runs stand out, and how
# MAGIC   the latest run compares.
# MAGIC - **Section 5:** which task accounts for most of each job's time.
# MAGIC - **Section 6:** whether jobs you deleted still have run history here.
