# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # Ch 4 · Exercise 07 — `file_watch`  [FE]
# MAGIC A deliberately tiny task for trigger experiments. It reports what is in the
# MAGIC watched folder and when it ran — enough to prove a trigger fired, cheap enough
# MAGIC that an accidental extra run costs seconds.
# MAGIC
# MAGIC Used by the throwaway job `de_cert_trigger_lab`, NOT by `de_cert_week3_jobs`.

# COMMAND ----------

from datetime import datetime, timezone

CATALOG, SCHEMA = "workspace", "de_cert"
WATCH = f"/Volumes/{CATALOG}/{SCHEMA}/raw/_trigger_lab"

# COMMAND ----------

# Create the watched folder if it isn't there yet (idempotent).
try:
    dbutils.fs.mkdirs(WATCH)
except Exception as ex:
    print("mkdirs note:", str(ex).splitlines()[0][:80])

now = datetime.now(timezone.utc).isoformat(timespec="seconds")
try:
    files = [f.name for f in dbutils.fs.ls(WATCH)]
except Exception:
    files = []

print(f"=== file_watch ran at {now} UTC ===")
print(f"watching: {WATCH}")
print(f"files present: {len(files)}")
for f in sorted(files):
    print(f"  - {f}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Reading the result
# MAGIC - **Manual run:** baseline — note the timestamp.
# MAGIC - **Cron run:** the run's `Launched` field reads **Scheduled** instead of Manually.
# MAGIC - **File-arrival run:** the new file appears in the list above, and the run
# MAGIC   started on its own shortly after the file landed.
# MAGIC
# MAGIC A trigger changes **what starts the run** — never what the task does.
