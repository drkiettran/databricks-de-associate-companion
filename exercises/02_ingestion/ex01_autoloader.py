# Databricks notebook source
# MAGIC %md
# MAGIC # Ch 2 · Exercise 01 — Auto Loader file-drop to a bronze streaming table  [FE]
# MAGIC
# MAGIC **Goal:** ingest daily `sales` files from a Unity Catalog Volume into a
# MAGIC bronze Delta table using Auto Loader, then prove it is *incremental* —
# MAGIC a second run picks up only newly arrived files.
# MAGIC
# MAGIC Runs on Free Edition (serverless). The only serverless-specific detail is
# MAGIC the trigger: `availableNow=True` (time-based/continuous triggers are not
# MAGIC supported on serverless).

# COMMAND ----------
# MAGIC %md ## 1. Setup — catalog, schema, volume, and generated data

# COMMAND ----------
import sys, os
# Make the repo's common/ importable regardless of where the notebook lives.
sys.path.append(os.path.abspath(os.path.join(os.getcwd(), "..", "..")))
from common.config import CATALOG, SCHEMA, VOLUME, volume_path, table_name

spark.sql(f"CREATE CATALOG IF NOT EXISTS {CATALOG}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA}")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {CATALOG}.{SCHEMA}.{VOLUME}")

incoming    = volume_path("incoming")   # where sales files "arrive"
schema_loc  = volume_path("_schema/sales")
checkpoint  = volume_path("_checkpoints/sales_bronze")
bronze      = table_name("sales_bronze")

dbutils.fs.mkdirs(incoming)
print("incoming:", incoming)
print("bronze table:", bronze)

# COMMAND ----------
# MAGIC %md
# MAGIC ## 2. Generate data, then stage the first two "days" as file drops
# MAGIC Run the generator once (writes daily files under `.../raw/sales/`), then
# MAGIC copy day 1 and day 2 into `incoming/`.

# COMMAND ----------
# MAGIC %sh
# MAGIC # Note: --outdir must match your CATALOG/SCHEMA/VOLUME (defaults shown).
# MAGIC python ../../data/generate_data.py --outdir /Volumes/main/companion/raw --days 14

# COMMAND ----------
sales_src = volume_path("sales")          # /Volumes/<cat>/<schema>/<volume>/sales
files = [f.path for f in dbutils.fs.ls(sales_src)]
for f in sorted(files)[:2]:               # stage the first two daily files
    name = f.rstrip("/").split("/")[-1]
    dbutils.fs.cp(f, f"{incoming}/{name}")
display(dbutils.fs.ls(incoming))

# COMMAND ----------
# MAGIC %md ## 3. First Auto Loader run — ingest what's arrived

# COMMAND ----------
def run_autoloader():
    (spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("cloudFiles.schemaLocation", schema_loc)
        .load(incoming)
     .writeStream
        .option("checkpointLocation", checkpoint)
        .trigger(availableNow=True)      # required on serverless
        .toTable(bronze))
    # availableNow processes all available data then stops; wait for it.
    for q in spark.streams.active:
        q.awaitTermination()

run_autoloader()
print("rows after run 1:", spark.table(bronze).count())

# COMMAND ----------
# MAGIC %md
# MAGIC ## 4. Drop more files, run again — only the new files are ingested
# MAGIC The checkpoint remembers what was already processed, so the row count
# MAGIC grows only by the newly staged files.

# COMMAND ----------
for f in sorted(files)[2:5]:              # stage three more daily files
    name = f.rstrip("/").split("/")[-1]
    dbutils.fs.cp(f, f"{incoming}/{name}")

run_autoloader()
print("rows after run 2:", spark.table(bronze).count())

# COMMAND ----------
# MAGIC %md
# MAGIC ## 5. Verify
# MAGIC You should see the count increase by exactly the three new files' rows.
# MAGIC Inspect the bronze table and confirm Auto Loader inferred the schema.

# COMMAND ----------
display(spark.sql(f"DESCRIBE TABLE {bronze}"))
display(spark.sql(f"SELECT transaction_time, customer_id, amount FROM {bronze} LIMIT 10"))

# COMMAND ----------
# MAGIC %md
# MAGIC ### Cleanup (optional)
# MAGIC ```python
# MAGIC spark.sql(f"DROP TABLE IF EXISTS {bronze}")
# MAGIC dbutils.fs.rm(checkpoint, True); dbutils.fs.rm(schema_loc, True)
# MAGIC dbutils.fs.rm(incoming, True)
# MAGIC ```
# MAGIC
# MAGIC **Exam connections:** Auto Loader `cloudFiles` + `schemaLocation`;
# MAGIC checkpoint-driven incremental/idempotent ingestion; the serverless
# MAGIC `availableNow` trigger requirement.
