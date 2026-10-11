# Databricks notebook source
# MAGIC %md
# MAGIC # Ch 2 · Exercise 02 — Auto Loader: incremental file ingestion  [FE]
# MAGIC
# MAGIC Ingest the seeded daily sales files with Auto Loader (`cloudFiles`) into a
# MAGIC bronze streaming table, prove incrementality via the checkpoint, and trigger
# MAGIC schema evolution. Serverless-safe (`availableNow` trigger). Run the repo setup (README → Quickstart)
# MAGIC first.

# COMMAND ----------

# MAGIC %md ## 1. Config + fresh-start reset

# COMMAND ----------

CATALOG, SCHEMA = "workspace", "de_cert"
vol        = f"/Volumes/{CATALOG}/{SCHEMA}/raw"
source     = f"{vol}/sales"                 # the 14 seeded daily files
landing    = f"{vol}/_al_landing"           # files "arrive" here in batches
schema_loc = f"{vol}/_al_schema"
checkpoint = f"{vol}/_al_checkpoint"
bronze     = f"{CATALOG}.{SCHEMA}.sales_bronze_al"

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA}")

# Reset so the whole notebook is re-runnable (checkpoint + schema + landing + table
# are reset together — they're a matched set).
spark.sql(f"DROP TABLE IF EXISTS {bronze}")
for p in (landing, schema_loc, checkpoint):
    dbutils.fs.rm(p, True)
dbutils.fs.mkdirs(landing)
print("bronze table:", bronze)
print("landing:", landing)

# COMMAND ----------

# MAGIC %md ## 2. Stage batch 1 — three daily files "arrive"

# COMMAND ----------

files = sorted(f.path for f in dbutils.fs.ls(source))
def stage(paths):
    names = []
    for p in paths:
        name = p.rstrip("/").split("/")[-1]
        dbutils.fs.cp(p, f"{landing}/{name}")
        names.append(name)
    return names

print("staged:", stage(files[0:3]))
display(dbutils.fs.ls(landing))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Define and run Auto Loader
# MAGIC `inferColumnTypes=true` (real types, not strings); `addNewColumns` evolution;
# MAGIC `_rescued_data` for anything off-schema; `availableNow` (serverless).

# COMMAND ----------

def run_autoloader():
    q = (spark.readStream.format("cloudFiles")
            .option("cloudFiles.format", "json")
            .option("cloudFiles.schemaLocation", schema_loc)
            .option("cloudFiles.inferColumnTypes", "true")
            .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
            .option("rescuedDataColumn", "_rescued_data")
            .load(landing)
            .selectExpr("*",
                        "_metadata.file_path AS _source_file",
                        "current_timestamp() AS _ingested_at")
         .writeStream
            .option("checkpointLocation", checkpoint)
            .option("mergeSchema", "true")   # sink side: let the Delta table accept new columns
            .trigger(availableNow=True)
            .toTable(bronze))
    q.awaitTermination()

run_autoloader()
print("Bronze rows after batch 1:", spark.table(bronze).count())   # ~600

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Batch 2 — three more files arrive; only new ones ingest
# MAGIC The checkpoint remembers batch 1, so the count grows only by the new files.

# COMMAND ----------

print("staged:", stage(files[3:6]))
run_autoloader()
print("Bronze rows after batch 2:", spark.table(bronze).count())   # ~1200

# COMMAND ----------

# MAGIC %md ## 5. Inspect — inferred schema + ingestion metadata

# COMMAND ----------

display(spark.sql(f"DESCRIBE {bronze}"))     # note real types (bigint/double), _rescued_data, metadata cols

# COMMAND ----------

display(spark.sql(f"""
  SELECT transaction_id, transaction_time, amount, _source_file, _ingested_at
  FROM {bronze} LIMIT 10
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Schema evolution — a new column arrives
# MAGIC Drop in a record with an extra field `channel`. Evolution takes **two** things:
# MAGIC - **Source side** `schemaEvolutionMode = addNewColumns`: the first run detects the
# MAGIC   new column, records it, and **fails once** (caught below) — expected, not a bug.
# MAGIC - **Sink side** `.option("mergeSchema","true")` on the write (set in `run_autoloader`):
# MAGIC   lets the Delta table actually add the column. Without it you get
# MAGIC   `DELTA_METADATA_MISMATCH` (and on Free Edition the session-level autoMerge is
# MAGIC   blocked by Table ACLs, so the writer option is required).
# MAGIC
# MAGIC So: first run fails at the source (caught), re-run succeeds with the column added.

# COMMAND ----------

import json
rec = {"transaction_id": 999999, "transaction_time": "2024-01-20 10:00:00",
       "customer_id": 1, "product_id": 1, "quantity": 1,
       "unit_price": 9.99, "amount": 9.99, "channel": "web"}   # <- new field
with open(f"{landing}/sales_evolve.ndjson", "w") as f:
    f.write(json.dumps(rec) + "\n")

try:
    run_autoloader()
except Exception as e:
    print("New column detected (expected with addNewColumns); re-running.")
    print(str(e)[:160])

run_autoloader()   # succeeds with the evolved schema
print("Bronze rows after evolution:", spark.table(bronze).count())

# COMMAND ----------

# MAGIC %sql
# MAGIC -- the new `channel` column now exists (null for earlier rows)
# MAGIC SELECT channel, COUNT(*) AS n
# MAGIC FROM workspace.de_cert.sales_bronze_al
# MAGIC GROUP BY channel;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Cleanup (optional)
# MAGIC ```python
# MAGIC spark.sql(f"DROP TABLE IF EXISTS {bronze}")
# MAGIC for p in (landing, schema_loc, checkpoint): dbutils.fs.rm(p, True)
# MAGIC ```
# MAGIC
# MAGIC **Exam connections:** `cloudFiles` + required `schemaLocation` and
# MAGIC `checkpointLocation`; checkpoint-driven incremental / exactly-once; `inferColumnTypes`
# MAGIC (default false → strings); schema evolution needs **both** the source-side
# MAGIC `schemaEvolutionMode=addNewColumns` **and** the sink-side `mergeSchema` on the write;
# MAGIC `_rescued_data`; `availableNow` on serverless. Auto Loader for continuous/high-volume,
# MAGIC `COPY INTO` for periodic/bounded.

# COMMAND ----------

spark.sql(f"DROP TABLE IF EXISTS {bronze}")
for p in (landing, schema_loc, checkpoint): dbutils.fs.rm(p, True)