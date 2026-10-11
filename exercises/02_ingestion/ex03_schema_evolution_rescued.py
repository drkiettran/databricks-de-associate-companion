# Databricks notebook source
# MAGIC %md
# MAGIC # Ch 2 · Exercise 03 — Evolution modes, rescued data, metadata  [FE]
# MAGIC
# MAGIC Exercise 02 (Auto Loader) used `addNewColumns` (new columns get **added**). Here we use **`rescue`**
# MAGIC (new columns get **captured in `_rescued_data`** instead), see rescued data from a
# MAGIC **type mismatch**, and inspect the full `_metadata` struct. Run the repo setup (README → Quickstart) first.

# COMMAND ----------

# MAGIC %md ## 1. Config + fresh-start reset

# COMMAND ----------

CATALOG, SCHEMA = "workspace", "de_cert"
vol        = f"/Volumes/{CATALOG}/{SCHEMA}/raw"
source     = f"{vol}/sales"
landing    = f"{vol}/_al4_landing"
schema_loc = f"{vol}/_al4_schema"
checkpoint = f"{vol}/_al4_checkpoint"
bronze     = f"{CATALOG}.{SCHEMA}.sales_bronze_rescue"

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA}")
spark.sql(f"DROP TABLE IF EXISTS {bronze}")
for p in (landing, schema_loc, checkpoint):
    dbutils.fs.rm(p, True)
dbutils.fs.mkdirs(landing)
print("bronze table:", bronze)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Ingest in RESCUE mode
# MAGIC `schemaEvolutionMode = rescue`: the schema never grows and the stream never fails
# MAGIC on new columns. Note there's **no** `mergeSchema` on the write — rescue mode never
# MAGIC adds columns, so the sink schema is stable.

# COMMAND ----------

def run_rescue():
    q = (spark.readStream.format("cloudFiles")
            .option("cloudFiles.format", "json")
            .option("cloudFiles.schemaLocation", schema_loc)
            .option("cloudFiles.inferColumnTypes", "true")
            .option("cloudFiles.schemaEvolutionMode", "rescue")
            .option("rescuedDataColumn", "_rescued_data")
            .load(landing)
            .selectExpr("*",
                        "_metadata.file_path AS _source_file",
                        "current_timestamp() AS _ingested_at")
         .writeStream
            .option("checkpointLocation", checkpoint)
            .trigger(availableNow=True)
            .toTable(bronze))
    q.awaitTermination()

# stage two base files to establish the schema
files = sorted(f.path for f in dbutils.fs.ls(source))
for p in files[0:2]:
    dbutils.fs.cp(p, f"{landing}/{p.rstrip('/').split('/')[-1]}")

run_rescue()
print("Rows after base load:", spark.table(bronze).count())   # ~400
display(spark.sql(f"DESCRIBE {bronze}"))                        # note _rescued_data column

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. A new column arrives — RESCUE captures it (does NOT add it)
# MAGIC Contrast with Exercise 02: under `rescue` the stream does not fail, no `channel` column
# MAGIC is added, and the `channel` values land in `_rescued_data`.

# COMMAND ----------

import json
rec = {"transaction_id": 900001, "transaction_time": "2024-01-21 09:00:00",
       "customer_id": 5, "product_id": 5, "quantity": 2,
       "unit_price": 9.99, "amount": 19.98, "channel": "web"}   # new field
with open(f"{landing}/evolve_channel.ndjson", "w") as f:
    f.write(json.dumps(rec) + "\n")

run_rescue()   # no failure, no re-run needed

print("Columns (note: NO 'channel' column):", spark.table(bronze).columns)

# COMMAND ----------

# MAGIC %sql
# MAGIC -- the channel value is captured in _rescued_data instead
# MAGIC SELECT _source_file, _rescued_data
# MAGIC FROM workspace.de_cert.sales_bronze_rescue
# MAGIC WHERE _rescued_data IS NOT NULL;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. A type mismatch — also rescued
# MAGIC A record whose `quantity` is a string can't fit the `BIGINT` column: that row's
# MAGIC `quantity` becomes `NULL` and the original value is preserved in `_rescued_data`.

# COMMAND ----------

bad = {"transaction_id": 900002, "transaction_time": "2024-01-21 10:00:00",
       "customer_id": 6, "product_id": 6, "quantity": "lots",   # <- string in a BIGINT column
       "unit_price": 4.99, "amount": 4.99}
with open(f"{landing}/bad_quantity.ndjson", "w") as f:
    f.write(json.dumps(bad) + "\n")

run_rescue()

# COMMAND ----------

# MAGIC %sql
# MAGIC -- quantity is NULL for the bad row; the raw value is in _rescued_data
# MAGIC SELECT transaction_id, quantity, _rescued_data
# MAGIC FROM workspace.de_cert.sales_bronze_rescue
# MAGIC WHERE transaction_id IN (900001, 900002);

# COMMAND ----------

# MAGIC %md ## 5. The full `_metadata` struct (ingestion lineage)

# COMMAND ----------

display(
    spark.read.format("json").load(landing)
    .selectExpr("_metadata.file_name",
                "_metadata.file_size",
                "_metadata.file_modification_time")
    .limit(10)
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. The four evolution modes at a glance
# MAGIC
# MAGIC | Mode | New column | Stream fails? | Use when |
# MAGIC |---|---|---|---|
# MAGIC | `addNewColumns` (default*) | added to schema | yes, once, then succeeds | schema should grow with the source |
# MAGIC | `rescue` | captured in `_rescued_data` | no | stable schema, don't lose data |
# MAGIC | `failOnNewColumns` | not added | yes, stays failed | schema changes must be reviewed |
# MAGIC | `none` | ignored / dropped | no | pin schema, ignore extras |
# MAGIC
# MAGIC \*Default is `addNewColumns` when a `schemaLocation` is set and no schema is given;
# MAGIC with an explicit schema and no schema location, the default is `none`.
# MAGIC
# MAGIC `failOnNewColumns` isn't demoed here because it fails hard and stays failed until
# MAGIC you update the schema — swap the mode in `run_rescue` (with a fresh
# MAGIC checkpoint/schema location) if you want to see it.
# MAGIC
# MAGIC ### Cleanup (optional)
# MAGIC ```python
# MAGIC spark.sql(f"DROP TABLE IF EXISTS {bronze}")
# MAGIC for p in (landing, schema_loc, checkpoint): dbutils.fs.rm(p, True)
# MAGIC ```
# MAGIC
# MAGIC **Exam connections:** four `schemaEvolutionMode` values and their behavior;
# MAGIC `_rescued_data` captures **both** off-schema columns and type-mismatched values;
# MAGIC `_metadata` (file_path/name/size/modification_time) for lineage.

# COMMAND ----------

spark.sql(f"DROP TABLE IF EXISTS {bronze}")
for p in (landing, schema_loc, checkpoint): dbutils.fs.rm(p, True)