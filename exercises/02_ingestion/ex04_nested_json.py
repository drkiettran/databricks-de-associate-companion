# Databricks notebook source
# MAGIC %md
# MAGIC # Ch 2 · Exercise 04 — Semi-structured / nested JSON  [FE]
# MAGIC
# MAGIC Ingest `order_events` (nested `address` struct + `items` array), then flatten it:
# MAGIC dot-access structs, `explode` arrays, and parse JSON stored as a string. Closes
# MAGIC out Section 2. Run the repo setup (README → Quickstart) first.

# COMMAND ----------

# MAGIC %md ## 1. Config + fresh-start reset

# COMMAND ----------

CATALOG, SCHEMA = "workspace", "de_cert"
vol        = f"/Volumes/{CATALOG}/{SCHEMA}/raw"
landing    = f"{vol}/_al5_landing"
schema_loc = f"{vol}/_al5_schema"
checkpoint = f"{vol}/_al5_checkpoint"
bronze     = f"{CATALOG}.{SCHEMA}.events_bronze"
silver     = f"{CATALOG}.{SCHEMA}.events_silver"

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA}")
spark.sql(f"DROP TABLE IF EXISTS {bronze}")
spark.sql(f"DROP TABLE IF EXISTS {silver}")
for p in (landing, schema_loc, checkpoint):
    dbutils.fs.rm(p, True)
dbutils.fs.mkdirs(landing)

# stage the nested-JSON file into the landing dir
dbutils.fs.cp(f"{vol}/order_events.ndjson", f"{landing}/order_events.ndjson")
print("bronze:", bronze)

# COMMAND ----------

# MAGIC %md ## 2. Ingest to bronze (nesting preserved) and inspect the schema

# COMMAND ----------

q = (spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("cloudFiles.schemaLocation", schema_loc)
        .option("cloudFiles.inferColumnTypes", "true")
        .load(landing)
        .selectExpr("*", "_metadata.file_path AS _source_file")
     .writeStream
        .option("checkpointLocation", checkpoint)
        .trigger(availableNow=True)
        .toTable(bronze))
q.awaitTermination()

print("Event rows:", spark.table(bronze).count())
spark.table(bronze).printSchema()   # note: address = struct, items = array<struct>

# COMMAND ----------

# MAGIC %md ## 3. Access nested struct fields — dot notation

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT event_id, channel, address.city, address.state, total
# MAGIC FROM workspace.de_cert.events_bronze
# MAGIC LIMIT 10;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Flatten the `items` array — `explode`
# MAGIC One row per line item; then dot-access the element's fields.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT event_id, item.product_id, item.qty, item.price
# MAGIC FROM workspace.de_cert.events_bronze
# MAGIC LATERAL VIEW explode(items) exploded AS item
# MAGIC LIMIT 10;

# COMMAND ----------

# MAGIC %md ## 5. Full flatten to a tidy silver table (one row per line item)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE workspace.de_cert.events_silver AS
# MAGIC SELECT event_id, event_time, customer_id, channel,
# MAGIC        address.city  AS city,
# MAGIC        address.state AS state,
# MAGIC        item.product_id, item.qty, item.price
# MAGIC FROM workspace.de_cert.events_bronze
# MAGIC LATERAL VIEW explode(items) exploded AS item;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*) AS line_items,
# MAGIC        COUNT(DISTINCT event_id) AS events
# MAGIC FROM workspace.de_cert.events_silver;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. explode vs explode_outer
# MAGIC `explode` **drops** rows whose array is null/empty; `explode_outer` **keeps** them
# MAGIC (with a NULL element). Here every event has items, so counts match — but on real
# MAGIC data with empty arrays they differ, and `explode_outer` is what avoids losing rows.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   (SELECT COUNT(*) FROM workspace.de_cert.events_bronze LATERAL VIEW explode(items)       e AS i) AS with_explode,
# MAGIC   (SELECT COUNT(*) FROM workspace.de_cert.events_bronze LATERAL VIEW explode_outer(items) e AS i) AS with_explode_outer;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Parsing JSON stored as a STRING
# MAGIC When JSON lives in a string column (not natively parsed), use `from_json` /
# MAGIC `schema_of_json` / `get_json_object`.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   from_json('{"city":"Reston","state":"VA"}', 'city STRING, state STRING') AS parsed_struct,
# MAGIC   get_json_object('{"a":{"b":42}}', '$.a.b')                                AS single_value,
# MAGIC   schema_of_json('{"x":1,"y":"z"}')                                         AS inferred_schema;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Cleanup (optional)
# MAGIC ```python
# MAGIC spark.sql(f"DROP TABLE IF EXISTS {bronze}"); spark.sql(f"DROP TABLE IF EXISTS {silver}")
# MAGIC for p in (landing, schema_loc, checkpoint): dbutils.fs.rm(p, True)
# MAGIC ```
# MAGIC
# MAGIC **Exam connections:** native JSON reads produce structs/arrays; dot notation for
# MAGIC structs; `explode` (drops empty) vs `explode_outer` (keeps) for arrays; explode
# MAGIC before dot-accessing array-of-struct fields; `from_json`/`schema_of_json`/
# MAGIC `get_json_object` are for JSON stored as a **string**.

# COMMAND ----------

spark.sql(f"DROP TABLE IF EXISTS {bronze}"); spark.sql(f"DROP TABLE IF EXISTS {silver}")
for p in (landing, schema_loc, checkpoint): dbutils.fs.rm(p, True)