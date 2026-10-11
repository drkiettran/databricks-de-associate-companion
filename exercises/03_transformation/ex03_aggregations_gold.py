# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # Ch 3 · Exercise 03 — Aggregations and the First Gold Tables  [FE]
# MAGIC Section 3 (Data Transformation and Modeling). Runs entirely on Free Edition serverless.
# MAGIC
# MAGIC **Goal:** master aggregation semantics (COUNT family, NULL behavior, WHERE vs HAVING),
# MAGIC then complete the medallion by writing three gold tables from `device_telemetry_enriched`.
# MAGIC All data synthetic and notional. Assertions gate every checkpoint.

# COMMAND ----------

# MAGIC %md ## 0. Config + rebuild-if-missing guard

# COMMAND ----------

from pyspark.sql import functions as F, Window

CATALOG, SCHEMA = "workspace", "de_cert"
ENRICHED = f"{CATALOG}.{SCHEMA}.device_telemetry_enriched"
G_REGION = f"{CATALOG}.{SCHEMA}.gold_readings_by_region"
G_HOURLY = f"{CATALOG}.{SCHEMA}.gold_hourly_temp_rollup"
G_STATUS = f"{CATALOG}.{SCHEMA}.gold_device_current_status"
spark.sql(f"USE CATALOG {CATALOG}"); spark.sql(f"USE SCHEMA {SCHEMA}")

if not spark.catalog.tableExists(ENRICHED):
    print("Enriched missing — rebuilding the Exercise 02 result in one step.")
    spark.sql(f"""
      CREATE OR REPLACE TABLE {ENRICHED} AS
      SELECT * FROM VALUES
        ('east','D001',try_to_timestamp('2026-07-01 08:00:00'),CAST(22.0 AS DOUBLE),'ACTIVE','sensor','2025-05-01','US-East'),
        ('UNKNOWN','D002',try_to_timestamp('2026-07-01 08:05:00'),CAST(NULL AS DOUBLE),'ACTIVE','hvac_unit','2025-01-15',CAST(NULL AS STRING)),
        ('west','D003',try_to_timestamp('2026-07-01 08:10:00'),CAST(19.8 AS DOUBLE),'INACTIVE','thermostat','2023-06-30','US-West'),
        ('north','D004',try_to_timestamp('2026-07-01 08:20:00'),CAST(NULL AS DOUBLE),'ACTIVE',CAST(NULL AS STRING),CAST(NULL AS STRING),'US-North'),
        (CAST(NULL AS STRING),'D005',try_to_timestamp('2026-07-01 08:25:00'),CAST(21.1 AS DOUBLE),'ACTIVE',CAST(NULL AS STRING),CAST(NULL AS STRING),CAST(NULL AS STRING))
      AS t(region, device_id, event_ts, temp_c, status, device_type, installed, region_name)""")

e = spark.table(ENRICHED)
assert e.count() == 5
display(e.orderBy("device_id"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. The COUNT family — three different questions
# MAGIC Rows, non-NULL values, distinct values. The gap between the first two IS your NULL count.

# COMMAND ----------

r = e.agg(F.count("*").alias("rows"),
          F.count("temp_c").alias("nonnull_temps"),
          F.countDistinct("status").alias("distinct_status")).first()
print(f"COUNT(*): {r['rows']}   COUNT(temp_c): {r['nonnull_temps']}   COUNT(DISTINCT status): {r['distinct_status']}")
assert (r["rows"], r["nonnull_temps"], r["distinct_status"]) == (5, 3, 2)
print("NULL temps by the idiom COUNT(*)-COUNT(col):", r["rows"] - r["nonnull_temps"])

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*) AS rows, COUNT(temp_c) AS nonnull_temps,
# MAGIC        COUNT(*) - COUNT(temp_c) AS null_temps,
# MAGIC        COUNT(DISTINCT status) AS distinct_status
# MAGIC FROM device_telemetry_enriched;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Aggregates ignore NULLs — and SUM of nothing is NULL, not 0

# COMMAND ----------

r = e.agg(F.avg("temp_c").alias("avg_t"), F.sum("temp_c").alias("sum_t")).first()
print(f"AVG over the 3 non-NULL temps: {r['avg_t']:.3f}   (NOT sum/5)")
assert abs(r["avg_t"] - (22.0 + 19.8 + 21.1) / 3) < 1e-9

allnull = e.filter(F.col("device_id").isin("D002", "D004"))       # both temps NULL
s = allnull.agg(F.sum("temp_c")).first()[0]
print("SUM over an all-NULL group:", s, " -> use COALESCE(SUM(x),0) when a report needs zero")
assert s is None

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. GROUP BY collects NULL keys into ONE group
# MAGIC The third act of the NULL story: filters DROP them, joins NEVER MATCH them,
# MAGIC GROUP BY GROUPS them.

# COMMAND ----------

g = e.groupBy("region").agg(F.count("*").alias("n")).orderBy(F.col("region").asc_nulls_first())
display(g)
null_groups = g.filter(F.col("region").isNull()).collect()
assert len(null_groups) == 1 and null_groups[0]["n"] == 1, "one NULL group, containing D005"
print("The NULL-region row (D005) formed its own single group — it did not vanish.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. WHERE vs HAVING — watch both failures, then the correct shape

# COMMAND ----------

try:
    spark.sql("SELECT region, AVG(temp_c) a FROM device_telemetry_enriched "
              "WHERE AVG(temp_c) > 20 GROUP BY region").collect()
    print("no error?!")
except Exception as ex:
    print("aggregate in WHERE fails (expected):", str(ex).splitlines()[0][:70])

try:
    spark.sql("SELECT temp_c*2 AS doubled FROM device_telemetry_enriched WHERE doubled > 40").collect()
    print("no error?!")
except Exception as ex:
    print("SELECT alias in WHERE fails (expected):", str(ex).splitlines()[0][:60])

# COMMAND ----------

# MAGIC %sql
# MAGIC -- the correct shape: row filter -> group -> group filter -> sort (alias fine in ORDER BY)
# MAGIC SELECT region, AVG(temp_c) AS avg_t, COUNT(*) AS n
# MAGIC FROM   device_telemetry_enriched
# MAGIC WHERE  status = 'ACTIVE'
# MAGIC GROUP  BY region
# MAGIC HAVING AVG(temp_c) > 20
# MAGIC ORDER  BY avg_t DESC;

# COMMAND ----------

# MAGIC %md ## 5. ANSI arithmetic: division by zero throws; try_divide is the habit

# COMMAND ----------

try:
    spark.sql("SELECT 1/0").collect()
    print("no error?!")
except Exception as ex:
    print("1/0 under ANSI (expected):", str(ex).splitlines()[0][:55])
print("try_divide(1,0) ->", spark.sql("SELECT try_divide(1,0) AS r").first()["r"])

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Gold table 1 — readings by region (dimensional aggregate)
# MAGIC COUNT(*) and COUNT(temp_c) side by side: gold carries its own data-quality visibility.

# COMMAND ----------

(e.groupBy("region", "region_name")
   .agg(F.count("*").alias("reading_count"),
        F.count("temp_c").alias("temp_count"),
        F.avg("temp_c").alias("avg_temp_c"),
        F.min("temp_c").alias("min_temp_c"),
        F.max("temp_c").alias("max_temp_c"),
        F.max("event_ts").alias("latest_reading_ts"))
   .write.mode("overwrite").saveAsTable(G_REGION))
gr = spark.table(G_REGION)
assert gr.count() == 5, "five region groups, including the NULL-region group"
assert gr.filter(F.col("region").isNull()).count() == 1
display(gr.orderBy(F.col("region").asc_nulls_first()))

# COMMAND ----------

# MAGIC %md ## 7. Gold table 2 — hourly rollup (time-series aggregate)

# COMMAND ----------

(e.groupBy(F.date_trunc("hour", "event_ts").alias("hour"), "region")
   .agg(F.avg("temp_c").alias("avg_temp_c"),
        F.count("*").alias("reading_count"))
   .write.mode("overwrite").saveAsTable(G_HOURLY))
gh = spark.table(G_HOURLY)
assert gh.count() == 5, "all readings share one hour: one row per (hour, region)"
display(gh.orderBy("region"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. Gold table 3 — current device status (snapshot)
# MAGIC The Exercise 01 latest-per-key window pattern, third appearance — plus when/otherwise banding.
# MAGIC Note where the NULL temps land: they fail every condition and take the fallback.

# COMMAND ----------

w = Window.partitionBy("device_id").orderBy(F.col("event_ts").desc())
(e.withColumn("rn", F.row_number().over(w)).filter(F.col("rn") == 1).drop("rn")
   .withColumn("temp_band",
       F.when(F.col("temp_c") >= 21.0, "warm")
        .when(F.col("temp_c") >= 19.5, "mild")
        .otherwise("unknown"))
   .select("device_id", "status", "temp_c", "temp_band", "event_ts", "device_type")
   .write.mode("overwrite").saveAsTable(G_STATUS))
gs = spark.table(G_STATUS)
assert gs.count() == 5
bands = {r["temp_band"]: r["n"] for r in gs.groupBy("temp_band").agg(F.count("*").alias("n")).collect()}
print("bands:", bands)
assert bands == {"warm": 2, "mild": 1, "unknown": 2}, "NULL temps land in otherwise, not dropped"
display(gs.orderBy("device_id"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 9. The medallion census — the whole pipeline as data at rest

# COMMAND ----------

census = [
    ("bronze", "device_telemetry_bronze"),
    ("silver", "device_telemetry_silver"),
    ("silver (enriched)", "device_telemetry_enriched"),
    ("gold", "gold_readings_by_region"),
    ("gold", "gold_hourly_temp_rollup"),
    ("gold", "gold_device_current_status"),
]
print(f"{'layer':<18} {'table':<32} rows")
for layer, name in census:
    fq = f"{CATALOG}.{SCHEMA}.{name}"
    n = spark.table(fq).count() if spark.catalog.tableExists(fq) else "(missing)"
    print(f"{layer:<18} {name:<32} {n}")
print("\nOne bronze -> one cleaned silver -> one enriched silver -> three shaped golds.")
print("Cleaning happened ONCE; shaping happened PER CONSUMER. That is the medallion.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## What to check
# MAGIC - The census output (or confirmation all assertions passed) + anything surprising.
# MAGIC
# MAGIC Next: **ex04_gold_objects_quality_tuning** — gold object types, data quality, and
# MAGIC tuning (shuffle.partitions hands-on; broadcast conceptual).