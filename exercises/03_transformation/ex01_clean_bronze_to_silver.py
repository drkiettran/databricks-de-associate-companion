# Databricks notebook source
# MAGIC %md
# MAGIC # Ch 3 · Exercise 01 — Bronze to Silver Cleaning  [FE]
# MAGIC Section 3 (Data Transformation and Modeling). Runs entirely on Free Edition serverless.
# MAGIC
# MAGIC **Goal:** turn a dirty synthetic `device_telemetry_bronze` table into a clean, deduplicated
# MAGIC `device_telemetry_silver` table. Every step is shown in **both** PySpark and Spark SQL.
# MAGIC
# MAGIC All data is **synthetic and notional** — generated in-notebook (no external download, so the
# MAGIC serverless egress block is irrelevant). Nothing here represents any real system or records.
# MAGIC
# MAGIC **Expected result: 8 bronze rows -> 4 silver rows.**

# COMMAND ----------

# MAGIC %md ## 0. Config — point these at your DE cert schema

# COMMAND ----------

CATALOG = "workspace"      # Free Edition default catalog
SCHEMA  = "de_cert"        # change if you named your schema differently
BRONZE  = f"{CATALOG}.{SCHEMA}.device_telemetry_bronze"
SILVER  = f"{CATALOG}.{SCHEMA}.device_telemetry_silver"

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA}")
spark.sql(f"USE CATALOG {CATALOG}")
spark.sql(f"USE SCHEMA {SCHEMA}")
print("Writing to:", BRONZE, "->", SILVER)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Generate the dirty bronze table (synthetic)
# MAGIC Every row below plants a specific kind of dirt. Read the trailing comments — each one maps
# MAGIC to a cleaning rule you will apply later.

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType

# All columns land as STRING, exactly as a raw/bronze ingestion typically would.
schema = StructType([
    StructField("device_id",  StringType()),
    StructField("event_ts",   StringType()),
    StructField("temp_c",     StringType()),
    StructField("status",     StringType()),
    StructField("region",     StringType()),
    StructField("_ingest_ts", StringType()),
])

rows = [
    ("D001", "2026-07-01 08:00:00", "21.5", "  active ", "east",  "2026-07-01 08:00:05"),  # baseline
    ("D001", "2026-07-01 08:00:00", "21.5", "  active ", "east",  "2026-07-01 08:00:05"),  # EXACT duplicate
    ("D001", "2026-07-01 08:00:00", "22.0", "ACTIVE",    "east",  "2026-07-01 09:10:00"),  # dup KEY, later ingest, corrected temp
    ("D002", "2026-07-01 08:05:00", "N/A",  "Active",    None,    "2026-07-01 08:05:03"),  # malformed temp + null region
    ("D003", "2026-07-01 08:10:00", "19.8", "inactive",  "west",  "2026-07-01 08:10:04"),  # baseline
    ("D003", None,                  "20.1", "inactive",  "west",  "2026-07-01 08:11:00"),  # NULL key (event_ts missing)
    ("UNKNOWN","2026-07-01 08:15:00","0.0", "TEST",      "n/a",   "2026-07-01 08:15:00"),  # junk sentinel row
    ("D004", "2026-07-01 08:20:00", "",     "active",    "north", "2026-07-01 08:20:02"),  # empty-string temp
]

bronze_df = spark.createDataFrame(rows, schema)
bronze_df.write.mode("overwrite").saveAsTable(BRONZE)
print("BRONZE rows:", spark.table(BRONZE).count())     # expect 8
display(spark.table(BRONZE))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. The ANSI trap — watch a strict cast FAIL, then fix it
# MAGIC On Free Edition serverless, `spark.sql.ansi.enabled` is **true**, so a strict cast of a
# MAGIC malformed string throws instead of returning NULL. Run this cell and read the error.

# COMMAND ----------

print("ANSI enabled?", spark.conf.get("spark.sql.ansi.enabled"))   # expect: true on FE serverless

from pyspark.sql import functions as F

# --- strict cast: this SHOULD blow up on 'N/A' and '' ---
try:
    spark.table(BRONZE).withColumn("temp_c", F.col("temp_c").cast("double")).collect()
    print("No error (are you on a classic cluster with ANSI off?)")
except Exception as e:
    print("STRICT CAST FAILED (expected on FE):", str(e).splitlines()[0])

# --- tolerant cast: 'N/A' and '' become NULL, no crash ---
ok = spark.table(BRONZE).withColumn("temp_c", F.expr("try_cast(temp_c AS double)"))
print("try_cast nulls:", ok.filter(F.col("temp_c").isNull()).count(), "(from 'N/A' and '')")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Build silver — PySpark path
# MAGIC Cast (tolerant) -> standardize -> drop junk -> null-handle -> dedup latest-per-key -> done.

# COMMAND ----------

from pyspark.sql import Window

silver_df = (spark.table(BRONZE)
    # 3a. tolerant type casting
    .withColumn("temp_c",     F.expr("try_cast(temp_c AS double)"))
    .withColumn("event_ts",   F.try_to_timestamp("event_ts"))
    .withColumn("_ingest_ts", F.try_to_timestamp("_ingest_ts"))
    # 3b. standardize + drop junk sentinel rows
    .withColumn("status", F.upper(F.trim("status")))
    .filter(~F.col("status").isin("TEST"))
    .filter(F.col("device_id") != "UNKNOWN")
    # 3c. null handling: keys must be present; default the region
    .na.drop(subset=["device_id", "event_ts"])
    .withColumn("region", F.coalesce(F.col("region"), F.lit("UNKNOWN")))
)

# 3d. dedup: keep the LATEST record per (device_id, event_ts)
w = Window.partitionBy("device_id", "event_ts").orderBy(F.col("_ingest_ts").desc())
silver_df = (silver_df
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .drop("rn"))

print("SILVER rows (PySpark):", silver_df.count())     # expect 4
display(silver_df.orderBy("device_id", "event_ts"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Build silver — Spark SQL path (parity check)
# MAGIC Same result, expressed in SQL. Confirm the count matches Section 3.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMP VIEW silver_sql AS
# MAGIC WITH normed AS (
# MAGIC   SELECT device_id,
# MAGIC          try_to_timestamp(event_ts)   AS event_ts,
# MAGIC          try_cast(temp_c AS DOUBLE)    AS temp_c,
# MAGIC          UPPER(TRIM(status))          AS status,
# MAGIC          COALESCE(region, 'UNKNOWN')  AS region,
# MAGIC          try_to_timestamp(_ingest_ts) AS _ingest_ts
# MAGIC   FROM   device_telemetry_bronze
# MAGIC   WHERE  UPPER(TRIM(status)) <> 'TEST' AND device_id <> 'UNKNOWN'
# MAGIC ),
# MAGIC keyed AS (
# MAGIC   SELECT *,
# MAGIC          ROW_NUMBER() OVER (PARTITION BY device_id, event_ts
# MAGIC                             ORDER BY _ingest_ts DESC) AS rn
# MAGIC   FROM   normed
# MAGIC   WHERE  device_id IS NOT NULL AND event_ts IS NOT NULL
# MAGIC )
# MAGIC SELECT * EXCEPT(rn) FROM keyed WHERE rn = 1;
# MAGIC SELECT COUNT(*) AS silver_rows_sql FROM silver_sql;   -- expect 4

# COMMAND ----------

# MAGIC %md ## 5. Persist silver as a managed Delta table (CTAS-equivalent)

# COMMAND ----------

silver_df.write.mode("overwrite").saveAsTable(SILVER)
print("Wrote", SILVER)
print("Final SILVER count:", spark.table(SILVER).count())    # expect 4
display(spark.table(SILVER).orderBy("device_id", "event_ts"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Validate — what should be true of silver
# MAGIC If any assertion fails, a cleaning rule misfired. Note which one in your report.

# COMMAND ----------

s = spark.table(SILVER)
assert s.count() == 4,                                              "row count should be 4"
assert s.filter(F.col("device_id").isNull()).count() == 0,         "no null device_id"
assert s.filter(F.col("event_ts").isNull()).count() == 0,          "no null event_ts"
assert s.filter(F.col("status") == "TEST").count() == 0,           "no junk sentinel survived"
assert s.filter(F.col("region").isNull()).count() == 0,            "region defaulted"
# D001 should carry the LATER correction (22.0), not the original 21.5
d001 = s.filter(F.col("device_id") == "D001").select("temp_c").first()[0]
assert d001 == 22.0,                                               "dedup kept the latest D001"
print("All silver validations passed. 8 bronze -> 4 silver, latest-per-key honored.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## What to check
# MAGIC - Silver row count (should be 4) and anything surprising.
# MAGIC - Did the strict-cast cell in Section 2 actually throw? (confirms ANSI is on for your FE)
# MAGIC
# MAGIC Next: **ex02_joins_silver_enrichment** — joins and set operations.
