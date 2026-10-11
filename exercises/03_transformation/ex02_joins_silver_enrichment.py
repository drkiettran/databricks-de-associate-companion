# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # Ch 3 · Exercise 02 — Joins and Set Operations  [FE]
# MAGIC Section 3 (Data Transformation and Modeling). Runs entirely on Free Edition serverless.
# MAGIC
# MAGIC **Goal:** enrich `device_telemetry_silver` against two synthetic reference tables,
# MAGIC exercising every exam-named join type and the union family, in **both** PySpark and SQL.
# MAGIC All data is synthetic and notional. Expected final output: **5 enriched rows**.

# COMMAND ----------

# MAGIC %md ## 0. Config

# COMMAND ----------

CATALOG, SCHEMA = "workspace", "de_cert"
SILVER   = f"{CATALOG}.{SCHEMA}.device_telemetry_silver"
META     = f"{CATALOG}.{SCHEMA}.device_metadata"
REGIONS  = f"{CATALOG}.{SCHEMA}.region_lookup"
ENRICHED = f"{CATALOG}.{SCHEMA}.device_telemetry_enriched"
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA}")
spark.sql(f"USE CATALOG {CATALOG}"); spark.sql(f"USE SCHEMA {SCHEMA}")

# COMMAND ----------

# MAGIC %md ## 1. Verify (or rebuild) Exercise 01's silver, then add a NULL-region row

# COMMAND ----------

from pyspark.sql import functions as F, Window

if not spark.catalog.tableExists(SILVER):
    print("Silver missing — rebuilding the Exercise 01 result in one step.")
    rows = [("D001","2026-07-01 08:00:00","22.0","ACTIVE","east"),
            ("D002","2026-07-01 08:05:00",None,  "ACTIVE","UNKNOWN"),
            ("D003","2026-07-01 08:10:00","19.8","INACTIVE","west"),
            ("D004","2026-07-01 08:20:00",None,  "ACTIVE","north")]
    (spark.createDataFrame(rows, ["device_id","event_ts","temp_c","status","region"])
        .withColumn("temp_c", F.expr("try_cast(temp_c AS double)"))
        .withColumn("event_ts", F.try_to_timestamp("event_ts"))
        .write.mode("overwrite").saveAsTable(SILVER))

# Add D005 with a NULL region (idempotent: only if absent) — today's null-join specimen
if spark.table(SILVER).filter(F.col("device_id")=="D005").count() == 0:
    d005 = spark.createDataFrame(
        [("D005","2026-07-01 08:25:00",21.1,"ACTIVE",None)],
        schema=spark.table(SILVER).select("device_id","event_ts","temp_c","status","region").schema
    ) if False else spark.sql(
        "SELECT 'D005' device_id, try_to_timestamp('2026-07-01 08:25:00') event_ts, "
        "CAST(21.1 AS DOUBLE) temp_c, 'ACTIVE' status, CAST(NULL AS STRING) region")
    d005.write.mode("append").saveAsTable(SILVER)

silver = spark.table(SILVER).select("device_id","event_ts","temp_c","status","region")
print("SILVER rows:", silver.count())      # expect 5
display(silver.orderBy("device_id"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Reference tables — with two deliberate landmines
# MAGIC `device_metadata`: one device with NO telemetry (D777) and one **duplicate key** (D001 twice).
# MAGIC `region_lookup`: includes a **NULL-region row** — bait for the null-join lesson.

# COMMAND ----------

meta_rows = [
    ("D001","thermostat","2024-11-02"),
    ("D001","sensor",    "2025-05-01"),          # DUPLICATE key — newer record
    ("D002","hvac_unit", "2025-01-15"),
    ("D003","thermostat","2023-06-30"),
    ("D777","legacy",    "2020-01-01"),          # no telemetry for this device
]
spark.createDataFrame(meta_rows, ["device_id","device_type","installed"]) \
     .write.mode("overwrite").saveAsTable(META)

region_rows = [("east","US-East"),("west","US-West"),("north","US-North"),(None,"NO-REGION")]
spark.createDataFrame(region_rows, ["region","region_name"]) \
     .write.mode("overwrite").saveAsTable(REGIONS)
print("META rows:", spark.table(META).count(), "| REGIONS rows:", spark.table(REGIONS).count())

# COMMAND ----------

# MAGIC %md ## 3. Join types — checkpoint counts after each
# MAGIC Metadata has a duplicate D001 on purpose; watch what it does to each join.

# COMMAND ----------

meta = spark.table(META)

inner = silver.join(meta, "device_id", "inner")
print("inner :", inner.count(), " (expect 4 = D001 x2 explosion + D002 + D003; D004/D005/D777 gone)")

left = silver.join(meta, "device_id", "left")
print("left  :", left.count(), " (expect 6 = 5 kept + D001 doubled; D004/D005 carry NULL device_type)")

semi = silver.join(meta, "device_id", "left_semi")
print("semi  :", semi.count(), " cols:", len(semi.columns), " (expect 3 rows, 5 cols — filter, no explosion, no meta cols)")

anti = silver.join(meta, "device_id", "left_anti")
print("anti  :", anti.count(), " (expect 2 — the devices with NO metadata)")
display(anti)

cross = silver.crossJoin(spark.table(REGIONS))
print("cross :", cross.count(), " (expect 20 = 5 x 4)")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3b. Same joins in SQL

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 'inner' AS join_type, COUNT(*) AS n FROM device_telemetry_silver s JOIN device_metadata m USING (device_id)
# MAGIC UNION ALL SELECT 'left', COUNT(*) FROM device_telemetry_silver s LEFT JOIN device_metadata m USING (device_id)
# MAGIC UNION ALL SELECT 'semi', COUNT(*) FROM device_telemetry_silver s LEFT SEMI JOIN device_metadata m USING (device_id)
# MAGIC UNION ALL SELECT 'anti', COUNT(*) FROM device_telemetry_silver s LEFT ANTI JOIN device_metadata m USING (device_id)
# MAGIC UNION ALL SELECT 'cross', COUNT(*) FROM device_telemetry_silver s CROSS JOIN region_lookup r;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Row explosion: find it, then fix it with the Exercise 01 dedup pattern
# MAGIC D001 doubled because metadata has two D001 rows. Dedup the DIMENSION first — latest per key.

# COMMAND ----------

w = Window.partitionBy("device_id").orderBy(F.col("installed").desc())
meta_dedup = (meta.withColumn("rn", F.row_number().over(w))
                  .filter(F.col("rn") == 1).drop("rn"))
print("meta_dedup rows:", meta_dedup.count(), "(expect 4 — one D001, the newer 'sensor' record)")
left_fixed = silver.join(meta_dedup, "device_id", "left")
print("left after dedup:", left_fixed.count(), "(expect 5 — explosion gone)")
assert left_fixed.count() == 5

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Nulls never match — then null-safe equality
# MAGIC D005 has region NULL. region_lookup HAS a NULL-region row. They still will not meet.

# COMMAND ----------

regions = spark.table(REGIONS)

plain = silver.join(regions, "region", "left")
d005 = plain.filter(F.col("device_id")=="D005").select("region_name").first()[0]
print("plain left join, D005 region_name:", d005, "(expect None — NULLs never match)")

nsafe = silver.join(regions, silver.region.eqNullSafe(regions.region), "left") \
              .drop(regions.region)
d005b = nsafe.filter(F.col("device_id")=="D005").select("region_name").first()[0]
print("eqNullSafe join, D005 region_name:", d005b, "(expect NO-REGION)")

# COMMAND ----------

# MAGIC %sql
# MAGIC -- SQL null-safe equals: <=>
# MAGIC SELECT s.device_id, r.region_name
# MAGIC FROM device_telemetry_silver s LEFT JOIN region_lookup r ON s.region <=> r.region
# MAGIC WHERE s.device_id = 'D005';   -- expect NO-REGION

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. The duplicate-column trap

# COMMAND ----------

expr_join = silver.join(meta_dedup, silver.device_id == meta_dedup.device_id, "inner")
print("columns named device_id:", sum(1 for c in expr_join.columns if c == "device_id"), "(expect 2)")
try:
    expr_join.select("device_id").collect()
    print("no ambiguity error?!")
except Exception as e:
    print("select('device_id') FAILED as expected:", str(e).splitlines()[0][:60])
list_join = silver.join(meta_dedup, ["device_id"], "inner")
print("list-form join device_id columns:", sum(1 for c in list_join.columns if c == "device_id"), "(expect 1)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Broadcast join — confirm it in the plan

# COMMAND ----------

# FE serverless lesson: most Spark confs are REMOVED on serverless. Only four
# are manually available (legacy.timeParserPolicy, session.timeZone,
# shuffle.partitions, ansi.enabled). Reading anything else — including
# autoBroadcastJoinThreshold — raises CONFIG_NOT_AVAILABLE (SQLSTATE 42K0I).
try:
    print("threshold:", spark.conf.get("spark.sql.autoBroadcastJoinThreshold"))
except Exception as e:
    print("Conf not exposed on serverless (expected on FE):", str(e).splitlines()[0][:80])
    print("The classic/open-source default is 10485760 bytes (10 MB) — exam fact;")
    print("on serverless the platform manages broadcast decisions automatically.")

# The broadcast HINT still works everywhere — that is the lever you own on FE:
b = silver.join(F.broadcast(meta_dedup), "device_id", "left")
b.explain()          # look for BroadcastHashJoin in the physical plan

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. The union family — no-dedup, positional corruption, and the fixes

# COMMAND ----------

extra = spark.sql("SELECT 'east' AS region, 'US-East' AS region_name")
print("union count:", regions.union(extra).count(), "(expect 5 — .union() does NOT dedupe)")
print("union().distinct():", regions.union(extra).distinct().count(), "(expect 4 — SQL-UNION behavior)")

swapped = spark.sql("SELECT 'US-Central' AS region_name, 'central' AS region")  # order swapped!
bad = regions.union(swapped)
print("positional trap, last row:", bad.tail(1)[0].asDict(), " <- fields silently swapped")
good = regions.unionByName(swapped)
print("unionByName, last row   :", good.tail(1)[0].asDict(), " <- correct")

wide = regions.withColumn("zone", F.lit(1))
try:
    regions.unionByName(wide)
except Exception as e:
    print("width mismatch throws:", str(e).splitlines()[0][:50])
ok = regions.unionByName(wide, allowMissingColumns=True)
print("allowMissingColumns=True columns:", ok.columns)

# COMMAND ----------

# MAGIC %md ## 9. Write the enriched table — Exercise 03's input

# COMMAND ----------

enriched = (silver
    .join(meta_dedup, ["device_id"], "left")
    .join(regions.filter(F.col("region").isNotNull()), ["region"], "left"))
enriched.write.mode("overwrite").saveAsTable(ENRICHED)
n = spark.table(ENRICHED).count()
print("ENRICHED rows:", n)
assert n == 5, "left-join enrichment must keep all 5 telemetry rows"
display(spark.table(ENRICHED).orderBy("device_id"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## What to check
# MAGIC - Enriched row count (expect 5) and any checkpoint that surprised you.
# MAGIC
# MAGIC Next: **ex03_aggregations_gold** — aggregations and the medallion end-to-end.