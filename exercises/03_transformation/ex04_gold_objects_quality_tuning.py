# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # Ch 3 · Exercise 04 — Gold Object Types, Data Quality, Tuning  [FE + verify]
# MAGIC
# MAGIC Fixes applied: AQE-state read try/except'd (CONFIG_NOT_AVAILABLE); partition count via
# MAGIC spark_partition_id() — the RDD API (.rdd.getNumPartitions) raises NOT_IMPLEMENTED on serverless.
# MAGIC
# MAGIC Section 3 closer. Mixes [FE-runnable] cells with [verify-on-your-workspace] cells;
# MAGIC the divergences are the point. Report which object types your workspace allows.

# COMMAND ----------

# MAGIC %md ## 0. Config

# COMMAND ----------

from pyspark.sql import functions as F
CATALOG, SCHEMA = "workspace", "de_cert"
spark.sql(f"USE CATALOG {CATALOG}"); spark.sql(f"USE SCHEMA {SCHEMA}")
G_REGION = f"{CATALOG}.{SCHEMA}.gold_readings_by_region"
SILVER   = f"{CATALOG}.{SCHEMA}.device_telemetry_silver"
assert spark.catalog.tableExists(G_REGION), "run ex03_aggregations_gold first (needs the gold tables)"

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. View vs managed table  [FE]
# MAGIC A view stores the QUERY (recomputed live); a managed table stores a SNAPSHOT.

# COMMAND ----------

spark.sql(f"""CREATE OR REPLACE VIEW {SCHEMA}.v_active_regions AS
  SELECT region, avg_temp_c, reading_count
  FROM {G_REGION} WHERE reading_count > 0""")
print("view rows now:", spark.table(f"{SCHEMA}.v_active_regions").count())
print("A view is a stored query: re-read it after the gold table changes and it reflects the change,")
print("with no rewrite. A managed table would show the OLD snapshot until you re-ran its write.")
display(spark.table(f"{SCHEMA}.v_active_regions"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Materialized view & streaming table  [VERIFY — report what happens]
# MAGIC Both are powered by serverless Lakeflow pipelines. From a notebook on FE the path is
# MAGIC Beta/region-limited and may be blocked. Try each; record the exact error if it fails.

# COMMAND ----------

# --- Materialized view attempt ---
try:
    spark.sql(f"""CREATE MATERIALIZED VIEW {SCHEMA}.mv_region_avg AS
      SELECT region, AVG(avg_temp_c) AS avg_of_avgs FROM {G_REGION} GROUP BY region""")
    print("MV created — your workspace ALLOWS materialized views from a notebook.")
    display(spark.table(f"{SCHEMA}.mv_region_avg"))
except Exception as ex:
    print("MV blocked from notebook (report this):", str(ex).splitlines()[0][:100])

# COMMAND ----------

# --- Streaming table attempt (often STREAMING_TABLE_OPERATION_NOT_ALLOWED on Free Edition) ---
try:
    spark.sql(f"""CREATE STREAMING TABLE {SCHEMA}.st_demo AS
      SELECT * FROM STREAM read_files('/Volumes/{CATALOG}/{SCHEMA}/landing', format => 'json')""")
    print("Streaming table created — your workspace ALLOWS this path.")
except Exception as ex:
    print("Streaming table blocked (expected on FE notebook; report the code):",
          str(ex).splitlines()[0][:100])

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Data quality — three levels
# MAGIC ### 3a. Census [FE]: measure, don't enforce

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*) AS total,
# MAGIC        COUNT(*) FILTER (WHERE temp_c IS NULL)              AS null_temps,
# MAGIC        COUNT(*) FILTER (WHERE temp_c < -50 OR temp_c > 100) AS impossible_temps,
# MAGIC        COUNT(*) FILTER (WHERE region IS NULL)              AS null_region
# MAGIC FROM device_telemetry_silver;

# COMMAND ----------

# MAGIC %md ### 3b. CHECK constraint [FE, Delta]: enforce at write time

# COMMAND ----------

spark.sql(f"ALTER TABLE {SILVER} DROP CONSTRAINT IF EXISTS valid_temp")
spark.sql(f"""ALTER TABLE {SILVER}
  ADD CONSTRAINT valid_temp CHECK (temp_c IS NULL OR temp_c BETWEEN -50 AND 100)""")
print("Constraint added. Now a violating write should FAIL:")
try:
    spark.sql(f"""INSERT INTO {SILVER} VALUES
      ('D999', try_to_timestamp('2026-07-01 09:00:00'), 999.0, 'ACTIVE', 'east', try_to_timestamp('2026-07-01 09:00:01'))""")
    print("  insert succeeded — unexpected; check column order/constraint")
except Exception as ex:
    print("  ✓ violating insert rejected:", str(ex).splitlines()[0][:80])

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3c. Pipeline expectations [CONCEPTUAL on FE]
# MAGIC Inside a Lakeflow Declarative Pipeline you declare per-row rules. Three actions:
# MAGIC ```sql
# MAGIC -- default (warn): keep the row, record the metric
# MAGIC CONSTRAINT valid_temp EXPECT (temp_c BETWEEN -50 AND 100)
# MAGIC -- drop the bad row, keep running
# MAGIC CONSTRAINT valid_temp EXPECT (temp_c BETWEEN -50 AND 100) ON VIOLATION DROP ROW
# MAGIC -- halt the pipeline
# MAGIC CONSTRAINT valid_temp EXPECT (temp_c BETWEEN -50 AND 100) ON VIOLATION FAIL UPDATE
# MAGIC ```
# MAGIC Pipeline creation is restricted on FE — know the syntax and the three actions.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Tuning [FE] — shuffle.partitions and the AQE subtlety

# COMMAND ----------

# Free Edition finding: serverless exposes only FOUR configs. spark.sql.adaptive.enabled is
# NOT one of them — even READING it raises CONFIG_NOT_AVAILABLE. AQE is on, mediating every
# shuffle, and unobservable via conf. The query profile is the only window into it.
try:
    print("AQE enabled:", spark.conf.get("spark.sql.adaptive.enabled"))
except Exception as ex:
    print("AQE state unreadable on FE (expected — not an allowlisted config):",
          str(ex).splitlines()[0][:80])

spark.conf.set("spark.sql.shuffle.partitions", "16")   # allowlisted -> this works on FE
print("shuffle.partitions set to:", spark.conf.get("spark.sql.shuffle.partitions"))

demo = spark.range(0, 200000).withColumn("k", F.col("id") % 50)
# Free Edition finding: the RDD API is blocked on serverless — .rdd.getNumPartitions()
# raises NOT_IMPLEMENTED. The DataFrame-native idiom: spark_partition_id() tags each
# row with its partition id; distinct-count of those ids = the partition count.
agg = demo.groupBy("k").count()
actual = agg.select(F.spark_partition_id().alias("pid")).distinct().count()
print(f"actual partitions after shuffle: {actual}")
print("If this is LESS than 16, AQE coalesced the target down — expected. shuffle.partitions is a")
print("TARGET, not a guarantee. Read the real number from the query profile, not the config.")

# COMMAND ----------

# --- broadcast threshold: still not readable on serverless (see ex02_joins) ---
try:
    print("threshold:", spark.conf.get("spark.sql.autoBroadcastJoinThreshold"))
except Exception as ex:
    print("autoBroadcastJoinThreshold still blocked (expected):", str(ex).splitlines()[0][:70])
# the broadcast HINT remains your lever:
g = spark.table(G_REGION)
_ = g.join(F.broadcast(g.select("region").limit(2)), "region", "left")
print("broadcast HINT works regardless — it's the join-strategy lever on FE.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## What to check
# MAGIC - Section 2: did MV and/or streaming table create, or what errors? (the key FE finding)
# MAGIC - Section 3b: confirm the CHECK-violating insert was rejected.
# MAGIC - Section 4: the actual partition count you observed.
# MAGIC
# MAGIC This completes the Section 3 exercises. Next: **04_jobs**.

# COMMAND ----------

# MAGIC %md ### cleanup (optional)

# COMMAND ----------

# spark.sql(f"ALTER TABLE {SILVER} DROP CONSTRAINT IF EXISTS valid_temp")
# spark.sql(f"DROP VIEW IF EXISTS {SCHEMA}.v_active_regions")