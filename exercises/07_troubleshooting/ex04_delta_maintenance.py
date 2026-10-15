# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # Ch 7 · Exercise 04 — Delta maintenance: OPTIMIZE, clustering, VACUUM  [FE + VERIFY]
# MAGIC
# MAGIC Runs on Free Edition. It fragments a Delta table into many small files, then applies the layout and
# MAGIC maintenance operations the exam asks about, reading the table's history and file layout after each so you
# MAGIC can see what changed. Several steps are `[VERIFY]` — Free Edition may not support all of them, so each is
# MAGIC wrapped: a blocked step prints its error and the notebook continues.
# MAGIC
# MAGIC Works in its own schema, `workspace.de_cert_maint`, removed by the teardown cell.

# COMMAND ----------

CATALOG, SCHEMA = "workspace", "de_cert_maint"
FQ = f"{CATALOG}.{SCHEMA}"
TBL = f"{FQ}.sales"

def probe(label, sql, show=True):
    print(f"\n--- {label}")
    try:
        df = spark.sql(sql)
        rows = df.collect() if show else []
    except Exception as ex:
        lines = str(ex).strip().splitlines()
        print("  BLOCKED —", lines[0][:240])
        for extra in lines[1:6]:
            print("           ", extra[:240])
        return False
    print("  OK")
    for r in rows:
        print("   ", tuple(r))
    return True

def file_count():
    return spark.sql(f"DESCRIBE DETAIL {TBL}").collect()[0]["numFiles"]

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {FQ}")
spark.sql(f"DROP TABLE IF EXISTS {TBL}")
print("working schema:", FQ)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Fragment a table — many small files
# MAGIC Ten separate appends make ten (or more) small files. Small files are what OPTIMIZE compacts.

# COMMAND ----------

from pyspark.sql import functions as F
spark.sql(f"CREATE TABLE {TBL} (id INT, region STRING, amt DOUBLE) USING delta")
for i in range(10):
    (spark.range(i*1000, (i+1)*1000)
        .select(F.col("id").cast("int"),
                F.when(F.col("id") % 2 == 0, "east").otherwise("west").alias("region"),
                (F.rand() * 100).alias("amt"))
        .write.mode("append").format("delta").saveAsTable(TBL))
print("rows:", spark.table(TBL).count(), "| files:", file_count())

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. OPTIMIZE — compact the small files
# MAGIC Rewrites many small files into fewer large ones. The row count is unchanged; the file count drops.

# COMMAND ----------

before = file_count()
probe("OPTIMIZE", f"OPTIMIZE {TBL}", show=False)
print(f"files: {before} before -> {file_count()} after   (rows still {spark.table(TBL).count()})")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Z-ordering — co-locate by a column  [VERIFY]
# MAGIC The older layout method: `OPTIMIZE ... ZORDER BY` sorts related values into the same files, so queries
# MAGIC filtering on that column read fewer of them. Still documented, but no longer the recommended default.

# COMMAND ----------

probe("OPTIMIZE ZORDER BY (region)", f"OPTIMIZE {TBL} ZORDER BY (region)", show=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Liquid Clustering — the modern layout  [VERIFY]
# MAGIC `CLUSTER BY` replaces both partitioning and Z-ordering for new tables, and its keys can change without
# MAGIC rewriting the data. `CLUSTER BY AUTO` lets Databricks pick keys from query history — but that depends on
# MAGIC predictive optimization (section 6).

# COMMAND ----------

probe("ALTER TABLE ... CLUSTER BY (region)", f"ALTER TABLE {TBL} CLUSTER BY (region)", show=False)
probe("OPTIMIZE (incremental clustering)", f"OPTIMIZE {TBL}", show=False)
probe("ALTER TABLE ... CLUSTER BY AUTO", f"ALTER TABLE {TBL} CLUSTER BY AUTO", show=False)
probe("clustering columns, from DESCRIBE DETAIL",
      f"SELECT clusteringColumns FROM (DESCRIBE DETAIL {TBL})")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. History — every operation is logged
# MAGIC `DESCRIBE HISTORY` shows the table's version log: each write, OPTIMIZE, and clustering change.

# COMMAND ----------

probe("DESCRIBE HISTORY — operation per version",
      f"SELECT version, operation FROM (DESCRIBE HISTORY {TBL}) ORDER BY version")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Predictive optimization  [VERIFY — expected to be unavailable on FE]
# MAGIC When enabled, Databricks runs OPTIMIZE and VACUUM automatically on Unity Catalog managed tables. It's
# MAGIC enabled by default on paid accounts, and set at the account, catalog, or schema level. Free Edition
# MAGIC reported it DISABLED when these schemas were created, so enabling it here is expected to fail.

# COMMAND ----------

probe("is predictive optimization on for this schema?",
      f"SELECT * FROM (DESCRIBE SCHEMA EXTENDED {FQ})")
probe("ALTER SCHEMA ... ENABLE PREDICTIVE OPTIMIZATION",
      f"ALTER SCHEMA {FQ} ENABLE PREDICTIVE OPTIMIZATION", show=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. VACUUM — remove old data files
# MAGIC OPTIMIZE and updates leave old files behind, kept for time travel. VACUUM deletes files past the
# MAGIC retention threshold — **7 days by default**. A shorter retention needs the safety check turned off, and
# MAGIC Databricks strongly recommends staying at 7 days or more, since VACUUM removes the files time travel needs.

# COMMAND ----------

probe("VACUUM with default (7-day) retention — a dry run",
      f"VACUUM {TBL} RETAIN 168 HOURS DRY RUN")
probe("VACUUM RETAIN 0 HOURS without the override (expected to be blocked)",
      f"VACUUM {TBL} RETAIN 0 HOURS DRY RUN", show=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## What to check
# MAGIC - **Section 2:** the file count falls after OPTIMIZE while the row count holds.
# MAGIC - **Section 3:** whether `ZORDER BY` is supported on Free Edition.
# MAGIC - **Section 4:** whether `CLUSTER BY` and `CLUSTER BY AUTO` are supported, and what `clusteringColumns` shows.
# MAGIC - **Section 5:** the operations in the history — WRITE, OPTIMIZE, CLUSTER BY, and so on, one per version.
# MAGIC - **Section 6:** whether predictive optimization is on, and whether enabling it is allowed on Free Edition.
# MAGIC - **Section 7:** the default VACUUM retention, and whether RETAIN 0 HOURS is blocked by the safety check.

# COMMAND ----------

# MAGIC %md ## Teardown

# COMMAND ----------

spark.sql(f"DROP SCHEMA IF EXISTS {FQ} CASCADE")
print("removed", FQ)