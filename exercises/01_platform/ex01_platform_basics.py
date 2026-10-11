# Databricks notebook source
# MAGIC %md
# MAGIC # Ch 1 · Exercise 01 — Platform basics: Delta, time travel, the UC namespace  [FE]
# MAGIC
# MAGIC Self-contained for Free Edition (uses `workspace` / `de_cert`). Run the repo setup (README → Quickstart)
# MAGIC first so the schema exists. See Section-1 concepts as real objects: a managed
# MAGIC Delta table, its transaction history and time travel, and the three-level
# MAGIC Unity Catalog namespace.

# COMMAND ----------

# MAGIC %md ## 1. Config

# COMMAND ----------

CATALOG, SCHEMA = "workspace", "de_cert"
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA}")   # harmless if it exists
tbl = f"{CATALOG}.{SCHEMA}.platform_demo"
print("Table:", tbl)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Create a managed Delta table and write to it a few times
# MAGIC Each write is a new transaction (a new version in the log).

# COMMAND ----------

spark.sql(f"DROP TABLE IF EXISTS {tbl}")
spark.sql(f"""
CREATE TABLE {tbl} (
  id INT,
  region STRING,
  amount DOUBLE
) USING DELTA
""")

spark.sql(f"INSERT INTO {tbl} VALUES (1, 'east', 100.0), (2, 'west', 250.0)")   # v1
spark.sql(f"INSERT INTO {tbl} VALUES (3, 'east', 175.5)")                        # v2
spark.sql(f"UPDATE {tbl} SET amount = amount * 1.10 WHERE region = 'east'")      # v3

display(spark.table(tbl).orderBy("id"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Transaction history and time travel
# MAGIC The log records every commit. Query an earlier version with `VERSION AS OF`.

# COMMAND ----------

display(spark.sql(f"DESCRIBE HISTORY {tbl}"))

# COMMAND ----------

# Glance at the history above first to confirm version 0 = CREATE, 1 = first INSERT.
print("As of version 1 (before the second insert and the update):")
display(spark.sql(f"SELECT * FROM {tbl} VERSION AS OF 1 ORDER BY id"))

print("Current:")
display(spark.sql(f"SELECT * FROM {tbl} ORDER BY id"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Confirm it's Delta and see where it lives
# MAGIC `DESCRIBE DETAIL` shows format = delta and the managed storage location.

# COMMAND ----------

display(spark.sql(f"DESCRIBE DETAIL {tbl}"))

# COMMAND ----------

# MAGIC %md ## 5. Walk the three-level namespace

# COMMAND ----------

display(spark.sql("SHOW CATALOGS"))
display(spark.sql(f"SHOW SCHEMAS IN {CATALOG}"))
display(spark.sql(f"SHOW TABLES IN {CATALOG}.{SCHEMA}"))
display(spark.sql(f"SHOW VOLUMES IN {CATALOG}.{SCHEMA}"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Compute — concept check (no code)
# MAGIC On Free Edition you're on **serverless** — no cluster picker, so the choices
# MAGIC below are what you'd exercise on a paid workspace:
# MAGIC
# MAGIC | Workload | Best compute | Why |
# MAGIC |---|---|---|
# MAGIC | Nightly automated ETL | **Job compute** | ephemeral, lower DBU rate |
# MAGIC | Interactive development | **All-purpose compute** | shared, interactive |
# MAGIC | BI dashboards / ad-hoc SQL | **SQL warehouse** (serverless) | fast start for BI |
# MAGIC | Bursty, no infra mgmt | **Serverless** | auto-managed, fast start |
# MAGIC | Network control / custom runtime / GPU | **Classic** | runs in your cloud VPC |
# MAGIC
# MAGIC **Cost:** billed in DBUs = compute-hours × rate (by type/tier) + cloud infra
# MAGIC for classic. Auto-termination limits idle classic cost.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Cleanup (optional)
# MAGIC ```python
# MAGIC spark.sql(f"DROP TABLE IF EXISTS {tbl}")
# MAGIC ```
# MAGIC
# MAGIC **Exam connections:** Delta = Parquet + transaction log (ACID + time travel);
# MAGIC three-level namespace `catalog.schema.object`; serverless vs classic;
# MAGIC job vs all-purpose vs SQL warehouse and the DBU cost model.