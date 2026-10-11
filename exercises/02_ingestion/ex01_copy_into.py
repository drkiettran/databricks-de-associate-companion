# Databricks notebook source
# MAGIC %md
# MAGIC # Ch 2 · Exercise 01 — COPY INTO: idempotent batch ingestion  [FE]
# MAGIC
# MAGIC Load the seeded daily sales files from the Volume into a bronze Delta table
# MAGIC with `COPY INTO`, and see the behaviors the exam tests: idempotency, `force`,
# MAGIC schema inference, and `PATTERN`. Run the repo setup (README → Quickstart) first.

# COMMAND ----------

# MAGIC %md ## 1. Set the working namespace

# COMMAND ----------

# MAGIC %sql
# MAGIC USE CATALOG workspace;
# MAGIC USE SCHEMA de_cert;

# COMMAND ----------

# MAGIC %md ## 2. Create the bronze target (explicit schema)

# COMMAND ----------

# MAGIC %sql
# MAGIC -- JSON integer fields infer as BIGINT, so declare integer columns as BIGINT
# MAGIC -- (declaring INT causes DELTA_FAILED_TO_MERGE_FIELDS on load).
# MAGIC DROP TABLE IF EXISTS sales_bronze_ci;
# MAGIC CREATE TABLE sales_bronze_ci (
# MAGIC   transaction_id   BIGINT,
# MAGIC   transaction_time STRING,
# MAGIC   customer_id      BIGINT,
# MAGIC   product_id       BIGINT,
# MAGIC   quantity         BIGINT,
# MAGIC   unit_price       DOUBLE,
# MAGIC   amount           DOUBLE
# MAGIC );

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Dry run first — VALIDATE (previews without loading)

# COMMAND ----------

# MAGIC %sql
# MAGIC COPY INTO sales_bronze_ci
# MAGIC FROM '/Volumes/workspace/de_cert/raw/sales'
# MAGIC FILEFORMAT = JSON
# MAGIC VALIDATE;

# COMMAND ----------

# MAGIC %md ## 4. Load all 14 daily files

# COMMAND ----------

# MAGIC %sql
# MAGIC COPY INTO sales_bronze_ci
# MAGIC FROM '/Volumes/workspace/de_cert/raw/sales'
# MAGIC FILEFORMAT = JSON;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*) AS rows_after_first_load FROM sales_bronze_ci;   -- ~2800

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Idempotency — re-run loads NOTHING new
# MAGIC `COPY INTO` tracks which files it already loaded, so the identical statement
# MAGIC is a no-op on the data. The count should be **unchanged**.

# COMMAND ----------

# MAGIC %sql
# MAGIC COPY INTO sales_bronze_ci
# MAGIC FROM '/Volumes/workspace/de_cert/raw/sales'
# MAGIC FILEFORMAT = JSON;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*) AS rows_after_rerun FROM sales_bronze_ci;        -- still ~2800

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. force — re-load everything (bypasses the tracking)
# MAGIC Use sparingly: it re-ingests already-loaded files, so the count **doubles**.

# COMMAND ----------

# MAGIC %sql
# MAGIC COPY INTO sales_bronze_ci
# MAGIC FROM '/Volumes/workspace/de_cert/raw/sales'
# MAGIC FILEFORMAT = JSON
# MAGIC COPY_OPTIONS ('force' = 'true');

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*) AS rows_after_force FROM sales_bronze_ci;        -- ~5600 (doubled)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Schema inference into an empty table (mergeSchema)
# MAGIC Alternative to an explicit schema: create an empty table and let `COPY INTO`
# MAGIC infer the columns on the first load.

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS sales_bronze_ci_auto;
# MAGIC CREATE TABLE sales_bronze_ci_auto;

# COMMAND ----------

# MAGIC %sql
# MAGIC COPY INTO sales_bronze_ci_auto
# MAGIC FROM '/Volumes/workspace/de_cert/raw/sales'
# MAGIC FILEFORMAT = JSON
# MAGIC FORMAT_OPTIONS ('inferColumnTypes' = 'true')
# MAGIC COPY_OPTIONS   ('mergeSchema' = 'true');

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE sales_bronze_ci_auto;   -- columns were inferred from the data

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. PATTERN — load only a subset of files
# MAGIC Load just the first three days with a filename glob.

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS sales_first3;
# MAGIC CREATE TABLE sales_first3 (
# MAGIC   transaction_id BIGINT, transaction_time STRING, customer_id BIGINT,
# MAGIC   product_id BIGINT, quantity BIGINT, unit_price DOUBLE, amount DOUBLE
# MAGIC );

# COMMAND ----------

# MAGIC %sql
# MAGIC COPY INTO sales_first3
# MAGIC FROM '/Volumes/workspace/de_cert/raw/sales'
# MAGIC FILEFORMAT = JSON
# MAGIC PATTERN = 'sales_2024-01-0[1-3].ndjson';

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*) AS rows_first3 FROM sales_first3;                -- ~600 (3 files)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Cleanup (optional)
# MAGIC ```sql
# MAGIC DROP TABLE IF EXISTS sales_bronze_ci;
# MAGIC DROP TABLE IF EXISTS sales_bronze_ci_auto;
# MAGIC DROP TABLE IF EXISTS sales_first3;
# MAGIC ```
# MAGIC
# MAGIC **Exam connections:** `COPY INTO` is idempotent (tracks loaded files);
# MAGIC re-running does not duplicate, `force` re-loads; `FORMAT_OPTIONS` parse vs
# MAGIC `COPY_OPTIONS` load behavior; `mergeSchema` for inference/evolution; `PATTERN`
# MAGIC to filter files; `VALIDATE` for a dry run. Choose `COPY INTO` for
# MAGIC bounded/periodic batch, Auto Loader for continuous/high-volume.

# COMMAND ----------

# %sql
# DROP TABLE IF EXISTS sales_bronze_ci;
# DROP TABLE IF EXISTS sales_bronze_ci_auto;
# DROP TABLE IF EXISTS sales_first3;