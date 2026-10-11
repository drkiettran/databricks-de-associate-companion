# Databricks notebook source
# MAGIC %md
# MAGIC # Ch 2 · Exercise 05 — CTAS + Lakeflow Connect  [FE runnable: CTAS · conceptual: connectors]
# MAGIC
# MAGIC Completes Section 2. The CTAS cells run on Free Edition; the Lakeflow Connect
# MAGIC cells are **conceptual reference** (managed connectors need real sources and
# MAGIC classic compute — a paid workspace). Run the repo setup (README → Quickstart) first.

# COMMAND ----------

# MAGIC %md ## 1. CTAS — one-shot bulk load (runnable)

# COMMAND ----------

# MAGIC %sql
# MAGIC -- CTAS: create a table from a query result. read_files() reads the seeded
# MAGIC -- files in BATCH (no STREAM), so this is a one-time snapshot load.
# MAGIC CREATE OR REPLACE TABLE workspace.de_cert.sales_ctas AS
# MAGIC SELECT * FROM read_files('/Volumes/workspace/de_cert/raw/sales', format => 'json');

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*) AS rows FROM workspace.de_cert.sales_ctas;   -- ~2800 (all files, once)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. CTAS has NO incremental tracking
# MAGIC Unlike `COPY INTO` / Auto Loader, CTAS doesn't remember what it loaded. Running it
# MAGIC again just re-reads **everything** (here via `CREATE OR REPLACE`), so the count is
# MAGIC the same — it reloaded all files, it didn't add "new" ones. Use CTAS for a
# MAGIC one-time snapshot; use `COPY INTO` / Auto Loader for ongoing ingestion.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE workspace.de_cert.sales_ctas AS
# MAGIC SELECT * FROM read_files('/Volumes/workspace/de_cert/raw/sales', format => 'json');
# MAGIC -- same ~2800: a full reload, not an incremental append

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. The ingestion decision matrix (reference)
# MAGIC
# MAGIC | Method | Source | Nature | Choose when |
# MAGIC |---|---|---|---|
# MAGIC | CTAS | files/tables | one-shot, no tracking | single full load / snapshot |
# MAGIC | COPY INTO | files | idempotent batch | bounded / periodic file drops |
# MAGIC | Auto Loader | files | incremental stream | continuous / high-volume files |
# MAGIC | Lakeflow Connect (managed) | DB / SaaS / message bus | managed, incremental | non-file sources, minimal code |
# MAGIC | Standard connector / Structured Streaming | various | customizable | more control / unsupported source |
# MAGIC
# MAGIC Guidance: **start with the most managed layer; drop down only if it doesn't fit.**

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Lakeflow Connect — component anatomy (conceptual; not runnable here)
# MAGIC
# MAGIC All managed-connector pipelines are **UC-governed, serverless, and write to
# MAGIC streaming tables**. Components by type:
# MAGIC
# MAGIC | Connector type | Components |
# MAGIC |---|---|
# MAGIC | **SaaS** (Salesforce, Workday) | Connection + ingestion pipeline + destination streaming tables |
# MAGIC | **Database CDC** (SQL Server, MySQL, Postgres) | Connection + **ingestion gateway (classic compute)** + **staging Volume** + ingestion pipeline + streaming tables |
# MAGIC | **Query-based** | Connection + ingestion pipeline (cursor column + high-water mark; **no gateway/staging**) |
# MAGIC | **Streaming** | continuous read from a message bus into streaming tables |
# MAGIC
# MAGIC The database-CDC **gateway runs on classic compute** — which is why this type isn't
# MAGIC available on Free Edition (serverless-only).

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Reference: creating a connection (conceptual — needs a real source)
# MAGIC A `Connection` is a Unity Catalog securable storing source auth. The ingestion
# MAGIC pipeline itself is created via the UI (Add data → connector) or a pipeline spec.
# MAGIC **Do not run** on Free Edition — shown to learn the shape.
# MAGIC
# MAGIC ```sql
# MAGIC -- e.g. a SQL Server connection (illustrative)
# MAGIC CREATE CONNECTION my_sqlserver TYPE sqlserver
# MAGIC   OPTIONS (host '<host>', port '1433', user '<user>', password '<secret>');
# MAGIC
# MAGIC -- then, in the UI: create an ingestion gateway + pipeline against this connection,
# MAGIC -- selecting source tables; the pipeline writes to destination streaming tables.
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ### Cleanup (optional)
# MAGIC ```sql
# MAGIC DROP TABLE IF EXISTS workspace.de_cert.sales_ctas;
# MAGIC ```
# MAGIC
# MAGIC **Exam connections:** CTAS = one-shot, no incremental tracking; managed-connector
# MAGIC pipelines are UC-governed + serverless + write to streaming tables; database CDC
# MAGIC connectors add a classic-compute **gateway** + a **staging Volume**; query-based
# MAGIC connectors use a **cursor column + high-water mark** with no gateway/staging;
# MAGIC "start most managed, drop down."

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS workspace.de_cert.sales_ctas;