# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # Ch 6 · Exercise 02 — Column masks, row filters, and ABAC  [FE]
# MAGIC
# MAGIC Masks and filters decide what each user sees by asking which groups they belong to. With
# MAGIC only one user available, this exercise flips the answer a different way: it changes **which
# MAGIC group** the function checks. You are a member of `account users`, and not of `pii_readers`,
# MAGIC which doesn't exist — so swapping between the two shows both outcomes to the same person.
# MAGIC
# MAGIC Works in its own schema, `workspace.de_cert_gov`, removed by the teardown cell at the end.

# COMMAND ----------

CATALOG, SCHEMA = "workspace", "de_cert_gov"
FQ = f"{CATALOG}.{SCHEMA}"
TBL = f"{FQ}.customers"

def probe(label, sql, show=True):
    """Run one statement to completion; report OK or the full error. Returns True on success."""
    print(f"\n--- {label}")
    try:
        df = spark.sql(sql)
        rows = df.collect() if show else []   # force execution BEFORE declaring success
    except Exception as ex:
        lines = str(ex).strip().splitlines()
        print("  BLOCKED —", lines[0][:300])
        for extra in lines[1:8]:               # keep the explanation that follows the first line
            print("           ", extra[:300])
        return False
    print("  OK")
    for r in rows:
        print("   ", tuple(r))
    return True

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {FQ}")
spark.sql(f"DROP TABLE IF EXISTS {TBL}")
spark.sql(f"""CREATE TABLE {TBL} (id INT, name STRING, ssn STRING, region STRING)""")
spark.sql(f"""INSERT INTO {TBL} VALUES
  (1, 'Ana',  '123-45-6789', 'east'),
  (2, 'Ben',  '234-56-7890', 'west'),
  (3, 'Cai',  '345-67-8901', 'east'),
  (4, 'Dee',  '456-78-9012', 'west')""")
print("table:", TBL, "| rows:", spark.table(TBL).count())

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Who are you, as Unity Catalog sees it?
# MAGIC Masks and filters are built on these identity functions.

# COMMAND ----------

probe("identity functions", """
  SELECT current_user()                            AS user,
         is_account_group_member('account users')  AS in_account_users,
         is_account_group_member('pii_readers')    AS in_pii_readers
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. A column mask
# MAGIC Only members of `pii_readers` see the full SSN; everyone else sees the last four digits.

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE FUNCTION {FQ}.mask_ssn(ssn STRING)
RETURN CASE WHEN is_account_group_member('pii_readers') THEN ssn
            ELSE concat('***-**-', right(ssn, 4)) END
""")
masked = probe("apply the mask", f"ALTER TABLE {TBL} ALTER COLUMN ssn SET MASK {FQ}.mask_ssn", show=False)
probe("query as the table's owner", f"SELECT id, name, ssn FROM {TBL} ORDER BY id")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Change who the mask trusts — without touching the table
# MAGIC Replace the function so it checks `account users`, a group you're in. The mask stays
# MAGIC attached; only its logic changes.

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE FUNCTION {FQ}.mask_ssn(ssn STRING)
RETURN CASE WHEN is_account_group_member('account users') THEN ssn
            ELSE concat('***-**-', right(ssn, 4)) END
""")
probe("query again, same table, new function", f"SELECT id, name, ssn FROM {TBL} ORDER BY id")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. A row filter
# MAGIC Members of `pii_readers` see every row; everyone else sees only `east`.

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE FUNCTION {FQ}.east_only(region STRING)
RETURN IF(is_account_group_member('pii_readers'), TRUE, region = 'east')
""")
filtered = probe("apply the row filter", f"ALTER TABLE {TBL} SET ROW FILTER {FQ}.east_only ON (region)", show=False)
probe("query with filter and mask both attached", f"SELECT id, name, ssn, region FROM {TBL} ORDER BY id")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Where the table records them

# COMMAND ----------

detail = spark.sql(f"DESCRIBE TABLE EXTENDED {TBL}")
display(detail.filter("lower(col_name) rlike 'filter|mask' or lower(data_type) rlike 'filter|mask'"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. A type mismatch between the function and the column
# MAGIC The filter function below declares an `INT` parameter, but `region` is a `STRING`. The
# MAGIC Section 4 filter is dropped first, so the query tests only the mismatched one.

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE FUNCTION {FQ}.bad_filter(region INT)
RETURN region = 1
""")
probe("drop the current filter first", f"ALTER TABLE {TBL} DROP ROW FILTER", show=False)
probe("attach the mismatched filter",
      f"ALTER TABLE {TBL} SET ROW FILTER {FQ}.bad_filter ON (region)", show=False)
probe("query with the mismatched filter", f"SELECT id, region FROM {TBL} ORDER BY id")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Remove them

# COMMAND ----------

probe("drop the row filter", f"ALTER TABLE {TBL} DROP ROW FILTER", show=False)
probe("drop the mask", f"ALTER TABLE {TBL} ALTER COLUMN ssn DROP MASK", show=False)
probe("query with neither", f"SELECT id, name, ssn, region FROM {TBL} ORDER BY id")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. The older approach: a dynamic view
# MAGIC The same rules expressed in a view instead of on the table.

# COMMAND ----------

probe("create a dynamic view", f"""
CREATE OR REPLACE VIEW {FQ}.customers_safe AS
SELECT id, name,
       CASE WHEN is_account_group_member('pii_readers') THEN ssn
            ELSE concat('***-**-', right(ssn, 4)) END AS ssn,
       region
FROM {TBL}
WHERE is_account_group_member('pii_readers') OR region = 'east'
""", show=False)
probe("query the view", f"SELECT * FROM {FQ}.customers_safe ORDER BY id")
probe("query the base table directly", f"SELECT id, ssn, region FROM {TBL} ORDER BY id")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 9. ABAC — policies that target columns by tag
# MAGIC Tag the column, then try a policy that masks every column carrying that tag.

# COMMAND ----------

probe("tag the ssn column", f"ALTER TABLE {TBL} ALTER COLUMN ssn SET TAGS ('pii' = 'ssn')", show=False)
probe("read the tag back", f"""
  SELECT column_name, tag_name, tag_value
  FROM {CATALOG}.information_schema.column_tags
  WHERE schema_name = '{SCHEMA}' AND table_name = 'customers'
""")
probe("create an ABAC policy", f"""
CREATE POLICY mask_tagged_ssn
ON SCHEMA {FQ}
COLUMN MASK {FQ}.mask_ssn
TO `account users`
FOR TABLES
MATCH COLUMNS has_tag_value('pii', 'ssn') AS ssn_col
ON COLUMN ssn_col
""", show=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## What to check
# MAGIC - **Section 1:** your user name, and which group checks return true.
# MAGIC - **Section 2:** whether the SSNs are masked when you — the table's owner — query them.
# MAGIC - **Section 3:** whether replacing the function changed the result without re-applying the mask.
# MAGIC - **Section 4:** which rows remain, and whether the mask still applies alongside the filter.
# MAGIC - **Section 5:** how `DESCRIBE TABLE EXTENDED` records the filter and mask.
# MAGIC - **Section 6:** whether the mismatched filter errors, or silently returns wrong rows.
# MAGIC - **Section 7:** whether the full data comes back.
# MAGIC - **Section 8:** whether the view protects data that the base table, queried directly, does not.
# MAGIC - **Section 9:** whether tagging works, and what creating the ABAC policy returns.

# COMMAND ----------

# MAGIC %md ## Teardown

# COMMAND ----------

spark.sql(f"DROP SCHEMA IF EXISTS {FQ} CASCADE")
print("removed", FQ)