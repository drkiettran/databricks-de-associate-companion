# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # Ch 6 · Exercise 01 — Managed vs external tables, and privileges  [FE]
# MAGIC
# MAGIC Works in its own schema, `workspace.de_cert_gov`, so nothing here touches the other
# MAGIC exercises' tables. Each operation that Free Edition may not support is wrapped:
# MAGIC a blocked operation prints its error and the notebook keeps going.
# MAGIC
# MAGIC Run top to bottom. The teardown cell at the end removes the schema.

# COMMAND ----------

CATALOG, SCHEMA = "workspace", "de_cert_gov"
FQ = f"{CATALOG}.{SCHEMA}"
TBL = f"{FQ}.gov_managed"

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
print("working schema:", FQ)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. A managed table
# MAGIC No `LOCATION` clause, so Unity Catalog decides where the data lives.

# COMMAND ----------

spark.sql(f"DROP TABLE IF EXISTS {TBL}")
spark.sql(f"CREATE TABLE {TBL} (id INT, region STRING)")
spark.sql(f"INSERT INTO {TBL} VALUES (1, 'east'), (2, 'west'), (3, 'east')")

detail = spark.sql(f"DESCRIBE TABLE EXTENDED {TBL}")
display(detail.filter("col_name IN ('Type', 'Location', 'Owner', 'Provider')"))
print("rows:", spark.table(TBL).count())

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Drop it, then try to get it back
# MAGIC Dropping a managed table removes its data as well as its definition. `UNDROP` is meant
# MAGIC to recover it within a retention window. Does it work here?

# COMMAND ----------

spark.sql(f"DROP TABLE {TBL}")
print("dropped. still listed?",
      spark.sql(f"SHOW TABLES IN {FQ}").filter("tableName = 'gov_managed'").count() > 0)

probe("list dropped tables", f"SHOW TABLES DROPPED IN {FQ}")
recovered = probe("UNDROP the table", f"UNDROP TABLE {TBL}", show=False)

if recovered:
    print("rows after UNDROP:", spark.table(TBL).count(), "(expect 3 — data came back too)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. External tables — does this edition allow them?
# MAGIC An external table needs a storage location Unity Catalog doesn't manage, normally an
# MAGIC **external location** backed by a **storage credential**. These checks look for either.

# COMMAND ----------

probe("storage credentials", "SHOW STORAGE CREDENTIALS")
probe("external locations", "SHOW EXTERNAL LOCATIONS")

# LOCATION needs a cloud storage URI (s3://, abfss://, gs://), so a Volume path is rejected.
probe("table at a Volume path (expected to fail)",
      f"CREATE TABLE {FQ}.gov_external (id INT) "
      f"LOCATION '/Volumes/{CATALOG}/de_cert/raw/_gov_external_test'", show=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Privileges — what's granted by default
# MAGIC Read the existing grants before changing anything, and notice what `SHOW GRANTS` leaves out.

# COMMAND ----------

probe("grants on the workspace catalog", f"SHOW GRANTS ON CATALOG {CATALOG}")
probe("grants on workspace.default", f"SHOW GRANTS ON SCHEMA {CATALOG}.default")
probe("grants on this exercise's schema", f"SHOW GRANTS ON SCHEMA {FQ}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Grant, inherit, revoke
# MAGIC Grant `SELECT` on the **schema**, then look at the **table**. Privileges inherit downward,
# MAGIC so a schema grant should apply to every table in it.

# COMMAND ----------

GROUP = "`account users`"   # the built-in group containing every account user

granted = probe("GRANT USE SCHEMA and SELECT on the schema",
                f"GRANT USE SCHEMA, SELECT ON SCHEMA {FQ} TO {GROUP}", show=False)
probe("grants on the schema, after", f"SHOW GRANTS ON SCHEMA {FQ}")
probe("grants on the TABLE — is the schema grant visible here?", f"SHOW GRANTS ON TABLE {TBL}")

if granted:
    probe("REVOKE SELECT", f"REVOKE SELECT ON SCHEMA {FQ} FROM {GROUP}", show=False)
    probe("grants on the schema, after revoke", f"SHOW GRANTS ON SCHEMA {FQ}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. DENY
# MAGIC Some database systems let you explicitly deny a privilege. Try it.

# COMMAND ----------

probe("DENY SELECT", f"DENY SELECT ON TABLE {TBL} TO {GROUP}", show=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## What to check
# MAGIC - **Section 1:** the table's `Type` is `MANAGED`. On Free Edition its `Location` comes back blank.
# MAGIC - **Section 2:** whether `UNDROP` brought the table back, and its rows with it.
# MAGIC - **Section 3:** whether any storage credential or external location exists, and the exact
# MAGIC   error from creating a table at a Volume path.
# MAGIC - **Section 4:** whether `SHOW GRANTS` lists anything. It shows explicit grants only —
# MAGIC   not ownership, and not the workspace defaults.
# MAGIC - **Section 5:** on the table, the schema's `SELECT` appears with `ObjectType` `SCHEMA` —
# MAGIC   inherited, with its source named.
# MAGIC - **Section 6:** the exact error `DENY` produces.

# COMMAND ----------

# MAGIC %md ## Teardown

# COMMAND ----------

spark.sql(f"DROP SCHEMA IF EXISTS {FQ} CASCADE")
print("removed", FQ)
