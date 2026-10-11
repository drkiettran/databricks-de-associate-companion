# Databricks notebook source
# /// script
# [tool.databricks.environment]
# base_environment = "databricks_ai_v5"
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # Verify environment — DE cert workspace  [FE, read-only]
# MAGIC
# MAGIC Confirms the exercise environment is intact and undrifted. **Reads only** — never
# MAGIC creates, drops, or modifies anything. Safe to run any time, as often as you like.
# MAGIC
# MAGIC Two tiers:
# MAGIC 1. **Foundation** — schema, Volume, and seeded source data (from `common/setup.py` + `data/generate_data.py`).
# MAGIC 2. **Section 3 medallion** — the persistent tables, view, and constraint that
# MAGIC    Days 2–5 leave standing, checked for *correctness*, not just existence.
# MAGIC
# MAGIC Every failure names the notebook that fixes it.
# MAGIC
# MAGIC Set `STRICT = True` to raise on failure — use that when running this as a Lakeflow
# MAGIC Job task, so an unhealthy environment fails the run loudly instead of printing quietly.

# COMMAND ----------

STRICT = False          # True -> raise at the end if anything failed (job-task mode)
VERBOSE_SKIPS = True    # show informational/transient findings

CATALOG, SCHEMA, VOLUME = "workspace", "de_cert", "raw"
VOL = f"/Volumes/{CATALOG}/{SCHEMA}/{VOLUME}"
FQ = f"{CATALOG}.{SCHEMA}"

# COMMAND ----------

# MAGIC %md ## The check engine

# COMMAND ----------

results = []   # (tier, name, status, detail, fix)

PASS, FAIL, INFO = "PASS", "FAIL", "INFO"


def record(tier, name, status, detail="", fix=""):
    results.append((tier, name, status, detail, fix))


def check(tier, name, fn, fix=""):
    """Run one check. fn() returns (ok: bool, detail: str)."""
    try:
        ok, detail = fn()
        record(tier, name, PASS if ok else FAIL, detail, "" if ok else fix)
    except Exception as ex:
        record(tier, name, FAIL, f"error: {str(ex).splitlines()[0][:90]}", fix)


def table_exists(name):
    return spark.catalog.tableExists(f"{FQ}.{name}")


def row_count(name):
    return spark.table(f"{FQ}.{name}").count()


def expect_rows(name, expected, note=""):
    """Existence + exact row count."""
    def _fn():
        if not table_exists(name):
            return False, "table missing"
        n = row_count(name)
        suffix = f"  {note}" if note else ""
        return n == expected, f"{n} rows (expect {expected}){suffix}"
    return _fn


# COMMAND ----------

# MAGIC %md ## Tier 1 — foundation (schema, Volume, seed data)

# COMMAND ----------

def _schema():
    rows = spark.sql(f"SHOW SCHEMAS IN {CATALOG}").collect()
    names = {r[0] for r in rows}
    return SCHEMA in names, f"schemas in {CATALOG}: {len(names)}"


def _volume():
    rows = spark.sql(f"SHOW VOLUMES IN {FQ}").collect()
    names = {r[1] if len(r) > 1 else r[0] for r in rows}
    return VOLUME in names, f"volume '{VOLUME}' " + ("found" if VOLUME in names else "MISSING")


def _seed_files():
    """The 14 daily sales files plus the four other seeded artifacts."""
    try:
        top = {f.name.rstrip("/") for f in dbutils.fs.ls(VOL)}
    except Exception:
        return False, "volume path not listable"
    want = {"sales", "customers", "products", "customers_cdc", "order_events.ndjson"}
    missing = want - top
    if missing:
        return False, f"missing from volume: {sorted(missing)}"
    n_sales = len([f for f in dbutils.fs.ls(f"{VOL}/sales") if f.name.endswith(".ndjson")])
    return n_sales == 14, f"all 5 artifacts present; sales files: {n_sales} (expect 14)"


def _seed_contents():
    """Row counts of the seeded source data — catches a partial/failed re-seed."""
    checks = {
        "customers/customers.ndjson": 200,
        "products/products.ndjson": 50,
        "customers_cdc/customers_cdc.ndjson": 120,
        "order_events.ndjson": 300,
    }
    bad = []
    for rel, expected in checks.items():
        try:
            n = spark.read.json(f"{VOL}/{rel}").count()
            if n != expected:
                bad.append(f"{rel.split('/')[-1]}={n}(want {expected})")
        except Exception:
            bad.append(f"{rel.split('/')[-1]}=unreadable")
    return not bad, "all seed counts correct" if not bad else "; ".join(bad)


SETUP_FIX = "re-run common/setup.py, then data/generate_data.py (both idempotent)"

check("foundation", "schema exists",      _schema,        SETUP_FIX)
check("foundation", "volume exists",      _volume,        SETUP_FIX)
check("foundation", "seed files present", _seed_files,    SETUP_FIX)
check("foundation", "seed row counts",    _seed_contents, SETUP_FIX)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Tier 2 — Section 3 medallion
# MAGIC These tables are **meant to persist**: ex02 reads ex01's output, ex03 reads ex02's,
# MAGIC ex04 reads ex03's. Counts come from the exercises' own assertions.

# COMMAND ----------

D2 = "re-run exercises/03_transformation/ex01_clean_bronze_to_silver.py"
D3 = "re-run exercises/03_transformation/ex02_joins_silver_enrichment.py"
D4 = "re-run exercises/03_transformation/ex03_aggregations_gold.py"
D5 = "re-run exercises/03_transformation/ex04_gold_objects_quality_tuning.py"

# --- existence + counts ---
check("medallion", "bronze table",
      expect_rows("device_telemetry_bronze", 8), D2)
check("medallion", "silver table",
      expect_rows("device_telemetry_silver", 5, "(4 if ex02 hasn't added D005 yet)"), D2)
check("medallion", "device_metadata dim",
      expect_rows("device_metadata", 5), D3)
check("medallion", "region_lookup dim",
      expect_rows("region_lookup", 4), D3)
check("medallion", "enriched table",
      expect_rows("device_telemetry_enriched", 5), D3)
check("medallion", "gold: readings_by_region",
      expect_rows("gold_readings_by_region", 5), D4)
check("medallion", "gold: hourly_temp_rollup",
      expect_rows("gold_hourly_temp_rollup", 5), D4)
check("medallion", "gold: device_current_status",
      expect_rows("gold_device_current_status", 5), D4)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Correctness, not just existence
# MAGIC A table can exist with the right row count and still be wrong. These re-assert the
# MAGIC *semantics* the exercises taught — the things that would silently drift.

# COMMAND ----------

from pyspark.sql import functions as F


def _silver_clean():
    if not table_exists("device_telemetry_silver"):
        return False, "silver missing"
    s = spark.table(f"{FQ}.device_telemetry_silver")
    problems = []
    if s.filter(F.col("device_id").isNull()).count():
        problems.append("null device_id")
    if s.filter(F.col("event_ts").isNull()).count():
        problems.append("null event_ts")
    if s.filter(F.col("status") == "TEST").count():
        problems.append("junk sentinel survived")
    return not problems, "keys complete, no junk" if not problems else "; ".join(problems)


def _silver_dedup():
    """D001 must carry the LATER correction (22.0), not the original 21.5."""
    if not table_exists("device_telemetry_silver"):
        return False, "silver missing"
    r = (spark.table(f"{FQ}.device_telemetry_silver")
              .filter(F.col("device_id") == "D001").select("temp_c").collect())
    if len(r) != 1:
        return False, f"expected exactly 1 D001 row, found {len(r)}"
    return r[0][0] == 22.0, f"D001 temp_c = {r[0][0]} (expect 22.0 — latest-per-key)"


def _gold_null_group():
    """The NULL-region row must survive as its own single group."""
    if not table_exists("gold_readings_by_region"):
        return False, "gold table missing"
    n = spark.table(f"{FQ}.gold_readings_by_region").filter(F.col("region").isNull()).count()
    return n == 1, f"{n} NULL-region group(s) (expect 1 — GROUP BY collects NULLs)"


def _gold_bands():
    """NULL temps must land in otherwise(), not vanish."""
    if not table_exists("gold_device_current_status"):
        return False, "gold table missing"
    bands = {r["temp_band"]: r["n"] for r in
             spark.table(f"{FQ}.gold_device_current_status")
                  .groupBy("temp_band").agg(F.count("*").alias("n")).collect()}
    want = {"warm": 2, "mild": 1, "unknown": 2}
    return bands == want, f"{bands} (expect {want})"


check("correctness", "silver: keys complete, junk gone", _silver_clean, D2)
check("correctness", "silver: dedup kept latest D001",   _silver_dedup, D2)
check("correctness", "gold: NULL-region group present",  _gold_null_group, D4)
check("correctness", "gold: temp bands incl. unknown",   _gold_bands, D4)

# COMMAND ----------

# MAGIC %md ### ex04 objects — the view and the CHECK constraint

# COMMAND ----------

def _view():
    rows = spark.sql(f"SHOW VIEWS IN {FQ}").collect()
    names = {r["viewName"] if "viewName" in r.asDict() else r[1] for r in rows}
    found = "v_active_regions" in names
    return found, "v_active_regions " + ("present" if found else "MISSING")


def _constraint():
    """valid_temp CHECK lives in the Delta table properties — read-only lookup."""
    if not table_exists("device_telemetry_silver"):
        return False, "silver missing"
    props = {r[0]: r[1] for r in
             spark.sql(f"SHOW TBLPROPERTIES {FQ}.device_telemetry_silver").collect()}
    key = "delta.constraints.valid_temp"
    return key in props, ("valid_temp CHECK present" if key in props
                          else "valid_temp CHECK absent")


check("day5", "view v_active_regions", _view, D5)
check("day5", "CHECK constraint valid_temp", _constraint, D5)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Informational — transient Section 2 tables
# MAGIC Most Section 2 exercises clean up after themselves. Anything left here is leftover
# MAGIC state, not a problem — noted so you know why it's in the schema.

# COMMAND ----------

transient = ["sales_bronze_ci", "sales_bronze_ci_auto", "sales_first3",
             "sales_bronze_al", "sales_bronze_rescue", "sales_ctas",
             "events_bronze", "events_silver"]
present = [t for t in transient if table_exists(t)]
if present:
    record("info", "leftover Section 2 tables", INFO,
           f"{len(present)} present: {', '.join(present)}",
           "harmless — drop if you want a tidy schema")
else:
    record("info", "leftover Section 2 tables", INFO, "none (exercises cleaned up)", "")

# COMMAND ----------

# MAGIC %md ## Report

# COMMAND ----------

w_tier = max(len(r[0]) for r in results)
w_name = max(len(r[1]) for r in results)

print("=" * 100)
print(f"ENVIRONMENT VERIFICATION — {FQ}")
print("=" * 100)

current = None
for tier, name, status, detail, fix in results:
    if status == INFO and not VERBOSE_SKIPS:
        continue
    if tier != current:
        print(f"\n[{tier.upper()}]")
        current = tier
    mark = {"PASS": "  ok  ", "FAIL": " FAIL ", "INFO": " note "}[status]
    print(f"{mark} {name:<{w_name}}  {detail}")
    if fix:
        print(f"{'':6} {'':<{w_name}}  -> {fix}")

failed = [r for r in results if r[2] == FAIL]
passed = [r for r in results if r[2] == PASS]

print("\n" + "=" * 100)
if failed:
    print(f"RESULT: {len(failed)} FAILED, {len(passed)} passed")
    print("\nTo repair, run these (in order — each rebuilds what the next one needs):")
    for fix in dict.fromkeys(r[4] for r in failed if r[4]):
        print(f"  - {fix}")
    print("\nNote: ex02 and ex03 have rebuild-if-missing guards, so running the LAST")
    print("failing day often repairs the upstream tables in one step.")
else:
    print(f"RESULT: all {len(passed)} checks passed — environment is intact.")
print("=" * 100)

if STRICT and failed:
    raise AssertionError(f"environment verification failed: {len(failed)} check(s). "
                         f"First: {failed[0][1]} — {failed[0][3]}")