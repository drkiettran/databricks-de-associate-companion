"""
One-call environment setup for the companion exercises.

Run from a Databricks notebook. Creates the catalog, schema, and Volume defined
in common/config.py, then prints the Volume path to generate data into.

    from common.setup import setup_environment
    raw_path = setup_environment()          # uses the active SparkSession
    raw_path = setup_environment(spark)     # or pass one explicitly

On Free Edition the default catalog is `workspace` and CREATE CATALOG is not
permitted. config.py defaults to `workspace` for that reason, so the catalog step
is skipped gracefully and the schema and Volume are created inside it.
"""

from .config import CATALOG, SCHEMA, VOLUME, volume_path


def _active_spark():
    """Return the active SparkSession — works on classic and serverless compute."""
    from pyspark.sql import SparkSession
    return SparkSession.builder.getOrCreate()


def setup_environment(spark=None):
    """Create catalog/schema/volume if missing. Returns the raw Volume path."""
    if spark is None:
        spark = _active_spark()

    try:
        spark.sql(f"CREATE CATALOG IF NOT EXISTS {CATALOG}")
    except Exception as exc:  # Free Edition: no CREATE CATALOG privilege
        print(f"(Could not create catalog {CATALOG}: {str(exc).splitlines()[0][:120]})")
        print(" Assuming it already exists. Override with COMPANION_CATALOG.")
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA}")
    spark.sql(f"CREATE VOLUME IF NOT EXISTS {CATALOG}.{SCHEMA}.{VOLUME}")

    raw = volume_path()
    print(f"Catalog/schema/volume ready: {CATALOG}.{SCHEMA}.{VOLUME}")
    print(f"Generate data into: {raw}")
    print("Example (in a %sh cell):")
    print(f"  python data/generate_data.py --outdir {raw} --days 14")
    return raw
