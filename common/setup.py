"""
One-call environment setup for the companion exercises.

Run this from a Databricks notebook (it needs an active `spark` session). It
creates the catalog, schema, and Volume defined in common/config.py, then tells
you the Volume path to generate data into.

    %run ./common/setup            # if imported as a notebook, or:
    from common.setup import setup_environment
    raw_path = setup_environment()

On Free Edition the default catalog may differ; set COMPANION_CATALOG to a
catalog you can create objects in (e.g. your workspace default catalog).
"""

from .config import CATALOG, SCHEMA, VOLUME, volume_path


def setup_environment(spark=None):
    """Create catalog/schema/volume if missing. Returns the raw Volume path."""
    if spark is None:
        try:
            spark = spark  # noqa: F821 — provided by the notebook runtime
        except NameError as exc:  # pragma: no cover
            raise RuntimeError("Pass a SparkSession, or run inside a notebook") from exc

    # Free Edition often lacks CREATE CATALOG privilege — in that case set
    # COMPANION_CATALOG to an existing catalog you can write to and this step
    # is skipped gracefully.
    try:
        spark.sql(f"CREATE CATALOG IF NOT EXISTS {CATALOG}")
    except Exception as exc:  # pragma: no cover
        print(f"(Could not create catalog {CATALOG}: {exc}.")
        print(f" Assuming it already exists. Override with COMPANION_CATALOG.)")
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA}")
    spark.sql(f"CREATE VOLUME IF NOT EXISTS {CATALOG}.{SCHEMA}.{VOLUME}")

    raw = volume_path()
    print(f"Catalog/schema/volume ready: {CATALOG}.{SCHEMA}.{VOLUME}")
    print(f"Generate data into: {raw}")
    print("Example (in a %sh cell):")
    print(f"  python data/generate_data.py --outdir {raw} --days 14 --dirty")
    return raw
