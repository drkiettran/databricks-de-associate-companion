"""
Shared configuration for the companion exercises.

Override any of these with environment variables so the same code runs against
Free Edition and a paid workspace without edits:

    COMPANION_CATALOG   (default: workspace)
    COMPANION_SCHEMA    (default: de_cert)
    COMPANION_VOLUME    (default: raw)

Defaults target Databricks Free Edition, whose default catalog is `workspace` and
which does not permit CREATE CATALOG. On a paid workspace, set COMPANION_CATALOG
to a catalog you can write to (commonly `main`).
"""

import os

CATALOG = os.environ.get("COMPANION_CATALOG", "workspace")
SCHEMA = os.environ.get("COMPANION_SCHEMA", "de_cert")
VOLUME = os.environ.get("COMPANION_VOLUME", "raw")


def volume_path(*parts: str) -> str:
    """Return a /Volumes/... path under the configured catalog/schema/volume."""
    base = f"/Volumes/{CATALOG}/{SCHEMA}/{VOLUME}"
    return "/".join([base, *parts]) if parts else base


def table_name(name: str) -> str:
    """Return a fully qualified catalog.schema.table name."""
    return f"{CATALOG}.{SCHEMA}.{name}"
