# Environments: Free Edition vs paid workspace

These exercises are designed to run in **both** environments. Do each once on
Free Edition; repeat the `[Paid]`-only items on a paid workspace.

## Getting the repo into a workspace (no email needed)

1. In the Databricks workspace sidebar, choose **Create → Git folder**.
2. Paste this repository's public HTTPS URL and clone.
3. Pull updates any time from the Git folder UI.

Because the repo is public and contains only synthetic-data code, nothing
sensitive ever moves between machines — the workspace pulls the code directly
from the git host.

## Creating the data landing zone

Free Edition has no DBFS, so use a **Unity Catalog Volume** as the file landing
zone. From a notebook:

```python
from common.setup import setup_environment
raw = setup_environment(spark)   # creates catalog/schema/volume, prints the path
```

Then generate data into that Volume (a `%sh` cell):

```bash
python data/generate_data.py --outdir /Volumes/main/companion/raw --days 14 --dirty
```

Simulate a single "file drop" for Auto Loader by writing one file at a time:

```python
dbutils.fs.put("/Volumes/main/companion/raw/incoming/sales_day1.ndjson", payload, True)
```

## What runs where

| Capability | Free Edition | Paid workspace |
|---|---|---|
| COPY INTO, Auto Loader (`availableNow`), streaming tables, SDP | yes | yes |
| Bronze→silver→gold, joins, MERGE INTO, tuning | yes | yes |
| Lakeflow Jobs (schedule, file-arrival, table-update triggers) | basic | full |
| Git Folders, Automation Bundles, Databricks CLI | yes | yes |
| Unity Catalog grants, managed vs external, row/column controls | yes | yes |
| Managed connectors (CDC gateway, SaaS) | no | yes |
| Classic compute + real Spark UI | no | yes |
| OOM / cluster-startup diagnostics | no | yes |

## Serverless streaming caveat (applies to Free Edition and serverless jobs)

On serverless compute you **must** set a supported Structured Streaming
trigger. Use `Trigger.AvailableNow()` for incremental/triggered runs:

```python
(spark.readStream.format("cloudFiles")
   .option("cloudFiles.format", "json")
   .option("cloudFiles.schemaLocation", schema_path)
   .load(source_path)
 .writeStream
   .option("checkpointLocation", checkpoint_path)
   .trigger(availableNow=True)
   .toTable("main.companion.sales_bronze"))
```

Time-based (`Trigger.ProcessingTime`) and continuous triggers are **not
supported** on serverless and raise `INFINITE_STREAMING_TRIGGER_NOT_SUPPORTED`.
For always-on streams, use a Lakeflow Spark Declarative Pipeline in continuous
mode (paid workspace).

## Using the paid workspace responsibly

* Use only the **synthetic** data from this repo — never real or controlled data.
* Paid compute bills real DBUs: pick the smallest compute that works, set
  auto-termination, and stop clusters when done.
* Confirm cert study on a corporate workspace is permitted under your
  organization's acceptable-use policy before running up compute.
