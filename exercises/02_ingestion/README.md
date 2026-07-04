# Ch 2 — Data Ingestion and Loading (Exam Section 2, 21%)

**Objectives:** batch/streaming/incremental patterns; COPY INTO; Auto Loader
(schema inference + evolution); Lakeflow Connect standard vs managed connectors;
semi-structured (JSON, nested) ingestion; choosing a method by volume/frequency/
type/governance.

**Environment:** most exercises [FE]; managed connectors are [Paid].

Included now:
- `ex01_autoloader.py` — [FE] worked exemplar: Volume file-drop → Auto Loader
  (`availableNow`) → bronze streaming table, then incremental re-run.

Planned (added with the book chapter):
- COPY INTO idempotent batch load; CTAS one-shot.
- Schema evolution + `_rescued_data`.
- Nested JSON explode/flatten from `order_events`.
- [Paid] a managed connector (SQL Server CDC / SaaS) with gateway + staging.
