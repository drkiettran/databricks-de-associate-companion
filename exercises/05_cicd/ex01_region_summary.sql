-- Ch 5 · Exercise 01 — a pipeline promoted across environments.
--
-- Deployed by the bundle once per target. The source is shared; the output lands
-- in each target's own schema (de_cert_dev / de_cert_test / de_cert_prod), which
-- is what isolates one environment's results from another's.
--
-- Reads the table written by exercises/04_jobs/ex01_extract.py.

CREATE OR REFRESH MATERIALIZED VIEW region_summary
COMMENT "Per-region rollup of day1_job_input, deployed per environment by the bundle"
AS
SELECT
  region,
  COUNT(*)              AS n,
  ROUND(AVG(value), 2)  AS avg_value
FROM workspace.de_cert.day1_job_input
GROUP BY region;
