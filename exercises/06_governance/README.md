# Ch 6 — Governance and Security (Exam Section 7, 15%)

**Objectives:** managed vs external tables (create, drop, recover, convert); `GRANT` and `REVOKE`,
privilege traversal, and inheritance — and why Unity Catalog has no `DENY`; column masks, row
filters, ABAC policies, and dynamic views.

**Environment:** [FE] — both notebooks run end to end on Free Edition. One step is expected to
fail there: ABAC policies match only **governed** tags, which are defined at the account level,
so the policy step reports `Unknown tag policy key`.

## Exercises

- **`ex01_tables_and_privileges.py`** — drop and recover a managed table, probe for external
  storage, read the default grants, then grant, inherit, and revoke on a schema, and try `DENY`.
- **`ex02_masks_filters_abac.py`** — attach a column mask and a row filter, flip their outcome by
  changing which group they trust, test a type mismatch, compare a dynamic view with direct
  table access, and attempt an ABAC policy.

Each notebook works in its own schema, `workspace.de_cert_gov`, and removes it at the end.
Every operation that may not be supported prints `OK` or `BLOCKED` with the error, so a
blocked step is a result to read rather than a failure.
