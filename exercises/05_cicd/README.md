# Ch 5 — Implementing CI/CD (Exam Section 5, 10%)

**Objectives:** Git folders (clone, commit, push, branch, and the pull-request handoff);
Declarative Automation Bundles (`databricks.yml`, targets, variables and overrides);
promoting a job and a pipeline across dev, test, and prod with the Databricks CLI.

**Environment:** [FE] — everything runs on Free Edition.

## What's here

- **The bundle** lives at the repository root: `databricks.yml` plus the resource files in
  `resources/`. It deploys the Chapter 4 jobs exercise and a pipeline as code.
- **`ex01_region_summary.sql`** (this folder) is the pipeline's source — a materialized view
  that each target writes into its own schema.

## Running the bundle

Free Edition has one workspace, so the three environments are separated by schema. Create them
first, outside the bundle, so tearing a deployment down never removes a schema's data:

```bash
databricks schemas create de_cert_dev  workspace
databricks schemas create de_cert_test workspace
databricks schemas create de_cert_prod workspace
```

Then, from the repository root:

```bash
databricks bundle validate -t dev
databricks bundle deploy   -t dev
databricks bundle run      -t dev companion_jobs_demo
databricks bundle destroy  -t dev
```

Repeat with `-t test` and `-t prod`. Things worth noticing:

- `deploy` creates the job and pipeline but **runs nothing**; `run` is separate.
- The three targets behave differently: `dev` uses development mode, `test` sets no mode and
  relies on presets, and `prod` uses production mode with an explicit root path.
- Every target deploys the job's schedule **paused**.
- `destroy` deletes the pipeline **and the tables it manages**, unless the pipeline sets
  `cascade_on_destroy: false`.

## Git folders

In your workspace: **Create → Git folder**, and point it at this repository's HTTPS URL. Commit,
push, branch, and pull from the folder's Git dialog. Pull requests are opened at the Git
provider — Databricks has no pull-request feature.
