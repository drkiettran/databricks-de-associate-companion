# Databricks Data Engineer Associate — Practitioner's Companion (Exercises)

Hands-on exercises and a self-contained synthetic **data generator** for the
Databricks Certified Data Engineer Associate certification (exam version dated
May 4, 2026). This repository is the code companion to the *Practitioner's
Companion* study book: clone it, generate the data, and work the exercises in
your own Databricks workspace.

Every exercise runs on **Databricks Free Edition** (serverless) *or* a **paid
workspace** (serverless + classic compute). Exercises are tagged accordingly:

* **[FE]** — runs on Free Edition
* **[Paid]** — needs a paid workspace (managed connectors, classic compute, the
  Spark UI); Free Edition can't provide these

## Why a data generator instead of a downloaded dataset?

So the repo is **independent and controlled**. The generator uses only the
Python standard library, needs no network access, and is seeded for
reproducibility — the same `--seed` always produces the same data. That means
it runs cleanly inside restricted workspaces with limited outbound access, and
there is no third-party dataset license to vet. You control schema, volume, and
data quality entirely through flags.

## Quickstart

### 1. Get the code into your workspace

Nothing needs to be emailed anywhere. Either:

* **Databricks Git Folders** (recommended): in the workspace, *Create → Git
  folder* and point it at this public repo's HTTPS URL. Pull updates any time.
* **Local clone:** `git clone <repo-url>` on any machine with git.

### 2. Generate the data

Locally:

```bash
python data/generate_data.py --outdir ./data/output
```

Or from a Databricks notebook, writing straight into a Unity Catalog Volume so
the files are ready for ingestion exercises:

```python
# In a Databricks notebook cell
%sh python /Workspace/<path-to-repo>/data/generate_data.py \
    --outdir /Volumes/<catalog>/<schema>/<volume>/raw --days 14 --dirty
```

See `common/setup.py` for a one-call helper that creates the catalog, schema,
and volume for you.

> **Free Edition note:** you may not have privilege to `CREATE CATALOG`. If so,
> set `COMPANION_CATALOG` to your workspace's existing default catalog; the
> helper still creates the schema and volume inside it.

### 3. Work the exercises

Open `exercises/` and go in order. Each folder has a `README.md` with the
objective, the `[FE]`/`[Paid]` tag, and starter code.

## Repository structure

```
.
├── data/
│   ├── generate_data.py     # self-contained synthetic data generator
│   └── README.md            # data dictionary + what each dataset is for
├── common/
│   ├── config.py            # default catalog/schema/volume names + paths
│   └── setup.py             # create catalog/schema/volume; run the generator
├── exercises/
│   ├── 01_platform/         # Ch 1 — platform, Delta, Unity Catalog, compute
│   ├── 02_ingestion/        # Ch 2 — COPY INTO, Auto Loader, Lakeflow Connect
│   ├── 03_transformation/   # Ch 3 — cleaning, joins, medallion, quality
│   ├── 04_jobs/             # Ch 4 — Lakeflow Jobs orchestration
│   ├── 05_cicd/             # Ch 5 — Git Folders, Automation Bundles, CLI
│   ├── 06_governance/       # Ch 6 — Unity Catalog governance & security
│   └── 07_troubleshooting/  # Ch 7 — query profile, Spark UI, optimization
├── docs/
│   └── environment.md       # Free Edition vs paid; Git Folders import; volumes
├── requirements.txt
├── LICENSE                  # MIT
└── README.md
```

## Exercise ↔ book-chapter ↔ exam-section map

| Folder | Book chapter | Exam section | Weight |
|---|---|---|---|
| 01_platform | Ch 1 | Databricks Intelligence Platform | 6% |
| 02_ingestion | Ch 2 | Data Ingestion and Loading | 21% |
| 03_transformation | Ch 3 | Data Transformation and Modeling | 22% |
| 04_jobs | Ch 4 | Working with Lakeflow Jobs | 16% |
| 05_cicd | Ch 5 | Implementing CI/CD | 10% |
| 06_governance | Ch 6 | Governance and Security | 15% |
| 07_troubleshooting | Ch 7 | Troubleshooting, Monitoring, Optimization | 10% |

Exercises are added chapter by chapter as the companion book is written. Folders
without full exercises yet contain a `README.md` describing what will land there.

## Contributing / releasing

The generator produces only synthetic data, so anything you commit here is safe
to make public. Never commit generated output (it's gitignored) or any
real/organizational data — regenerate locally instead.

## License

MIT — see `LICENSE`.
